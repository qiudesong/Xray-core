package metrics

import (
	"context"
	"encoding/json"
	stdnet "net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/app/proxyman"
	_ "github.com/xtls/xray-core/app/proxyman/inbound"
	_ "github.com/xtls/xray-core/app/proxyman/outbound"
	appstats "github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features"
	"github.com/xtls/xray-core/features/extension"
	feature_outbound "github.com/xtls/xray-core/features/outbound"
	feature_stats "github.com/xtls/xray-core/features/stats"
	"google.golang.org/protobuf/proto"
)

func TestMetricsCanRestartInSameProcess(t *testing.T) {
	for i := 0; i < 2; i++ {
		server := startMetricsTestServer(t)
		readMetricsVars(t, server)
		readMetricsPprof(t, server)
		readPrometheusMetrics(t, server)
		if err := server.Close(); err != nil {
			t.Fatalf("failed to close metrics server: %v", err)
		}
	}
}

func TestMetricsCanRunMultipleInstancesInSameProcess(t *testing.T) {
	server1 := startMetricsTestServer(t)
	t.Cleanup(func() {
		_ = server1.Close()
	})
	server2 := startMetricsTestServer(t)
	t.Cleanup(func() {
		_ = server2.Close()
	})

	readMetricsVars(t, server1)
	readMetricsVars(t, server2)
	readPrometheusMetrics(t, server1)
	readPrometheusMetrics(t, server2)
}

func TestPrometheusExportsTrafficAndStandardRuntimeMetrics(t *testing.T) {
	server := startMetricsTestServer(t)
	t.Cleanup(func() {
		_ = server.Close()
	})

	statsManager := server.GetFeature(feature_stats.ManagerType()).(feature_stats.Manager)
	setCounter(t, statsManager, "inbound>>>socks-in>>>traffic>>>uplink", 123)
	setCounter(t, statsManager, "inbound>>>socks-in>>>traffic>>>downlink", 234)
	setCounter(t, statsManager, "outbound>>>direct>>>traffic>>>uplink", 345)
	setCounter(t, statsManager, "outbound>>>direct>>>traffic>>>downlink", 456)
	setCounter(t, statsManager, "user>>>user@example.com>>>traffic>>>uplink", 789)
	setCounter(t, statsManager, "outbound>>>direct>>>connections>>>uplink", 10)
	setCounter(t, statsManager, "outbound>>>direct>>>traffic>>>unknown", 11)

	body := readPrometheusMetrics(t, server)
	for _, expected := range []string{
		"# TYPE xray_inbound_uplink_bytes_total counter",
		`xray_inbound_uplink_bytes_total{tag="socks-in"} 123`,
		"# TYPE xray_inbound_downlink_bytes_total counter",
		`xray_inbound_downlink_bytes_total{tag="socks-in"} 234`,
		"# TYPE xray_outbound_uplink_bytes_total counter",
		`xray_outbound_uplink_bytes_total{tag="direct"} 345`,
		"# TYPE xray_outbound_downlink_bytes_total counter",
		`xray_outbound_downlink_bytes_total{tag="direct"} 456`,
		"# TYPE xray_uptime_seconds gauge",
		"# TYPE go_goroutines gauge",
		"# TYPE go_memstats_alloc_bytes gauge",
		"# TYPE go_memstats_alloc_bytes_total counter",
		"# TYPE process_start_time_seconds gauge",
	} {
		if !strings.Contains(body, expected) {
			t.Errorf("Prometheus output missing %q", expected)
		}
	}
	for _, excluded := range []string{
		"user@example.com",
		"# HELP xray_up ",
		"# HELP xray_scrapes_total ",
		"# HELP xray_scrape_duration_seconds ",
		"# HELP xray_traffic_uplink_bytes_total ",
		"# HELP xray_traffic_downlink_bytes_total ",
		"# HELP xray_goroutines ",
		"# HELP xray_memstats_",
	} {
		if strings.Contains(body, excluded) {
			t.Errorf("Prometheus output unexpectedly contains %q", excluded)
		}
	}
}

func TestPrometheusExportsObservatoryMetrics(t *testing.T) {
	healthPing := &observatory.HealthPingMeasurementResult{
		All:       10,
		Fail:      2,
		Deviation: int64(5 * time.Millisecond),
		Average:   int64(42 * time.Millisecond),
		Max:       int64(55 * time.Millisecond),
		Min:       int64(30 * time.Millisecond),
	}
	server := startMetricsTestServerWithFeatures(t, &Config{Tag: "metrics_out"}, &staticObservatory{
		result: &observatory.ObservationResult{
			Status: []*observatory.OutboundStatus{{
				Alive:        true,
				Delay:        42,
				OutboundTag:  "proxy",
				LastSeenTime: 1700000000,
				LastTryTime:  1700000001,
				HealthPing:   healthPing,
			}},
		},
	})
	t.Cleanup(func() {
		_ = server.Close()
	})

	body := readPrometheusMetrics(t, server)
	for _, expected := range []string{
		`xray_observatory_healthy{outbound="proxy"} 1`,
		`xray_observatory_probe_duration_seconds{outbound="proxy"} 0.042`,
		"# TYPE xray_observatory_last_success_timestamp_seconds gauge",
		"# TYPE xray_observatory_last_probe_timestamp_seconds gauge",
		`xray_observatory_health_ping_window_samples{outbound="proxy"} 10`,
		`xray_observatory_health_ping_window_failed_samples{outbound="proxy"} 2`,
		`xray_observatory_health_ping_duration_standard_deviation_seconds{outbound="proxy"} 0.005`,
		`xray_observatory_health_ping_duration_average_seconds{outbound="proxy"} 0.042`,
		`xray_observatory_health_ping_duration_maximum_seconds{outbound="proxy"} 0.055`,
		`xray_observatory_health_ping_duration_minimum_seconds{outbound="proxy"} 0.03`,
	} {
		if !strings.Contains(body, expected) {
			t.Errorf("Prometheus output missing %q", expected)
		}
	}
}

func TestMetricsListenOnlyWithoutTagDoesNotRegisterOutbound(t *testing.T) {
	listen := pickMetricsListenAddress(t)
	server := startMetricsTestServerWithMetricsConfig(t, &Config{
		Listen: listen,
	})
	t.Cleanup(func() {
		_ = server.Close()
	})

	response, err := http.Get("http://" + listen + "/metrics")
	if err != nil {
		t.Fatalf("failed to read listen-only metrics: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected listen-only metrics status: %d", response.StatusCode)
	}

	outboundManager := server.GetFeature(feature_outbound.ManagerType()).(feature_outbound.Manager)
	if handlers := outboundManager.ListHandlers(context.Background()); len(handlers) != 0 {
		t.Fatalf("listen-only metrics registered outbound handlers: got %d, want 0", len(handlers))
	}
}

func startMetricsTestServer(t *testing.T) *core.Instance {
	return startMetricsTestServerWithMetricsConfig(t, &Config{
		Tag: "metrics_out",
	})
}

func startMetricsTestServerWithMetricsConfig(t *testing.T, metricsConfig *Config) *core.Instance {
	return startMetricsTestServerWithFeatures(t, metricsConfig)
}

func startMetricsTestServerWithFeatures(t *testing.T, metricsConfig *Config, additionalFeatures ...features.Feature) *core.Instance {
	t.Helper()

	server, err := core.New(metricsTestConfig(metricsConfig))
	if err != nil {
		t.Fatalf("failed to create metrics server: %v", err)
	}
	for _, feature := range additionalFeatures {
		if err := server.AddFeature(feature); err != nil {
			_ = server.Close()
			t.Fatalf("failed to add test feature: %v", err)
		}
	}
	if err := server.Start(); err != nil {
		_ = server.Close()
		t.Fatalf("failed to start metrics server: %v", err)
	}
	return server
}

func setCounter(t *testing.T, manager feature_stats.Manager, name string, value int64) {
	t.Helper()

	counter, err := manager.RegisterCounter(name)
	if err != nil {
		t.Fatalf("failed to register counter %q: %v", name, err)
	}
	counter.Set(value)
}

func metricsTestConfig(metricsConfig *Config) *core.Config {
	return &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&dispatcher.Config{}),
			serial.ToTypedMessage(&proxyman.InboundConfig{}),
			serial.ToTypedMessage(&proxyman.OutboundConfig{}),
			serial.ToTypedMessage(&appstats.Config{}),
			serial.ToTypedMessage(metricsConfig),
		},
	}
}

func pickMetricsListenAddress(t *testing.T) string {
	t.Helper()

	listener, err := stdnet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to pick metrics listen address: %v", err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

func readMetricsVars(t *testing.T, server *core.Instance) {
	t.Helper()

	recorder := httptest.NewRecorder()
	metricsHandler(t, server).httpHandler().ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/debug/vars", nil),
	)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected metrics vars status: %d", recorder.Code)
	}

	var payload map[string]interface{}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("failed to decode metrics vars: %v", err)
	}
	if _, found := payload["stats"]; !found {
		t.Fatal("metrics vars missing stats")
	}
	if _, found := payload["observatory"]; !found {
		t.Fatal("metrics vars missing observatory")
	}
}

func readMetricsPprof(t *testing.T, server *core.Instance) {
	t.Helper()

	recorder := httptest.NewRecorder()
	metricsHandler(t, server).httpHandler().ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/debug/pprof/goroutine?debug=1", nil),
	)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected metrics pprof status: %d", recorder.Code)
	}
}

func readPrometheusMetrics(t *testing.T, server *core.Instance) string {
	t.Helper()

	recorder := httptest.NewRecorder()
	metricsHandler(t, server).httpHandler().ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/metrics", nil),
	)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected Prometheus metrics status: %d: %s", recorder.Code, recorder.Body.String())
	}
	return recorder.Body.String()
}

func metricsHandler(t *testing.T, server *core.Instance) *MetricsHandler {
	t.Helper()

	feature := server.GetFeature((*MetricsHandler)(nil))
	handler, ok := feature.(*MetricsHandler)
	if !ok || handler == nil {
		t.Fatal("metrics handler not registered")
	}
	return handler
}

type staticObservatory struct {
	result *observatory.ObservationResult
}

func (o *staticObservatory) Type() interface{} {
	return extension.ObservatoryType()
}

func (o *staticObservatory) Start() error {
	return nil
}

func (o *staticObservatory) Close() error {
	return nil
}

func (o *staticObservatory) GetObservation(context.Context) (proto.Message, error) {
	return o.result, nil
}
