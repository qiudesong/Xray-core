package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	stdnet "net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/oschwald/geoip2-golang"
	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/app/proxyman"
	_ "github.com/xtls/xray-core/app/proxyman/inbound"
	_ "github.com/xtls/xray-core/app/proxyman/outbound"
	appstats "github.com/xtls/xray-core/app/stats"
	commongeodata "github.com/xtls/xray-core/common/geodata"
	"github.com/xtls/xray-core/common/log"
	xnet "github.com/xtls/xray-core/common/net"
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

func TestPrometheusAccessMetricsAreDisabledByDefault(t *testing.T) {
	server := startMetricsTestServer(t)
	t.Cleanup(func() {
		_ = server.Close()
	})

	log.Record(&log.AccessMessage{
		Status:      log.AccessAccepted,
		Email:       "user@example.com",
		InboundTag:  "socks-in",
		OutboundTag: "direct",
		Network:     "tcp",
	})
	log.PublishPeerEvent(log.PeerEvent{
		Side:    log.PeerSideInbound,
		Tag:     "socks-in",
		Network: "tcp",
		Address: log.AccessAddress{Value: "203.0.113.10", Type: log.AccessAddressTypeIP},
	})
	if body := readPrometheusMetrics(t, server); strings.Contains(body, "xray_access_") {
		t.Fatal("access metrics were exported while disabled")
	}
}

func TestPrometheusExportsStructuredAccessMetrics(t *testing.T) {
	server := startMetricsTestServerWithMetricsConfig(t, &Config{
		Tag: "metrics_out",
		Access: &AccessMetricsConfig{
			Enabled: true,
			Window:  int64(time.Minute),
			Role:    "server",
		},
	})
	t.Cleanup(func() {
		_ = server.Close()
	})

	handler := metricsHandler(t, server)
	setAccessGeoDatabases(handler.access,
		&staticCountryLookup{country: "CN"},
		&staticASNLookup{asn: 13335, organization: "Cloudflare, Inc."},
		&staticCityLookup{country: "CN", city: "Shanghai"},
	)
	log.Record(&log.AccessMessage{
		From:        xnet.TCPDestination(xnet.ParseAddress("203.0.113.10"), 12345),
		To:          xnet.TCPDestination(xnet.ParseAddress("198.51.100.20"), 443),
		Status:      log.AccessAccepted,
		Email:       "user@example.com",
		InboundTag:  "socks-in",
		OutboundTag: "direct",
		Network:     "tcp",
	})
	log.PublishPeerEvent(log.PeerEvent{
		Side:    log.PeerSideInbound,
		Tag:     "socks-in",
		Network: "tcp",
		Address: log.AccessAddress{Value: "203.0.113.10", Type: log.AccessAddressTypeIP},
	})
	log.Record(&log.AccessMessage{
		From:        &stdnet.TCPAddr{IP: stdnet.ParseIP("203.0.113.11"), Port: 12346},
		To:          xnet.TCPDestination(xnet.ParseAddress("198.51.100.21"), 443),
		Status:      log.AccessRejected,
		InboundTag:  "socks-in",
		OutboundTag: "direct",
		Network:     "tcp",
	})

	expected := []string{
		`xray_access_requests_total{inbound="socks-in",network="tcp",outbound="direct",role="server",status="accepted"} 1`,
		`xray_access_requests_total{inbound="socks-in",network="tcp",outbound="direct",role="server",status="rejected"} 1`,
		`xray_access_requests_window{role="server",status="accepted"} 1`,
		`xray_access_requests_window{role="server",status="rejected"} 1`,
		`xray_access_unique_authenticated_users_window{role="server"} 1`,
		`xray_access_peer_country_connections_total{country="CN",network="tcp",role="server",side="inbound",tag="socks-in"} 1`,
		`xray_access_peer_asn_connections_total{asn="13335",network="tcp",org="Cloudflare, Inc.",role="server",side="inbound",tag="socks-in"} 1`,
		`xray_access_peer_city_connections_total{city="Shanghai",country="CN",network="tcp",role="server",side="inbound",tag="socks-in"} 1`,
		`xray_access_address_requests_total{address="198.51.100.20",address_type="ip",role="server",side="to",status="accepted"} 1`,
		`xray_access_address_requests_total{address="198.51.100.21",address_type="ip",role="server",side="to",status="rejected"} 1`,
		`xray_access_events_dropped_total{role="server"} 0`,
		`xray_access_peer_events_dropped_total{role="server",side="inbound"} 0`,
		`xray_access_request_series_dropped_total{role="server"} 0`,
		`xray_access_peer_asn_series_dropped_total{role="server",side="inbound"} 0`,
		`xray_access_peer_city_series_dropped_total{role="server",side="inbound"} 0`,
		`xray_access_address_series_dropped_total{role="server"} 0`,
		`xray_access_user_tracking_dropped_total{role="server"} 0`,
	}
	body := waitForPrometheusMetrics(t, server, expected)
	for _, removed := range []string{
		"xray_access_country_requests_total",
		"xray_access_asn_requests_total",
		"xray_access_city_requests_total",
		"xray_access_asn_series_dropped_total",
		"xray_access_city_series_dropped_total",
	} {
		if strings.Contains(body, removed) {
			t.Errorf("Prometheus output still contains removed metric %q", removed)
		}
	}
	for _, ip := range []string{"203.0.113.10", "203.0.113.11"} {
		if strings.Contains(body, ip) {
			t.Fatalf("Prometheus output unexpectedly contains source IP %q", ip)
		}
	}
}

func TestPrometheusDoesNotExportUnconfiguredGeoMetrics(t *testing.T) {
	server := startMetricsTestServerWithMetricsConfig(t, &Config{
		Tag: "metrics_out",
		Access: &AccessMetricsConfig{
			Enabled: true,
			Window:  int64(time.Minute),
			Role:    "server",
		},
	})
	t.Cleanup(func() {
		_ = server.Close()
	})

	log.Record(&log.AccessMessage{Status: log.AccessAccepted})
	body := waitForPrometheusMetrics(t, server, []string{`xray_access_requests_total`})
	for _, excluded := range []string{
		"xray_access_peer_country_connections_total",
		"xray_access_peer_asn_connections_total",
		"xray_access_peer_city_connections_total",
	} {
		if strings.Contains(body, excluded) {
			t.Errorf("Prometheus output unexpectedly contains unconfigured geo metric %q", excluded)
		}
	}
}

func TestPrometheusClientRoleUsesOutboundPeerForGeoAndFromForAddress(t *testing.T) {
	server := startMetricsTestServerWithMetricsConfig(t, &Config{
		Tag: "metrics_out",
		Access: &AccessMetricsConfig{
			Enabled: true,
			Window:  int64(time.Minute),
			Role:    "client",
		},
	})
	t.Cleanup(func() {
		_ = server.Close()
	})

	geoIP := stdnet.ParseIP("203.0.113.50")
	handler := metricsHandler(t, server)
	setAccessGeoDatabases(handler.access,
		&staticCountryLookup{country: "US", expectedIP: geoIP},
		&staticASNLookup{asn: 64500, organization: "Example Network", expectedIP: geoIP},
		&staticCityLookup{country: "US", city: "New York", expectedIP: geoIP},
	)
	log.Record(&log.AccessMessage{
		From:        xnet.TCPDestination(xnet.ParseAddress("192.168.1.10"), 12345),
		To:          xnet.TCPDestination(xnet.ParseAddress("203.0.113.50"), 443),
		Status:      log.AccessAccepted,
		InboundTag:  "socks-in",
		OutboundTag: "direct",
		Network:     "tcp",
	})
	log.PublishPeerEvent(log.PeerEvent{
		Side:    log.PeerSideInbound,
		Tag:     "ignored-inbound",
		Network: "tcp",
		Address: log.AccessAddress{Value: geoIP.String(), Type: log.AccessAddressTypeIP},
	})
	log.PublishPeerEvent(log.PeerEvent{
		Side:    log.PeerSideOutbound,
		Tag:     "direct",
		Network: "tcp",
		Address: log.AccessAddress{Value: geoIP.String(), Type: log.AccessAddressTypeIP},
	})

	body := waitForPrometheusMetrics(t, server, []string{
		`xray_access_peer_country_connections_total{country="US",network="tcp",role="client",side="outbound",tag="direct"} 1`,
		`xray_access_peer_asn_connections_total{asn="64500",network="tcp",org="Example Network",role="client",side="outbound",tag="direct"} 1`,
		`xray_access_peer_city_connections_total{city="New York",country="US",network="tcp",role="client",side="outbound",tag="direct"} 1`,
		`xray_access_address_requests_total{address="192.168.1.10",address_type="ip",role="client",side="from",status="accepted"} 1`,
	})
	if strings.Contains(body, `address="203.0.113.50"`) {
		t.Fatal("client role unexpectedly exported AccessMessage.To in the address metric")
	}
}

func TestAccessMetricsAllowsMissingMMDBAssets(t *testing.T) {
	t.Setenv("xray.location.asset", t.TempDir())
	metrics, err := newAccessMetrics(&AccessMetricsConfig{Enabled: true, Role: "server"})
	if err != nil {
		t.Fatal(err)
	}
	status := metrics.geo.Status()
	if status.Country || status.ASN || status.City {
		t.Fatal("unexpected GeoIP database loaded")
	}
}

func TestAccessMetricsRejectsInvalidMMDBAsset(t *testing.T) {
	assetDirectory := t.TempDir()
	t.Setenv("xray.location.asset", assetDirectory)
	if err := os.WriteFile(filepath.Join(assetDirectory, commongeodata.CountryMMDB), []byte("not an MMDB"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := newAccessMetrics(&AccessMetricsConfig{Enabled: true, Role: "server"})
	if err == nil || !strings.Contains(err.Error(), "failed to open "+commongeodata.CountryMMDB) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAccessMetricsCloseDoesNotDrainBufferedEvents(t *testing.T) {
	metrics, err := newAccessMetrics(&AccessMetricsConfig{
		Enabled:   true,
		Role:      "server",
		QueueSize: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	lookup := &blockingCountryLookup{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	setAccessGeoDatabases(metrics, lookup, nil, nil)
	metrics.Start()

	released := false
	t.Cleanup(func() {
		if !released {
			close(lookup.release)
		}
		_ = metrics.Close()
	})

	event := log.PeerEvent{
		Side:    log.PeerSideInbound,
		Tag:     "inbound",
		Network: "tcp",
		Address: log.AccessAddress{Value: "203.0.113.10", Type: log.AccessAddressTypeIP},
	}
	log.PublishPeerEvent(event)
	select {
	case <-lookup.entered:
	case <-time.After(time.Second):
		t.Fatal("access metrics worker did not start processing the first event")
	}

	for range 8 {
		log.PublishPeerEvent(event)
	}

	closeResult := make(chan error, 1)
	go func() {
		closeResult <- metrics.Close()
	}()
	select {
	case <-metrics.stop:
	case <-time.After(time.Second):
		close(lookup.release)
		released = true
		t.Fatal("access metrics close did not signal the worker to stop")
	}
	close(lookup.release)
	released = true

	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("access metrics close waited for buffered events")
	}

	if lookup.calls != 1 {
		t.Fatalf("processed %d events during close, want 1", lookup.calls)
	}
}

func TestAccessMetricsCountsDroppedPeerEvents(t *testing.T) {
	metrics, err := newAccessMetrics(&AccessMetricsConfig{Enabled: true, Role: "server", QueueSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	lookup := &blockingCountryLookup{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	setAccessGeoDatabases(metrics, lookup, nil, nil)
	metrics.Start()
	released := false
	t.Cleanup(func() {
		if !released {
			close(lookup.release)
		}
		_ = metrics.Close()
	})

	event := log.PeerEvent{
		Side:    log.PeerSideInbound,
		Network: "tcp",
		Address: log.AccessAddress{Value: "203.0.113.11", Type: log.AccessAddressTypeIP},
	}
	log.PublishPeerEvent(event)
	select {
	case <-lookup.entered:
	case <-time.After(time.Second):
		t.Fatal("access metrics worker did not start processing the peer event")
	}
	log.PublishPeerEvent(event)
	log.PublishPeerEvent(event)
	if dropped := metrics.snapshot(time.Now()).peerEventsDropped; dropped != 1 {
		t.Fatalf("dropped peer events = %d, want 1", dropped)
	}

	close(lookup.release)
	released = true
}

func TestAccessMetricsRejectInvalidRole(t *testing.T) {
	_, err := newAccessMetrics(&AccessMetricsConfig{Enabled: true, Role: "invalid"})
	if err == nil {
		t.Fatal("expected invalid access role to be rejected")
	}
}

func TestAccessMetricsRequiresRoleWhenEnabled(t *testing.T) {
	_, err := newAccessMetrics(&AccessMetricsConfig{Enabled: true})
	if err == nil {
		t.Fatal("expected missing access role to be rejected when enabled")
	}
	metrics, err := newAccessMetrics(&AccessMetricsConfig{Enabled: false})
	if err != nil || metrics != nil {
		t.Fatalf("disabled access metrics should not require a role: metrics=%v, err=%v", metrics, err)
	}
}

func TestAccessMetricsPreservesNetworkLabel(t *testing.T) {
	metrics, err := newAccessMetrics(&AccessMetricsConfig{Enabled: true, Role: "server"})
	if err != nil {
		t.Fatal(err)
	}
	metrics.record(log.AccessEvent{
		Time:    time.Now(),
		Message: log.AccessMessage{Network: "TCP-custom", Status: log.AccessAccepted},
	})

	snapshot := metrics.snapshot(time.Now())
	if len(snapshot.requests) != 1 || snapshot.requests[0].labels.network != "TCP-custom" {
		t.Fatalf("network label was modified: %+v", snapshot.requests)
	}
}

func TestAccessMetricsUsesUnknownForMissingNetworkLabel(t *testing.T) {
	metrics, err := newAccessMetrics(&AccessMetricsConfig{Enabled: true, Role: "server"})
	if err != nil {
		t.Fatal(err)
	}
	metrics.record(log.AccessEvent{
		Time:    time.Now(),
		Message: log.AccessMessage{Status: log.AccessRejected},
	})

	snapshot := metrics.snapshot(time.Now())
	if len(snapshot.requests) != 1 || snapshot.requests[0].labels.network != accessUnknown {
		t.Fatalf("missing network label was not normalized: %+v", snapshot.requests)
	}
}

func TestAccessAddressSupportsStructuredAddressTypes(t *testing.T) {
	metrics := new(accessMetrics)
	tests := []struct {
		name     string
		value    interface{}
		want     string
		wantType log.AccessAddressType
	}{
		{name: "destination", value: xnet.TCPDestination(xnet.ParseAddress("203.0.113.60"), 443), want: "203.0.113.60", wantType: log.AccessAddressTypeIP},
		{name: "network address", value: &stdnet.TCPAddr{IP: stdnet.ParseIP("203.0.113.61"), Port: 443}, want: "203.0.113.61", wantType: log.AccessAddressTypeIP},
		{name: "URL", value: &url.URL{Scheme: "https", Host: "203.0.113.62:443"}, want: "203.0.113.62", wantType: log.AccessAddressTypeIP},
		{name: "IP string", value: "203.0.113.63", want: "203.0.113.63", wantType: log.AccessAddressTypeIP},
		{name: "URL string", value: "https://203.0.113.64:443/path", want: "203.0.113.64", wantType: log.AccessAddressTypeIP},
		{name: "domain destination", value: xnet.TCPDestination(xnet.DomainAddress("Example.COM."), 443), want: "Example.COM.", wantType: log.AccessAddressTypeDomain},
		{name: "domain URL", value: &url.URL{Scheme: "https", Host: "Example.COM.:443"}, want: "Example.COM.", wantType: log.AccessAddressTypeDomain},
		{name: "origin-form URL", value: &url.URL{Path: "/path"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			address := metrics.accessAddress(test.value)
			if address.Value != test.want || address.Type != test.wantType {
				t.Fatalf("unexpected address: got %+v, want value %q type %q", address, test.want, test.wantType)
			}
		})
	}
}

func TestAccessMetricsPreferStructuredDestination(t *testing.T) {
	metrics, err := newAccessMetrics(&AccessMetricsConfig{Enabled: true, Role: "server"})
	if err != nil {
		t.Fatal(err)
	}
	metrics.record(log.AccessEvent{
		Time: time.Now(),
		Message: log.AccessMessage{
			To:          &url.URL{Path: "/path"},
			Destination: log.AccessAddress{Value: "Example.COM.", Type: log.AccessAddressTypeDomain},
			Status:      log.AccessAccepted,
		},
	})

	snapshot := metrics.snapshot(time.Now())
	if len(snapshot.addressRequests) != 1 {
		t.Fatalf("unexpected address samples: %+v", snapshot.addressRequests)
	}
	labels := snapshot.addressRequests[0].labels
	if labels.address != "Example.COM." || labels.addressType != "domain" || labels.side != accessSideTo {
		t.Fatalf("unexpected structured destination labels: %+v", labels)
	}
}

func TestAccessMetricsClientUsesPeerEventForGeo(t *testing.T) {
	metrics, err := newAccessMetrics(&AccessMetricsConfig{Enabled: true, Role: "client"})
	if err != nil {
		t.Fatal(err)
	}
	geoIP := stdnet.ParseIP("203.0.113.70")
	setAccessGeoDatabases(metrics, &staticCountryLookup{country: "US", expectedIP: geoIP}, nil, nil)
	metrics.recordPeer(log.PeerEvent{
		Side:    log.PeerSideOutbound,
		Tag:     "direct",
		Network: "tcp",
		Address: log.AccessAddress{Value: geoIP.String(), Type: log.AccessAddressTypeIP},
	})

	snapshot := metrics.snapshot(time.Now())
	if len(snapshot.peerCountryConnections) != 1 || snapshot.peerCountryConnections[0].labels.country != "US" {
		t.Fatalf("peer event did not produce GeoIP metrics: %+v", snapshot.peerCountryConnections)
	}
}

func TestAccessMetricsPreservesGeoLabels(t *testing.T) {
	metrics, err := newAccessMetrics(&AccessMetricsConfig{Enabled: true, Role: "server"})
	if err != nil {
		t.Fatal(err)
	}
	setAccessGeoDatabases(metrics,
		&staticCountryLookup{country: "cn"},
		&staticASNLookup{asn: 64500, organization: " Example Network "},
		&staticCityLookup{country: "cn", city: " Shanghai "},
	)
	metrics.recordPeer(log.PeerEvent{
		Side:    log.PeerSideInbound,
		Tag:     "socks-in",
		Network: "udp",
		Address: log.AccessAddress{Value: "203.0.113.80", Type: log.AccessAddressTypeIP},
	})

	snapshot := metrics.snapshot(time.Now())
	if len(snapshot.peerCountryConnections) != 1 || len(snapshot.peerASNConnections) != 1 || len(snapshot.peerCityConnections) != 1 {
		t.Fatalf("unexpected GeoIP samples: country=%+v ASN=%+v city=%+v", snapshot.peerCountryConnections, snapshot.peerASNConnections, snapshot.peerCityConnections)
	}
	if got := snapshot.peerCountryConnections[0].labels.country; got != "cn" {
		t.Fatalf("country label was normalized: got %q", got)
	}
	if got := snapshot.peerASNConnections[0].labels.org; got != " Example Network " {
		t.Fatalf("organization label was normalized: got %q", got)
	}
	if got := snapshot.peerCityConnections[0].labels.city; got != " Shanghai " {
		t.Fatalf("city label was normalized: got %q", got)
	}
	labels := snapshot.peerCountryConnections[0].labels
	if labels.side != "inbound" || labels.tag != "socks-in" || labels.network != "udp" {
		t.Fatalf("unexpected peer labels: %+v", labels)
	}
}

func TestAccessMetricsPeerUsesUnknownForMissingLabelsAndLookupErrors(t *testing.T) {
	metrics, err := newAccessMetrics(&AccessMetricsConfig{Enabled: true, Role: "server"})
	if err != nil {
		t.Fatal(err)
	}
	setAccessGeoDatabases(metrics, new(staticCountryLookup), new(staticASNLookup), new(staticCityLookup))
	metrics.recordPeer(log.PeerEvent{
		Side:    log.PeerSideInbound,
		Address: log.AccessAddress{Value: "203.0.113.81", Type: log.AccessAddressTypeIP},
	})

	snapshot := metrics.snapshot(time.Now())
	if len(snapshot.peerCountryConnections) != 1 || len(snapshot.peerASNConnections) != 1 || len(snapshot.peerCityConnections) != 1 {
		t.Fatalf("lookup errors did not produce stable unknown samples: country=%+v ASN=%+v city=%+v", snapshot.peerCountryConnections, snapshot.peerASNConnections, snapshot.peerCityConnections)
	}
	country := snapshot.peerCountryConnections[0].labels
	if country.country != accessUnknown || country.tag != accessUnknown || country.network != accessUnknown {
		t.Fatalf("unexpected normalized country labels: %+v", country)
	}
	if asn := snapshot.peerASNConnections[0].labels; asn.asn != accessUnknown || asn.org != accessUnknown {
		t.Fatalf("unexpected ASN fallback labels: %+v", asn)
	}
	if city := snapshot.peerCityConnections[0].labels; city.country != accessUnknown || city.city != accessUnknown {
		t.Fatalf("unexpected city fallback labels: %+v", city)
	}
}

func TestAccessMetricsIgnoresPeerFromOppositeSide(t *testing.T) {
	metrics, err := newAccessMetrics(&AccessMetricsConfig{Enabled: true, Role: "server"})
	if err != nil {
		t.Fatal(err)
	}
	setAccessGeoDatabases(metrics, &staticCountryLookup{country: "US"}, nil, nil)
	metrics.recordPeer(log.PeerEvent{
		Side:    log.PeerSideOutbound,
		Tag:     "direct",
		Network: "tcp",
		Address: log.AccessAddress{Value: "203.0.113.82", Type: log.AccessAddressTypeIP},
	})
	if snapshot := metrics.snapshot(time.Now()); len(snapshot.peerCountryConnections) != 0 {
		t.Fatalf("server metrics recorded outbound peer: %+v", snapshot.peerCountryConnections)
	}
}

func TestAccessMetricsSkipGeoForPrivatePeer(t *testing.T) {
	tests := []struct {
		name string
		role string
		side log.PeerSide
	}{
		{
			name: "server",
			role: "server",
			side: log.PeerSideInbound,
		},
		{
			name: "client",
			role: "client",
			side: log.PeerSideOutbound,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metrics, err := newAccessMetrics(&AccessMetricsConfig{Enabled: true, Role: test.role})
			if err != nil {
				t.Fatal(err)
			}
			setAccessGeoDatabases(metrics,
				&staticCountryLookup{country: "CN"},
				&staticASNLookup{asn: 64512, organization: "Private Network"},
				&staticCityLookup{country: "CN", city: "Private City"},
			)
			metrics.recordPeer(log.PeerEvent{
				Side:    test.side,
				Tag:     "peer",
				Network: "tcp",
				Address: log.AccessAddress{Value: "192.168.1.20", Type: log.AccessAddressTypeIP},
			})

			snapshot := metrics.snapshot(time.Now())
			if len(snapshot.peerCountryConnections) != 0 || len(snapshot.peerASNConnections) != 0 || len(snapshot.peerCityConnections) != 0 {
				t.Fatal("private peer unexpectedly produced GeoIP metrics")
			}
		})
	}
}

func TestAccessMetricsCapsGeoAndAddressSeries(t *testing.T) {
	metrics, err := newAccessMetrics(&AccessMetricsConfig{Enabled: true, Role: "server"})
	if err != nil {
		t.Fatal(err)
	}
	setAccessGeoDatabases(metrics, nil, &staticASNLookup{asn: 64512}, &staticCityLookup{country: "ZZ", city: "overflow"})
	for i := 0; i < maxTrackedAccessGeoSeries; i++ {
		metrics.peerASNConnections[accessPeerASNLabels{asn: fmt.Sprint(i), side: "inbound", tag: "peer", network: "tcp"}] = 1
		metrics.peerCityConnections[accessPeerCityLabels{country: "ZZ", city: fmt.Sprint(i), side: "inbound", tag: "peer", network: "tcp"}] = 1
	}
	for i := 0; i < maxTrackedAccessAddressSeries; i++ {
		metrics.addressRequests[accessAddressLabels{
			address:     fmt.Sprintf("host-%d.example.com", i),
			addressType: string(log.AccessAddressTypeDomain),
			side:        accessSideTo,
			status:      string(log.AccessAccepted),
		}] = 1
	}

	metrics.recordPeer(log.PeerEvent{
		Side:    log.PeerSideInbound,
		Tag:     "peer",
		Network: "tcp",
		Address: log.AccessAddress{Value: "203.0.113.12", Type: log.AccessAddressTypeIP},
	})
	metrics.record(log.AccessEvent{
		Time: time.Now(),
		Message: log.AccessMessage{
			From:   &stdnet.TCPAddr{IP: stdnet.ParseIP("203.0.113.12")},
			To:     &stdnet.TCPAddr{IP: stdnet.ParseIP("198.18.0.1")},
			Status: log.AccessAccepted,
		},
	})

	snapshot := metrics.snapshot(time.Now())
	if snapshot.peerASNSeriesDropped != 1 {
		t.Fatalf("unexpected ASN series drop count: got %d, want 1", snapshot.peerASNSeriesDropped)
	}
	if snapshot.peerCitySeriesDropped != 1 {
		t.Fatalf("unexpected city series drop count: got %d, want 1", snapshot.peerCitySeriesDropped)
	}
	if snapshot.addressSeriesDropped != 1 {
		t.Fatalf("unexpected address series drop count: got %d, want 1", snapshot.addressSeriesDropped)
	}
}

func TestAccessMetricsConfigUsesFieldNumber99(t *testing.T) {
	field := (&Config{}).ProtoReflect().Descriptor().Fields().ByName("access")
	if field == nil {
		t.Fatal("access field is missing")
	}
	if number := field.Number(); number != 99 {
		t.Fatalf("unexpected access field number: got %d, want 99", number)
	}
	roleField := (&AccessMetricsConfig{}).ProtoReflect().Descriptor().Fields().ByName("role")
	if roleField == nil {
		t.Fatal("access role field is missing")
	}
	if number := roleField.Number(); number != 7 {
		t.Fatalf("unexpected access role field number: got %d, want 7", number)
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

func waitForPrometheusMetrics(t *testing.T, server *core.Instance, expected []string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		body := readPrometheusMetrics(t, server)
		missing := ""
		for _, metric := range expected {
			if !strings.Contains(body, metric) {
				missing = metric
				break
			}
		}
		if missing == "" {
			return body
		}
		if time.Now().After(deadline) {
			t.Fatalf("Prometheus output missing %q:\n%s", missing, body)
		}
		time.Sleep(10 * time.Millisecond)
	}
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

func setAccessGeoDatabases(metrics *accessMetrics, countryDB countryLookup, asnDB asnLookup, cityDB cityLookup) {
	metrics.geo = &staticGeoProvider{country: countryDB, asn: asnDB, city: cityDB}
}

type staticGeoProvider struct {
	country countryLookup
	asn     asnLookup
	city    cityLookup
}

func (p *staticGeoProvider) Lookup(ip stdnet.IP) commongeodata.MMDBLookup {
	lookup := commongeodata.MMDBLookup{
		CountryEnabled: p.country != nil,
		ASNEnabled:     p.asn != nil,
		CityEnabled:    p.city != nil,
	}
	if p.country != nil {
		lookup.Country, lookup.CountryError = p.country.Country(ip)
	}
	if p.asn != nil {
		lookup.ASN, lookup.ASNError = p.asn.ASN(ip)
	}
	if p.city != nil {
		lookup.City, lookup.CityError = p.city.City(ip)
	}
	return lookup
}

func (p *staticGeoProvider) Status() commongeodata.MMDBStatus {
	return commongeodata.MMDBStatus{
		Country: p.country != nil,
		ASN:     p.asn != nil,
		City:    p.city != nil,
	}
}

type staticObservatory struct {
	result *observatory.ObservationResult
}

type staticCountryLookup struct {
	country    string
	expectedIP stdnet.IP
}

type blockingCountryLookup struct {
	entered chan struct{}
	release chan struct{}
	calls   int
}

func (l *blockingCountryLookup) Country(stdnet.IP) (*geoip2.Country, error) {
	l.calls++
	if l.calls == 1 {
		close(l.entered)
		<-l.release
	}
	record := new(geoip2.Country)
	record.Country.IsoCode = "US"
	return record, nil
}

func (*blockingCountryLookup) Close() error {
	return nil
}

func (l *staticCountryLookup) Country(ip stdnet.IP) (*geoip2.Country, error) {
	if l.expectedIP != nil && !ip.Equal(l.expectedIP) {
		return nil, fmt.Errorf("unexpected country lookup IP: %s", ip)
	}
	if l.country == "" {
		return nil, fmt.Errorf("country is empty")
	}
	record := new(geoip2.Country)
	record.Country.IsoCode = l.country
	return record, nil
}

func (*staticCountryLookup) Close() error {
	return nil
}

type staticASNLookup struct {
	asn          uint
	organization string
	expectedIP   stdnet.IP
}

func (l *staticASNLookup) ASN(ip stdnet.IP) (*geoip2.ASN, error) {
	if l.expectedIP != nil && !ip.Equal(l.expectedIP) {
		return nil, fmt.Errorf("unexpected ASN lookup IP: %s", ip)
	}
	if l.asn == 0 {
		return nil, fmt.Errorf("ASN is empty")
	}
	return &geoip2.ASN{
		AutonomousSystemNumber:       l.asn,
		AutonomousSystemOrganization: l.organization,
	}, nil
}

func (*staticASNLookup) Close() error {
	return nil
}

type staticCityLookup struct {
	country    string
	city       string
	expectedIP stdnet.IP
}

func (l *staticCityLookup) City(ip stdnet.IP) (*geoip2.City, error) {
	if l.expectedIP != nil && !ip.Equal(l.expectedIP) {
		return nil, fmt.Errorf("unexpected city lookup IP: %s", ip)
	}
	if l.city == "" {
		return nil, fmt.Errorf("city is empty")
	}
	record := new(geoip2.City)
	record.Country.IsoCode = l.country
	record.City.Names = map[string]string{"en": l.city}
	return record, nil
}

func (*staticCityLookup) Close() error {
	return nil
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
