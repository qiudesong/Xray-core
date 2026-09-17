package multiobservatory

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	v2net "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport/internet/tagged"
	"google.golang.org/protobuf/proto"
)

func TestCheckerHTTPAndDownloadValidation(t *testing.T) {
	server := newIPv4TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("1234"))
	}))
	defer server.Close()

	previousDialer := tagged.Dialer
	tagged.Dialer = func(ctx context.Context, _ routing.Dispatcher, _ v2net.Destination, _ string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "tcp", server.Listener.Addr().String())
	}
	defer func() { tagged.Dialer = previousDialer }()

	checker := newChecker(nil)
	base := &HealthConfig{
		Method: "http", Url: server.URL, Timeout: int64(time.Second),
		AcceptedStatusMin: 200, AcceptedStatusMax: 399,
	}
	httpSample := checker.check(context.Background(), "proxy", base, nil)
	if !httpSample.Success || httpSample.HTTPStatus != http.StatusOK || httpSample.TTFB <= 0 {
		t.Fatalf("unexpected HTTP sample: %#v", httpSample)
	}

	downloadConfig := proto.Clone(base).(*HealthConfig)
	downloadConfig.Method = "download"
	downloadConfig.MinBytes = 5
	downloadSample := checker.check(context.Background(), "proxy", downloadConfig, nil)
	if downloadSample.Success || downloadSample.Error.Stage != "validate" || downloadSample.Error.Reason != "insufficient_bytes" {
		t.Fatalf("unexpected download sample: %#v", downloadSample)
	}
}

func TestCheckerIPUsesDirectBaselineWithoutExportingValue(t *testing.T) {
	baselineServer := newIPv4TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(cloudflareTraceBody("198.51.100.8", "US"))
	}))
	defer baselineServer.Close()
	proxyServer := newIPv4TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(cloudflareTraceBody("203.0.113.7", "JP"))
	}))
	defer proxyServer.Close()

	previousDialer := tagged.Dialer
	tagged.Dialer = func(ctx context.Context, _ routing.Dispatcher, _ v2net.Destination, _ string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "tcp", proxyServer.Listener.Addr().String())
	}
	defer func() { tagged.Dialer = previousDialer }()

	checker := newChecker(nil)
	parser := testCloudflareParser(t)
	if err := checker.prewarmBaseline(context.Background(), baselineServer.URL, parser); err != nil {
		t.Fatal(err)
	}
	sample := checker.check(context.Background(), "proxy", &HealthConfig{
		Method: "ip", IpProvider: testCloudflareProvider(baselineServer.URL), Timeout: int64(time.Second),
		AcceptedStatusMin: 200, AcceptedStatusMax: 399,
	}, parser)
	if !sample.Success {
		t.Fatalf("unexpected IP sample: %#v", sample)
	}
	if sample.Location.Country != "JP" || sample.Location.Source != locationSourceCloudflare {
		t.Fatalf("unexpected IP location: %#v", sample.Location)
	}
}

func TestCheckerIPRejectsUnchangedPublicIP(t *testing.T) {
	server := newIPv4TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(cloudflareTraceBody("198.51.100.8", "US"))
	}))
	defer server.Close()

	previousDialer := tagged.Dialer
	tagged.Dialer = func(ctx context.Context, _ routing.Dispatcher, _ v2net.Destination, _ string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "tcp", server.Listener.Addr().String())
	}
	defer func() { tagged.Dialer = previousDialer }()

	checker := newChecker(nil)
	parser := testCloudflareParser(t)
	if err := checker.prewarmBaseline(context.Background(), server.URL, parser); err != nil {
		t.Fatal(err)
	}
	sample := checker.check(context.Background(), "proxy", &HealthConfig{
		Method: "ip", IpProvider: testCloudflareProvider(server.URL), Timeout: int64(time.Second),
		AcceptedStatusMin: 200, AcceptedStatusMax: 399,
	}, parser)
	if sample.Success || sample.Error.Stage != probeStageValidate || sample.Error.Reason != probeReasonIPUnchanged {
		t.Fatalf("unexpected IP sample: %#v", sample)
	}
}

func TestDirectBaselineIgnoresEnvironmentProxy(t *testing.T) {
	var proxyRequests atomic.Int64
	proxyServer := newIPv4TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyRequests.Add(1)
		_, _ = w.Write([]byte("198.51.100.8\n"))
	}))
	defer proxyServer.Close()
	t.Setenv("HTTP_PROXY", proxyServer.URL)
	t.Setenv("HTTPS_PROXY", proxyServer.URL)
	t.Setenv("NO_PROXY", "")

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	err := newChecker(nil).refreshBaseline(ctx, "http://baseline.invalid", testCloudflareParser(t))
	if err == nil {
		t.Fatal("expected direct baseline request to fail DNS resolution")
	}
	if requests := proxyRequests.Load(); requests != 0 {
		t.Fatalf("direct baseline unexpectedly used environment proxy: %d requests", requests)
	}
}

func TestScheduledBaselineRefreshBypassesFreshTTL(t *testing.T) {
	var requests atomic.Int32
	server := newIPv4TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := requests.Add(1)
		address := "198.51.100.8"
		if request > 1 {
			address = "198.51.100.9"
		}
		_, _ = w.Write(cloudflareTraceBody(address, "US"))
	}))
	defer server.Close()

	checker := newChecker(nil)
	parser := testCloudflareParser(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := checker.prewarmBaseline(ctx, server.URL, parser); err != nil {
		t.Fatal(err)
	}
	first, err := checker.directBaseline(ctx, server.URL, parser)
	if err != nil {
		t.Fatal(err)
	}
	cached, err := checker.directBaseline(ctx, server.URL, parser)
	if err != nil {
		t.Fatal(err)
	}
	if first != cached || requests.Load() != 1 {
		t.Fatalf("fresh baseline was not cached: first=%s cached=%s requests=%d", first, cached, requests.Load())
	}
	if err := checker.refreshBaseline(ctx, server.URL, parser); err != nil {
		t.Fatal(err)
	}
	refreshed, err := checker.directBaseline(ctx, server.URL, parser)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.String() != "198.51.100.9" || requests.Load() != 2 {
		t.Fatalf("scheduled refresh = %s, requests=%d", refreshed, requests.Load())
	}
}

func TestFailedScheduledRefreshKeepsLastValidBaseline(t *testing.T) {
	var fail atomic.Bool
	server := newIPv4TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write(cloudflareTraceBody("198.51.100.8", "US"))
	}))
	defer server.Close()

	checker := newChecker(nil)
	parser := testCloudflareParser(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := checker.prewarmBaseline(ctx, server.URL, parser); err != nil {
		t.Fatal(err)
	}
	key := baselineKey{target: server.URL, parserType: parser.Type()}
	before, found := checker.loadBaseline(key)
	if !found {
		t.Fatal("baseline was not cached")
	}
	fail.Store(true)
	if err := checker.refreshBaseline(ctx, server.URL, parser); err == nil {
		t.Fatal("failed scheduled refresh returned nil")
	}
	after, found := checker.loadBaseline(key)
	if !found || after != before {
		t.Fatalf("failed refresh changed cached baseline: before=%#v after=%#v", before, after)
	}
}

func TestDirectBaselineAgeSemantics(t *testing.T) {
	target := "http://127.0.0.1:0"
	parser := testCloudflareParser(t)
	key := baselineKey{target: target, parserType: parser.Type()}
	now := time.Now()
	value := netip.MustParseAddr("198.51.100.8")
	tests := []struct {
		name      string
		cached    *cachedBaseline
		wantError bool
	}{
		{name: "fresh", cached: &cachedBaseline{value: value, expiresAt: now.Add(time.Minute), staleAt: now.Add(21 * time.Minute)}},
		{name: "bounded stale", cached: &cachedBaseline{value: value, expiresAt: now.Add(-time.Minute), staleAt: now.Add(20 * time.Minute)}},
		{name: "expired", cached: &cachedBaseline{value: value, expiresAt: now.Add(-21 * time.Minute), staleAt: now.Add(-time.Minute)}, wantError: true},
		{name: "missing", wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			checker := newChecker(nil)
			if test.cached != nil {
				checker.baselines[key] = *test.cached
			}
			got, err := checker.directBaseline(context.Background(), target, parser)
			if test.wantError {
				if err == nil {
					t.Fatalf("baseline = %s, want error", got)
				}
				return
			}
			if err != nil || got != value {
				t.Fatalf("baseline = %s, %v", got, err)
			}
		})
	}
}

func TestCheckerIPDoesNotRefreshMissingBaseline(t *testing.T) {
	var baselineRequests atomic.Int32
	baselineServer := newIPv4TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		baselineRequests.Add(1)
		_, _ = w.Write(cloudflareTraceBody("198.51.100.8", "US"))
	}))
	defer baselineServer.Close()
	proxyServer := newIPv4TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(120 * time.Millisecond)
		_, _ = w.Write(cloudflareTraceBody("203.0.113.7", "JP"))
	}))
	defer proxyServer.Close()

	previousDialer := tagged.Dialer
	tagged.Dialer = func(ctx context.Context, _ routing.Dispatcher, _ v2net.Destination, _ string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "tcp", proxyServer.Listener.Addr().String())
	}
	defer func() { tagged.Dialer = previousDialer }()

	sample := newChecker(nil).check(context.Background(), "proxy", &HealthConfig{
		Method: ProbeMethodIP, IpProvider: testCloudflareProvider(baselineServer.URL), Timeout: int64(200 * time.Millisecond),
		AcceptedStatusMin: 200, AcceptedStatusMax: 399,
	}, testCloudflareParser(t))
	if sample.Success || sample.Error.Reason != probeReasonBaselineUnavailable {
		t.Fatalf("unexpected IP sample: %#v", sample)
	}
	if requests := baselineRequests.Load(); requests != 0 {
		t.Fatalf("probe triggered %d direct baseline requests", requests)
	}
}

func testCloudflareParser(t *testing.T) ipResponseParser {
	t.Helper()
	parser, err := buildIPResponseParser(IPProbeProviderTypeCloudflareTrace)
	if err != nil {
		t.Fatal(err)
	}
	return parser
}

func testCloudflareProvider(target string) *IPProbeProviderConfig {
	return &IPProbeProviderConfig{Type: IPProbeProviderTypeCloudflareTrace, Url: target}
}

func cloudflareTraceBody(ip, country string) []byte {
	return []byte("ip=" + ip + "\nloc=" + country + "\n")
}

func newIPv4TestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.Listener = listener
	server.Start()
	return server
}
