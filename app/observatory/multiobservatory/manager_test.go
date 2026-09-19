package multiobservatory

import (
	"context"
	stderrors "errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/extension"
	"google.golang.org/protobuf/proto"
)

type staticSelector struct {
	tags []string
}

func (s *staticSelector) Select([]string) []string {
	return append([]string(nil), s.tags...)
}

type blockingChecker struct {
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

type contextCapturingChecker struct {
	inboundTags chan string
}

func (c *contextCapturingChecker) check(ctx context.Context, _ string, _ *HealthConfig, _ ipResponseParser) extension.ProbeSample {
	inbound := session.InboundFromContext(ctx)
	if inbound == nil {
		c.inboundTags <- ""
	} else {
		c.inboundTags <- inbound.Tag
	}
	return extension.ProbeSample{Success: true, CheckedAt: time.Now()}
}

type prewarmingChecker struct {
	mu              sync.Mutex
	targets         []string
	refreshTargets  []string
	refreshObserved chan struct{}
	refreshError    error
}

type cancelablePrewarmingChecker struct {
	started  chan struct{}
	canceled chan struct{}
	release  chan struct{}
}

func (c *cancelablePrewarmingChecker) check(context.Context, string, *HealthConfig, ipResponseParser) extension.ProbeSample {
	return extension.ProbeSample{Success: true, CheckedAt: time.Now()}
}

func (c *cancelablePrewarmingChecker) prewarmBaseline(ctx context.Context, _ string, _ ipResponseParser) error {
	close(c.started)
	select {
	case <-ctx.Done():
		close(c.canceled)
		return ctx.Err()
	case <-c.release:
		return nil
	}
}

func (*cancelablePrewarmingChecker) refreshBaseline(context.Context, string, ipResponseParser) error {
	return nil
}

func (c *prewarmingChecker) check(context.Context, string, *HealthConfig, ipResponseParser) extension.ProbeSample {
	return extension.ProbeSample{Success: true, CheckedAt: time.Now()}
}

func (c *prewarmingChecker) prewarmBaseline(_ context.Context, target string, _ ipResponseParser) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.targets = append(c.targets, target)
	return nil
}

func (c *prewarmingChecker) refreshBaseline(_ context.Context, target string, _ ipResponseParser) error {
	c.mu.Lock()
	c.refreshTargets = append(c.refreshTargets, target)
	c.mu.Unlock()
	if c.refreshObserved != nil {
		select {
		case c.refreshObserved <- struct{}{}:
		default:
		}
	}
	return c.refreshError
}

func (c *blockingChecker) check(ctx context.Context, _ string, _ *HealthConfig, _ ipResponseParser) extension.ProbeSample {
	c.calls.Add(1)
	select {
	case c.entered <- struct{}{}:
	default:
	}
	select {
	case <-c.release:
		return extension.ProbeSample{Success: true, CheckedAt: time.Now()}
	case <-ctx.Done():
		return extension.ProbeSample{
			Success: false, CheckedAt: time.Now(),
			Error: extension.ProbeError{Stage: "response", Reason: "canceled"},
		}
	}
}

func TestNormalizeHealthConfigResolvesDefaultIPProvider(t *testing.T) {
	config := &HealthConfig{SubjectSelector: []string{"proxy-"}, Method: ProbeMethodIP}
	if err := normalizeHealthConfig(config); err != nil {
		t.Fatal(err)
	}
	if config.IpProvider == nil || config.IpProvider.Type != IPProbeProviderTypeCloudflareTrace || config.IpProvider.Url != DefaultCloudflareTraceURL {
		t.Fatalf("IP provider = %#v", config.IpProvider)
	}
	if config.Url != "" {
		t.Fatalf("top-level IP URL = %q, want empty", config.Url)
	}
}

func TestNormalizeHealthConfigResolvesDefaultHTTPURL(t *testing.T) {
	config := &HealthConfig{SubjectSelector: []string{"proxy-"}, Method: ProbeMethodHTTP}
	if err := normalizeHealthConfig(config); err != nil {
		t.Fatal(err)
	}
	if config.Url != DefaultHTTPProbeURL {
		t.Fatalf("HTTP probe URL = %q, want %q", config.Url, DefaultHTTPProbeURL)
	}
}

func TestNormalizeHealthConfigRejectsAmbiguousIPTargets(t *testing.T) {
	for _, config := range []*HealthConfig{
		{SubjectSelector: []string{"proxy-"}, Method: ProbeMethodIP, Url: "https://trace.example.invalid/"},
		{SubjectSelector: []string{"proxy-"}, Method: ProbeMethodIP, IpProvider: &IPProbeProviderConfig{Url: "https://trace.example.invalid/"}},
		{SubjectSelector: []string{"proxy-"}, Method: ProbeMethodHTTP, Url: "https://health.example.invalid/", IpProvider: &IPProbeProviderConfig{Type: IPProbeProviderTypeCloudflareTrace}},
		{SubjectSelector: []string{"proxy-"}, Method: ProbeMethodDownload},
	} {
		if err := normalizeHealthConfig(config); err == nil {
			t.Fatalf("invalid health config was accepted: %#v", config)
		}
	}
}

func TestManagerStartsWhileIPBaselinePrewarmRunsAndCloseCancelsIt(t *testing.T) {
	probeChecker := &cancelablePrewarmingChecker{
		started: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{}),
	}
	manager := &Manager{
		ctx: context.Background(), selector: &staticSelector{}, checker: probeChecker, store: newResultStore(),
		health: map[string]*healthObserver{
			"ip": {
				tag: "ip", config: &HealthConfig{
					Method: ProbeMethodIP, IpProvider: testCloudflareProvider("https://ip.example"),
					Timeout: int64(time.Hour), Interval: int64(time.Hour),
				},
				ipParser: cloudflareTraceParser{}, sem: make(chan struct{}, 1),
			},
		},
		legacy: make(map[string]extension.Observatory), tags: []string{"ip"},
	}
	startDone := make(chan error, 1)
	go func() { startDone <- manager.Start() }()
	select {
	case <-probeChecker.started:
	case <-time.After(time.Second):
		t.Fatal("baseline prewarm did not start")
	}
	select {
	case err := <-startDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(probeChecker.release)
		if err := <-startDone; err != nil {
			t.Fatal(err)
		}
		_ = manager.Close()
		t.Fatal("manager Start blocked on baseline prewarm")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-probeChecker.canceled:
	default:
		t.Fatal("manager Close did not cancel baseline prewarm")
	}
}

func TestManagerActivelyRefreshesIPBaselineAfterFailureAndStopsOnClose(t *testing.T) {
	probeChecker := &prewarmingChecker{
		refreshObserved: make(chan struct{}, 1),
		refreshError:    stderrors.New("refresh failed"),
	}
	parser := cloudflareTraceParser{}
	manager := &Manager{
		ctx: context.Background(), selector: &staticSelector{}, checker: probeChecker, store: newResultStore(),
		health: map[string]*healthObserver{
			"ip": {
				tag:      "ip",
				config:   &HealthConfig{Method: ProbeMethodIP, IpProvider: testCloudflareProvider("https://ip.example"), Timeout: int64(time.Second), Interval: int64(time.Hour)},
				ipParser: parser,
			},
		},
		legacy: make(map[string]extension.Observatory), tags: []string{"ip"},
		baselineRefreshInterval: 10 * time.Millisecond,
	}
	if err := manager.Start(); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		select {
		case <-probeChecker.refreshObserved:
		case <-time.After(time.Second):
			t.Fatalf("scheduled baseline refresh attempt %d did not run", attempt+1)
		}
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	probeChecker.mu.Lock()
	refreshesAfterClose := len(probeChecker.refreshTargets)
	probeChecker.mu.Unlock()
	time.Sleep(30 * time.Millisecond)
	probeChecker.mu.Lock()
	defer probeChecker.mu.Unlock()
	if refreshesAfterClose == 0 || len(probeChecker.refreshTargets) != refreshesAfterClose {
		t.Fatalf("refreshes before/after close = %d/%d", refreshesAfterClose, len(probeChecker.refreshTargets))
	}
}

func TestManagerBaselineSnapshotsTrackLatestRefresh(t *testing.T) {
	key := baselineKey{target: "https://secret.example/trace", parserType: IPProbeProviderTypeCloudflareTrace}
	manager := &Manager{}
	manager.initializeBaselineStatuses(map[baselineKey]baselineTarget{
		key: {probes: []string{"probe-b", "probe-a"}},
	})
	manager.recordBaselineRefresh(key, nil)
	snapshots, err := manager.GetProbeBaselineSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 2 || snapshots[0].ProbeTag != "probe-a" || snapshots[1].ProbeTag != "probe-b" {
		t.Fatalf("baseline snapshots are not stably sorted: %#v", snapshots)
	}
	if !snapshots[0].LastRefreshSuccess || snapshots[0].Provider != IPProbeProviderTypeCloudflareTrace || snapshots[0].LastAttempt.IsZero() {
		t.Fatalf("unexpected successful baseline snapshot: %#v", snapshots[0])
	}
	manager.recordBaselineRefresh(key, stderrors.New("refresh failed"))
	snapshots, err = manager.GetProbeBaselineSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshots[0].LastRefreshSuccess {
		t.Fatalf("failed refresh reported success: %#v", snapshots[0])
	}
}

func TestRunCyclePreventsProbeReentry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probeChecker := &blockingChecker{entered: make(chan struct{}, 1), release: make(chan struct{})}
	manager := &Manager{
		ctx: ctx, runCtx: ctx, selector: &staticSelector{tags: []string{"proxy"}},
		checker: probeChecker, store: newResultStore(),
	}
	policy := &slidingWindowPolicy{
		windowSize: 1, minimumSamples: 1, failureThreshold: 0.5, recoveryThreshold: 0.2,
	}
	observer := &healthObserver{
		tag: "health", config: &HealthConfig{Method: "http", Interval: int64(time.Minute)},
		policy: policy, sem: make(chan struct{}, 1),
	}

	manager.runCycle(observer)
	select {
	case <-probeChecker.entered:
	case <-time.After(time.Second):
		t.Fatal("probe did not start")
	}
	manager.runCycle(observer)
	if calls := probeChecker.calls.Load(); calls != 1 {
		t.Fatalf("concurrent probe calls = %d, want 1", calls)
	}
	close(probeChecker.release)
	manager.wg.Wait()
	if snapshots := manager.store.snapshots(time.Now(), extension.ProbeSnapshotFilter{}); len(snapshots) != 1 {
		t.Fatalf("stored snapshots = %d, want 1", len(snapshots))
	}
}

func TestRunCycleInjectsVirtualInbound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probeChecker := &contextCapturingChecker{inboundTags: make(chan string, 1)}
	manager := &Manager{
		ctx: ctx, runCtx: ctx, selector: &staticSelector{tags: []string{"vpn"}},
		checker: probeChecker, store: newResultStore(),
	}
	observer := &healthObserver{
		tag: "health-ip", config: &HealthConfig{Method: ProbeMethodIP, Interval: int64(time.Minute)},
		policy: &slidingWindowPolicy{
			windowSize: 1, minimumSamples: 1, failureThreshold: 0.5, recoveryThreshold: 0.2,
		},
		sem: make(chan struct{}, 1),
	}

	manager.runCycle(observer)
	select {
	case tag := <-probeChecker.inboundTags:
		if tag != "observatory/health-ip" {
			t.Fatalf("probe inbound tag = %q, want %q", tag, "observatory/health-ip")
		}
	case <-time.After(time.Second):
		t.Fatal("probe did not start")
	}
	manager.wg.Wait()
}

func TestSnapshotsToObservationPreservesWindowStatistics(t *testing.T) {
	snapshot := extension.ProbeSnapshot{
		OutboundTag: "proxy", EffectiveState: extension.ProbeStateHealthy,
		Latest: extension.ProbeSample{
			Success: true, CheckedAt: time.Unix(1700000000, 0),
			Duration: 75 * time.Millisecond, TTFB: 50 * time.Millisecond,
		},
		WindowSamples: 10, WindowFailures: 2,
		WindowLatencyDeviation: 5 * time.Millisecond, WindowLatencyAverage: 42 * time.Millisecond,
		WindowLatencyMaximum: 55 * time.Millisecond, WindowLatencyMinimum: 30 * time.Millisecond,
	}

	result := snapshotsToObservation([]extension.ProbeSnapshot{snapshot})
	if len(result.Status) != 1 {
		t.Fatalf("statuses = %d, want 1", len(result.Status))
	}
	status := result.Status[0]
	if !status.Alive || status.Delay != 50 {
		t.Fatalf("status = %#v, want alive with TTFB delay", status)
	}
	want := &observatory.HealthPingMeasurementResult{
		All: 10, Fail: 2,
		Deviation: int64(5 * time.Millisecond), Average: int64(42 * time.Millisecond),
		Max: int64(55 * time.Millisecond), Min: int64(30 * time.Millisecond),
	}
	if !proto.Equal(status.HealthPing, want) {
		t.Fatalf("health ping = %#v, want %#v", status.HealthPing, want)
	}
}

func TestManagerStartPrewarmsUniqueIPBaselines(t *testing.T) {
	probeChecker := &prewarmingChecker{}
	parser := cloudflareTraceParser{}
	manager := &Manager{
		ctx: context.Background(), selector: &staticSelector{}, checker: probeChecker, store: newResultStore(),
		health: map[string]*healthObserver{
			"ip-a": {tag: "ip-a", config: &HealthConfig{Method: ProbeMethodIP, IpProvider: testCloudflareProvider("https://ip.example"), Timeout: int64(time.Second), Interval: int64(time.Hour)}, ipParser: parser},
			"ip-b": {tag: "ip-b", config: &HealthConfig{Method: ProbeMethodIP, IpProvider: testCloudflareProvider("https://ip.example"), Timeout: int64(2 * time.Second), Interval: int64(time.Hour)}, ipParser: parser},
			"http": {config: &HealthConfig{Method: ProbeMethodHTTP, Url: "https://health.example", Timeout: int64(time.Second), Interval: int64(time.Hour)}},
		},
		legacy: make(map[string]extension.Observatory), tags: []string{"http", "ip-a", "ip-b"},
	}
	if err := manager.Start(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	probeChecker.mu.Lock()
	defer probeChecker.mu.Unlock()
	if len(probeChecker.targets) != 1 || probeChecker.targets[0] != "https://ip.example" {
		t.Fatalf("prewarmed targets = %v", probeChecker.targets)
	}
}
