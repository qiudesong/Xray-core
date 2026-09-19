package multiobservatory

import (
	"testing"
	"time"

	"github.com/xtls/xray-core/features/extension"
)

func TestSlidingWindowPolicyThresholdsAndHysteresis(t *testing.T) {
	policy := &slidingWindowPolicy{
		windowSize: 10, minimumSamples: 3, failureThreshold: 0.5, recoveryThreshold: 0.2,
	}
	now := time.Unix(1700000000, 0)
	success := extension.ProbeSample{Success: true}
	failure := extension.ProbeSample{Success: false}

	tests := []struct {
		name     string
		samples  []extension.ProbeSample
		previous extension.ProbeState
		want     extension.ProbeState
	}{
		{name: "insufficient", samples: []extension.ProbeSample{success, success}, want: extension.ProbeStateUnknown},
		{name: "healthy", samples: []extension.ProbeSample{success, success, success}, want: extension.ProbeStateHealthy},
		{name: "unhealthy", samples: []extension.ProbeSample{failure, failure, success}, want: extension.ProbeStateUnhealthy},
		{
			name:     "hysteresis keeps previous unhealthy",
			samples:  []extension.ProbeSample{success, success, success, success, success, success, success, failure, failure, failure},
			previous: extension.ProbeStateUnhealthy, want: extension.ProbeStateUnhealthy,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision := policy.Evaluate(HealthContext{Samples: test.samples, PreviousState: test.previous, Now: now})
			if decision.State != test.want {
				t.Fatalf("state = %q, want %q", decision.State, test.want)
			}
		})
	}
}

func TestSlidingWindowPolicyFactoryDefaults(t *testing.T) {
	built, err := buildHealthPolicy("slidingWindow", &SlidingWindowPolicyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	policy := built.(*slidingWindowPolicy)
	if policy.windowSize != 10 || policy.minimumSamples != 3 || policy.failureThreshold != 0.5 || policy.recoveryThreshold != 0.2 {
		t.Fatalf("unexpected defaults: %#v", policy)
	}
}

func TestSlidingWindowPolicyFactoryRejectsWrongSettingsType(t *testing.T) {
	if _, err := buildHealthPolicy("slidingWindow", &HealthPolicyConfig{}); err == nil {
		t.Fatal("wrong policy settings type was accepted")
	}
	zero := uint32(0)
	if _, err := buildHealthPolicy("slidingWindow", &SlidingWindowPolicyConfig{WindowSize: &zero}); err == nil {
		t.Fatal("explicit zero window size was accepted")
	}
}

func TestSlidingWindowPolicyFactoryLimitsWindowSize(t *testing.T) {
	maximum := maximumSlidingWindowSize
	if _, err := buildHealthPolicy("slidingWindow", &SlidingWindowPolicyConfig{WindowSize: &maximum}); err != nil {
		t.Fatalf("maximum window size was rejected: %v", err)
	}

	tooLarge := maximumSlidingWindowSize + 1
	if _, err := buildHealthPolicy("slidingWindow", &SlidingWindowPolicyConfig{WindowSize: &tooLarge}); err == nil {
		t.Fatalf("window size above maximum %d was accepted", maximumSlidingWindowSize)
	}
}

func TestResultStoreUsesPolicyAndMarksStale(t *testing.T) {
	policy := &slidingWindowPolicy{
		windowSize: 10, minimumSamples: 3, failureThreshold: 0.5, recoveryThreshold: 0.2,
	}
	store := newResultStore()
	checkedAt := time.Unix(1700000000, 0)
	for index := 0; index < 3; index++ {
		store.record("health", "proxy", "http", time.Second, policy, extension.ProbeSample{
			Success: true, CheckedAt: checkedAt.Add(time.Duration(index) * time.Second), Duration: 50 * time.Millisecond,
		})
	}

	fresh := store.snapshots(checkedAt.Add(2500*time.Millisecond), extension.ProbeSnapshotFilter{})
	if len(fresh) != 1 || fresh[0].EffectiveState != extension.ProbeStateHealthy {
		t.Fatalf("fresh snapshot = %#v, want healthy", fresh)
	}
	stale := store.snapshots(checkedAt.Add(5*time.Second), extension.ProbeSnapshotFilter{})
	if len(stale) != 1 || stale[0].EffectiveState != extension.ProbeStateStale {
		t.Fatalf("stale snapshot = %#v, want stale", stale)
	}
	if stale[0].PolicyState != extension.ProbeStateHealthy {
		t.Fatalf("stale snapshot changed policy state to %q", stale[0].PolicyState)
	}
}

func TestResultStoreSeparatesProbeAndOutboundKeys(t *testing.T) {
	policy := &slidingWindowPolicy{
		windowSize: 1, minimumSamples: 1, failureThreshold: 0.5, recoveryThreshold: 0.2,
	}
	store := newResultStore()
	now := time.Now()
	store.record("probe-a", "proxy", "http", time.Minute, policy, extension.ProbeSample{Success: true, CheckedAt: now})
	store.record("probe-b", "proxy", "download", time.Minute, policy, extension.ProbeSample{Success: false, CheckedAt: now})
	snapshots := store.snapshots(now, extension.ProbeSnapshotFilter{})
	if len(snapshots) != 2 || snapshots[0].ProbeTag != "probe-a" || snapshots[1].ProbeTag != "probe-b" {
		t.Fatalf("unexpected snapshots: %#v", snapshots)
	}
}

func TestResultStorePrunesOnlyRemovedOutboundsForProbe(t *testing.T) {
	policy := &slidingWindowPolicy{windowSize: 1, minimumSamples: 1, failureThreshold: 0.5, recoveryThreshold: 0.2}
	store := newResultStore()
	now := time.Now()
	store.record("probe-a", "keep", "http", time.Minute, policy, extension.ProbeSample{Success: true, CheckedAt: now})
	store.record("probe-a", "remove", "http", time.Minute, policy, extension.ProbeSample{Success: true, CheckedAt: now})
	store.record("probe-b", "remove", "http", time.Minute, policy, extension.ProbeSample{Success: true, CheckedAt: now})
	store.retain("probe-a", []string{"keep"})
	snapshots := store.snapshots(now, extension.ProbeSnapshotFilter{})
	if len(snapshots) != 2 || snapshots[0].OutboundTag != "keep" || snapshots[1].ProbeTag != "probe-b" {
		t.Fatalf("unexpected retained snapshots: %#v", snapshots)
	}
}

func TestResultStoreRetainsLastSuccessfulLocation(t *testing.T) {
	policy := &slidingWindowPolicy{windowSize: 1, minimumSamples: 1, failureThreshold: 0.5, recoveryThreshold: 0.2}
	store := newResultStore()
	now := time.Now()
	location := extension.ProbeLocation{Source: locationSourceCloudflare, Country: "JP", ObservedAt: now}
	store.record("probe", "proxy", ProbeMethodIP, time.Minute, policy, extension.ProbeSample{
		Success: true, CheckedAt: now, Location: location,
	})
	store.record("probe", "proxy", ProbeMethodIP, time.Minute, policy, extension.ProbeSample{
		Success: false, CheckedAt: now.Add(time.Second),
		Error: extension.ProbeError{Stage: probeStageResponse, Reason: probeReasonTimeout},
	})
	snapshots := store.snapshots(now.Add(time.Second), extension.ProbeSnapshotFilter{})
	if len(snapshots) != 1 || snapshots[0].LastLocation != location {
		t.Fatalf("last successful location = %#v", snapshots)
	}
}

func TestResultStoreUsesFixedSlidingWindow(t *testing.T) {
	policy := &slidingWindowPolicy{
		windowSize: 4, minimumSamples: 1, failureThreshold: 0.5, recoveryThreshold: 0.2,
	}
	store := newResultStore()
	key := probeKey{probe: "probe", outbound: "proxy"}
	now := time.Unix(1700000000, 0)
	samples := []extension.ProbeSample{
		{Success: true, CheckedAt: now, Duration: time.Millisecond, TTFB: 5 * time.Millisecond},
		{Success: false, CheckedAt: now.Add(time.Second), Duration: 2 * time.Millisecond, TTFB: 20 * time.Millisecond},
		{Success: true, CheckedAt: now.Add(2 * time.Second), Duration: 3 * time.Millisecond, TTFB: 10 * time.Millisecond},
		{Success: true, CheckedAt: now.Add(3 * time.Second), Duration: 4 * time.Millisecond, TTFB: 30 * time.Millisecond},
		{Success: false, CheckedAt: now.Add(4 * time.Second), Duration: 5 * time.Millisecond, TTFB: 40 * time.Millisecond},
	}
	for _, sample := range samples[:4] {
		store.record(key.probe, key.outbound, ProbeMethodHTTP, time.Minute, policy, sample)
	}

	store.mu.RLock()
	record := store.records[key]
	store.mu.RUnlock()
	record.mu.RLock()
	storage := &record.history.samples[0]
	record.mu.RUnlock()

	store.record(key.probe, key.outbound, ProbeMethodHTTP, time.Minute, policy, samples[4])

	record.mu.RLock()
	ordered := record.history.snapshot()
	if got := &record.history.samples[0]; got != storage {
		t.Fatal("sliding window reallocated after reaching capacity")
	}
	if len(record.history.samples) != 4 || cap(record.history.samples) != 4 {
		t.Fatalf("window len/cap = %d/%d, want 4/4", len(record.history.samples), cap(record.history.samples))
	}
	if record.history.failures != 2 {
		t.Fatalf("window failures = %d, want 2", record.history.failures)
	}
	record.mu.RUnlock()

	for index, want := range samples[1:] {
		if ordered[index] != want {
			t.Fatalf("ordered sample %d = %#v, want %#v", index, ordered[index], want)
		}
	}
	snapshots := store.snapshots(now.Add(4*time.Second), extension.ProbeSnapshotFilter{})
	if len(snapshots) != 1 {
		t.Fatalf("snapshots = %d, want 1", len(snapshots))
	}
	snapshot := snapshots[0]
	if snapshot.WindowSamples != 4 || snapshot.WindowFailures != 2 {
		t.Fatalf("window samples/failures = %d/%d, want 4/2", snapshot.WindowSamples, snapshot.WindowFailures)
	}
	if snapshot.WindowLatencyMinimum != 10*time.Millisecond || snapshot.WindowLatencyAverage != 20*time.Millisecond || snapshot.WindowLatencyMaximum != 30*time.Millisecond || snapshot.WindowLatencyDeviation != 10*time.Millisecond {
		t.Fatalf("unexpected successful TTFB statistics: %#v", snapshot)
	}
	if snapshot.LatencyCount != 3 || snapshot.LatencySum != 45*time.Millisecond {
		t.Fatalf("successful TTFB histogram count/sum = %d/%s, want 3/45ms", snapshot.LatencyCount, snapshot.LatencySum)
	}
	if snapshot.LatencyBuckets[10*time.Millisecond] != 2 || snapshot.LatencyBuckets[50*time.Millisecond] != 3 {
		t.Fatalf("unexpected successful TTFB histogram buckets: %#v", snapshot.LatencyBuckets)
	}
}

type blockingHealthPolicy struct {
	entered chan struct{}
	release chan struct{}
}

func (p *blockingHealthPolicy) Evaluate(ctx HealthContext) HealthDecision {
	close(p.entered)
	<-p.release
	return HealthDecision{State: extension.ProbeStateHealthy, EvaluatedAt: ctx.Now}
}

func TestResultStoreUpdatesIndependentRecordsConcurrently(t *testing.T) {
	store := newResultStore()
	policy := &slidingWindowPolicy{
		windowSize: 1, minimumSamples: 1, failureThreshold: 0.5, recoveryThreshold: 0.2,
	}
	now := time.Now()
	for _, outbound := range []string{"slow", "fast"} {
		store.record("probe", outbound, ProbeMethodHTTP, time.Minute, policy, extension.ProbeSample{Success: true, CheckedAt: now})
	}

	blocking := &blockingHealthPolicy{entered: make(chan struct{}), release: make(chan struct{})}
	slowDone := make(chan struct{})
	go func() {
		defer close(slowDone)
		store.record("probe", "slow", ProbeMethodHTTP, time.Minute, blocking, extension.ProbeSample{Success: true, CheckedAt: now})
	}()
	<-blocking.entered

	fastDone := make(chan struct{})
	go func() {
		defer close(fastDone)
		store.record("probe", "fast", ProbeMethodHTTP, time.Minute, policy, extension.ProbeSample{Success: true, CheckedAt: now})
	}()
	select {
	case <-fastDone:
	case <-time.After(time.Second):
		close(blocking.release)
		<-slowDone
		t.Fatal("an update to one record blocked an independent record")
	}
	close(blocking.release)
	<-slowDone
}

func BenchmarkResultStoreRecordFullSlidingWindow(b *testing.B) {
	policy := &slidingWindowPolicy{
		windowSize: maximumSlidingWindowSize, minimumSamples: 1,
		failureThreshold: 0.5, recoveryThreshold: 0.2,
	}
	store := newResultStore()
	sample := extension.ProbeSample{Success: true, CheckedAt: time.Unix(1700000000, 0), Duration: 50 * time.Millisecond}
	for range maximumSlidingWindowSize {
		store.record("probe", "proxy", ProbeMethodHTTP, time.Second, policy, sample)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		store.record("probe", "proxy", ProbeMethodHTTP, time.Second, policy, sample)
	}
}
