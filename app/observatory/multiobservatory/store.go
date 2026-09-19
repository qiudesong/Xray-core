package multiobservatory

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/xtls/xray-core/features/extension"
)

var probeHistogramBuckets = []time.Duration{
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
	2500 * time.Millisecond,
	5 * time.Second,
	10 * time.Second,
}

type probeKey struct {
	probe    string
	outbound string
}

type errorKey struct {
	stage  string
	reason string
}

type probeHistogram struct {
	count   uint64
	sum     time.Duration
	buckets map[time.Duration]uint64
}

func newProbeHistogram() probeHistogram {
	histogram := probeHistogram{buckets: make(map[time.Duration]uint64, len(probeHistogramBuckets))}
	for _, upperBound := range probeHistogramBuckets {
		histogram.buckets[upperBound] = 0
	}
	return histogram
}

func (h *probeHistogram) observe(value time.Duration) {
	h.count++
	h.sum += value
	for _, upperBound := range probeHistogramBuckets {
		if value <= upperBound {
			h.buckets[upperBound]++
		}
	}
}

func (h *probeHistogram) snapshot() (uint64, time.Duration, map[time.Duration]uint64) {
	buckets := make(map[time.Duration]uint64, len(h.buckets))
	for upperBound, count := range h.buckets {
		buckets[upperBound] = count
	}
	return h.count, h.sum, buckets
}

type sampleWindow struct {
	samples  []extension.ProbeSample
	next     int
	failures int
	limit    int
}

func (w *sampleWindow) add(sample extension.ProbeSample, limit int) {
	if w.limit != limit {
		w.resize(limit)
	}
	if len(w.samples) < w.limit {
		w.samples = append(w.samples, sample)
		if !sample.Success {
			w.failures++
		}
		return
	}

	evicted := w.samples[w.next]
	if !evicted.Success {
		w.failures--
	}
	w.samples[w.next] = sample
	if !sample.Success {
		w.failures++
	}
	w.next = (w.next + 1) % len(w.samples)
}

func (w *sampleWindow) resize(limit int) {
	previous := w.snapshot()
	if len(previous) > limit {
		previous = previous[len(previous)-limit:]
	}
	w.samples = make([]extension.ProbeSample, 0, limit)
	w.samples = append(w.samples, previous...)
	w.next = 0
	w.failures = 0
	for _, sample := range w.samples {
		if !sample.Success {
			w.failures++
		}
	}
	w.limit = limit
}

func (w *sampleWindow) snapshot() []extension.ProbeSample {
	result := make([]extension.ProbeSample, len(w.samples))
	if len(w.samples) == 0 {
		return result
	}
	first := copy(result, w.samples[w.next:])
	copy(result[first:], w.samples[:w.next])
	return result
}

func (w *sampleWindow) latest() (extension.ProbeSample, bool) {
	if len(w.samples) == 0 {
		return extension.ProbeSample{}, false
	}
	index := len(w.samples) - 1
	if len(w.samples) == w.limit {
		index = (w.next + len(w.samples) - 1) % len(w.samples)
	}
	return w.samples[index], true
}

type probeRecord struct {
	mu            sync.RWMutex
	method        string
	interval      time.Duration
	history       sampleWindow
	policyState   extension.ProbeState
	policyReason  string
	lastSuccess   time.Time
	lastLocation  extension.ProbeLocation
	checksTotal   uint64
	checksSuccess uint64
	checksFailure uint64
	errors        map[errorKey]uint64
	duration      probeHistogram
	latency       probeHistogram
}

type resultStore struct {
	mu       sync.RWMutex
	policyMu sync.Mutex
	records  map[probeKey]*probeRecord
}

func newResultStore() *resultStore {
	return &resultStore{records: make(map[probeKey]*probeRecord)}
}

func (s *resultStore) record(probe, outbound, method string, interval time.Duration, policy HealthPolicy, sample extension.ProbeSample) {
	key := probeKey{probe: probe, outbound: outbound}
	record := s.getOrCreateRecord(key)
	record.mu.Lock()
	defer record.mu.Unlock()
	record.method = method
	record.interval = interval
	record.history.add(sample, s.policyHistoryLimit(policy))
	record.checksTotal++
	if sample.Success {
		record.checksSuccess++
		record.lastSuccess = sample.CheckedAt
		record.latency.observe(sample.TTFB)
		if sample.Location.Country != "" {
			record.lastLocation = sample.Location
		}
	} else {
		record.checksFailure++
		record.errors[errorKey{stage: sample.Error.Stage, reason: sample.Error.Reason}]++
	}
	record.duration.observe(sample.Duration)
	var decision HealthDecision
	if windowPolicy, ok := policy.(windowHealthPolicy); ok {
		decision = windowPolicy.EvaluateWindow(windowHealthContext{
			SampleCount: len(record.history.samples), FailureCount: record.history.failures,
			PreviousState: record.policyState, Now: sample.CheckedAt, Interval: interval,
		})
	} else {
		context := HealthContext{
			Samples: record.history.snapshot(), PreviousState: record.policyState,
			Now: sample.CheckedAt, Interval: interval,
		}
		// Third-party policies may be stateful. Preserve the previous serialized
		// evaluation contract while allowing built-in window policies to run in
		// parallel for independent records.
		s.policyMu.Lock()
		decision = policy.Evaluate(context)
		s.policyMu.Unlock()
	}
	record.policyState = decision.State
	record.policyReason = decision.Reason
}

func (s *resultStore) getOrCreateRecord(key probeKey) *probeRecord {
	s.mu.RLock()
	record := s.records[key]
	s.mu.RUnlock()
	if record != nil {
		return record
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if record = s.records[key]; record != nil {
		return record
	}
	record = &probeRecord{
		errors:   make(map[errorKey]uint64),
		duration: newProbeHistogram(),
		latency:  newProbeHistogram(),
	}
	s.records[key] = record
	return record
}

func (*resultStore) policyHistoryLimit(policy HealthPolicy) int {
	if limited, ok := policy.(historyLimitedPolicy); ok {
		if limit := limited.HistoryLimit(); limit > 0 {
			return limit
		}
	}
	return defaultProbeHistoryLimit
}

func (s *resultStore) retain(probe string, outbounds []string) {
	keep := make(map[string]struct{}, len(outbounds))
	for _, outbound := range outbounds {
		keep[outbound] = struct{}{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.records {
		if key.probe != probe {
			continue
		}
		if _, found := keep[key.outbound]; !found {
			delete(s.records, key)
		}
	}
}

func (s *resultStore) snapshots(now time.Time, filter extension.ProbeSnapshotFilter) []extension.ProbeSnapshot {
	type keyedRecord struct {
		key    probeKey
		record *probeRecord
	}
	s.mu.RLock()
	records := make([]keyedRecord, 0, len(s.records))
	for key, record := range s.records {
		if filter.ProbeTag != "" && key.probe != filter.ProbeTag {
			continue
		}
		if filter.OutboundTag != "" && key.outbound != filter.OutboundTag {
			continue
		}
		records = append(records, keyedRecord{key: key, record: record})
	}
	s.mu.RUnlock()

	result := make([]extension.ProbeSnapshot, 0, len(records))
	for _, item := range records {
		key, record := item.key, item.record
		record.mu.RLock()
		latest, found := record.history.latest()
		if !found {
			record.mu.RUnlock()
			continue
		}
		effective := record.policyState
		if now.Sub(latest.CheckedAt) > staleIntervalMultiplier*record.interval {
			effective = extension.ProbeStateStale
		}
		errors := make([]extension.ProbeErrorCount, 0, len(record.errors))
		for key, count := range record.errors {
			errors = append(errors, extension.ProbeErrorCount{Stage: key.stage, Reason: key.reason, Count: count})
		}
		sort.Slice(errors, func(i, j int) bool {
			if errors[i].Stage != errors[j].Stage {
				return errors[i].Stage < errors[j].Stage
			}
			return errors[i].Reason < errors[j].Reason
		})
		durationCount, durationSum, durationBuckets := record.duration.snapshot()
		latencyCount, latencySum, latencyBuckets := record.latency.snapshot()
		windowSamples, windowFailures, latency := currentWindow(record)
		snapshot := extension.ProbeSnapshot{
			ProbeTag: key.probe, OutboundTag: key.outbound, Method: record.method,
			PolicyState: record.policyState, EffectiveState: effective, PolicyReason: record.policyReason,
			Interval: record.interval, Latest: latest, LastSuccess: record.lastSuccess, LastLocation: record.lastLocation,
			ChecksTotal: record.checksTotal, ChecksSuccess: record.checksSuccess, ChecksFailure: record.checksFailure,
			Errors: errors, DurationCount: durationCount, DurationSum: durationSum,
			DurationBuckets: durationBuckets, LatencyCount: latencyCount, LatencySum: latencySum,
			LatencyBuckets: latencyBuckets, WindowSamples: uint64(windowSamples), WindowFailures: uint64(windowFailures),
			WindowLatencyDeviation: latency.deviation, WindowLatencyAverage: latency.average,
			WindowLatencyMaximum: latency.maximum, WindowLatencyMinimum: latency.minimum,
		}
		record.mu.RUnlock()
		result = append(result, snapshot)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ProbeTag != result[j].ProbeTag {
			return result[i].ProbeTag < result[j].ProbeTag
		}
		return result[i].OutboundTag < result[j].OutboundTag
	})
	return result
}

type windowLatencyStatistics struct {
	deviation time.Duration
	average   time.Duration
	maximum   time.Duration
	minimum   time.Duration
}

func currentWindow(record *probeRecord) (int, int, windowLatencyStatistics) {
	samples := record.history.samples
	if len(samples) == 0 {
		return 0, 0, windowLatencyStatistics{}
	}

	var successfulSamples int
	var minimum, maximum time.Duration
	var mean, squaredDifference float64
	for _, sample := range samples {
		if !sample.Success {
			continue
		}
		latency := sample.TTFB
		if successfulSamples == 0 {
			minimum, maximum = latency, latency
		} else {
			minimum = min(minimum, latency)
			maximum = max(maximum, latency)
		}
		successfulSamples++
		difference := float64(latency) - mean
		mean += difference / float64(successfulSamples)
		squaredDifference += difference * (float64(latency) - mean)
	}
	if successfulSamples == 0 {
		return len(samples), record.history.failures, windowLatencyStatistics{}
	}
	return len(samples), record.history.failures, windowLatencyStatistics{
		deviation: time.Duration(math.Sqrt(squaredDifference / float64(successfulSamples))),
		average:   time.Duration(mean),
		maximum:   maximum,
		minimum:   minimum,
	}
}
