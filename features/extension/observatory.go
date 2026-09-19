package extension

import (
	"context"
	"time"

	"github.com/xtls/xray-core/features"
	"google.golang.org/protobuf/proto"
)

type ProbeState string

const (
	ProbeStateUnknown   ProbeState = "unknown"
	ProbeStateHealthy   ProbeState = "healthy"
	ProbeStateUnhealthy ProbeState = "unhealthy"
	ProbeStateStale     ProbeState = "stale"
)

type ProbeError struct {
	Stage  string
	Reason string
}

// ProbeLocation contains bounded, non-sensitive metadata returned by an IP
// probe provider. The public IP is intentionally not retained here.
type ProbeLocation struct {
	Source     string
	Country    string
	ObservedAt time.Time
}

type ProbeSample struct {
	Success    bool
	CheckedAt  time.Time
	Duration   time.Duration
	TTFB       time.Duration
	HTTPStatus int
	Bytes      int64
	Location   ProbeLocation
	Error      ProbeError
}

type ProbeErrorCount struct {
	Stage  string
	Reason string
	Count  uint64
}

type ProbeSnapshot struct {
	ProbeTag       string
	OutboundTag    string
	Method         string
	PolicyState    ProbeState
	EffectiveState ProbeState
	PolicyReason   string
	Interval       time.Duration
	Latest         ProbeSample
	LastSuccess    time.Time
	LastLocation   ProbeLocation
	ChecksTotal    uint64
	ChecksSuccess  uint64
	ChecksFailure  uint64
	Errors         []ProbeErrorCount
	// Duration is measured for every completed probe attempt.
	DurationCount   uint64
	DurationSum     time.Duration
	DurationBuckets map[time.Duration]uint64
	// Latency is the TTFB measured only for successful probe attempts.
	LatencyCount           uint64
	LatencySum             time.Duration
	LatencyBuckets         map[time.Duration]uint64
	WindowSamples          uint64
	WindowFailures         uint64
	WindowLatencyDeviation time.Duration
	WindowLatencyAverage   time.Duration
	WindowLatencyMaximum   time.Duration
	WindowLatencyMinimum   time.Duration
}

type ProbeSnapshotFilter struct {
	ProbeTag    string
	OutboundTag string
}

// ProbeSnapshotProvider exposes immutable, sorted probe snapshots. Scraping a
// provider must never trigger network activity.
type ProbeSnapshotProvider interface {
	GetProbeSnapshots(ctx context.Context, filter ProbeSnapshotFilter) ([]ProbeSnapshot, error)
}

type ProbeBaselineSnapshot struct {
	ProbeTag           string
	Provider           string
	LastRefreshSuccess bool
	LastAttempt        time.Time
}

// ProbeBaselineSnapshotProvider exposes baseline refresh health without
// exposing the baseline URL or public IP.
type ProbeBaselineSnapshotProvider interface {
	GetProbeBaselineSnapshots(ctx context.Context) ([]ProbeBaselineSnapshot, error)
}

type Observatory interface {
	features.Feature

	GetObservation(ctx context.Context) (proto.Message, error)
}

type BurstObservatory interface {
	Observatory
	Check(tag []string)
}

func ObservatoryType() interface{} {
	return (*Observatory)(nil)
}
