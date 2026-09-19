package multiobservatory

import (
	"sync"
	"time"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/features/extension"
	"google.golang.org/protobuf/proto"
)

type HealthContext struct {
	Samples       []extension.ProbeSample
	PreviousState extension.ProbeState
	Now           time.Time
	Interval      time.Duration
}

type HealthDecision struct {
	State       extension.ProbeState
	Reason      string
	EvaluatedAt time.Time
}

type HealthPolicy interface {
	Evaluate(context HealthContext) HealthDecision
}

type historyLimitedPolicy interface {
	HistoryLimit() int
}

type windowHealthContext struct {
	SampleCount   int
	FailureCount  int
	PreviousState extension.ProbeState
	Now           time.Time
	Interval      time.Duration
}

// windowHealthPolicy allows policies that only need aggregate window data to
// avoid materializing a copy of the complete sample history for every probe.
type windowHealthPolicy interface {
	EvaluateWindow(context windowHealthContext) HealthDecision
}

type HealthPolicyFactory interface {
	Type() string
	Build(settings proto.Message) (HealthPolicy, error)
}

var policyRegistry = struct {
	sync.RWMutex
	factories map[string]HealthPolicyFactory
}{factories: make(map[string]HealthPolicyFactory)}

func RegisterHealthPolicy(factory HealthPolicyFactory) error {
	if factory == nil || factory.Type() == "" {
		return errors.New("health policy factory and type are required")
	}
	policyRegistry.Lock()
	defer policyRegistry.Unlock()
	if _, found := policyRegistry.factories[factory.Type()]; found {
		return errors.New("health policy already registered: ", factory.Type())
	}
	policyRegistry.factories[factory.Type()] = factory
	return nil
}

func buildHealthPolicy(policyType string, settings proto.Message) (HealthPolicy, error) {
	policyRegistry.RLock()
	factory, found := policyRegistry.factories[policyType]
	policyRegistry.RUnlock()
	if !found {
		return nil, errors.New("unknown health policy type: ", policyType)
	}
	return factory.Build(settings)
}

func ValidateHealthPolicy(policyType string, settings proto.Message) error {
	_, err := buildHealthPolicy(policyType, settings)
	return err
}

type slidingWindowPolicyFactory struct{}

func (slidingWindowPolicyFactory) Type() string {
	return HealthPolicyTypeSlidingWindow
}

func (slidingWindowPolicyFactory) Build(settings proto.Message) (HealthPolicy, error) {
	config := &SlidingWindowPolicyConfig{}
	if settings != nil {
		var ok bool
		config, ok = settings.(*SlidingWindowPolicyConfig)
		if !ok {
			return nil, errors.New("invalid sliding window policy settings type")
		}
	}
	windowSize := defaultSlidingWindowSize
	if config.WindowSize != nil {
		windowSize = config.GetWindowSize()
	}
	minimumSamples := defaultSlidingWindowMinimumSamples
	if config.MinimumSamples != nil {
		minimumSamples = config.GetMinimumSamples()
	}
	failureThreshold := defaultSlidingWindowFailureThreshold
	if config.FailureThreshold != nil {
		failureThreshold = *config.FailureThreshold
	}
	recoveryThreshold := defaultSlidingWindowRecoveryThreshold
	if config.RecoveryThreshold != nil {
		recoveryThreshold = *config.RecoveryThreshold
	}
	if windowSize == 0 || windowSize > maximumSlidingWindowSize {
		return nil, errors.New("sliding window size must be between 1 and ", maximumSlidingWindowSize)
	}
	if minimumSamples == 0 || minimumSamples > windowSize ||
		failureThreshold < 0 || failureThreshold > 1 || recoveryThreshold < 0 || recoveryThreshold > 1 ||
		recoveryThreshold >= failureThreshold {
		return nil, errors.New("invalid sliding window policy thresholds")
	}
	return &slidingWindowPolicy{
		windowSize: windowSize, minimumSamples: minimumSamples,
		failureThreshold: failureThreshold, recoveryThreshold: recoveryThreshold,
	}, nil
}

type slidingWindowPolicy struct {
	windowSize        uint32
	minimumSamples    uint32
	failureThreshold  float64
	recoveryThreshold float64
}

func (p *slidingWindowPolicy) HistoryLimit() int {
	return int(p.windowSize)
}

func (p *slidingWindowPolicy) Evaluate(ctx HealthContext) HealthDecision {
	samples := ctx.Samples
	windowSize := int(p.windowSize)
	if len(samples) > windowSize {
		samples = samples[len(samples)-windowSize:]
	}
	failures := 0
	for _, sample := range samples {
		if !sample.Success {
			failures++
		}
	}
	return p.EvaluateWindow(windowHealthContext{
		SampleCount: len(samples), FailureCount: failures, PreviousState: ctx.PreviousState,
		Now: ctx.Now, Interval: ctx.Interval,
	})
}

func (p *slidingWindowPolicy) EvaluateWindow(ctx windowHealthContext) HealthDecision {
	decision := HealthDecision{State: extension.ProbeStateUnknown, Reason: policyReasonInsufficientSamples, EvaluatedAt: ctx.Now}
	if ctx.SampleCount < int(p.minimumSamples) {
		return decision
	}
	rate := float64(ctx.FailureCount) / float64(ctx.SampleCount)
	switch {
	case rate >= p.failureThreshold:
		decision.State = extension.ProbeStateUnhealthy
		decision.Reason = policyReasonFailureThreshold
	case rate <= p.recoveryThreshold:
		decision.State = extension.ProbeStateHealthy
		decision.Reason = policyReasonRecoveryThreshold
	default:
		decision.State = ctx.PreviousState
		decision.Reason = policyReasonHysteresis
		if decision.State == "" {
			decision.State = extension.ProbeStateUnknown
		}
	}
	return decision
}

func init() {
	if err := RegisterHealthPolicy(slidingWindowPolicyFactory{}); err != nil {
		panic(err)
	}
}
