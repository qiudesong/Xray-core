package multiobservatory

import (
	"context"
	"math/rand/v2"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features"
	"github.com/xtls/xray-core/features/extension"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	"google.golang.org/protobuf/proto"
)

const probeInboundTagPrefix = "observatory/"

type healthObserver struct {
	tag      string
	config   *HealthConfig
	policy   HealthPolicy
	ipParser ipResponseParser
	running  sync.Map
	sem      chan struct{}
}

type baselineTarget struct {
	timeout time.Duration
	parser  ipResponseParser
	probes  []string
}

type baselineStatus struct {
	probes             []string
	provider           string
	lastRefreshSuccess bool
	lastAttempt        time.Time
}

type observerView struct {
	manager *Manager
	tag     string
	legacy  extension.Observatory
}

func (v *observerView) Type() interface{} { return extension.ObservatoryType() }
func (v *observerView) Start() error      { return nil }
func (v *observerView) Close() error      { return nil }

func (v *observerView) GetObservation(ctx context.Context) (proto.Message, error) {
	if v.legacy != nil {
		return v.legacy.GetObservation(ctx)
	}
	snapshots, err := v.manager.GetProbeSnapshots(ctx, extension.ProbeSnapshotFilter{ProbeTag: v.tag})
	if err != nil {
		return nil, err
	}
	return snapshotsToObservation(snapshots), nil
}

type Manager struct {
	config     *Config
	ctx        context.Context
	selector   outbound.HandlerSelector
	dispatcher routing.Dispatcher
	checker    probeChecker
	store      *resultStore

	views  map[string]*observerView
	health map[string]*healthObserver
	legacy map[string]extension.Observatory
	tags   []string

	lifecycleMu sync.Mutex
	started     bool
	closed      bool
	runCtx      context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup

	baselineRefreshInterval time.Duration
	baselineMu              sync.RWMutex
	baselineStatuses        map[baselineKey]baselineStatus
}

func New(ctx context.Context, config *Config) (*Manager, error) {
	var outboundManager outbound.Manager
	var dispatcher routing.Dispatcher
	err := core.RequireFeatures(ctx, func(om outbound.Manager, rd routing.Dispatcher) {
		outboundManager = om
		dispatcher = rd
	})
	if err != nil {
		return nil, errors.New("cannot get multi observatory dependencies").Base(err)
	}
	selector, ok := outboundManager.(outbound.HandlerSelector)
	if !ok {
		return nil, errors.New("outbound manager does not support handler selection")
	}
	manager := &Manager{
		config: config, ctx: ctx, selector: selector, dispatcher: dispatcher,
		checker: newChecker(dispatcher), store: newResultStore(),
		views: make(map[string]*observerView), health: make(map[string]*healthObserver),
		legacy: make(map[string]extension.Observatory),
	}
	for _, observerConfig := range config.Observers {
		if observerConfig == nil || observerConfig.Tag == "" || observerConfig.Settings == nil {
			return nil, errors.New("multi observatory observer tag and settings are required")
		}
		if _, found := manager.views[observerConfig.Tag]; found {
			return nil, errors.New("duplicate multi observatory observer tag: ", observerConfig.Tag)
		}
		switch observerConfig.Type {
		case ObserverTypeHealth:
			instance, err := observerConfig.Settings.GetInstance()
			if err != nil {
				return nil, err
			}
			healthConfig, ok := instance.(*HealthConfig)
			if !ok {
				return nil, errors.New("health observer has invalid settings: ", observerConfig.Tag)
			}
			if err := normalizeHealthConfig(healthConfig); err != nil {
				return nil, errors.New("invalid health observer settings: ", observerConfig.Tag).Base(err)
			}
			policy, err := buildConfiguredHealthPolicy(healthConfig.Policy)
			if err != nil {
				return nil, errors.New("cannot build health policy for ", observerConfig.Tag).Base(err)
			}
			var ipParser ipResponseParser
			if healthConfig.Method == ProbeMethodIP {
				ipParser, err = buildIPResponseParser(healthConfig.IpProvider.Type)
				if err != nil {
					return nil, errors.New("cannot build IP response parser for ", observerConfig.Tag).Base(err)
				}
			}
			concurrency := healthConfig.Concurrency
			if concurrency == 0 {
				concurrency = DefaultHealthProbeConcurrency
			}
			manager.health[observerConfig.Tag] = &healthObserver{
				tag: observerConfig.Tag, config: healthConfig, policy: policy,
				ipParser: ipParser,
				sem:      make(chan struct{}, concurrency),
			}
			manager.views[observerConfig.Tag] = &observerView{manager: manager, tag: observerConfig.Tag}
		case ObserverTypeDefault, ObserverTypeBurst:
			instance, err := observerConfig.Settings.GetInstance()
			if err != nil {
				return nil, err
			}
			created, err := common.CreateObject(ctx, instance)
			if err != nil {
				return nil, errors.New("cannot create observer ", observerConfig.Tag).Base(err)
			}
			legacy, ok := created.(extension.Observatory)
			if !ok {
				return nil, errors.New("observer is not an Observatory: ", observerConfig.Tag)
			}
			manager.legacy[observerConfig.Tag] = legacy
			manager.views[observerConfig.Tag] = &observerView{manager: manager, tag: observerConfig.Tag, legacy: legacy}
		default:
			return nil, errors.New("unknown observer type: ", observerConfig.Type)
		}
		manager.tags = append(manager.tags, observerConfig.Tag)
	}
	if len(manager.tags) == 0 {
		return nil, errors.New("multi observatory requires at least one observer")
	}
	sort.Strings(manager.tags)
	return manager, nil
}

func normalizeHealthConfig(config *HealthConfig) error {
	config.Method = strings.ToLower(strings.TrimSpace(config.Method))
	if config.Method != ProbeMethodHTTP && config.Method != ProbeMethodIP && config.Method != ProbeMethodDownload {
		return errors.New("health method must be http, ip, or download")
	}
	if len(config.SubjectSelector) == 0 {
		return errors.New("health subjectSelector is required")
	}
	if config.Method == ProbeMethodIP {
		if strings.TrimSpace(config.Url) != "" {
			return errors.New("health url is configured through ipProvider for the ip method")
		}
		provider, err := ResolveIPProbeProvider(config.IpProvider)
		if err != nil {
			return err
		}
		config.IpProvider = provider
	} else {
		if config.IpProvider != nil {
			return errors.New("ipProvider is only valid for the ip method")
		}
		config.Url = strings.TrimSpace(config.Url)
		if config.Method == ProbeMethodHTTP && config.Url == "" {
			config.Url = DefaultHTTPProbeURL
		}
		if config.Url == "" {
			return errors.New("health url is required for the download method")
		}
		parsedURL, err := url.Parse(config.Url)
		if err != nil || parsedURL.Host == "" || (parsedURL.Scheme != URLSchemeHTTP && parsedURL.Scheme != URLSchemeHTTPS) {
			return errors.New("health url must be an absolute HTTP or HTTPS URL")
		}
	}
	if config.Interval == 0 {
		config.Interval = int64(DefaultHealthProbeInterval)
	}
	if config.Timeout == 0 {
		config.Timeout = int64(DefaultHealthProbeTimeout)
	}
	if config.Interval < 0 || config.Timeout < 0 {
		return errors.New("health interval and timeout must be positive")
	}
	if config.Concurrency == 0 {
		config.Concurrency = DefaultHealthProbeConcurrency
	}
	if config.AcceptedStatusMin == 0 {
		config.AcceptedStatusMin = DefaultAcceptedStatusMin
	}
	if config.AcceptedStatusMax == 0 {
		config.AcceptedStatusMax = DefaultAcceptedStatusMax
	}
	if config.AcceptedStatusMin < MinimumHTTPStatusCode || config.AcceptedStatusMin > config.AcceptedStatusMax || config.AcceptedStatusMax > MaximumHTTPStatusCode {
		return errors.New("invalid accepted HTTP status range")
	}
	if config.MinBytes < 0 || config.MinBytes > MaxProbeBodyBytes {
		return errors.New("health minBytes must be between 0 and ", MaxProbeBodyBytes)
	}
	if config.Policy == nil {
		config.Policy = &HealthPolicyConfig{}
	}
	if config.Policy.Type == "" {
		config.Policy.Type = HealthPolicyTypeSlidingWindow
	}
	if config.Policy.Settings == nil {
		if config.Policy.Type != HealthPolicyTypeSlidingWindow {
			return errors.New("health policy settings are required for type: ", config.Policy.Type)
		}
		config.Policy.Settings = serial.ToTypedMessage(&SlidingWindowPolicyConfig{})
	}
	_, err := buildConfiguredHealthPolicy(config.Policy)
	return err
}

func buildConfiguredHealthPolicy(config *HealthPolicyConfig) (HealthPolicy, error) {
	if config == nil || config.Type == "" || config.Settings == nil {
		return nil, errors.New("health policy type and settings are required")
	}
	settings, err := config.Settings.GetInstance()
	if err != nil {
		return nil, errors.New("cannot decode health policy settings").Base(err)
	}
	return buildHealthPolicy(config.Type, settings)
}

func (m *Manager) Type() interface{} { return extension.ObservatoryType() }

func (m *Manager) GetFeaturesByTag(tag string) (features.Feature, error) {
	view, found := m.views[tag]
	if !found {
		return nil, errors.New("unable to find observatory with tag: ", tag)
	}
	return view, nil
}

func (m *Manager) GetFeaturesTag() []string {
	return append([]string(nil), m.tags...)
}

func (m *Manager) GetObservation(ctx context.Context) (proto.Message, error) {
	view := m.views[m.tags[0]]
	return view.GetObservation(ctx)
}

func (m *Manager) Start() error {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if m.started {
		return nil
	}
	if m.closed {
		return errors.New("multi observatory is closed")
	}
	startedLegacy := make([]extension.Observatory, 0, len(m.legacy))
	for _, tag := range m.tags {
		legacy := m.legacy[tag]
		if legacy == nil {
			continue
		}
		if err := legacy.Start(); err != nil {
			for i := len(startedLegacy) - 1; i >= 0; i-- {
				_ = startedLegacy[i].Close()
			}
			return errors.New("cannot start observer ", tag).Base(err)
		}
		startedLegacy = append(startedLegacy, legacy)
	}
	m.runCtx, m.cancel = context.WithCancel(m.ctx)
	m.startIPBaselineMaintenance()
	for _, tag := range m.tags {
		observer := m.health[tag]
		if observer == nil {
			continue
		}
		m.wg.Add(1)
		go m.runObserver(observer)
	}
	m.started = true
	return nil
}

func (m *Manager) startIPBaselineMaintenance() {
	maintainer, ok := m.checker.(baselineMaintainer)
	if !ok {
		return
	}
	targets := make(map[baselineKey]baselineTarget)
	for _, observer := range m.health {
		if observer.config.Method != ProbeMethodIP {
			continue
		}
		timeout := time.Duration(observer.config.Timeout)
		if timeout <= 0 {
			timeout = DefaultHealthProbeTimeout
		}
		// Use the shortest configured timeout when observers share a URL so
		// startup prewarming remains bounded by the strictest probe budget.
		key := baselineKey{target: observer.config.IpProvider.Url, parserType: observer.ipParser.Type()}
		target, found := targets[key]
		if !found {
			target = baselineTarget{timeout: timeout, parser: observer.ipParser}
		} else if timeout < target.timeout {
			target.timeout = timeout
		}
		target.probes = append(target.probes, observer.tag)
		targets[key] = target
	}
	m.initializeBaselineStatuses(targets)
	for key, target := range targets {
		m.wg.Add(1)
		go m.runBaselineMaintenance(maintainer, key, target)
	}
}

func (m *Manager) runBaselineMaintenance(maintainer baselineMaintainer, key baselineKey, target baselineTarget) {
	defer m.wg.Done()
	ctx, cancel := context.WithTimeout(m.runCtx, target.timeout)
	// Prewarming is best effort and runs in the background. A failed lookup
	// must not prevent Xray from starting; scheduled maintenance will continue
	// retrying while the manager is running.
	err := maintainer.prewarmBaseline(ctx, key.target, target.parser)
	cancel()
	m.recordBaselineRefresh(key, err)
	if m.runCtx.Err() != nil {
		return
	}
	for {
		timer := time.NewTimer(m.nextBaselineRefreshDelay())
		select {
		case <-m.runCtx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		ctx, cancel := context.WithTimeout(m.runCtx, target.timeout)
		// A scheduled refresh is proactive and independent of the 10-minute
		// fresh cache window. Failure leaves the last valid value untouched;
		// probes remain read-only and may use it until the 30-minute max age.
		err = maintainer.refreshBaseline(ctx, key.target, target.parser)
		cancel()
		m.recordBaselineRefresh(key, err)
	}
}

func (m *Manager) initializeBaselineStatuses(targets map[baselineKey]baselineTarget) {
	m.baselineMu.Lock()
	defer m.baselineMu.Unlock()
	if m.baselineStatuses == nil {
		m.baselineStatuses = make(map[baselineKey]baselineStatus, len(targets))
	}
	for key, target := range targets {
		probes := append([]string(nil), target.probes...)
		sort.Strings(probes)
		m.baselineStatuses[key] = baselineStatus{probes: probes, provider: key.parserType}
	}
}

func (m *Manager) recordBaselineRefresh(key baselineKey, refreshError error) {
	now := time.Now()
	m.baselineMu.Lock()
	defer m.baselineMu.Unlock()
	status := m.baselineStatuses[key]
	status.lastAttempt = now
	status.lastRefreshSuccess = refreshError == nil
	m.baselineStatuses[key] = status
}

func (m *Manager) GetProbeBaselineSnapshots(ctx context.Context) ([]extension.ProbeBaselineSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.baselineMu.RLock()
	defer m.baselineMu.RUnlock()
	var snapshots []extension.ProbeBaselineSnapshot
	for _, status := range m.baselineStatuses {
		for _, probe := range status.probes {
			snapshots = append(snapshots, extension.ProbeBaselineSnapshot{
				ProbeTag: probe, Provider: status.provider,
				LastRefreshSuccess: status.lastRefreshSuccess, LastAttempt: status.lastAttempt,
			})
		}
	}
	sort.Slice(snapshots, func(i, j int) bool {
		if snapshots[i].ProbeTag != snapshots[j].ProbeTag {
			return snapshots[i].ProbeTag < snapshots[j].ProbeTag
		}
		return snapshots[i].Provider < snapshots[j].Provider
	})
	return snapshots, nil
}

func (m *Manager) nextBaselineRefreshDelay() time.Duration {
	interval := m.baselineRefreshInterval
	if interval <= 0 {
		interval = directBaselineRefreshInterval
	}
	jitter := interval / directBaselineRefreshJitterDivisor
	if jitter <= 0 {
		return interval
	}
	return interval - jitter + time.Duration(rand.Int64N(int64(2*jitter)+1))
}

func (m *Manager) Close() error {
	m.lifecycleMu.Lock()
	if m.closed {
		m.lifecycleMu.Unlock()
		return nil
	}
	m.closed = true
	if m.cancel != nil {
		m.cancel()
	}
	m.lifecycleMu.Unlock()
	m.wg.Wait()
	var closeErrors []error
	for _, tag := range m.tags {
		if legacy := m.legacy[tag]; legacy != nil {
			closeErrors = append(closeErrors, legacy.Close())
		}
	}
	return errors.Combine(closeErrors...)
}

func (m *Manager) runObserver(observer *healthObserver) {
	defer m.wg.Done()
	interval := time.Duration(observer.config.Interval)
	if interval <= 0 {
		interval = DefaultHealthProbeInterval
	}
	initialDelay := time.Duration(rand.Int64N(int64(interval)))
	timer := time.NewTimer(initialDelay)
	defer timer.Stop()
	select {
	case <-m.runCtx.Done():
		return
	case <-timer.C:
	}
	m.runCycle(observer)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.runCtx.Done():
			return
		case <-ticker.C:
			m.runCycle(observer)
		}
	}
}

func (m *Manager) runCycle(observer *healthObserver) {
	outbounds := m.selector.Select(observer.config.SubjectSelector)
	m.store.retain(observer.tag, outbounds)
	for _, outboundTag := range outbounds {
		if _, running := observer.running.LoadOrStore(outboundTag, struct{}{}); running {
			continue
		}
		m.wg.Add(1)
		go func(outboundTag string) {
			defer m.wg.Done()
			defer observer.running.Delete(outboundTag)
			select {
			case observer.sem <- struct{}{}:
				defer func() { <-observer.sem }()
			case <-m.runCtx.Done():
				return
			}
			// Attribute internally generated probe traffic to a virtual inbound so
			// route metrics distinguish it from other session-less traffic.
			probeCtx := session.ContextWithInbound(m.runCtx, &session.Inbound{
				Tag: probeInboundTagPrefix + observer.tag,
			})
			sample := m.checker.check(probeCtx, outboundTag, observer.config, observer.ipParser)
			m.store.record(observer.tag, outboundTag, observer.config.Method, time.Duration(observer.config.Interval), observer.policy, sample)
		}(outboundTag)
	}
}

func (m *Manager) GetProbeSnapshots(ctx context.Context, filter extension.ProbeSnapshotFilter) ([]extension.ProbeSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if filter.ProbeTag != "" {
		if _, found := m.views[filter.ProbeTag]; !found {
			return nil, errors.New("unable to find observatory with tag: ", filter.ProbeTag)
		}
	}
	result := m.store.snapshots(time.Now(), filter)
	known := make(map[probeKey]struct{}, len(result))
	for _, snapshot := range result {
		known[probeKey{probe: snapshot.ProbeTag, outbound: snapshot.OutboundTag}] = struct{}{}
	}
	if m.selector != nil {
		for _, tag := range m.tags {
			if filter.ProbeTag != "" && filter.ProbeTag != tag {
				continue
			}
			health := m.health[tag]
			if health == nil {
				continue
			}
			for _, outboundTag := range m.selector.Select(health.config.SubjectSelector) {
				if filter.OutboundTag != "" && filter.OutboundTag != outboundTag {
					continue
				}
				key := probeKey{probe: tag, outbound: outboundTag}
				if _, found := known[key]; found {
					continue
				}
				result = append(result, extension.ProbeSnapshot{
					ProbeTag: tag, OutboundTag: outboundTag, Method: health.config.Method,
					PolicyState: extension.ProbeStateUnknown, EffectiveState: extension.ProbeStateUnknown,
					PolicyReason: policyReasonNotProbed, Interval: time.Duration(health.config.Interval),
				})
			}
		}
	}
	for _, tag := range m.tags {
		if filter.ProbeTag != "" && filter.ProbeTag != tag {
			continue
		}
		legacy := m.legacy[tag]
		if legacy == nil {
			continue
		}
		message, err := legacy.GetObservation(ctx)
		if err != nil {
			return nil, err
		}
		observation, ok := message.(*observatory.ObservationResult)
		if !ok {
			return nil, errors.New("observer returned an unsupported result: ", tag)
		}
		result = append(result, legacySnapshots(tag, observation, filter.OutboundTag)...)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ProbeTag != result[j].ProbeTag {
			return result[i].ProbeTag < result[j].ProbeTag
		}
		return result[i].OutboundTag < result[j].OutboundTag
	})
	return result, nil
}

func legacySnapshots(tag string, result *observatory.ObservationResult, outboundFilter string) []extension.ProbeSnapshot {
	snapshots := make([]extension.ProbeSnapshot, 0, len(result.Status))
	for _, status := range result.Status {
		if status == nil || (outboundFilter != "" && status.OutboundTag != outboundFilter) {
			continue
		}
		state := extension.ProbeStateUnhealthy
		if status.Alive {
			state = extension.ProbeStateHealthy
		}
		snapshot := extension.ProbeSnapshot{
			ProbeTag: tag, OutboundTag: status.OutboundTag, Method: ProbeMethodHTTP,
			PolicyState: state, EffectiveState: state,
			Latest: extension.ProbeSample{
				Success: status.Alive, CheckedAt: time.Unix(status.LastTryTime, 0),
				Duration: time.Duration(status.Delay) * time.Millisecond,
				TTFB:     time.Duration(status.Delay) * time.Millisecond,
			},
			LastSuccess: time.Unix(status.LastSeenTime, 0),
		}
		if !status.Alive {
			snapshot.Latest.Error = extension.ProbeError{Stage: probeStageResponse, Reason: probeReasonFailed}
		}
		if status.HealthPing != nil {
			snapshot.WindowSamples = uint64(status.HealthPing.All)
			snapshot.WindowFailures = uint64(status.HealthPing.Fail)
			snapshot.WindowLatencyDeviation = time.Duration(status.HealthPing.Deviation)
			snapshot.WindowLatencyAverage = time.Duration(status.HealthPing.Average)
			snapshot.WindowLatencyMaximum = time.Duration(status.HealthPing.Max)
			snapshot.WindowLatencyMinimum = time.Duration(status.HealthPing.Min)
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots
}

func snapshotsToObservation(snapshots []extension.ProbeSnapshot) *observatory.ObservationResult {
	result := &observatory.ObservationResult{}
	for _, snapshot := range snapshots {
		delay := snapshot.Latest.TTFB
		if delay <= 0 {
			delay = snapshot.Latest.Duration
		}
		status := &observatory.OutboundStatus{
			Alive: snapshot.EffectiveState == extension.ProbeStateHealthy,
			Delay: delay.Milliseconds(), OutboundTag: snapshot.OutboundTag,
			LastTryTime: snapshot.Latest.CheckedAt.Unix(), LastSeenTime: snapshot.LastSuccess.Unix(),
		}
		if snapshot.Latest.CheckedAt.IsZero() {
			status.LastTryTime = 0
		}
		if snapshot.LastSuccess.IsZero() {
			status.LastSeenTime = 0
		}
		if !snapshot.Latest.Success {
			status.LastErrorReason = snapshot.Latest.Error.Stage + "/" + snapshot.Latest.Error.Reason
		}
		if snapshot.WindowSamples > 0 {
			status.HealthPing = &observatory.HealthPingMeasurementResult{
				All: int64(snapshot.WindowSamples), Fail: int64(snapshot.WindowFailures),
				Deviation: int64(snapshot.WindowLatencyDeviation), Average: int64(snapshot.WindowLatencyAverage),
				Max: int64(snapshot.WindowLatencyMaximum), Min: int64(snapshot.WindowLatencyMinimum),
			}
		}
		result.Status = append(result.Status, status)
	}
	return result
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		return New(ctx, config.(*Config))
	}))
}

var (
	_ extension.Observatory                   = (*Manager)(nil)
	_ extension.ProbeSnapshotProvider         = (*Manager)(nil)
	_ extension.ProbeBaselineSnapshotProvider = (*Manager)(nil)
	_ features.TaggedFeatures                 = (*Manager)(nil)
)
