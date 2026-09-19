package dispatcher

import (
	"sync"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/features/policy"
	feature_stats "github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/transport"
)

type routeStatsKey struct {
	inbound  string
	outbound string
	network  string
}

type routeStatsCounters struct {
	uplink   feature_stats.Counter
	downlink feature_stats.Counter
}

// routeStatsManager owns the route-specific counter cache and link wrapping.
// Keeping it separate leaves DefaultDispatcher responsible only for selecting
// the point at which a route becomes known.
type routeStatsManager struct {
	stats           feature_stats.Manager
	uplinkEnabled   bool
	downlinkEnabled bool
	counters        sync.Map
}

func newRouteStatsManager(policyManager policy.Manager, statsManager feature_stats.Manager) *routeStatsManager {
	statsPolicy := policyManager.ForSystem().Stats
	return &routeStatsManager{
		stats:           statsManager,
		uplinkEnabled:   statsPolicy.OutboundUplink,
		downlinkEnabled: statsPolicy.OutboundDownlink,
	}
}

func (m *routeStatsManager) wrapLink(link *transport.Link, inbound, outbound, network string) {
	if m == nil || (!m.uplinkEnabled && !m.downlinkEnabled) {
		return
	}
	key := routeStatsKey{
		inbound:  feature_stats.NormalizeTrafficLabel(inbound),
		outbound: feature_stats.NormalizeTrafficLabel(outbound),
		network:  feature_stats.NormalizeTrafficLabel(network),
	}
	counters := m.countersFor(key)
	if counters.uplink != nil {
		link.Reader = newRouteStatsReader(link.Reader, counters.uplink)
	}
	if counters.downlink != nil {
		link.Writer = &SizeStatWriter{Counter: counters.downlink, Writer: link.Writer}
	}
}

func (m *routeStatsManager) countersFor(key routeStatsKey) routeStatsCounters {
	if cached, found := m.counters.Load(key); found {
		return cached.(routeStatsCounters)
	}

	var counters routeStatsCounters
	if m.uplinkEnabled {
		name := feature_stats.RouteTrafficCounterName(
			key.inbound, key.outbound, key.network, feature_stats.TrafficDirectionUplink,
		)
		counters.uplink, _ = m.stats.GetOrRegisterCounter(name)
	}
	if m.downlinkEnabled {
		name := feature_stats.RouteTrafficCounterName(
			key.inbound, key.outbound, key.network, feature_stats.TrafficDirectionDownlink,
		)
		counters.downlink, _ = m.stats.GetOrRegisterCounter(name)
	}

	actual, _ := m.counters.LoadOrStore(key, counters)
	return actual.(routeStatsCounters)
}

type routeSizeStatReader struct {
	counter feature_stats.Counter
	reader  buf.Reader
}

func (r *routeSizeStatReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	mb, err := r.reader.ReadMultiBuffer()
	r.counter.Add(int64(mb.Len()))
	return mb, err
}

func (r *routeSizeStatReader) Close() error {
	return common.Close(r.reader)
}

func (r *routeSizeStatReader) Interrupt() {
	common.Interrupt(r.reader)
}

type timeoutRouteStatsReader struct {
	*routeSizeStatReader
	timeoutReader buf.TimeoutReader
}

func (r *timeoutRouteStatsReader) ReadMultiBufferTimeout(timeout time.Duration) (buf.MultiBuffer, error) {
	mb, err := r.timeoutReader.ReadMultiBufferTimeout(timeout)
	r.counter.Add(int64(mb.Len()))
	return mb, err
}

func newRouteStatsReader(reader buf.Reader, counter feature_stats.Counter) buf.Reader {
	statReader := &routeSizeStatReader{counter: counter, reader: reader}
	if timeoutReader, ok := reader.(buf.TimeoutReader); ok {
		return &timeoutRouteStatsReader{routeSizeStatReader: statReader, timeoutReader: timeoutReader}
	}
	return statReader
}
