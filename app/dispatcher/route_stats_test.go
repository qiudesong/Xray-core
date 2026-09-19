package dispatcher

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	apppolicy "github.com/xtls/xray-core/app/policy"
	appstats "github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/transport"
)

type routeStatsReader struct {
	payload     []byte
	interrupted bool
	closed      bool
}

type routeStatsHandler struct {
	tag      string
	response []byte
}

func (h *routeStatsHandler) Start() error { return nil }

func (h *routeStatsHandler) Close() error { return nil }

func (h *routeStatsHandler) Tag() string { return h.tag }

func (h *routeStatsHandler) Dispatch(_ context.Context, link *transport.Link) {
	mb, _ := link.Reader.ReadMultiBuffer()
	buf.ReleaseMulti(mb)
	if len(h.response) > 0 {
		_ = link.Writer.WriteMultiBuffer(buf.MergeBytes(nil, h.response))
	}
}

func (h *routeStatsHandler) SenderSettings() *serial.TypedMessage { return nil }

func (h *routeStatsHandler) ProxySettings() *serial.TypedMessage { return nil }

type routeStatsOutboundManager struct {
	defaultHandler outbound.Handler
	handlers       map[string]outbound.Handler
}

func (*routeStatsOutboundManager) Type() interface{} { return outbound.ManagerType() }

func (*routeStatsOutboundManager) Start() error { return nil }

func (*routeStatsOutboundManager) Close() error { return nil }

func (m *routeStatsOutboundManager) GetHandler(tag string) outbound.Handler { return m.handlers[tag] }

func (m *routeStatsOutboundManager) GetDefaultHandler() outbound.Handler { return m.defaultHandler }

func (m *routeStatsOutboundManager) AddHandler(_ context.Context, handler outbound.Handler) error {
	if m.handlers == nil {
		m.handlers = make(map[string]outbound.Handler)
	}
	m.handlers[handler.Tag()] = handler
	return nil
}

func (m *routeStatsOutboundManager) RemoveHandler(_ context.Context, tag string) error {
	delete(m.handlers, tag)
	return nil
}

func (m *routeStatsOutboundManager) ListHandlers(context.Context) []outbound.Handler {
	handlers := make([]outbound.Handler, 0, len(m.handlers))
	for _, handler := range m.handlers {
		handlers = append(handlers, handler)
	}
	return handlers
}

func (r *routeStatsReader) next() (buf.MultiBuffer, error) {
	if len(r.payload) == 0 {
		return nil, io.EOF
	}
	payload := r.payload
	r.payload = nil
	return buf.MergeBytes(nil, payload), nil
}

func (r *routeStatsReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	return r.next()
}

func (r *routeStatsReader) ReadMultiBufferTimeout(time.Duration) (buf.MultiBuffer, error) {
	return r.next()
}

func (r *routeStatsReader) Interrupt() {
	r.interrupted = true
}

func (r *routeStatsReader) Close() error {
	r.closed = true
	return nil
}

func newRouteStatsTestDispatcher(t *testing.T, uplink, downlink bool) (*DefaultDispatcher, stats.Manager) {
	t.Helper()
	policyManager, err := apppolicy.New(context.Background(), &apppolicy.Config{
		System: &apppolicy.SystemPolicy{
			Stats: &apppolicy.SystemPolicy_Stats{
				OutboundUplink:   uplink,
				OutboundDownlink: downlink,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	statsManager, err := appstats.NewManager(context.Background(), &appstats.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return &DefaultDispatcher{
		policy:     policyManager,
		stats:      statsManager,
		routeStats: newRouteStatsManager(policyManager, statsManager),
	}, statsManager
}

func TestRouteStatsCountsBothDirectionsAndNormalizesLabels(t *testing.T) {
	dispatcher, statsManager := newRouteStatsTestDispatcher(t, true, true)
	reader := &routeStatsReader{payload: []byte("uplink")}
	link := &transport.Link{Reader: reader, Writer: buf.Discard}
	dispatcher.routeStats.wrapLink(link, "", "", "tcp")

	if _, ok := link.Reader.(buf.TimeoutReader); !ok {
		t.Fatal("route stats wrapper did not preserve TimeoutReader")
	}
	mb, err := link.Reader.ReadMultiBuffer()
	if err != nil {
		t.Fatal(err)
	}
	buf.ReleaseMulti(mb)
	if err := link.Writer.WriteMultiBuffer(buf.MergeBytes(nil, []byte("downlink"))); err != nil {
		t.Fatal(err)
	}

	uplink := statsManager.GetCounter("route>>>unknown>>>unknown>>>tcp>>>traffic>>>uplink")
	downlink := statsManager.GetCounter("route>>>unknown>>>unknown>>>tcp>>>traffic>>>downlink")
	if uplink == nil || uplink.Value() != int64(len("uplink")) {
		t.Fatalf("unexpected route uplink counter: %v", uplink)
	}
	if downlink == nil || downlink.Value() != int64(len("downlink")) {
		t.Fatalf("unexpected route downlink counter: %v", downlink)
	}

	common.Interrupt(link.Reader)
	if !reader.interrupted {
		t.Fatal("route stats reader did not propagate interrupt")
	}
	if err := common.Close(link.Reader); err != nil {
		t.Fatal(err)
	}
	if !reader.closed {
		t.Fatal("route stats reader did not propagate close")
	}
}

func TestRouteStatsTimeoutReadIsCountedOnce(t *testing.T) {
	dispatcher, statsManager := newRouteStatsTestDispatcher(t, true, false)
	link := &transport.Link{
		Reader: &routeStatsReader{payload: []byte("timeout read")},
		Writer: buf.Discard,
	}
	dispatcher.routeStats.wrapLink(link, "in", "out", "udp")

	timeoutReader := link.Reader.(buf.TimeoutReader)
	mb, err := timeoutReader.ReadMultiBufferTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	buf.ReleaseMulti(mb)
	counter := statsManager.GetCounter("route>>>in>>>out>>>udp>>>traffic>>>uplink")
	if counter == nil || counter.Value() != int64(len("timeout read")) {
		t.Fatalf("unexpected timeout read counter: %v", counter)
	}
	if counter := statsManager.GetCounter("route>>>in>>>out>>>udp>>>traffic>>>downlink"); counter != nil {
		t.Fatal("disabled downlink route counter was registered")
	}
}

func TestRouteStatsCounterCacheConcurrentInitialization(t *testing.T) {
	dispatcher, statsManager := newRouteStatsTestDispatcher(t, true, true)
	key := routeStatsKey{inbound: "in", outbound: "out", network: "unix"}

	var wait sync.WaitGroup
	for range 64 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			counters := dispatcher.routeStats.countersFor(key)
			if counters.uplink == nil || counters.downlink == nil {
				t.Error("route counters were not initialized")
			}
		}()
	}
	wait.Wait()

	count := 0
	statsManager.VisitCounters(func(name string, _ stats.Counter) bool {
		if name == "route>>>in>>>out>>>unix>>>traffic>>>uplink" || name == "route>>>in>>>out>>>unix>>>traffic>>>downlink" {
			count++
		}
		return true
	})
	if count != 2 {
		t.Fatalf("unexpected registered route counter count: got %d, want 2", count)
	}
}

func TestRoutedDispatchTracksSelectedDefaultHandler(t *testing.T) {
	dispatcher, statsManager := newRouteStatsTestDispatcher(t, true, true)
	dispatcher.ohm = &routeStatsOutboundManager{
		defaultHandler: &routeStatsHandler{tag: "direct", response: []byte("response")},
	}
	ctx := session.ContextWithInbound(context.Background(), &session.Inbound{Tag: "socks-in"})
	ctx = session.ContextWithContent(ctx, new(session.Content))
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{}})
	link := &transport.Link{
		Reader: &routeStatsReader{payload: []byte("request")},
		Writer: buf.Discard,
	}

	dispatcher.routedDispatch(ctx, link, net.TCPDestination(net.DomainAddress("example.com"), 443))

	uplink := statsManager.GetCounter("route>>>socks-in>>>direct>>>tcp>>>traffic>>>uplink")
	downlink := statsManager.GetCounter("route>>>socks-in>>>direct>>>tcp>>>traffic>>>downlink")
	if uplink == nil || uplink.Value() != int64(len("request")) {
		t.Fatalf("unexpected routed uplink counter: %v", uplink)
	}
	if downlink == nil || downlink.Value() != int64(len("response")) {
		t.Fatalf("unexpected routed downlink counter: %v", downlink)
	}
}

func TestRoutedDispatchWithoutHandlerDoesNotCreateRouteCounters(t *testing.T) {
	dispatcher, statsManager := newRouteStatsTestDispatcher(t, true, true)
	dispatcher.ohm = new(routeStatsOutboundManager)
	ctx := session.ContextWithInbound(context.Background(), &session.Inbound{Tag: "socks-in"})
	ctx = session.ContextWithContent(ctx, new(session.Content))
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{}})
	link := &transport.Link{
		Reader: &routeStatsReader{payload: []byte("request")},
		Writer: buf.Discard,
	}

	dispatcher.routedDispatch(ctx, link, net.UDPDestination(net.DomainAddress("example.com"), 53))

	statsManager.VisitCounters(func(name string, _ stats.Counter) bool {
		if len(name) >= len("route>>>") && name[:len("route>>>")] == "route>>>" {
			t.Errorf("unexpected route counter without handler: %s", name)
		}
		return true
	})
}
