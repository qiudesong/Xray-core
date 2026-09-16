package outbound_test

import (
	"context"
	"fmt"
	"io"
	stdnet "net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/policy"
	"github.com/xtls/xray-core/app/proxyman"
	. "github.com/xtls/xray-core/app/proxyman/outbound"
	"github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	core "github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/proxy/freedom"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
)

func TestInterfaces(t *testing.T) {
	_ = outbound.Handler(new(Handler))
	_ = outbound.Manager(new(Manager))
}

const xrayKey core.XrayKey = 1

func TestOutboundWithoutStatCounter(t *testing.T) {
	config := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&stats.Config{}),
			serial.ToTypedMessage(&policy.Config{
				System: &policy.SystemPolicy{
					Stats: &policy.SystemPolicy_Stats{
						InboundUplink: true,
					},
				},
			}),
		},
	}

	v, _ := core.New(config)
	v.AddFeature(outbound.Manager(new(Manager)))
	ctx := context.WithValue(context.Background(), xrayKey, v)
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{}})
	h, _ := NewHandler(ctx, &core.OutboundHandlerConfig{
		Tag:           "tag",
		ProxySettings: serial.ToTypedMessage(&freedom.Config{FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}}}),
	})
	conn, _ := h.(*Handler).Dial(ctx, net.TCPDestination(net.DomainAddress("localhost"), 13146))
	_, ok := conn.(*stat.CounterConnection)
	if ok {
		t.Errorf("Expected conn to not be CounterConnection")
	}
}

func TestOutboundWithStatCounter(t *testing.T) {
	config := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&stats.Config{}),
			serial.ToTypedMessage(&policy.Config{
				System: &policy.SystemPolicy{
					Stats: &policy.SystemPolicy_Stats{
						OutboundUplink:   true,
						OutboundDownlink: true,
					},
				},
			}),
		},
	}

	v, _ := core.New(config)
	v.AddFeature(outbound.Manager(new(Manager)))
	ctx := context.WithValue(context.Background(), xrayKey, v)
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{}})
	h, _ := NewHandler(ctx, &core.OutboundHandlerConfig{
		Tag:           "tag",
		ProxySettings: serial.ToTypedMessage(&freedom.Config{FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}}}),
	})
	conn, _ := h.(*Handler).Dial(ctx, net.TCPDestination(net.DomainAddress("localhost"), 13146))
	_, ok := conn.(*stat.CounterConnection)
	if !ok {
		t.Errorf("Expected conn to be CounterConnection")
	}
}

type peerDialConnection struct {
	remote stdnet.Addr
}

func (*peerDialConnection) Read([]byte) (int, error)           { return 0, io.EOF }
func (c *peerDialConnection) Write(buffer []byte) (int, error) { return len(buffer), nil }
func (*peerDialConnection) Close() error                       { return nil }
func (*peerDialConnection) LocalAddr() stdnet.Addr             { return &stdnet.TCPAddr{} }
func (c *peerDialConnection) RemoteAddr() stdnet.Addr          { return c.remote }
func (*peerDialConnection) SetDeadline(time.Time) error        { return nil }
func (*peerDialConnection) SetReadDeadline(time.Time) error    { return nil }
func (*peerDialConnection) SetWriteDeadline(time.Time) error   { return nil }

func TestDialPublishesPeerOnlyAfterSuccess(t *testing.T) {
	protocol := fmt.Sprintf("peer-test-%p", t)
	if err := internet.RegisterProtocolConfigCreator(protocol, func() interface{} { return struct{}{} }); err != nil {
		t.Fatal(err)
	}
	if err := internet.RegisterTransportDialer(protocol, func(_ context.Context, dest net.Destination, _ *internet.MemoryStreamConfig) (stat.Connection, error) {
		if dest.Port == 2 {
			return nil, fmt.Errorf("dial failed")
		}
		return &peerDialConnection{remote: &stdnet.TCPAddr{IP: stdnet.ParseIP("203.0.113.30"), Port: int(dest.Port)}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	config := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&stats.Config{}),
			serial.ToTypedMessage(&policy.Config{}),
		},
	}
	v, err := core.New(config)
	if err != nil {
		t.Fatal(err)
	}
	v.AddFeature(outbound.Manager(new(Manager)))
	ctx := context.WithValue(context.Background(), xrayKey, v)
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{}})
	handler, err := NewHandler(ctx, &core.OutboundHandlerConfig{
		Tag: "direct",
		SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
			StreamSettings: &internet.StreamConfig{ProtocolName: protocol},
		}),
		ProxySettings: serial.ToTypedMessage(&freedom.Config{FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}}}),
	})
	if err != nil {
		t.Fatal(err)
	}
	h := handler.(*Handler)

	subscription := log.SubscribePeerEvents(log.PeerSideOutbound, 2)
	t.Cleanup(subscription.Close)
	conn, err := h.Dial(ctx, net.TCPDestination(net.ParseAddress("198.51.100.1"), 1))
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	select {
	case event := <-subscription.Events():
		if event.Tag != "direct" || event.Network != "tcp" || event.Address.Value != "203.0.113.30" {
			t.Fatalf("unexpected outbound peer event: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("successful dial did not publish peer event")
	}

	failedConn, err := h.Dial(ctx, net.TCPDestination(net.ParseAddress("198.51.100.2"), 2))
	if err == nil {
		_ = failedConn.Close()
		t.Fatal("configured failed dial unexpectedly succeeded")
	}
	select {
	case event := <-subscription.Events():
		t.Fatalf("failed dial published peer event: %+v", event)
	default:
	}
}

func TestTagsCache(t *testing.T) {
	test_duration := 10 * time.Second
	threads_num := 50
	delay := 10 * time.Millisecond
	tags_prefix := "node"

	tags := sync.Map{}
	counter := atomic.Uint64{}

	ohm, err := New(context.Background(), &proxyman.OutboundConfig{})
	if err != nil {
		t.Error("failed to create outbound handler manager")
	}
	config := &core.Config{
		App: []*serial.TypedMessage{},
	}
	v, _ := core.New(config)
	v.AddFeature(ohm)
	ctx := context.WithValue(context.Background(), xrayKey, v)

	stop_add_rm := false
	wg_add_rm := sync.WaitGroup{}
	addHandlers := func() {
		defer wg_add_rm.Done()
		for !stop_add_rm {
			time.Sleep(delay)
			idx := counter.Add(1)
			tag := fmt.Sprintf("%s%d", tags_prefix, idx)
			cfg := &core.OutboundHandlerConfig{
				Tag:           tag,
				ProxySettings: serial.ToTypedMessage(&freedom.Config{FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}}}),
			}
			if h, err := NewHandler(ctx, cfg); err == nil {
				if err := ohm.AddHandler(ctx, h); err == nil {
					// t.Log("add handler:", tag)
					tags.Store(tag, nil)
				} else {
					t.Error("failed to add handler:", tag)
				}
			} else {
				t.Error("failed to create handler:", tag)
			}
		}
	}

	rmHandlers := func() {
		defer wg_add_rm.Done()
		for !stop_add_rm {
			time.Sleep(delay)
			tags.Range(func(key interface{}, value interface{}) bool {
				if _, ok := tags.LoadAndDelete(key); ok {
					// t.Log("remove handler:", key)
					ohm.RemoveHandler(ctx, key.(string))
					return false
				}
				return true
			})
		}
	}

	selectors := []string{tags_prefix}
	wg_get := sync.WaitGroup{}
	stop_get := false
	getTags := func() {
		defer wg_get.Done()
		for !stop_get {
			time.Sleep(delay)
			_ = ohm.Select(selectors)
			// t.Logf("get tags: %v", tag)
		}
	}

	for i := 0; i < threads_num; i++ {
		wg_add_rm.Add(2)
		go rmHandlers()
		go addHandlers()
		wg_get.Add(1)
		go getTags()
	}

	time.Sleep(test_duration)
	stop_add_rm = true
	wg_add_rm.Wait()
	stop_get = true
	wg_get.Wait()
}
