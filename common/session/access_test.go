package session

import (
	"context"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
)

func TestRecordAccessEnrichesMessageFromInboundSession(t *testing.T) {
	subscription := log.SubscribeAccessEvents(1)
	t.Cleanup(subscription.Close)

	ctx := ContextWithInbound(context.Background(), &Inbound{
		Tag:    "socks-in",
		Source: net.UDPDestination(net.ParseAddress("203.0.113.10"), 12345),
	})
	RecordAccess(ctx, &log.AccessMessage{Status: log.AccessRejected})

	select {
	case event := <-subscription.Events():
		if got := event.Message.InboundTag; got != "socks-in" {
			t.Fatalf("inbound tag = %q, want socks-in", got)
		}
		if got := event.Message.Network; got != "udp" {
			t.Fatalf("network = %q, want udp", got)
		}
	case <-time.After(time.Second):
		t.Fatal("access event was not published")
	}
}

func TestRecordAccessPreservesExplicitMetadata(t *testing.T) {
	subscription := log.SubscribeAccessEvents(1)
	t.Cleanup(subscription.Close)

	ctx := ContextWithInbound(context.Background(), &Inbound{
		Tag:    "session-in",
		Source: net.UDPDestination(net.ParseAddress("203.0.113.10"), 12345),
	})
	RecordAccess(ctx, &log.AccessMessage{
		Status:     log.AccessRejected,
		InboundTag: "explicit-in",
		Network:    "tcp",
	})

	select {
	case event := <-subscription.Events():
		if got := event.Message.InboundTag; got != "explicit-in" {
			t.Fatalf("inbound tag = %q, want explicit-in", got)
		}
		if got := event.Message.Network; got != "tcp" {
			t.Fatalf("network = %q, want tcp", got)
		}
	case <-time.After(time.Second):
		t.Fatal("access event was not published")
	}
}
