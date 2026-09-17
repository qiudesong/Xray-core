package session

import (
	stdnet "net"
	"testing"
	"time"
)

func TestRecordPeer(t *testing.T) {
	subscription := SubscribePeerEvents(2)
	t.Cleanup(subscription.Close)

	RecordInboundPeer("ignored", &stdnet.TCPAddr{IP: stdnet.ParseIP("203.0.113.9"), Port: 80})
	RecordOutboundPeer("proxy", &stdnet.UDPAddr{IP: stdnet.ParseIP("203.0.113.10"), Port: 443})

	for _, expected := range []struct {
		side    PeerSide
		tag     string
		network string
		address string
	}{
		{side: PeerSideInbound, tag: "ignored", network: "tcp", address: "203.0.113.9"},
		{side: PeerSideOutbound, tag: "proxy", network: "udp", address: "203.0.113.10"},
	} {
		select {
		case event := <-subscription.Events():
			if event.Side != expected.side || event.Tag != expected.tag || event.Network != expected.network || event.Address.Value != expected.address {
				t.Fatalf("unexpected peer event: %+v", event)
			}
		case <-time.After(time.Second):
			t.Fatal("peer event was not published")
		}
	}
}

func TestRecordPeerIgnoresUnsupportedAddress(t *testing.T) {
	subscription := SubscribePeerEvents(1)
	t.Cleanup(subscription.Close)

	RecordInboundPeer("unix", &stdnet.UnixAddr{Name: "/tmp/xray.sock", Net: "unix"})
	select {
	case event := <-subscription.Events():
		t.Fatalf("unsupported address produced event: %+v", event)
	default:
	}
}
