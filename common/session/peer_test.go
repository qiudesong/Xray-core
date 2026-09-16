package session

import (
	stdnet "net"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/log"
)

func TestRecordPeer(t *testing.T) {
	subscription := log.SubscribePeerEvents(log.PeerSideOutbound, 1)
	t.Cleanup(subscription.Close)

	RecordInboundPeer("ignored", &stdnet.TCPAddr{IP: stdnet.ParseIP("203.0.113.9"), Port: 80})
	RecordOutboundPeer("proxy", &stdnet.UDPAddr{IP: stdnet.ParseIP("203.0.113.10"), Port: 443})

	select {
	case event := <-subscription.Events():
		if event.Tag != "proxy" || event.Network != "udp" || event.Address.Value != "203.0.113.10" {
			t.Fatalf("unexpected peer event: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("peer event was not published")
	}
}

func TestRecordPeerIgnoresUnsupportedAddress(t *testing.T) {
	subscription := log.SubscribePeerEvents(log.PeerSideInbound, 1)
	t.Cleanup(subscription.Close)

	RecordInboundPeer("unix", &stdnet.UnixAddr{Name: "/tmp/xray.sock", Net: "unix"})
	select {
	case event := <-subscription.Events():
		t.Fatalf("unsupported address produced event: %+v", event)
	default:
	}
}
