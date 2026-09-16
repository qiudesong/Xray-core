package inbound

import (
	"context"
	stdnet "net"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport/internet/stat"
)

type peerTestInbound struct{}

func (*peerTestInbound) Network() []net.Network {
	return []net.Network{net.Network_TCP}
}

func (*peerTestInbound) Process(context.Context, net.Network, stat.Connection, routing.Dispatcher) error {
	return nil
}

type peerTestConnection struct {
	stdnet.Conn
	local  stdnet.Addr
	remote stdnet.Addr
}

func (c *peerTestConnection) LocalAddr() stdnet.Addr  { return c.local }
func (c *peerTestConnection) RemoteAddr() stdnet.Addr { return c.remote }

func TestTCPWorkerPublishesAcceptedPeer(t *testing.T) {
	subscription := log.SubscribePeerEvents(log.PeerSideInbound, 1)
	t.Cleanup(subscription.Close)

	left, right := stdnet.Pipe()
	t.Cleanup(func() { _ = right.Close() })
	connection := &peerTestConnection{
		Conn:   left,
		local:  &stdnet.TCPAddr{IP: stdnet.ParseIP("127.0.0.1"), Port: 1080},
		remote: &stdnet.TCPAddr{IP: stdnet.ParseIP("203.0.113.20"), Port: 12345},
	}
	worker := &tcpWorker{
		address: net.LocalHostIP,
		port:    1080,
		proxy:   new(peerTestInbound),
		tag:     "socks-in",
		ctx:     context.Background(),
	}
	worker.callback(connection)

	select {
	case event := <-subscription.Events():
		if event.Tag != "socks-in" || event.Network != "tcp" || event.Address.Value != "203.0.113.20" {
			t.Fatalf("unexpected TCP peer event: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("accepted TCP connection did not publish peer event")
	}
}

func TestUDPWorkerPublishesPeerOncePerLogicalSession(t *testing.T) {
	subscription := log.SubscribePeerEvents(log.PeerSideInbound, 2)
	t.Cleanup(subscription.Close)

	worker := &udpWorker{
		address:    net.LocalHostIP,
		port:       1080,
		tag:        "dns-in",
		activeConn: make(map[connID]*udpConn),
	}
	id := connID{src: net.UDPDestination(net.ParseAddress("203.0.113.21"), 12345)}
	connection, existing := worker.getConnection(id)
	if existing {
		t.Fatal("first UDP session was reported as existing")
	}
	_, existing = worker.getConnection(id)
	if !existing {
		t.Fatal("second UDP packet did not reuse logical session")
	}
	_ = connection.Close()

	select {
	case event := <-subscription.Events():
		if event.Tag != "dns-in" || event.Network != "udp" || event.Address.Value != "203.0.113.21" {
			t.Fatalf("unexpected UDP peer event: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("new UDP logical session did not publish peer event")
	}
	select {
	case event := <-subscription.Events():
		t.Fatalf("reused UDP logical session published another event: %+v", event)
	default:
	}
}
