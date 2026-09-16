package session

import (
	stdnet "net"

	"github.com/xtls/xray-core/common/log"
)

// RecordInboundPeer publishes a successfully established inbound peer.
func RecordInboundPeer(tag string, peer stdnet.Addr) {
	recordPeer(log.PeerSideInbound, tag, peer)
}

// RecordOutboundPeer publishes a successfully established outbound peer.
func RecordOutboundPeer(tag string, peer stdnet.Addr) {
	recordPeer(log.PeerSideOutbound, tag, peer)
}

func recordPeer(side log.PeerSide, tag string, peer stdnet.Addr) {
	var ip stdnet.IP
	var network string
	switch address := peer.(type) {
	case *stdnet.TCPAddr:
		if address != nil {
			ip = address.IP
			network = "tcp"
		}
	case *stdnet.UDPAddr:
		if address != nil {
			ip = address.IP
			network = "udp"
		}
	default:
		return
	}
	if ip == nil {
		return
	}
	log.PublishPeerEvent(log.PeerEvent{
		Side:    side,
		Tag:     tag,
		Network: network,
		Address: log.AccessAddress{Value: ip.String(), Type: log.AccessAddressTypeIP},
	})
}
