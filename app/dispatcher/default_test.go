package dispatcher

import (
	"testing"

	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
)

func TestNewAccessAddress(t *testing.T) {
	tests := []struct {
		name        string
		destination net.Destination
		want        log.AccessAddress
	}{
		{
			name:        "IP",
			destination: net.TCPDestination(net.ParseAddress("203.0.113.1"), 443),
			want:        log.AccessAddress{Value: "203.0.113.1", Type: log.AccessAddressTypeIP},
		},
		{
			name:        "domain",
			destination: net.TCPDestination(net.DomainAddress("example.com"), 443),
			want:        log.AccessAddress{Value: "example.com", Type: log.AccessAddressTypeDomain},
		},
		{
			name:        "Unix",
			destination: net.UnixDestination(net.DomainAddress("/tmp/xray.sock")),
		},
		{
			name:        "empty address",
			destination: net.Destination{Network: net.Network_TCP},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := newAccessAddress(test.destination); got != test.want {
				t.Fatalf("unexpected access address: got %+v, want %+v", got, test.want)
			}
		})
	}
}
