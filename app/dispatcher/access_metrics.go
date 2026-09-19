package dispatcher

import (
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
)

func newAccessAddress(destination net.Destination) log.AccessAddress {
	if destination.Network == net.Network_UNIX || destination.Address == nil {
		return log.AccessAddress{}
	}

	switch family := destination.Address.Family(); {
	case family.IsIP():
		return log.AccessAddress{
			Value: destination.Address.IP().String(),
			Type:  log.AccessAddressTypeIP,
		}
	case family.IsDomain():
		return log.AccessAddress{
			Value: destination.Address.Domain(),
			Type:  log.AccessAddressTypeDomain,
		}
	default:
		return log.AccessAddress{}
	}
}
