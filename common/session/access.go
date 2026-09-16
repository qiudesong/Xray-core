package session

import (
	"context"

	"github.com/xtls/xray-core/common/log"
)

// RecordAccess enriches an access message with the available inbound session
// metadata before publishing it to the access log and structured observers.
func RecordAccess(ctx context.Context, message *log.AccessMessage) {
	if inbound := InboundFromContext(ctx); inbound != nil {
		if message.InboundTag == "" {
			message.InboundTag = inbound.Tag
		}
		if message.Network == "" {
			switch {
			case inbound.Source.IsValid():
				message.Network = inbound.Source.Network.SystemString()
			case inbound.Gateway.IsValid():
				message.Network = inbound.Gateway.Network.SystemString()
			}
		}
	}

	log.Record(message)
}
