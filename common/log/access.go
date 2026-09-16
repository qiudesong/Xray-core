package log

import (
	"context"
	"strings"

	"github.com/xtls/xray-core/common/serial"
)

type logKey int

const (
	accessMessageKey logKey = iota
)

type AccessStatus string

const (
	AccessAccepted = AccessStatus("accepted")
	AccessRejected = AccessStatus("rejected")
)

type AccessAddressType string

const (
	// AccessAddressTypeIP identifies an IP address.
	AccessAddressTypeIP AccessAddressType = "ip"
	// AccessAddressTypeDomain identifies a domain name.
	AccessAddressTypeDomain AccessAddressType = "domain"
)

// AccessAddress is structured address metadata attached to an access event.
type AccessAddress struct {
	Value string
	Type  AccessAddressType
}

type AccessMessage struct {
	From   interface{}
	To     interface{}
	Status AccessStatus
	Reason interface{}
	Email  string
	Detour string

	// Structured routing metadata for access event observers. These fields do
	// not change the access log's text representation.
	InboundTag  string
	OutboundTag string
	Network     string
	// Destination is the effective routing destination. It may differ from To,
	// for example when an HTTP inbound stores an origin-form URL in To.
	Destination AccessAddress
}

func (m *AccessMessage) String() string {
	builder := strings.Builder{}
	builder.WriteString("from")
	builder.WriteByte(' ')
	builder.WriteString(serial.ToString(m.From))
	builder.WriteByte(' ')
	builder.WriteString(string(m.Status))
	builder.WriteByte(' ')
	builder.WriteString(serial.ToString(m.To))

	if len(m.Detour) > 0 {
		builder.WriteString(" [")
		builder.WriteString(m.Detour)
		builder.WriteByte(']')
	}

	if reason := serial.ToString(m.Reason); len(reason) > 0 {
		builder.WriteString(" ")
		builder.WriteString(reason)
	}

	if len(m.Email) > 0 {
		builder.WriteString(" email: ")
		builder.WriteString(m.Email)
	}

	return builder.String()
}

func ContextWithAccessMessage(ctx context.Context, accessMessage *AccessMessage) context.Context {
	return context.WithValue(ctx, accessMessageKey, accessMessage)
}

func AccessMessageFromContext(ctx context.Context) *AccessMessage {
	if accessMessage, ok := ctx.Value(accessMessageKey).(*AccessMessage); ok {
		return accessMessage
	}
	return nil
}
