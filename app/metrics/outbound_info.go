package metrics

import (
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/xtls/xray-core/app/proxyman"
	xnet "github.com/xtls/xray-core/common/net"
	feature_outbound "github.com/xtls/xray-core/features/outbound"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	metadataKeySeparator = "\x00"
	decimalBase          = 10

	metricNameOutboundInfo               = "xray_outbound_info"
	metricNameOutboundEndpointInfo       = "xray_outbound_endpoint_info"
	metricNameOutboundConnectionAttempts = "xray_outbound_connection_attempts_total"

	fieldServerName = "server_name"
	fieldAddress    = "address"
	fieldPort       = "port"
	fieldVNext      = "vnext"
	fieldReceiver   = "receiver"
	fieldServer     = "server"
	fieldEndpoint   = "endpoint"
	fieldPeers      = "peers"

	wireGuardConfigMessage = "xray.proxy.wireguard.DeviceConfig"
	protobufConfigSuffix   = "Config"
	protobufClientConfig   = "ClientConfig"

	networkTCP         = "tcp"
	networkRaw         = "raw"
	networkSplitHTTP   = "splithttp"
	networkXHTTP       = "xhttp"
	networkMKCP        = "mkcp"
	networkKCP         = "kcp"
	networkGRPC        = "grpc"
	networkWebSocket   = "websocket"
	networkWebSocketWS = "ws"
	networkHTTPUpgrade = "httpupgrade"
	networkHysteria    = "hysteria"

	securityNone    = "none"
	securityTLS     = "tls"
	securityReality = "reality"
)

var knownOutboundProtocols = map[string]string{
	"xray.proxy.vless.outbound.Config":         "vless",
	"xray.proxy.vmess.outbound.Config":         "vmess",
	"xray.proxy.trojan.ClientConfig":           "trojan",
	"xray.proxy.shadowsocks.ClientConfig":      "shadowsocks",
	"xray.proxy.shadowsocks_2022.ClientConfig": "shadowsocks2022",
	"xray.proxy.socks.ClientConfig":            "socks",
	"xray.proxy.http.ClientConfig":             "http",
	"xray.proxy.hysteria.ClientConfig":         "hysteria",
	wireGuardConfigMessage:                     "wireguard",
	"xray.proxy.freedom.Config":                "freedom",
	"xray.proxy.blackhole.Config":              "blackhole",
}

type outboundEndpoint struct {
	server string
	port   string
}

type outboundMetadata struct {
	tag       string
	protocol  string
	network   string
	security  string
	sni       string
	endpoints []outboundEndpoint
}

type outboundCollector struct {
	handler                    *MetricsHandler
	outboundInfo               *prometheus.Desc
	outboundEndpointInfo       *prometheus.Desc
	outboundConnectionAttempts *prometheus.Desc
}

func newOutboundCollector(handler *MetricsHandler) *outboundCollector {
	return &outboundCollector{
		handler: handler,
		outboundInfo: prometheus.NewDesc(
			metricNameOutboundInfo, "Configured non-sensitive outbound information.",
			[]string{metricOutbound, metricProtocol, metricNetwork, metricSecurity}, nil,
		),
		outboundEndpointInfo: prometheus.NewDesc(
			metricNameOutboundEndpointInfo, "Configured outbound endpoint information.",
			[]string{metricOutbound, metricServer, metricPort, metricSNI}, nil,
		),
		outboundConnectionAttempts: prometheus.NewDesc(
			metricNameOutboundConnectionAttempts, "Outbound dispatch attempts since Xray started.",
			[]string{metricOutbound}, nil,
		),
	}
}

func (c *outboundCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.outboundInfo
	ch <- c.outboundEndpointInfo
	ch <- c.outboundConnectionAttempts
}

func (c *outboundCollector) Collect(ch chan<- prometheus.Metric) {
	if c.handler.ohm == nil {
		return
	}
	handlers := c.handler.ohm.ListHandlers(c.handler.ctx)
	sort.Slice(handlers, func(i, j int) bool { return handlers[i].Tag() < handlers[j].Tag() })
	for _, handler := range handlers {
		if counter, ok := handler.(feature_outbound.ConnectionAttemptCounter); ok {
			ch <- prometheus.MustNewConstMetric(
				c.outboundConnectionAttempts, prometheus.CounterValue, float64(counter.ConnectionAttempts()), handler.Tag(),
			)
		}
		// TODO: Cache metadata by handler instance to avoid decoding protobufs and
		// reflecting over endpoint settings on every scrape. Invalidate the cache
		// when handlers are added, removed, or replaced.
		metadata := c.inspectOutbound(handler)
		if metadata.protocol == "" {
			continue
		}
		ch <- prometheus.MustNewConstMetric(
			c.outboundInfo, prometheus.GaugeValue, 1,
			metadata.tag, metadata.protocol, metadata.network, metadata.security,
		)
		seen := make(map[string]struct{}, len(metadata.endpoints))
		for _, endpoint := range metadata.endpoints {
			key := endpoint.server + metadataKeySeparator + endpoint.port + metadataKeySeparator + metadata.sni
			if _, found := seen[key]; found {
				continue
			}
			seen[key] = struct{}{}
			ch <- prometheus.MustNewConstMetric(
				c.outboundEndpointInfo, prometheus.GaugeValue, 1,
				metadata.tag, endpoint.server, endpoint.port, metadata.sni,
			)
		}
	}
}

func (c *outboundCollector) inspectOutbound(handler feature_outbound.Handler) outboundMetadata {
	metadata := outboundMetadata{
		tag: handler.Tag(), network: networkTCP, security: securityNone,
	}
	if senderMessage := handler.SenderSettings(); senderMessage != nil {
		if instance, err := senderMessage.GetInstance(); err == nil {
			if sender, ok := instance.(*proxyman.SenderConfig); ok && sender.StreamSettings != nil {
				metadata.network = c.normalizeNetwork(sender.StreamSettings.ProtocolName)
				metadata.security = c.normalizeSecurity(sender.StreamSettings.SecurityType)
				for _, securityMessage := range sender.StreamSettings.SecuritySettings {
					security, err := securityMessage.GetInstance()
					if err != nil {
						continue
					}
					message := security.ProtoReflect()
					if field := c.fieldByName(message, fieldServerName); field != nil && field.Kind() == protoreflect.StringKind {
						metadata.sni = message.Get(field).String()
					}
					if metadata.sni != "" {
						break
					}
				}
			}
		}
	}
	proxyMessage := handler.ProxySettings()
	if proxyMessage == nil {
		return metadata
	}
	instance, err := proxyMessage.GetInstance()
	if err != nil {
		metadata.protocol = c.protocolName(proxyMessage.Type)
		return metadata
	}
	metadata.protocol = c.protocolName(proxyMessage.Type)
	metadata.endpoints = c.configuredEndpoints(instance.ProtoReflect())
	return metadata
}

func (c *outboundCollector) normalizeNetwork(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", networkRaw, networkTCP:
		return networkTCP
	case networkXHTTP, networkSplitHTTP:
		return networkSplitHTTP
	case networkKCP, networkMKCP:
		return networkMKCP
	case networkWebSocketWS, networkWebSocket:
		return networkWebSocket
	case networkGRPC:
		return networkGRPC
	case networkHTTPUpgrade:
		return networkHTTPUpgrade
	case networkHysteria:
		return networkHysteria
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func (c *outboundCollector) normalizeSecurity(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" || normalized == securityNone {
		return securityNone
	}
	parts := strings.Split(normalized, ".")
	if len(parts) >= 2 && parts[len(parts)-1] == strings.ToLower(protobufConfigSuffix) {
		normalized = parts[len(parts)-2]
	}
	switch normalized {
	case securityTLS:
		return securityTLS
	case securityReality:
		return securityReality
	default:
		return normalized
	}
}

func (c *outboundCollector) configuredEndpoints(message protoreflect.Message) []outboundEndpoint {
	if string(message.Descriptor().FullName()) == wireGuardConfigMessage {
		return c.wireGuardEndpoints(message)
	}
	if endpoint, found := c.messageEndpoint(message); found {
		return []outboundEndpoint{endpoint}
	}
	for _, name := range []string{fieldVNext, fieldReceiver, fieldServer} {
		field := c.fieldByName(message, name)
		if field == nil || field.Kind() != protoreflect.MessageKind || !message.Has(field) {
			continue
		}
		if endpoint, found := c.messageEndpoint(message.Get(field).Message()); found {
			return []outboundEndpoint{endpoint}
		}
	}
	return nil
}

func (c *outboundCollector) messageEndpoint(message protoreflect.Message) (outboundEndpoint, bool) {
	addressField := c.fieldByName(message, fieldAddress)
	portField := c.fieldByName(message, fieldPort)
	if addressField == nil || portField == nil || addressField.Kind() != protoreflect.MessageKind || !c.isUnsignedInteger(portField.Kind()) || !message.Has(addressField) {
		return outboundEndpoint{}, false
	}
	address, ok := message.Get(addressField).Message().Interface().(*xnet.IPOrDomain)
	if !ok {
		return outboundEndpoint{}, false
	}
	parsed := address.AsAddress()
	if parsed == nil {
		return outboundEndpoint{}, false
	}
	return outboundEndpoint{
		server: parsed.String(),
		port:   strconv.FormatUint(message.Get(portField).Uint(), decimalBase),
	}, true
}

func (c *outboundCollector) isUnsignedInteger(kind protoreflect.Kind) bool {
	return kind == protoreflect.Uint32Kind || kind == protoreflect.Uint64Kind ||
		kind == protoreflect.Fixed32Kind || kind == protoreflect.Fixed64Kind
}

func (c *outboundCollector) wireGuardEndpoints(message protoreflect.Message) []outboundEndpoint {
	var endpoints []outboundEndpoint
	appendEndpoint := func(value string) {
		server, port, err := net.SplitHostPort(value)
		if err != nil {
			server, port = value, ""
		}
		endpoints = append(endpoints, outboundEndpoint{server: server, port: port})
	}
	if field := c.fieldByName(message, fieldEndpoint); field != nil && field.IsList() {
		list := message.Get(field).List()
		for index := 0; index < list.Len(); index++ {
			appendEndpoint(list.Get(index).String())
		}
	}
	if field := c.fieldByName(message, fieldPeers); field != nil && field.IsList() {
		list := message.Get(field).List()
		for index := 0; index < list.Len(); index++ {
			peer := list.Get(index).Message()
			if endpointField := c.fieldByName(peer, fieldEndpoint); endpointField != nil && peer.Has(endpointField) {
				appendEndpoint(peer.Get(endpointField).String())
			}
		}
	}
	return endpoints
}

func (c *outboundCollector) fieldByName(message protoreflect.Message, name string) protoreflect.FieldDescriptor {
	fields := message.Descriptor().Fields()
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		if strings.EqualFold(string(field.Name()), name) {
			return field
		}
	}
	return nil
}

func (c *outboundCollector) protocolName(messageType string) string {
	if name, found := knownOutboundProtocols[messageType]; found {
		return name
	}
	parts := strings.Split(messageType, ".")
	if len(parts) < 2 {
		return messageType
	}
	if len(parts) >= 2 && (parts[len(parts)-1] == protobufConfigSuffix || parts[len(parts)-1] == protobufClientConfig) {
		return parts[len(parts)-2]
	}
	return parts[len(parts)-1]
}
