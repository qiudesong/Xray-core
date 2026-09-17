package metrics

import (
	"context"
	"strings"
	"testing"

	"github.com/xtls/xray-core/app/proxyman"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	feature_outbound "github.com/xtls/xray-core/features/outbound"
	vlessoutbound "github.com/xtls/xray-core/proxy/vless/outbound"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
	tlsconfig "github.com/xtls/xray-core/transport/internet/tls"
)

type metadataHandler struct {
	tag      string
	sender   *serial.TypedMessage
	proxy    *serial.TypedMessage
	attempts uint64
}

func (h *metadataHandler) Tag() string                             { return h.tag }
func (*metadataHandler) Start() error                              { return nil }
func (*metadataHandler) Close() error                              { return nil }
func (*metadataHandler) Dispatch(context.Context, *transport.Link) {}
func (h *metadataHandler) SenderSettings() *serial.TypedMessage    { return h.sender }
func (h *metadataHandler) ProxySettings() *serial.TypedMessage     { return h.proxy }
func (h *metadataHandler) ConnectionAttempts() uint64              { return h.attempts }

func TestInspectOutboundSeparatesBaseAndEndpointMetadata(t *testing.T) {
	handler := newMetadataHandler()
	metadata := new(outboundCollector).inspectOutbound(handler)
	if metadata.tag != "proxy-a" || metadata.protocol != "vless" || metadata.network != "websocket" || metadata.security != "tls" {
		t.Fatalf("unexpected base metadata: %#v", metadata)
	}
	if metadata.sni != "sni.example" || len(metadata.endpoints) != 1 || metadata.endpoints[0].server != "server.example" || metadata.endpoints[0].port != "443" {
		t.Fatalf("unexpected endpoint metadata: %#v", metadata)
	}
}

func TestInspectOutboundUsesEffectiveTransportDefaults(t *testing.T) {
	handler := newMetadataHandler()
	handler.sender = serial.ToTypedMessage(&proxyman.SenderConfig{})
	metadata := new(outboundCollector).inspectOutbound(handler)
	if metadata.network != networkTCP || metadata.security != securityNone {
		t.Fatalf("unexpected default transport metadata: %#v", metadata)
	}
}

func TestOutboundEndpointMetricIsExported(t *testing.T) {
	server := startMetricsTestServerWithMetricsConfig(t, &Config{Tag: "metrics_out"})
	t.Cleanup(func() { _ = server.Close() })
	manager := server.GetFeature(feature_outbound.ManagerType()).(feature_outbound.Manager)
	if err := manager.AddHandler(context.Background(), newMetadataHandler()); err != nil {
		t.Fatal(err)
	}
	body := readPrometheusMetrics(t, server)
	for _, expected := range []string{
		`xray_outbound_info{network="websocket",outbound="proxy-a",protocol="vless",security="tls"} 1`,
		`xray_outbound_endpoint_info{outbound="proxy-a",port="443",server="server.example",sni="sni.example"} 1`,
		`xray_outbound_connection_attempts_total{outbound="proxy-a"} 12`,
	} {
		if !strings.Contains(body, expected) {
			t.Errorf("Prometheus output missing %q", expected)
		}
	}
}

func TestNormalizeOutboundNetwork(t *testing.T) {
	collector := new(outboundCollector)
	tests := map[string]string{
		"":          networkTCP,
		"RAW":       networkTCP,
		"xhttp":     networkSplitHTTP,
		"KCP":       networkMKCP,
		"ws":        networkWebSocket,
		"GRPC":      networkGRPC,
		"hysteria":  networkHysteria,
		"futureNet": "futurenet",
	}
	for input, expected := range tests {
		if actual := collector.normalizeNetwork(input); actual != expected {
			t.Errorf("normalizeNetwork(%q) = %q, want %q", input, actual, expected)
		}
	}
}

func TestNormalizeOutboundSecurity(t *testing.T) {
	collector := new(outboundCollector)
	tests := map[string]string{
		"":                                       securityNone,
		"NONE":                                   securityNone,
		"TLS":                                    securityTLS,
		"xray.transport.internet.tls.Config":     securityTLS,
		"xray.transport.internet.reality.Config": securityReality,
		"xray.transport.internet.future.Config":  "future",
	}
	for input, expected := range tests {
		if actual := collector.normalizeSecurity(input); actual != expected {
			t.Errorf("normalizeSecurity(%q) = %q, want %q", input, actual, expected)
		}
	}
}

func newMetadataHandler() *metadataHandler {
	return &metadataHandler{
		tag: "proxy-a",
		sender: serial.ToTypedMessage(&proxyman.SenderConfig{StreamSettings: &internet.StreamConfig{
			ProtocolName: "ws", SecurityType: serial.GetMessageType(&tlsconfig.Config{}),
			SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&tlsconfig.Config{ServerName: "sni.example"})},
		}}),
		proxy: serial.ToTypedMessage(&vlessoutbound.Config{Vnext: &protocol.ServerEndpoint{
			Address: xnet.NewIPOrDomain(xnet.DomainAddress("server.example")), Port: 443,
		}}),
		attempts: 12,
	}
}
