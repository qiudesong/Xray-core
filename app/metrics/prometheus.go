package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	metricInbound  = "inbound"
	metricOutbound = "outbound"
	metricProbe    = "probe"
	metricMethod   = "method"
	metricProtocol = "protocol"
	metricNetwork  = "network"
	metricSecurity = "security"
	metricServer   = "server"
	metricPort     = "port"
	metricSNI      = "sni"
	metricState    = "state"
	metricResult   = "result"
	metricStage    = "stage"
	metricReason   = "reason"
	metricSource   = "source"
	metricCountry  = "country"
	metricProvider = "provider"
)

func newPrometheusHandler(handler *MetricsHandler) http.Handler {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		newTrafficCollector(handler),
		newOutboundCollector(handler),
		newObservatoryCollector(handler),
		newAccessCollector(handler),
	)
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}
