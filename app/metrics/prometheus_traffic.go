package metrics

import (
	"sort"

	"github.com/prometheus/client_golang/prometheus"
	feature_stats "github.com/xtls/xray-core/features/stats"
)

type trafficCollector struct {
	handler          *MetricsHandler
	inboundUplink    *prometheus.Desc
	inboundDownlink  *prometheus.Desc
	outboundUplink   *prometheus.Desc
	outboundDownlink *prometheus.Desc
	routeUplink      *prometheus.Desc
	routeDownlink    *prometheus.Desc
}

type trafficSample struct {
	dimension string
	tag       string
	direction string
	value     int64
}

type routeTrafficSample struct {
	inbound   string
	outbound  string
	network   string
	direction string
	value     int64
}

func newTrafficCollector(handler *MetricsHandler) *trafficCollector {
	return &trafficCollector{
		handler:         handler,
		inboundUplink:   prometheus.NewDesc("xray_inbound_uplink_bytes_total", "Bytes uploaded through an inbound since Xray started.", []string{"tag"}, nil),
		inboundDownlink: prometheus.NewDesc("xray_inbound_downlink_bytes_total", "Bytes downloaded through an inbound since Xray started.", []string{"tag"}, nil),
		outboundUplink:  prometheus.NewDesc("xray_outbound_uplink_bytes_total", "Bytes uploaded through an outbound since Xray started.", []string{"tag"}, nil),
		outboundDownlink: prometheus.NewDesc(
			"xray_outbound_downlink_bytes_total",
			"Bytes downloaded through an outbound since Xray started.",
			[]string{"tag"}, nil,
		),
		routeUplink: prometheus.NewDesc(
			"xray_route_uplink_bytes_total",
			"Logical payload bytes uploaded through a selected route since Xray started.",
			[]string{metricInbound, metricOutbound, metricNetwork}, nil,
		),
		routeDownlink: prometheus.NewDesc(
			"xray_route_downlink_bytes_total",
			"Logical payload bytes downloaded through a selected route since Xray started.",
			[]string{metricInbound, metricOutbound, metricNetwork}, nil,
		),
	}
}

func (c *trafficCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, description := range []*prometheus.Desc{
		c.inboundUplink,
		c.inboundDownlink,
		c.outboundUplink,
		c.outboundDownlink,
		c.routeUplink,
		c.routeDownlink,
	} {
		ch <- description
	}
}

func (c *trafficCollector) Collect(ch chan<- prometheus.Metric) {
	if c.handler.statsManager == nil {
		return
	}

	var samples []trafficSample
	var routeSamples []routeTrafficSample
	c.handler.statsManager.VisitCounters(func(name string, counter feature_stats.Counter) bool {
		traffic, ok := feature_stats.ParseTrafficCounter(name)
		if !ok {
			return true
		}
		switch traffic.Dimension {
		case feature_stats.TrafficDimensionInbound, feature_stats.TrafficDimensionOutbound:
			samples = append(samples, trafficSample{
				dimension: traffic.Dimension,
				tag:       traffic.Tag,
				direction: traffic.Direction,
				value:     counter.Value(),
			})
		case feature_stats.TrafficDimensionRoute:
			routeSamples = append(routeSamples, routeTrafficSample{
				inbound:   traffic.Inbound,
				outbound:  traffic.Outbound,
				network:   traffic.Network,
				direction: traffic.Direction,
				value:     counter.Value(),
			})
		}
		return true
	})

	sort.Slice(samples, func(i, j int) bool {
		if samples[i].direction != samples[j].direction {
			return samples[i].direction < samples[j].direction
		}
		if samples[i].dimension != samples[j].dimension {
			return samples[i].dimension < samples[j].dimension
		}
		return samples[i].tag < samples[j].tag
	})
	for _, sample := range samples {
		var description *prometheus.Desc
		switch sample.dimension {
		case feature_stats.TrafficDimensionInbound:
			if sample.direction == feature_stats.TrafficDirectionUplink {
				description = c.inboundUplink
			} else {
				description = c.inboundDownlink
			}
		case feature_stats.TrafficDimensionOutbound:
			if sample.direction == feature_stats.TrafficDirectionUplink {
				description = c.outboundUplink
			} else {
				description = c.outboundDownlink
			}
		}
		ch <- prometheus.MustNewConstMetric(description, prometheus.CounterValue, float64(sample.value), sample.tag)
	}

	sort.Slice(routeSamples, func(i, j int) bool {
		left, right := routeSamples[i], routeSamples[j]
		if left.direction != right.direction {
			return left.direction < right.direction
		}
		if left.inbound != right.inbound {
			return left.inbound < right.inbound
		}
		if left.outbound != right.outbound {
			return left.outbound < right.outbound
		}
		return left.network < right.network
	})
	for _, sample := range routeSamples {
		description := c.routeUplink
		if sample.direction == feature_stats.TrafficDirectionDownlink {
			description = c.routeDownlink
		}
		ch <- prometheus.MustNewConstMetric(
			description,
			prometheus.CounterValue,
			float64(sample.value),
			sample.inbound,
			sample.outbound,
			sample.network,
		)
	}
}
