package metrics

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/extension"
	feature_stats "github.com/xtls/xray-core/features/stats"
)

const (
	metricInbound  = "inbound"
	metricOutbound = "outbound"
	metricUplink   = "uplink"
	metricDownlink = "downlink"
)

type prometheusCollector struct {
	handler *MetricsHandler

	uptime                        *prometheus.Desc
	inboundUplink                 *prometheus.Desc
	inboundDownlink               *prometheus.Desc
	outboundUplink                *prometheus.Desc
	outboundDownlink              *prometheus.Desc
	observatoryHealthy            *prometheus.Desc
	observatoryProbeDuration      *prometheus.Desc
	observatoryLastSuccess        *prometheus.Desc
	observatoryLastProbe          *prometheus.Desc
	healthPingWindowSamples       *prometheus.Desc
	healthPingWindowFailedSamples *prometheus.Desc
	healthPingStandardDeviation   *prometheus.Desc
	healthPingAverage             *prometheus.Desc
	healthPingMaximum             *prometheus.Desc
	healthPingMinimum             *prometheus.Desc
	accessRequests                *prometheus.Desc
	accessWindowRequests          *prometheus.Desc
	accessUniqueUsers             *prometheus.Desc
	accessPeerCountryConnections  *prometheus.Desc
	accessPeerASNConnections      *prometheus.Desc
	accessPeerCityConnections     *prometheus.Desc
	accessAddressRequests         *prometheus.Desc
	accessEventsDropped           *prometheus.Desc
	accessPeerEventsDropped       *prometheus.Desc
	accessRequestSeriesDropped    *prometheus.Desc
	accessPeerASNSeriesDropped    *prometheus.Desc
	accessPeerCitySeriesDropped   *prometheus.Desc
	accessAddressSeriesDropped    *prometheus.Desc
	accessUserTrackingDropped     *prometheus.Desc
}

type trafficSample struct {
	dimension string
	tag       string
	direction string
	value     int64
}

func newPrometheusHandler(handler *MetricsHandler) http.Handler {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		newPrometheusCollector(handler),
	)
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}

func newPrometheusCollector(handler *MetricsHandler) *prometheusCollector {
	labels := []string{metricOutbound}
	return &prometheusCollector{
		handler: handler,

		uptime:          prometheus.NewDesc("xray_uptime_seconds", "Xray uptime in seconds.", nil, nil),
		inboundUplink:   prometheus.NewDesc("xray_inbound_uplink_bytes_total", "Bytes uploaded through an inbound since Xray started.", []string{"tag"}, nil),
		inboundDownlink: prometheus.NewDesc("xray_inbound_downlink_bytes_total", "Bytes downloaded through an inbound since Xray started.", []string{"tag"}, nil),
		outboundUplink:  prometheus.NewDesc("xray_outbound_uplink_bytes_total", "Bytes uploaded through an outbound since Xray started.", []string{"tag"}, nil),
		outboundDownlink: prometheus.NewDesc(
			"xray_outbound_downlink_bytes_total",
			"Bytes downloaded through an outbound since Xray started.",
			[]string{"tag"}, nil,
		),

		observatoryHealthy:       prometheus.NewDesc("xray_observatory_healthy", "Whether an observed outbound is usable.", labels, nil),
		observatoryProbeDuration: prometheus.NewDesc("xray_observatory_probe_duration_seconds", "Last outbound probe duration in seconds.", labels, nil),
		observatoryLastSuccess:   prometheus.NewDesc("xray_observatory_last_success_timestamp_seconds", "Unix timestamp when an outbound was last known to be alive.", labels, nil),
		observatoryLastProbe:     prometheus.NewDesc("xray_observatory_last_probe_timestamp_seconds", "Unix timestamp when an outbound was last probed.", labels, nil),
		healthPingWindowSamples: prometheus.NewDesc(
			"xray_observatory_health_ping_window_samples",
			"Number of probe samples in the current health-ping window.",
			labels, nil,
		),
		healthPingWindowFailedSamples: prometheus.NewDesc(
			"xray_observatory_health_ping_window_failed_samples",
			"Number of failed probe samples in the current health-ping window.",
			labels, nil,
		),
		healthPingStandardDeviation: prometheus.NewDesc(
			"xray_observatory_health_ping_duration_standard_deviation_seconds",
			"Standard deviation of probe durations in the current health-ping window.",
			labels, nil,
		),
		healthPingAverage: prometheus.NewDesc(
			"xray_observatory_health_ping_duration_average_seconds",
			"Average probe duration in the current health-ping window.",
			labels, nil,
		),
		healthPingMaximum: prometheus.NewDesc(
			"xray_observatory_health_ping_duration_maximum_seconds",
			"Maximum probe duration in the current health-ping window.",
			labels, nil,
		),
		healthPingMinimum: prometheus.NewDesc(
			"xray_observatory_health_ping_duration_minimum_seconds",
			"Minimum probe duration in the current health-ping window.",
			labels, nil,
		),
		accessRequests: prometheus.NewDesc(
			"xray_access_requests_total",
			"Access requests observed since Xray started.",
			[]string{metricInbound, metricOutbound, "network", "role", "status"}, nil,
		),
		accessWindowRequests: prometheus.NewDesc(
			"xray_access_requests_window",
			"Access requests observed in the configured bucketed sliding window.",
			[]string{"role", "status"}, nil,
		),
		accessUniqueUsers: prometheus.NewDesc(
			"xray_access_unique_authenticated_users_window",
			"Unique authenticated users with accepted requests in the configured sliding window.",
			[]string{"role"}, nil,
		),
		accessPeerCountryConnections: prometheus.NewDesc(
			"xray_access_peer_country_connections_total",
			"Established peer connections by country since Xray started.",
			[]string{"country", "role", "side", "tag", "network"}, nil,
		),
		accessPeerASNConnections: prometheus.NewDesc(
			"xray_access_peer_asn_connections_total",
			"Established peer connections by autonomous system number and organization since Xray started.",
			[]string{"asn", "org", "role", "side", "tag", "network"}, nil,
		),
		accessPeerCityConnections: prometheus.NewDesc(
			"xray_access_peer_city_connections_total",
			"Established peer connections by city since Xray started.",
			[]string{"country", "city", "role", "side", "tag", "network"}, nil,
		),
		accessAddressRequests: prometheus.NewDesc(
			"xray_access_address_requests_total",
			"Access requests by the reported source or destination address since Xray started.",
			[]string{"address", "address_type", "role", "side", "status"}, nil,
		),
		accessEventsDropped: prometheus.NewDesc(
			"xray_access_events_dropped_total",
			"Structured access events dropped because the metrics queue was full.",
			[]string{"role"}, nil,
		),
		accessPeerEventsDropped: prometheus.NewDesc(
			"xray_access_peer_events_dropped_total",
			"Peer connection events dropped because the metrics queue was full.",
			[]string{"role", "side"}, nil,
		),
		accessRequestSeriesDropped: prometheus.NewDesc(
			"xray_access_request_series_dropped_total",
			"Access requests not added to a new label series because the in-memory cardinality limit was reached.",
			[]string{"role"}, nil,
		),
		accessPeerASNSeriesDropped: prometheus.NewDesc(
			"xray_access_peer_asn_series_dropped_total",
			"Peer connections not added to a new ASN label series because the in-memory cardinality limit was reached.",
			[]string{"role", "side"}, nil,
		),
		accessPeerCitySeriesDropped: prometheus.NewDesc(
			"xray_access_peer_city_series_dropped_total",
			"Peer connections not added to a new city label series because the in-memory cardinality limit was reached.",
			[]string{"role", "side"}, nil,
		),
		accessAddressSeriesDropped: prometheus.NewDesc(
			"xray_access_address_series_dropped_total",
			"Access requests not added to a new address label series because the in-memory cardinality limit was reached.",
			[]string{"role"}, nil,
		),
		accessUserTrackingDropped: prometheus.NewDesc(
			"xray_access_user_tracking_dropped_total",
			"Authenticated users not tracked because the in-memory cardinality limit was reached.",
			[]string{"role"}, nil,
		),
	}
}

func (c *prometheusCollector) Describe(ch chan<- *prometheus.Desc) {
	descriptions := []*prometheus.Desc{
		c.uptime,
		c.inboundUplink,
		c.inboundDownlink,
		c.outboundUplink,
		c.outboundDownlink,
		c.observatoryHealthy,
		c.observatoryProbeDuration,
		c.observatoryLastSuccess,
		c.observatoryLastProbe,
		c.healthPingWindowSamples,
		c.healthPingWindowFailedSamples,
		c.healthPingStandardDeviation,
		c.healthPingAverage,
		c.healthPingMaximum,
		c.healthPingMinimum,
		c.accessRequests,
		c.accessWindowRequests,
		c.accessUniqueUsers,
		c.accessPeerCountryConnections,
		c.accessPeerASNConnections,
		c.accessPeerCityConnections,
		c.accessAddressRequests,
		c.accessEventsDropped,
		c.accessPeerEventsDropped,
		c.accessRequestSeriesDropped,
		c.accessPeerASNSeriesDropped,
		c.accessPeerCitySeriesDropped,
		c.accessAddressSeriesDropped,
		c.accessUserTrackingDropped,
	}
	for _, description := range descriptions {
		ch <- description
	}
}

func (c *prometheusCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(c.uptime, prometheus.GaugeValue, time.Since(c.handler.startedAt).Seconds())

	c.collectTraffic(ch)
	c.collectObservatory(ch)
	c.collectAccess(ch)
}

func (c *prometheusCollector) collectAccess(ch chan<- prometheus.Metric) {
	if c.handler.access == nil {
		return
	}
	snapshot := c.handler.access.snapshot(time.Now())
	sort.Slice(snapshot.requests, func(i, j int) bool {
		left, right := snapshot.requests[i].labels, snapshot.requests[j].labels
		if left.inbound != right.inbound {
			return left.inbound < right.inbound
		}
		if left.outbound != right.outbound {
			return left.outbound < right.outbound
		}
		if left.network != right.network {
			return left.network < right.network
		}
		return left.status < right.status
	})
	for _, sample := range snapshot.requests {
		ch <- prometheus.MustNewConstMetric(
			c.accessRequests,
			prometheus.CounterValue,
			float64(sample.value),
			sample.labels.inbound,
			sample.labels.outbound,
			sample.labels.network,
			snapshot.role,
			sample.labels.status,
		)
	}

	statuses := make([]string, 0, len(snapshot.windowRequests))
	for status := range snapshot.windowRequests {
		statuses = append(statuses, status)
	}
	sort.Strings(statuses)
	for _, status := range statuses {
		ch <- prometheus.MustNewConstMetric(c.accessWindowRequests, prometheus.GaugeValue, float64(snapshot.windowRequests[status]), snapshot.role, status)
	}

	sort.Slice(snapshot.peerCountryConnections, func(i, j int) bool {
		left, right := snapshot.peerCountryConnections[i].labels, snapshot.peerCountryConnections[j].labels
		if left.country != right.country {
			return left.country < right.country
		}
		if left.side != right.side {
			return left.side < right.side
		}
		if left.tag != right.tag {
			return left.tag < right.tag
		}
		return left.network < right.network
	})
	for _, sample := range snapshot.peerCountryConnections {
		ch <- prometheus.MustNewConstMetric(
			c.accessPeerCountryConnections,
			prometheus.CounterValue,
			float64(sample.value),
			sample.labels.country,
			snapshot.role,
			sample.labels.side,
			sample.labels.tag,
			sample.labels.network,
		)
	}

	sort.Slice(snapshot.peerASNConnections, func(i, j int) bool {
		left, right := snapshot.peerASNConnections[i].labels, snapshot.peerASNConnections[j].labels
		if left.asn != right.asn {
			return left.asn < right.asn
		}
		if left.org != right.org {
			return left.org < right.org
		}
		if left.side != right.side {
			return left.side < right.side
		}
		if left.tag != right.tag {
			return left.tag < right.tag
		}
		return left.network < right.network
	})
	for _, sample := range snapshot.peerASNConnections {
		ch <- prometheus.MustNewConstMetric(
			c.accessPeerASNConnections,
			prometheus.CounterValue,
			float64(sample.value),
			sample.labels.asn,
			sample.labels.org,
			snapshot.role,
			sample.labels.side,
			sample.labels.tag,
			sample.labels.network,
		)
	}

	sort.Slice(snapshot.peerCityConnections, func(i, j int) bool {
		left, right := snapshot.peerCityConnections[i].labels, snapshot.peerCityConnections[j].labels
		if left.country != right.country {
			return left.country < right.country
		}
		if left.city != right.city {
			return left.city < right.city
		}
		if left.side != right.side {
			return left.side < right.side
		}
		if left.tag != right.tag {
			return left.tag < right.tag
		}
		return left.network < right.network
	})
	for _, sample := range snapshot.peerCityConnections {
		ch <- prometheus.MustNewConstMetric(
			c.accessPeerCityConnections,
			prometheus.CounterValue,
			float64(sample.value),
			sample.labels.country,
			sample.labels.city,
			snapshot.role,
			sample.labels.side,
			sample.labels.tag,
			sample.labels.network,
		)
	}

	sort.Slice(snapshot.addressRequests, func(i, j int) bool {
		left, right := snapshot.addressRequests[i].labels, snapshot.addressRequests[j].labels
		if left.address != right.address {
			return left.address < right.address
		}
		if left.addressType != right.addressType {
			return left.addressType < right.addressType
		}
		if left.side != right.side {
			return left.side < right.side
		}
		return left.status < right.status
	})
	for _, sample := range snapshot.addressRequests {
		ch <- prometheus.MustNewConstMetric(
			c.accessAddressRequests,
			prometheus.CounterValue,
			float64(sample.value),
			sample.labels.address,
			sample.labels.addressType,
			snapshot.role,
			sample.labels.side,
			sample.labels.status,
		)
	}

	ch <- prometheus.MustNewConstMetric(c.accessUniqueUsers, prometheus.GaugeValue, float64(snapshot.uniqueUsers), snapshot.role)
	ch <- prometheus.MustNewConstMetric(c.accessEventsDropped, prometheus.CounterValue, float64(snapshot.eventsDropped), snapshot.role)
	ch <- prometheus.MustNewConstMetric(c.accessPeerEventsDropped, prometheus.CounterValue, float64(snapshot.peerEventsDropped), snapshot.role, snapshot.peerSide)
	ch <- prometheus.MustNewConstMetric(c.accessRequestSeriesDropped, prometheus.CounterValue, float64(snapshot.requestSeriesDropped), snapshot.role)
	if snapshot.asnEnabled {
		ch <- prometheus.MustNewConstMetric(c.accessPeerASNSeriesDropped, prometheus.CounterValue, float64(snapshot.peerASNSeriesDropped), snapshot.role, snapshot.peerSide)
	}
	if snapshot.cityEnabled {
		ch <- prometheus.MustNewConstMetric(c.accessPeerCitySeriesDropped, prometheus.CounterValue, float64(snapshot.peerCitySeriesDropped), snapshot.role, snapshot.peerSide)
	}
	ch <- prometheus.MustNewConstMetric(c.accessAddressSeriesDropped, prometheus.CounterValue, float64(snapshot.addressSeriesDropped), snapshot.role)
	ch <- prometheus.MustNewConstMetric(c.accessUserTrackingDropped, prometheus.CounterValue, float64(snapshot.userTrackingDropped), snapshot.role)
}

func (c *prometheusCollector) collectTraffic(ch chan<- prometheus.Metric) {
	if c.handler.statsManager == nil {
		return
	}

	var samples []trafficSample
	c.handler.statsManager.VisitCounters(func(name string, counter feature_stats.Counter) bool {
		parts := strings.Split(name, ">>>")
		if len(parts) != 4 || parts[2] != "traffic" {
			return true
		}
		if parts[0] != metricInbound && parts[0] != metricOutbound {
			return true
		}
		if parts[3] != metricUplink && parts[3] != metricDownlink {
			return true
		}
		samples = append(samples, trafficSample{
			dimension: parts[0],
			tag:       parts[1],
			direction: parts[3],
			value:     counter.Value(),
		})
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
		case metricInbound:
			switch sample.direction {
			case metricUplink:
				description = c.inboundUplink
			case metricDownlink:
				description = c.inboundDownlink
			}
		case metricOutbound:
			switch sample.direction {
			case metricUplink:
				description = c.outboundUplink
			case metricDownlink:
				description = c.outboundDownlink
			}
		}
		if description == nil {
			continue
		}
		ch <- prometheus.MustNewConstMetric(description, prometheus.CounterValue, float64(sample.value), sample.tag)
	}
}

func (c *prometheusCollector) collectObservatory(ch chan<- prometheus.Metric) {
	statuses, found := c.observationStatuses()
	if !found {
		return
	}
	for _, status := range statuses {
		if status == nil {
			continue
		}
		outbound := status.GetOutboundTag()
		alive := 0.0
		if status.GetAlive() {
			alive = 1
		}
		ch <- prometheus.MustNewConstMetric(c.observatoryHealthy, prometheus.GaugeValue, alive, outbound)
		ch <- prometheus.MustNewConstMetric(c.observatoryProbeDuration, prometheus.GaugeValue, c.millisecondsToSeconds(status.GetDelay()), outbound)
		ch <- prometheus.MustNewConstMetric(c.observatoryLastSuccess, prometheus.GaugeValue, float64(status.GetLastSeenTime()), outbound)
		ch <- prometheus.MustNewConstMetric(c.observatoryLastProbe, prometheus.GaugeValue, float64(status.GetLastTryTime()), outbound)
		c.collectHealthPing(ch, outbound, status.GetHealthPing())
	}
}

func (c *prometheusCollector) observationStatuses() ([]*observatory.OutboundStatus, bool) {
	instance := core.FromContext(c.handler.ctx)
	if instance == nil {
		return nil, false
	}
	feature := instance.GetFeature(extension.ObservatoryType())
	observatoryFeature, ok := feature.(extension.Observatory)
	if !ok {
		return nil, false
	}
	message, err := observatoryFeature.GetObservation(context.Background())
	if err != nil {
		return nil, false
	}
	result, ok := message.(*observatory.ObservationResult)
	if !ok {
		return nil, false
	}
	return result.GetStatus(), true
}

func (c *prometheusCollector) collectHealthPing(ch chan<- prometheus.Metric, outbound string, healthPing *observatory.HealthPingMeasurementResult) {
	if healthPing == nil {
		return
	}
	ch <- prometheus.MustNewConstMetric(c.healthPingWindowSamples, prometheus.GaugeValue, float64(healthPing.GetAll()), outbound)
	ch <- prometheus.MustNewConstMetric(c.healthPingWindowFailedSamples, prometheus.GaugeValue, float64(healthPing.GetFail()), outbound)
	ch <- prometheus.MustNewConstMetric(c.healthPingStandardDeviation, prometheus.GaugeValue, c.durationToSeconds(healthPing.GetDeviation()), outbound)
	ch <- prometheus.MustNewConstMetric(c.healthPingAverage, prometheus.GaugeValue, c.durationToSeconds(healthPing.GetAverage()), outbound)
	ch <- prometheus.MustNewConstMetric(c.healthPingMaximum, prometheus.GaugeValue, c.durationToSeconds(healthPing.GetMax()), outbound)
	ch <- prometheus.MustNewConstMetric(c.healthPingMinimum, prometheus.GaugeValue, c.durationToSeconds(healthPing.GetMin()), outbound)
}

func (c *prometheusCollector) millisecondsToSeconds(value int64) float64 {
	return float64(value) / float64(time.Second/time.Millisecond)
}

func (c *prometheusCollector) durationToSeconds(value int64) float64 {
	return time.Duration(value).Seconds()
}
