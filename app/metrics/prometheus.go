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
	}
	for _, description := range descriptions {
		ch <- description
	}
}

func (c *prometheusCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(c.uptime, prometheus.GaugeValue, time.Since(c.handler.startedAt).Seconds())

	c.collectTraffic(ch)
	c.collectObservatory(ch)
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
