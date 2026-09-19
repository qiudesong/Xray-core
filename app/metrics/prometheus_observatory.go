package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/extension"
)

const (
	metricNameProbeUp                     = "xray_observatory_probe_up"
	metricNameProbeState                  = "xray_observatory_probe_state"
	metricNameProbeLastDuration           = "xray_observatory_probe_last_duration_seconds"
	metricNameProbeLastTTFB               = "xray_observatory_probe_last_ttfb_seconds"
	metricNameProbeChecks                 = "xray_observatory_probe_checks_total"
	metricNameProbeErrors                 = "xray_observatory_probe_errors_total"
	metricNameProbeLastCheck              = "xray_observatory_probe_last_check_timestamp_seconds"
	metricNameProbeLastSuccess            = "xray_observatory_probe_last_success_timestamp_seconds"
	metricNameProbeHTTPStatus             = "xray_observatory_probe_http_status_code"
	metricNameProbeDownloadBytes          = "xray_observatory_probe_download_bytes"
	metricNameProbeDuration               = "xray_observatory_probe_duration_seconds"
	metricNameProbeLatency                = "xray_observatory_probe_latency_seconds"
	metricNameProbeLocationInfo           = "xray_observatory_probe_location_info"
	metricNameProbeLocationObserved       = "xray_observatory_probe_location_observed_timestamp_seconds"
	metricNameProbeWindowSamples          = "xray_observatory_probe_window_samples"
	metricNameProbeWindowFailures         = "xray_observatory_probe_window_failures"
	metricNameProbeWindowSuccessRatio     = "xray_observatory_probe_window_success_ratio"
	metricNameProbeWindowLatencyDeviation = "xray_observatory_probe_window_latency_standard_deviation_seconds"
	metricNameProbeWindowLatencyAverage   = "xray_observatory_probe_window_latency_average_seconds"
	metricNameProbeWindowLatencyMaximum   = "xray_observatory_probe_window_latency_maximum_seconds"
	metricNameProbeWindowLatencyMinimum   = "xray_observatory_probe_window_latency_minimum_seconds"
	metricNameBaselineRefreshUp           = "xray_observatory_baseline_refresh_up"
	metricNameBaselineLastRefresh         = "xray_observatory_baseline_last_refresh_timestamp_seconds"

	probeResultSuccess  = "success"
	probeResultFailure  = "failure"
	probeMethodDownload = "download"
	legacyProbeTag      = "default"
	legacyProbeMethod   = "http"
)

type observatoryCollector struct {
	handler                     *MetricsHandler
	probeUp                     *prometheus.Desc
	probeState                  *prometheus.Desc
	probeLastDuration           *prometheus.Desc
	probeLastTTFB               *prometheus.Desc
	probeChecks                 *prometheus.Desc
	probeErrors                 *prometheus.Desc
	probeLastCheck              *prometheus.Desc
	probeLastSuccess            *prometheus.Desc
	probeHTTPStatus             *prometheus.Desc
	probeDownloadBytes          *prometheus.Desc
	probeDuration               *prometheus.Desc
	probeLatency                *prometheus.Desc
	probeLocationInfo           *prometheus.Desc
	probeLocationObserved       *prometheus.Desc
	probeWindowSamples          *prometheus.Desc
	probeWindowFailures         *prometheus.Desc
	probeWindowSuccessRatio     *prometheus.Desc
	probeWindowLatencyDeviation *prometheus.Desc
	probeWindowLatencyAverage   *prometheus.Desc
	probeWindowLatencyMaximum   *prometheus.Desc
	probeWindowLatencyMinimum   *prometheus.Desc
	baselineRefreshUp           *prometheus.Desc
	baselineLastRefresh         *prometheus.Desc
}

func newObservatoryCollector(handler *MetricsHandler) *observatoryCollector {
	probeLabels := []string{metricProbe, metricOutbound, metricMethod}
	baselineLabels := []string{metricProbe, metricProvider}
	return &observatoryCollector{
		handler:                     handler,
		probeUp:                     prometheus.NewDesc(metricNameProbeUp, "Whether the latest probe succeeded and is fresh.", probeLabels, nil),
		probeState:                  prometheus.NewDesc(metricNameProbeState, "One-hot effective health state of an outbound probe.", appendLabels(probeLabels, metricState), nil),
		probeLastDuration:           prometheus.NewDesc(metricNameProbeLastDuration, "Duration of the latest probe.", probeLabels, nil),
		probeLastTTFB:               prometheus.NewDesc(metricNameProbeLastTTFB, "Time to first byte of the latest probe, exported only when that probe succeeded.", probeLabels, nil),
		probeChecks:                 prometheus.NewDesc(metricNameProbeChecks, "Completed outbound probes by result.", appendLabels(probeLabels, metricResult), nil),
		probeErrors:                 prometheus.NewDesc(metricNameProbeErrors, "Failed outbound probes by bounded stage and reason.", appendLabels(probeLabels, metricStage, metricReason), nil),
		probeLastCheck:              prometheus.NewDesc(metricNameProbeLastCheck, "Unix timestamp of the latest probe.", probeLabels, nil),
		probeLastSuccess:            prometheus.NewDesc(metricNameProbeLastSuccess, "Unix timestamp of the latest successful probe.", probeLabels, nil),
		probeHTTPStatus:             prometheus.NewDesc(metricNameProbeHTTPStatus, "HTTP status code returned by the latest probe.", probeLabels, nil),
		probeDownloadBytes:          prometheus.NewDesc(metricNameProbeDownloadBytes, "Bytes read by the latest download probe.", probeLabels, nil),
		probeDuration:               prometheus.NewDesc(metricNameProbeDuration, "Cumulative outbound probe duration histogram.", probeLabels, nil),
		probeLatency:                prometheus.NewDesc(metricNameProbeLatency, "Cumulative TTFB histogram of successful outbound probes.", probeLabels, nil),
		probeLocationInfo:           prometheus.NewDesc(metricNameProbeLocationInfo, "Country reported by the latest successful IP probe.", appendLabels(probeLabels, metricSource, metricCountry), nil),
		probeLocationObserved:       prometheus.NewDesc(metricNameProbeLocationObserved, "Unix timestamp when the latest successful IP probe location was observed.", appendLabels(probeLabels, metricSource, metricCountry), nil),
		probeWindowSamples:          prometheus.NewDesc(metricNameProbeWindowSamples, "Samples in the current probe evaluation window.", probeLabels, nil),
		probeWindowFailures:         prometheus.NewDesc(metricNameProbeWindowFailures, "Failed samples in the current probe evaluation window.", probeLabels, nil),
		probeWindowSuccessRatio:     prometheus.NewDesc(metricNameProbeWindowSuccessRatio, "Successful sample ratio in the current probe evaluation window.", probeLabels, nil),
		probeWindowLatencyDeviation: prometheus.NewDesc(metricNameProbeWindowLatencyDeviation, "TTFB standard deviation of successful probes in the current evaluation window.", probeLabels, nil),
		probeWindowLatencyAverage:   prometheus.NewDesc(metricNameProbeWindowLatencyAverage, "Average TTFB of successful probes in the current evaluation window.", probeLabels, nil),
		probeWindowLatencyMaximum:   prometheus.NewDesc(metricNameProbeWindowLatencyMaximum, "Maximum TTFB of successful probes in the current evaluation window.", probeLabels, nil),
		probeWindowLatencyMinimum:   prometheus.NewDesc(metricNameProbeWindowLatencyMinimum, "Minimum TTFB of successful probes in the current evaluation window.", probeLabels, nil),
		baselineRefreshUp:           prometheus.NewDesc(metricNameBaselineRefreshUp, "Whether the latest direct baseline refresh succeeded.", baselineLabels, nil),
		baselineLastRefresh:         prometheus.NewDesc(metricNameBaselineLastRefresh, "Unix timestamp of the latest direct baseline refresh attempt.", baselineLabels, nil),
	}
}

func appendLabels(labels []string, extra ...string) []string {
	result := make([]string, 0, len(labels)+len(extra))
	result = append(result, labels...)
	return append(result, extra...)
}

func (c *observatoryCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, description := range []*prometheus.Desc{
		c.probeUp, c.probeState, c.probeLastDuration, c.probeLastTTFB,
		c.probeChecks, c.probeErrors, c.probeLastCheck, c.probeLastSuccess,
		c.probeHTTPStatus, c.probeDownloadBytes, c.probeDuration, c.probeLatency,
		c.probeLocationInfo, c.probeLocationObserved, c.probeWindowSamples,
		c.probeWindowFailures, c.probeWindowSuccessRatio, c.probeWindowLatencyDeviation,
		c.probeWindowLatencyAverage, c.probeWindowLatencyMaximum,
		c.probeWindowLatencyMinimum, c.baselineRefreshUp, c.baselineLastRefresh,
	} {
		ch <- description
	}
}

func (c *observatoryCollector) Collect(ch chan<- prometheus.Metric) {
	c.collectProbes(ch)
	c.collectBaselines(ch)
}

func (c *observatoryCollector) collectProbes(ch chan<- prometheus.Metric) {
	snapshots, found := c.observationSnapshots()
	if !found {
		return
	}
	states := []extension.ProbeState{
		extension.ProbeStateUnknown, extension.ProbeStateHealthy,
		extension.ProbeStateUnhealthy, extension.ProbeStateStale,
	}
	for _, snapshot := range snapshots {
		labels := []string{snapshot.ProbeTag, snapshot.OutboundTag, snapshot.Method}
		up := 0.0
		if snapshot.Latest.Success && snapshot.EffectiveState != extension.ProbeStateStale {
			up = 1
		}
		ch <- prometheus.MustNewConstMetric(c.probeUp, prometheus.GaugeValue, up, labels...)
		for _, state := range states {
			value := 0.0
			if snapshot.EffectiveState == state {
				value = 1
			}
			ch <- prometheus.MustNewConstMetric(c.probeState, prometheus.GaugeValue, value, append(labels, string(state))...)
		}
		ch <- prometheus.MustNewConstMetric(c.probeLastDuration, prometheus.GaugeValue, snapshot.Latest.Duration.Seconds(), labels...)
		if snapshot.Latest.Success {
			ch <- prometheus.MustNewConstMetric(c.probeLastTTFB, prometheus.GaugeValue, snapshot.Latest.TTFB.Seconds(), labels...)
		}
		ch <- prometheus.MustNewConstMetric(c.probeChecks, prometheus.CounterValue, float64(snapshot.ChecksSuccess), append(labels, probeResultSuccess)...)
		ch <- prometheus.MustNewConstMetric(c.probeChecks, prometheus.CounterValue, float64(snapshot.ChecksFailure), append(labels, probeResultFailure)...)
		for _, probeError := range snapshot.Errors {
			ch <- prometheus.MustNewConstMetric(
				c.probeErrors, prometheus.CounterValue, float64(probeError.Count),
				append(labels, probeError.Stage, probeError.Reason)...,
			)
		}
		if !snapshot.Latest.CheckedAt.IsZero() {
			ch <- prometheus.MustNewConstMetric(c.probeLastCheck, prometheus.GaugeValue, float64(snapshot.Latest.CheckedAt.Unix()), labels...)
		}
		if !snapshot.LastSuccess.IsZero() {
			ch <- prometheus.MustNewConstMetric(c.probeLastSuccess, prometheus.GaugeValue, float64(snapshot.LastSuccess.Unix()), labels...)
		}
		if snapshot.Latest.HTTPStatus != 0 {
			ch <- prometheus.MustNewConstMetric(c.probeHTTPStatus, prometheus.GaugeValue, float64(snapshot.Latest.HTTPStatus), labels...)
		}
		if snapshot.Method == probeMethodDownload {
			ch <- prometheus.MustNewConstMetric(c.probeDownloadBytes, prometheus.GaugeValue, float64(snapshot.Latest.Bytes), labels...)
		}
		if snapshot.DurationCount > 0 {
			buckets := make(map[float64]uint64, len(snapshot.DurationBuckets))
			for upperBound, count := range snapshot.DurationBuckets {
				buckets[upperBound.Seconds()] = count
			}
			ch <- prometheus.MustNewConstHistogram(c.probeDuration, snapshot.DurationCount, snapshot.DurationSum.Seconds(), buckets, labels...)
		}
		if snapshot.LatencyCount > 0 {
			buckets := make(map[float64]uint64, len(snapshot.LatencyBuckets))
			for upperBound, count := range snapshot.LatencyBuckets {
				buckets[upperBound.Seconds()] = count
			}
			ch <- prometheus.MustNewConstHistogram(c.probeLatency, snapshot.LatencyCount, snapshot.LatencySum.Seconds(), buckets, labels...)
		}
		if snapshot.LastLocation.Source != "" && snapshot.LastLocation.Country != "" {
			locationLabels := append(labels, snapshot.LastLocation.Source, snapshot.LastLocation.Country)
			ch <- prometheus.MustNewConstMetric(c.probeLocationInfo, prometheus.GaugeValue, 1, locationLabels...)
			if !snapshot.LastLocation.ObservedAt.IsZero() {
				ch <- prometheus.MustNewConstMetric(c.probeLocationObserved, prometheus.GaugeValue, float64(snapshot.LastLocation.ObservedAt.Unix()), locationLabels...)
			}
		}
		ch <- prometheus.MustNewConstMetric(c.probeWindowSamples, prometheus.GaugeValue, float64(snapshot.WindowSamples), labels...)
		ch <- prometheus.MustNewConstMetric(c.probeWindowFailures, prometheus.GaugeValue, float64(snapshot.WindowFailures), labels...)
		if snapshot.WindowSamples > 0 {
			var successes uint64
			if snapshot.WindowFailures < snapshot.WindowSamples {
				successes = snapshot.WindowSamples - snapshot.WindowFailures
			}
			ch <- prometheus.MustNewConstMetric(c.probeWindowSuccessRatio, prometheus.GaugeValue, float64(successes)/float64(snapshot.WindowSamples), labels...)
			if successes > 0 {
				ch <- prometheus.MustNewConstMetric(c.probeWindowLatencyDeviation, prometheus.GaugeValue, snapshot.WindowLatencyDeviation.Seconds(), labels...)
				ch <- prometheus.MustNewConstMetric(c.probeWindowLatencyAverage, prometheus.GaugeValue, snapshot.WindowLatencyAverage.Seconds(), labels...)
				ch <- prometheus.MustNewConstMetric(c.probeWindowLatencyMaximum, prometheus.GaugeValue, snapshot.WindowLatencyMaximum.Seconds(), labels...)
				ch <- prometheus.MustNewConstMetric(c.probeWindowLatencyMinimum, prometheus.GaugeValue, snapshot.WindowLatencyMinimum.Seconds(), labels...)
			}
		}
	}
}

func (c *observatoryCollector) collectBaselines(ch chan<- prometheus.Metric) {
	instance := core.FromContext(c.handler.ctx)
	if instance == nil {
		return
	}
	provider, ok := instance.GetFeature(extension.ObservatoryType()).(extension.ProbeBaselineSnapshotProvider)
	if !ok {
		return
	}
	snapshots, err := provider.GetProbeBaselineSnapshots(context.Background())
	if err != nil {
		return
	}
	for _, snapshot := range snapshots {
		labels := []string{snapshot.ProbeTag, snapshot.Provider}
		refreshUp := 0.0
		if snapshot.LastRefreshSuccess {
			refreshUp = 1
		}
		ch <- prometheus.MustNewConstMetric(c.baselineRefreshUp, prometheus.GaugeValue, refreshUp, labels...)
		if !snapshot.LastAttempt.IsZero() {
			ch <- prometheus.MustNewConstMetric(c.baselineLastRefresh, prometheus.GaugeValue, float64(snapshot.LastAttempt.Unix()), labels...)
		}
	}
}

func (c *observatoryCollector) observationSnapshots() ([]extension.ProbeSnapshot, bool) {
	instance := core.FromContext(c.handler.ctx)
	if instance == nil {
		return nil, false
	}
	feature := instance.GetFeature(extension.ObservatoryType())
	observatoryFeature, ok := feature.(extension.Observatory)
	if !ok {
		return nil, false
	}
	if provider, ok := observatoryFeature.(extension.ProbeSnapshotProvider); ok {
		snapshots, err := provider.GetProbeSnapshots(context.Background(), extension.ProbeSnapshotFilter{})
		return snapshots, err == nil
	}
	message, err := observatoryFeature.GetObservation(context.Background())
	if err != nil {
		return nil, false
	}
	result, ok := message.(*observatory.ObservationResult)
	if !ok {
		return nil, false
	}
	snapshots := make([]extension.ProbeSnapshot, 0, len(result.Status))
	for _, item := range result.Status {
		if item == nil {
			continue
		}
		state := extension.ProbeStateUnhealthy
		if item.Alive {
			state = extension.ProbeStateHealthy
		}
		snapshot := extension.ProbeSnapshot{
			ProbeTag: legacyProbeTag, OutboundTag: item.OutboundTag, Method: legacyProbeMethod,
			PolicyState: state, EffectiveState: state,
			Latest: extension.ProbeSample{
				Success: item.Alive, CheckedAt: time.Unix(item.LastTryTime, 0),
				Duration: time.Duration(item.Delay) * time.Millisecond,
				TTFB:     time.Duration(item.Delay) * time.Millisecond,
			},
			LastSuccess: time.Unix(item.LastSeenTime, 0),
		}
		if item.HealthPing != nil {
			snapshot.WindowSamples = uint64(item.HealthPing.All)
			snapshot.WindowFailures = uint64(item.HealthPing.Fail)
			snapshot.WindowLatencyDeviation = time.Duration(item.HealthPing.Deviation)
			snapshot.WindowLatencyAverage = time.Duration(item.HealthPing.Average)
			snapshot.WindowLatencyMaximum = time.Duration(item.HealthPing.Max)
			snapshot.WindowLatencyMinimum = time.Duration(item.HealthPing.Min)
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, true
}
