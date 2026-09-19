package metrics

import (
	"sort"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/xtls/xray-core/common/session"
)

type accessCollector struct {
	handler                      *MetricsHandler
	accessRequests               *prometheus.Desc
	accessWindowRequests         *prometheus.Desc
	accessUniqueUsers            *prometheus.Desc
	accessPeerCountryConnections *prometheus.Desc
	accessPeerASNConnections     *prometheus.Desc
	accessPeerCityConnections    *prometheus.Desc
	accessEventsDropped          *prometheus.Desc
	accessPeerEventsDropped      *prometheus.Desc
	accessRequestSeriesDropped   *prometheus.Desc
	accessPeerASNSeriesDropped   *prometheus.Desc
	accessPeerCitySeriesDropped  *prometheus.Desc
	accessUserTrackingDropped    *prometheus.Desc
}

func newAccessCollector(handler *MetricsHandler) *accessCollector {
	return &accessCollector{
		handler: handler,
		accessRequests: prometheus.NewDesc(
			"xray_access_requests_total", "Access requests observed since Xray started.",
			[]string{"from", metricInbound, metricOutbound, "to", metricNetwork, "status"}, nil,
		),
		accessWindowRequests: prometheus.NewDesc(
			"xray_access_requests_window", "Access requests observed in the configured bucketed sliding window.",
			[]string{"status"}, nil,
		),
		accessUniqueUsers: prometheus.NewDesc(
			"xray_access_unique_authenticated_users_window", "Unique authenticated users with accepted requests in the configured sliding window.",
			nil, nil,
		),
		accessPeerCountryConnections: prometheus.NewDesc(
			"xray_access_peer_country_connections_total", "Established peer connections by country since Xray started.",
			[]string{"country", "side", "tag", metricNetwork}, nil,
		),
		accessPeerASNConnections: prometheus.NewDesc(
			"xray_access_peer_asn_connections_total", "Established peer connections by autonomous system number and organization since Xray started.",
			[]string{"asn", "org", "side", "tag", metricNetwork}, nil,
		),
		accessPeerCityConnections: prometheus.NewDesc(
			"xray_access_peer_city_connections_total", "Established peer connections by city since Xray started.",
			[]string{"country", "city", "side", "tag", metricNetwork}, nil,
		),
		accessEventsDropped: prometheus.NewDesc(
			"xray_access_events_dropped_total", "Structured access events dropped because the metrics queue was full.",
			nil, nil,
		),
		accessPeerEventsDropped: prometheus.NewDesc(
			"xray_access_peer_events_dropped_total", "Peer connection events dropped because the metrics queue was full.",
			[]string{"side"}, nil,
		),
		accessRequestSeriesDropped: prometheus.NewDesc(
			"xray_access_request_series_dropped_total", "Access requests not added to a new label series because the in-memory cardinality limit was reached.",
			nil, nil,
		),
		accessPeerASNSeriesDropped: prometheus.NewDesc(
			"xray_access_peer_asn_series_dropped_total", "Peer connections not added to a new ASN label series because the in-memory cardinality limit was reached.",
			[]string{"side"}, nil,
		),
		accessPeerCitySeriesDropped: prometheus.NewDesc(
			"xray_access_peer_city_series_dropped_total", "Peer connections not added to a new city label series because the in-memory cardinality limit was reached.",
			[]string{"side"}, nil,
		),
		accessUserTrackingDropped: prometheus.NewDesc(
			"xray_access_user_tracking_dropped_total", "Authenticated users not tracked because the in-memory cardinality limit was reached.",
			nil, nil,
		),
	}
}

func (c *accessCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, description := range []*prometheus.Desc{
		c.accessRequests, c.accessWindowRequests, c.accessUniqueUsers,
		c.accessPeerCountryConnections, c.accessPeerASNConnections,
		c.accessPeerCityConnections,
		c.accessEventsDropped, c.accessPeerEventsDropped,
		c.accessRequestSeriesDropped, c.accessPeerASNSeriesDropped,
		c.accessPeerCitySeriesDropped,
		c.accessUserTrackingDropped,
	} {
		ch <- description
	}
}

func (c *accessCollector) Collect(ch chan<- prometheus.Metric) {
	if c.handler.access == nil {
		return
	}
	snapshot := c.handler.access.snapshot(time.Now())
	sort.Slice(snapshot.requests, func(i, j int) bool {
		left, right := snapshot.requests[i].labels, snapshot.requests[j].labels
		if left.from != right.from {
			return left.from < right.from
		}
		if left.inbound != right.inbound {
			return left.inbound < right.inbound
		}
		if left.outbound != right.outbound {
			return left.outbound < right.outbound
		}
		if left.to != right.to {
			return left.to < right.to
		}
		if left.network != right.network {
			return left.network < right.network
		}
		return left.status < right.status
	})
	for _, sample := range snapshot.requests {
		ch <- prometheus.MustNewConstMetric(
			c.accessRequests, prometheus.CounterValue, float64(sample.value),
			sample.labels.from, sample.labels.inbound, sample.labels.outbound,
			sample.labels.to, sample.labels.network, sample.labels.status,
		)
	}

	statuses := make([]string, 0, len(snapshot.windowRequests))
	for status := range snapshot.windowRequests {
		statuses = append(statuses, status)
	}
	sort.Strings(statuses)
	for _, status := range statuses {
		ch <- prometheus.MustNewConstMetric(c.accessWindowRequests, prometheus.GaugeValue, float64(snapshot.windowRequests[status]), status)
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
			c.accessPeerCountryConnections, prometheus.CounterValue, float64(sample.value),
			sample.labels.country, sample.labels.side,
			sample.labels.tag, sample.labels.network,
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
			c.accessPeerASNConnections, prometheus.CounterValue, float64(sample.value),
			sample.labels.asn, sample.labels.org, sample.labels.side,
			sample.labels.tag, sample.labels.network,
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
			c.accessPeerCityConnections, prometheus.CounterValue, float64(sample.value),
			sample.labels.country, sample.labels.city, sample.labels.side,
			sample.labels.tag, sample.labels.network,
		)
	}

	ch <- prometheus.MustNewConstMetric(c.accessUniqueUsers, prometheus.GaugeValue, float64(snapshot.uniqueUsers))
	ch <- prometheus.MustNewConstMetric(c.accessEventsDropped, prometheus.CounterValue, float64(snapshot.eventsDropped))
	ch <- prometheus.MustNewConstMetric(c.accessRequestSeriesDropped, prometheus.CounterValue, float64(snapshot.requestSeriesDropped))
	for _, side := range []session.PeerSide{session.PeerSideInbound, session.PeerSideOutbound} {
		ch <- prometheus.MustNewConstMetric(c.accessPeerEventsDropped, prometheus.CounterValue, float64(snapshot.peerEventsDropped.value(side)), string(side))
		if snapshot.asnEnabled {
			ch <- prometheus.MustNewConstMetric(c.accessPeerASNSeriesDropped, prometheus.CounterValue, float64(snapshot.peerASNSeriesDropped.value(side)), string(side))
		}
		if snapshot.cityEnabled {
			ch <- prometheus.MustNewConstMetric(c.accessPeerCitySeriesDropped, prometheus.CounterValue, float64(snapshot.peerCitySeriesDropped.value(side)), string(side))
		}
	}
	ch <- prometheus.MustNewConstMetric(c.accessUserTrackingDropped, prometheus.CounterValue, float64(snapshot.userTrackingDropped))
}
