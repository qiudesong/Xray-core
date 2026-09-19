package stats

import "strings"

const (
	trafficCounterSeparator = ">>>"
	trafficCounterType      = "traffic"

	TrafficDimensionInbound  = "inbound"
	TrafficDimensionOutbound = "outbound"
	TrafficDimensionRoute    = "route"
	TrafficDirectionUplink   = "uplink"
	TrafficDirectionDownlink = "downlink"
	TrafficLabelUnknown      = "unknown"
)

// TrafficCounter identifies a supported traffic counter stored by Manager.
// Tag is set for inbound and outbound counters. Inbound, Outbound, and Network
// are set for route counters.
type TrafficCounter struct {
	Dimension string
	Tag       string
	Inbound   string
	Outbound  string
	Network   string
	Direction string
}

// NormalizeTrafficLabel replaces an empty traffic label with a stable value.
func NormalizeTrafficLabel(value string) string {
	if value == "" {
		return TrafficLabelUnknown
	}
	return value
}

// RouteTrafficCounterName returns the internal counter name for one route.
func RouteTrafficCounterName(inbound, outbound, network, direction string) string {
	return strings.Join([]string{
		TrafficDimensionRoute,
		NormalizeTrafficLabel(inbound),
		NormalizeTrafficLabel(outbound),
		NormalizeTrafficLabel(network),
		trafficCounterType,
		direction,
	}, trafficCounterSeparator)
}

// ParseTrafficCounter parses the traffic counter formats understood by metrics.
func ParseTrafficCounter(name string) (TrafficCounter, bool) {
	parts := strings.Split(name, trafficCounterSeparator)
	switch {
	case len(parts) == 4 && parts[2] == trafficCounterType &&
		(parts[0] == TrafficDimensionInbound || parts[0] == TrafficDimensionOutbound) &&
		validTrafficDirection(parts[3]):
		return TrafficCounter{
			Dimension: parts[0],
			Tag:       parts[1],
			Direction: parts[3],
		}, true
	case len(parts) == 6 && parts[0] == TrafficDimensionRoute &&
		parts[4] == trafficCounterType && validTrafficDirection(parts[5]):
		return TrafficCounter{
			Dimension: TrafficDimensionRoute,
			Inbound:   parts[1],
			Outbound:  parts[2],
			Network:   parts[3],
			Direction: parts[5],
		}, true
	default:
		return TrafficCounter{}, false
	}
}

func validTrafficDirection(direction string) bool {
	return direction == TrafficDirectionUplink || direction == TrafficDirectionDownlink
}
