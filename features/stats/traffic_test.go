package stats

import "testing"

func TestRouteTrafficCounterRoundTrip(t *testing.T) {
	name := RouteTrafficCounterName("in", "out", "tcp", TrafficDirectionUplink)
	counter, ok := ParseTrafficCounter(name)
	if !ok {
		t.Fatalf("failed to parse %q", name)
	}
	if counter.Dimension != TrafficDimensionRoute || counter.Inbound != "in" ||
		counter.Outbound != "out" || counter.Network != "tcp" ||
		counter.Direction != TrafficDirectionUplink {
		t.Fatalf("unexpected parsed counter: %+v", counter)
	}
}

func TestRouteTrafficCounterNormalizesEmptyLabels(t *testing.T) {
	name := RouteTrafficCounterName("", "", "", TrafficDirectionDownlink)
	counter, ok := ParseTrafficCounter(name)
	if !ok {
		t.Fatalf("failed to parse %q", name)
	}
	if counter.Inbound != TrafficLabelUnknown || counter.Outbound != TrafficLabelUnknown || counter.Network != TrafficLabelUnknown {
		t.Fatalf("empty labels were not normalized: %+v", counter)
	}
}

func TestParseTrafficCounterRejectsUnsupportedNames(t *testing.T) {
	for _, name := range []string{
		"user>>>alice>>>traffic>>>uplink",
		"inbound>>>tag>>>connections>>>uplink",
		"route>>>in>>>out>>>tcp>>>traffic>>>unknown",
		"route>>>in>>>out>>>tcp>>>traffic",
	} {
		if _, ok := ParseTrafficCounter(name); ok {
			t.Errorf("unexpectedly parsed %q", name)
		}
	}
}
