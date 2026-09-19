package multiobservatory

import "testing"

func TestBuiltInIPResponseParsers(t *testing.T) {
	tests := []struct {
		name        string
		parserType  string
		body        string
		wantIP      string
		wantCountry string
		wantSource  string
	}{
		{
			name: "cloudflare trace", parserType: IPProbeProviderTypeCloudflareTrace,
			body:   "fl=29f123\nip=203.0.113.7\nloc=cn\ncolo=SJC\n",
			wantIP: "203.0.113.7", wantCountry: "CN", wantSource: locationSourceCloudflare,
		},
		{
			name: "country is", parserType: IPProbeProviderTypeCountryIs,
			body:   `{"ip":"2001:db8::1","country":"us"}`,
			wantIP: "2001:db8::1", wantCountry: "US", wantSource: locationSourceCountryIs,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parser, err := buildIPResponseParser(test.parserType)
			if err != nil {
				t.Fatal(err)
			}
			observation, err := parser.Parse([]byte(test.body))
			if err != nil {
				t.Fatal(err)
			}
			if observation.address.String() != test.wantIP || observation.country != test.wantCountry || observation.source != test.wantSource {
				t.Fatalf("observation = %#v", observation)
			}
		})
	}
}

func TestIPResponseParsersRejectMissingOrInvalidValues(t *testing.T) {
	if _, err := buildIPResponseParser("unknown"); err == nil {
		t.Fatal("unknown IP response parser was accepted")
	}
	tests := []struct {
		parserType string
		body       string
	}{
		{IPProbeProviderTypeCloudflareTrace, "ip=not-an-ip\nloc=US\n"},
		{IPProbeProviderTypeCloudflareTrace, "ip=203.0.113.7\n"},
		{IPProbeProviderTypeCountryIs, `{"ip":"203.0.113.7","country":"United States"}`},
	}
	for _, test := range tests {
		parser, err := buildIPResponseParser(test.parserType)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := parser.Parse([]byte(test.body)); err == nil {
			t.Fatalf("parser %q accepted %q", test.parserType, test.body)
		}
	}
}

func TestResolveIPProbeProviderPresets(t *testing.T) {
	tests := []struct {
		name     string
		config   *IPProbeProviderConfig
		wantType string
		wantURL  string
	}{
		{name: "default", wantType: IPProbeProviderTypeCloudflareTrace, wantURL: DefaultCloudflareTraceURL},
		{name: "empty", config: &IPProbeProviderConfig{}, wantType: IPProbeProviderTypeCloudflareTrace, wantURL: DefaultCloudflareTraceURL},
		{name: "country is", config: &IPProbeProviderConfig{Type: IPProbeProviderTypeCountryIs}, wantType: IPProbeProviderTypeCountryIs, wantURL: DefaultCountryIsURL},
		{name: "compatible override", config: &IPProbeProviderConfig{Type: IPProbeProviderTypeCloudflareTrace, Url: "https://trace.example.invalid/"}, wantType: IPProbeProviderTypeCloudflareTrace, wantURL: "https://trace.example.invalid/"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := ResolveIPProbeProvider(test.config)
			if err != nil {
				t.Fatal(err)
			}
			if resolved.Type != test.wantType || resolved.Url != test.wantURL {
				t.Fatalf("provider = %#v, want type=%q url=%q", resolved, test.wantType, test.wantURL)
			}
		})
	}
}

func TestResolveIPProbeProviderRejectsInvalidConfig(t *testing.T) {
	for _, config := range []*IPProbeProviderConfig{
		{Type: "unknown"},
		{Url: "https://trace.example.invalid/"},
		{Type: IPProbeProviderTypeCloudflareTrace, Url: "relative"},
	} {
		if _, err := ResolveIPProbeProvider(config); err == nil {
			t.Fatalf("invalid provider was accepted: %#v", config)
		}
	}
}
