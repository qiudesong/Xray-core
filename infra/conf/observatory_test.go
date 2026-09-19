package conf

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/multiobservatory"
	"github.com/xtls/xray-core/infra/conf/cfgcommon/duration"
)

func TestMultiObservatoryHealthConfigDefaults(t *testing.T) {
	settings := json.RawMessage(`{
		"subjectSelector":["proxy-"],
		"method":"http"
	}`)
	message, err := (&MultiObservatoryConfig{Observers: []MultiObservatoryItem{{
		Type: "health", Tag: "health-http", Settings: settings,
	}}}).Build()
	if err != nil {
		t.Fatal(err)
	}
	config := message.(*multiobservatory.Config)
	healthMessage, err := config.Observers[0].Settings.GetInstance()
	if err != nil {
		t.Fatal(err)
	}
	health := healthMessage.(*multiobservatory.HealthConfig)
	if health.Interval != int64(30*time.Second) || health.Timeout != int64(10*time.Second) || health.Concurrency != 8 {
		t.Fatalf("unexpected health defaults: %#v", health)
	}
	if health.Url != multiobservatory.DefaultHTTPProbeURL {
		t.Fatalf("default HTTP probe URL = %q", health.Url)
	}
	if health.Policy == nil || health.Policy.Type != "slidingWindow" || health.Policy.Settings == nil {
		t.Fatalf("unexpected policy config: %#v", health)
	}
	policySettings, err := health.Policy.Settings.GetInstance()
	if err != nil {
		t.Fatal(err)
	}
	if settings, ok := policySettings.(*multiobservatory.SlidingWindowPolicyConfig); !ok ||
		settings.WindowSize != nil || settings.MinimumSamples != nil ||
		settings.FailureThreshold != nil || settings.RecoveryThreshold != nil {
		t.Fatalf("unexpected slidingWindow settings: %#v", policySettings)
	}
}

func TestMultiObservatoryPreservesExplicitHTTPURL(t *testing.T) {
	const explicitURL = "https://example.invalid/generate_204"
	settings := json.RawMessage(`{
		"subjectSelector":["proxy-"],
		"method":"http",
		"url":"` + explicitURL + `"
	}`)
	message, err := (&MultiObservatoryConfig{Observers: []MultiObservatoryItem{{
		Type: "health", Tag: "health-http", Settings: settings,
	}}}).Build()
	if err != nil {
		t.Fatal(err)
	}
	healthMessage, err := message.(*multiobservatory.Config).Observers[0].Settings.GetInstance()
	if err != nil {
		t.Fatal(err)
	}
	if got := healthMessage.(*multiobservatory.HealthConfig).Url; got != explicitURL {
		t.Fatalf("HTTP probe URL = %q, want %q", got, explicitURL)
	}
}

func TestMultiObservatoryRequiresDownloadURL(t *testing.T) {
	settings := json.RawMessage(`{"subjectSelector":["proxy-"],"method":"download"}`)
	if _, err := (&MultiObservatoryConfig{Observers: []MultiObservatoryItem{{
		Type: "health", Tag: "health-download", Settings: settings,
	}}}).Build(); err == nil {
		t.Fatal("download observer without URL was accepted")
	}
}

func TestMultiObservatoryBuildsStronglyTypedPolicySettings(t *testing.T) {
	settings := json.RawMessage(`{
		"subjectSelector":["proxy-"],
		"method":"http",
		"url":"https://example.invalid/generate_204",
		"policy":{"type":"slidingWindow","settings":{
			"windowSize":12,
			"minimumSamples":4,
			"failureThreshold":0.6,
			"recoveryThreshold":0.1
		}}
	}`)
	message, err := (&MultiObservatoryConfig{Observers: []MultiObservatoryItem{{
		Type: "health", Tag: "health-http", Settings: settings,
	}}}).Build()
	if err != nil {
		t.Fatal(err)
	}
	config := message.(*multiobservatory.Config)
	healthMessage, err := config.Observers[0].Settings.GetInstance()
	if err != nil {
		t.Fatal(err)
	}
	policyMessage, err := healthMessage.(*multiobservatory.HealthConfig).Policy.Settings.GetInstance()
	if err != nil {
		t.Fatal(err)
	}
	policy, ok := policyMessage.(*multiobservatory.SlidingWindowPolicyConfig)
	if !ok || policy.GetWindowSize() != 12 || policy.GetMinimumSamples() != 4 ||
		policy.GetFailureThreshold() != 0.6 || policy.GetRecoveryThreshold() != 0.1 {
		t.Fatalf("unexpected typed policy settings: %#v", policyMessage)
	}
}

func TestMultiObservatoryBuildsIPProviderPresets(t *testing.T) {
	for _, test := range []struct {
		name         string
		providerJSON string
		wantType     string
		wantURL      string
	}{
		{name: "default cloudflare", wantType: multiobservatory.IPProbeProviderTypeCloudflareTrace, wantURL: multiobservatory.DefaultCloudflareTraceURL},
		{name: "country is", providerJSON: `,"ipProvider":{"type":"countryIs"}`, wantType: multiobservatory.IPProbeProviderTypeCountryIs, wantURL: multiobservatory.DefaultCountryIsURL},
		{name: "compatible override", providerJSON: `,"ipProvider":{"type":"cloudflareTrace","url":"https://trace.example.invalid/"}`, wantType: multiobservatory.IPProbeProviderTypeCloudflareTrace, wantURL: "https://trace.example.invalid/"},
	} {
		t.Run(test.name, func(t *testing.T) {
			settings := json.RawMessage(`{"subjectSelector":["proxy-"],"method":"ip"` + test.providerJSON + `}`)
			message, err := (&MultiObservatoryConfig{Observers: []MultiObservatoryItem{{
				Type: "health", Tag: "health-ip", Settings: settings,
			}}}).Build()
			if err != nil {
				t.Fatal(err)
			}
			config := message.(*multiobservatory.Config)
			healthMessage, err := config.Observers[0].Settings.GetInstance()
			if err != nil {
				t.Fatal(err)
			}
			provider := healthMessage.(*multiobservatory.HealthConfig).IpProvider
			if provider == nil || provider.Type != test.wantType || provider.Url != test.wantURL {
				t.Fatalf("IP provider = %#v, want type=%q url=%q", provider, test.wantType, test.wantURL)
			}
		})
	}
}

func TestMultiObservatoryRejectsInvalidIPProviderPlacement(t *testing.T) {
	for _, settings := range []json.RawMessage{
		json.RawMessage(`{"subjectSelector":["proxy-"],"method":"ip","url":"https://example.invalid"}`),
		json.RawMessage(`{"subjectSelector":["proxy-"],"method":"ip","ipProvider":{"url":"https://example.invalid"}}`),
		json.RawMessage(`{"subjectSelector":["proxy-"],"method":"http","url":"https://example.invalid","ipProvider":{"type":"cloudflareTrace"}}`),
	} {
		if _, err := (&MultiObservatoryConfig{Observers: []MultiObservatoryItem{{
			Type: "health", Tag: "health-ip", Settings: settings,
		}}}).Build(); err == nil {
			t.Fatalf("invalid IP provider config was accepted: %s", settings)
		}
	}
}

func TestMultiObservatoryRejectsDuplicateTagsAndInvalidPolicy(t *testing.T) {
	valid := json.RawMessage(`{"subjectSelector":["proxy-"],"method":"http","url":"https://example.invalid"}`)
	if _, err := (&MultiObservatoryConfig{Observers: []MultiObservatoryItem{
		{Type: "health", Tag: "same", Settings: valid},
		{Type: "health", Tag: "same", Settings: valid},
	}}).Build(); err == nil {
		t.Fatal("duplicate observer tags were accepted")
	}
	invalid := json.RawMessage(`{
		"subjectSelector":["proxy-"],"method":"http","url":"https://example.invalid",
		"policy":{"type":"slidingWindow","settings":{"windowSize":2,"minimumSamples":3}}
	}`)
	if _, err := (&MultiObservatoryConfig{Observers: []MultiObservatoryItem{{
		Type: "health", Tag: "health", Settings: invalid,
	}}}).Build(); err == nil {
		t.Fatal("invalid policy was accepted")
	}
	oversized := json.RawMessage(`{
		"subjectSelector":["proxy-"],"method":"http","url":"https://example.invalid",
		"policy":{"type":"slidingWindow","settings":{"windowSize":1025}}
	}`)
	if _, err := (&MultiObservatoryConfig{Observers: []MultiObservatoryItem{{
		Type: "health", Tag: "health", Settings: oversized,
	}}}).Build(); err == nil {
		t.Fatal("oversized sliding window was accepted")
	}
}

func TestTopLevelObservatoriesAreMutuallyExclusive(t *testing.T) {
	config := &Config{
		Observatory: &ObservatoryConfig{SubjectSelector: []string{"a"}, ProbeInterval: duration.Duration(time.Second)},
		MultiObservatory: &MultiObservatoryConfig{Observers: []MultiObservatoryItem{{
			Type: "health", Tag: "health", Settings: json.RawMessage(`{"subjectSelector":["a"],"method":"http","url":"https://example.invalid"}`),
		}}},
	}
	if _, err := config.Build(); err == nil {
		t.Fatal("mutually exclusive observatories were accepted")
	}
}
