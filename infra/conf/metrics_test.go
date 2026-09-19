package conf

import (
	"testing"
	"time"

	"github.com/xtls/xray-core/infra/conf/cfgcommon/duration"
)

func TestMetricsAccessConfigBuild(t *testing.T) {
	config, err := (&MetricsConfig{
		Listen: "127.0.0.1:11111",
		Access: &AccessMetricsConfig{
			Enabled:     true,
			Window:      duration.Duration(10 * time.Minute),
			QueueSize:   8192,
			IncludeFrom: true,
			IncludeTo:   true,
		},
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	if config.GetAccess() == nil {
		t.Fatal("access metrics config is missing")
	}
	if !config.GetAccess().GetEnabled() {
		t.Fatal("access metrics are not enabled")
	}
	if got, want := time.Duration(config.GetAccess().GetWindow()), 10*time.Minute; got != want {
		t.Fatalf("unexpected window: got %s, want %s", got, want)
	}
	if got, want := config.GetAccess().GetQueueSize(), uint32(8192); got != want {
		t.Fatalf("unexpected queue size: got %d, want %d", got, want)
	}
	if !config.GetAccess().GetIncludeFrom() || !config.GetAccess().GetIncludeTo() {
		t.Fatal("access address dimensions were not enabled")
	}
}

func TestMetricsAccessConfigAddressDimensionsDefaultToDisabled(t *testing.T) {
	config, err := (&MetricsConfig{
		Listen: "127.0.0.1:11111",
		Access: &AccessMetricsConfig{Enabled: true},
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	if config.GetAccess() == nil || config.GetAccess().GetIncludeFrom() || config.GetAccess().GetIncludeTo() {
		t.Fatalf("unexpected default access config: %+v", config.GetAccess())
	}
}

func TestMetricsAccessConfigDisabledByDefault(t *testing.T) {
	config, err := (&MetricsConfig{Listen: "127.0.0.1:11111"}).Build()
	if err != nil {
		t.Fatal(err)
	}
	if config.GetAccess() != nil {
		t.Fatal("access metrics config should be absent by default")
	}
}
