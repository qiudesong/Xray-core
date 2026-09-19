package router

import (
	"testing"

	"github.com/xtls/xray-core/common/serial"
)

func TestBalancingRuleBuildRejectsInvalidStrategySettings(t *testing.T) {
	tests := []struct {
		name         string
		strategy     string
		settingsType string
	}{
		{
			name:         "leastping",
			strategy:     "leastping",
			settingsType: serial.GetMessageType(&StrategyLeastPingConfig{}),
		},
		{
			name:         "roundrobin",
			strategy:     "roundrobin",
			settingsType: serial.GetMessageType(&StrategyRoundRobinConfig{}),
		},
		{
			name:         "random",
			strategy:     "random",
			settingsType: serial.GetMessageType(&StrategyRandomConfig{}),
		},
		{
			name:         "default_random",
			strategy:     "",
			settingsType: serial.GetMessageType(&StrategyRandomConfig{}),
		},
	}

	for _, test := range tests {
		t.Run(test.name+"/decode_error", func(t *testing.T) {
			rule := &BalancingRule{
				Strategy: test.strategy,
				StrategySettings: &serial.TypedMessage{
					Type:  test.settingsType,
					Value: []byte{0xff},
				},
			}

			if _, err := rule.Build(nil, nil); err == nil {
				t.Fatal("invalid serialized strategy settings were accepted")
			}
		})

		t.Run(test.name+"/type_error", func(t *testing.T) {
			rule := &BalancingRule{
				Strategy:         test.strategy,
				StrategySettings: serial.ToTypedMessage(&StrategyLeastLoadConfig{}),
			}

			if _, err := rule.Build(nil, nil); err == nil {
				t.Fatal("strategy settings with the wrong protobuf type were accepted")
			}
		})
	}
}
