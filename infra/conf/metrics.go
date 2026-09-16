package conf

import (
	"strings"

	"github.com/xtls/xray-core/app/metrics"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/infra/conf/cfgcommon/duration"
)

type MetricsConfig struct {
	Tag    string               `json:"tag"`
	Listen string               `json:"listen"`
	Access *AccessMetricsConfig `json:"access,omitempty"`
}

type AccessMetricsConfig struct {
	Enabled   bool              `json:"enabled"`
	Window    duration.Duration `json:"window,omitempty"`
	QueueSize uint32            `json:"queueSize,omitempty"`
	Role      string            `json:"role,omitempty"`
}

func (c *MetricsConfig) Build() (*metrics.Config, error) {
	if c.Listen == "" && c.Tag == "" {
		return nil, errors.New("Metrics must have a tag or listen address.")
	}
	// If the tag is empty but have "listen" set a default "Metrics" for compatibility.
	if c.Tag == "" {
		c.Tag = "Metrics"
	}

	config := &metrics.Config{
		Tag:    c.Tag,
		Listen: c.Listen,
	}
	if c.Access != nil {
		role := strings.ToLower(strings.TrimSpace(c.Access.Role))
		if c.Access.Enabled && role != "server" && role != "client" {
			return nil, errors.New("metrics access role is required and must be server or client")
		}
		config.Access = &metrics.AccessMetricsConfig{
			Enabled:   c.Access.Enabled,
			Window:    int64(c.Access.Window),
			QueueSize: c.Access.QueueSize,
			Role:      role,
		}
	}
	return config, nil
}
