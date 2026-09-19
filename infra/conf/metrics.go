package conf

import (
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
	Enabled     bool              `json:"enabled"`
	Window      duration.Duration `json:"window,omitempty"`
	QueueSize   uint32            `json:"queueSize,omitempty"`
	IncludeFrom bool              `json:"includeFrom,omitempty"`
	IncludeTo   bool              `json:"includeTo,omitempty"`
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
		config.Access = &metrics.AccessMetricsConfig{
			Enabled:     c.Access.Enabled,
			Window:      int64(c.Access.Window),
			QueueSize:   c.Access.QueueSize,
			IncludeFrom: c.Access.IncludeFrom,
			IncludeTo:   c.Access.IncludeTo,
		}
	}
	return config, nil
}
