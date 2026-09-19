package conf

import (
	"encoding/json"
	"net/url"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/app/observatory/burst"
	"github.com/xtls/xray-core/app/observatory/multiobservatory"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/infra/conf/cfgcommon/duration"
)

type ObservatoryConfig struct {
	SubjectSelector   []string          `json:"subjectSelector"`
	ProbeURL          string            `json:"probeURL"`
	ProbeInterval     duration.Duration `json:"probeInterval"`
	EnableConcurrency bool              `json:"enableConcurrency"`
}

func (o *ObservatoryConfig) Build() (proto.Message, error) {
	return &observatory.Config{SubjectSelector: o.SubjectSelector, ProbeUrl: o.ProbeURL, ProbeInterval: int64(o.ProbeInterval), EnableConcurrency: o.EnableConcurrency}, nil
}

type BurstObservatoryConfig struct {
	SubjectSelector []string `json:"subjectSelector"`
	// health check settings
	HealthCheck *HealthCheckSettings `json:"pingConfig,omitempty"`
}

func (b BurstObservatoryConfig) Build() (proto.Message, error) {
	if b.HealthCheck == nil {
		return nil, errors.New("BurstObservatory requires a valid pingConfig")
	}
	if result, err := b.HealthCheck.Build(); err == nil {
		return &burst.Config{SubjectSelector: b.SubjectSelector, PingConfig: result.(*burst.HealthPingConfig)}, nil
	} else {
		return nil, err
	}
}

type MultiObservatoryItem struct {
	Type     string          `json:"type"`
	Tag      string          `json:"tag"`
	Settings json.RawMessage `json:"settings"`
}

type MultiObservatoryConfig struct {
	Observers []MultiObservatoryItem `json:"observers"`
}

type HealthPolicyConfig struct {
	Type     string          `json:"type"`
	Settings json.RawMessage `json:"settings"`
}

type SlidingWindowPolicyConfig struct {
	WindowSize        *uint32  `json:"windowSize"`
	MinimumSamples    *uint32  `json:"minimumSamples"`
	FailureThreshold  *float64 `json:"failureThreshold"`
	RecoveryThreshold *float64 `json:"recoveryThreshold"`
}

type IPProbeProviderConfig struct {
	Type string `json:"type"`
	URL  string `json:"url"`
}

type HealthObservatoryConfig struct {
	SubjectSelector   []string               `json:"subjectSelector"`
	Method            string                 `json:"method"`
	URL               string                 `json:"url"`
	Interval          duration.Duration      `json:"interval"`
	Timeout           duration.Duration      `json:"timeout"`
	Concurrency       uint32                 `json:"concurrency"`
	AcceptedStatusMin uint32                 `json:"acceptedStatusMin"`
	AcceptedStatusMax uint32                 `json:"acceptedStatusMax"`
	MinBytes          int64                  `json:"minBytes"`
	Policy            HealthPolicyConfig     `json:"policy"`
	IPProvider        *IPProbeProviderConfig `json:"ipProvider,omitempty"`
}

func (o *MultiObservatoryConfig) Build() (proto.Message, error) {
	if len(o.Observers) == 0 {
		return nil, errors.New("multiObservatory requires at least one observer")
	}
	result := &multiobservatory.Config{}
	seen := make(map[string]struct{}, len(o.Observers))
	for _, item := range o.Observers {
		tag := strings.TrimSpace(item.Tag)
		if tag == "" {
			return nil, errors.New("multiObservatory observer tag is required")
		}
		if _, found := seen[tag]; found {
			return nil, errors.New("duplicate multiObservatory observer tag: ", tag)
		}
		seen[tag] = struct{}{}

		observerType := strings.ToLower(strings.TrimSpace(item.Type))
		var settings proto.Message
		switch observerType {
		case multiobservatory.ObserverTypeDefault:
			var cfg ObservatoryConfig
			if err := json.Unmarshal(item.Settings, &cfg); err != nil {
				return nil, errors.New("invalid default observer settings for ", tag).Base(err)
			}
			built, err := cfg.Build()
			if err != nil {
				return nil, err
			}
			settings = built
		case multiobservatory.ObserverTypeBurst:
			var cfg BurstObservatoryConfig
			if err := json.Unmarshal(item.Settings, &cfg); err != nil {
				return nil, errors.New("invalid burst observer settings for ", tag).Base(err)
			}
			built, err := cfg.Build()
			if err != nil {
				return nil, err
			}
			settings = built
		case multiobservatory.ObserverTypeHealth:
			built, err := buildHealthObservatory(item.Settings)
			if err != nil {
				return nil, errors.New("invalid health observer settings for ", tag).Base(err)
			}
			settings = built
		default:
			return nil, errors.New("unknown multiObservatory observer type: ", item.Type)
		}
		result.Observers = append(result.Observers, &multiobservatory.ObserverConfig{
			Tag: tag, Type: observerType, Settings: serial.ToTypedMessage(settings),
		})
	}
	return result, nil
}

func buildHealthObservatory(raw json.RawMessage) (*multiobservatory.HealthConfig, error) {
	var cfg HealthObservatoryConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	method := strings.ToLower(strings.TrimSpace(cfg.Method))
	if method != multiobservatory.ProbeMethodHTTP && method != multiobservatory.ProbeMethodIP && method != multiobservatory.ProbeMethodDownload {
		return nil, errors.New("health method must be http, ip, or download")
	}
	if len(cfg.SubjectSelector) == 0 {
		return nil, errors.New("health subjectSelector is required")
	}
	probeURL := strings.TrimSpace(cfg.URL)
	var ipProvider *multiobservatory.IPProbeProviderConfig
	if method == multiobservatory.ProbeMethodIP {
		if probeURL != "" {
			return nil, errors.New("health url is configured through ipProvider for the ip method")
		}
		var configuredProvider *multiobservatory.IPProbeProviderConfig
		if cfg.IPProvider != nil {
			configuredProvider = &multiobservatory.IPProbeProviderConfig{
				Type: cfg.IPProvider.Type,
				Url:  cfg.IPProvider.URL,
			}
		}
		resolved, err := multiobservatory.ResolveIPProbeProvider(configuredProvider)
		if err != nil {
			return nil, err
		}
		ipProvider = resolved
	} else {
		if cfg.IPProvider != nil {
			return nil, errors.New("ipProvider is only valid for the ip method")
		}
		if method == multiobservatory.ProbeMethodHTTP && probeURL == "" {
			probeURL = multiobservatory.DefaultHTTPProbeURL
		}
		if probeURL == "" {
			return nil, errors.New("health url is required for the download method")
		}
		parsedURL, err := url.Parse(probeURL)
		if err != nil || parsedURL.Host == "" || (parsedURL.Scheme != multiobservatory.URLSchemeHTTP && parsedURL.Scheme != multiobservatory.URLSchemeHTTPS) {
			return nil, errors.New("health url must be an absolute HTTP or HTTPS URL")
		}
	}
	if cfg.MinBytes < 0 || cfg.MinBytes > multiobservatory.MaxProbeBodyBytes {
		return nil, errors.New("health minBytes must be between 0 and ", multiobservatory.MaxProbeBodyBytes)
	}
	policy, err := buildHealthPolicyConfig(cfg.Policy)
	if err != nil {
		return nil, err
	}
	interval := cfg.Interval
	if interval == 0 {
		interval = duration.Duration(multiobservatory.DefaultHealthProbeInterval)
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = duration.Duration(multiobservatory.DefaultHealthProbeTimeout)
	}
	if interval <= 0 || timeout <= 0 {
		return nil, errors.New("health interval and timeout must be positive")
	}
	concurrency := cfg.Concurrency
	if concurrency == 0 {
		concurrency = multiobservatory.DefaultHealthProbeConcurrency
	}
	statusMin, statusMax := cfg.AcceptedStatusMin, cfg.AcceptedStatusMax
	if statusMin == 0 {
		statusMin = multiobservatory.DefaultAcceptedStatusMin
	}
	if statusMax == 0 {
		statusMax = multiobservatory.DefaultAcceptedStatusMax
	}
	if statusMin < multiobservatory.MinimumHTTPStatusCode || statusMin > statusMax || statusMax > multiobservatory.MaximumHTTPStatusCode {
		return nil, errors.New("invalid accepted HTTP status range")
	}
	return &multiobservatory.HealthConfig{
		SubjectSelector: cfg.SubjectSelector, Method: method, Url: probeURL,
		Interval: int64(interval), Timeout: int64(timeout), Concurrency: concurrency,
		AcceptedStatusMin: statusMin, AcceptedStatusMax: statusMax, MinBytes: cfg.MinBytes,
		Policy: policy, IpProvider: ipProvider,
	}, nil
}

func buildHealthPolicyConfig(config HealthPolicyConfig) (*multiobservatory.HealthPolicyConfig, error) {
	policyType := strings.TrimSpace(config.Type)
	if policyType == "" {
		policyType = multiobservatory.HealthPolicyTypeSlidingWindow
	}
	var settings proto.Message
	switch policyType {
	case multiobservatory.HealthPolicyTypeSlidingWindow:
		var jsonConfig SlidingWindowPolicyConfig
		if len(config.Settings) > 0 {
			if err := json.Unmarshal(config.Settings, &jsonConfig); err != nil {
				return nil, errors.New("invalid slidingWindow policy settings").Base(err)
			}
		}
		settings = &multiobservatory.SlidingWindowPolicyConfig{
			WindowSize: jsonConfig.WindowSize, MinimumSamples: jsonConfig.MinimumSamples,
			FailureThreshold: jsonConfig.FailureThreshold, RecoveryThreshold: jsonConfig.RecoveryThreshold,
		}
	default:
		return nil, errors.New("unknown health policy type: ", policyType)
	}
	if err := multiobservatory.ValidateHealthPolicy(policyType, settings); err != nil {
		return nil, err
	}
	return &multiobservatory.HealthPolicyConfig{
		Type: policyType, Settings: serial.ToTypedMessage(settings),
	}, nil
}
