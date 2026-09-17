package multiobservatory

import "time"

const (
	ObserverTypeHealth  = "health"
	ObserverTypeDefault = "default"
	ObserverTypeBurst   = "burst"

	ProbeMethodHTTP     = "http"
	ProbeMethodIP       = "ip"
	ProbeMethodDownload = "download"

	URLSchemeHTTP  = "http"
	URLSchemeHTTPS = "https"

	DefaultHealthProbeInterval           = 30 * time.Second
	DefaultHealthProbeTimeout            = 10 * time.Second
	DefaultHealthProbeConcurrency uint32 = 8
	DefaultAcceptedStatusMin      uint32 = 200
	DefaultAcceptedStatusMax      uint32 = 399
	DefaultHTTPProbeURL                  = "https://api.ipify.org?format=text"
	MinimumHTTPStatusCode         uint32 = 100
	MaximumHTTPStatusCode         uint32 = 599

	HealthPolicyTypeSlidingWindow = "slidingWindow"

	IPProbeProviderTypeCloudflareTrace = "cloudflareTrace"
	IPProbeProviderTypeCountryIs       = "countryIs"
	DefaultCloudflareTraceURL          = "https://www.cloudflare.com/cdn-cgi/trace"
	DefaultCountryIsURL                = "https://api.country.is/"

	MaxProbeBodyBytes int64 = 16 << 20

	probeStagePrepare   = "prepare"
	probeStageDispatch  = "dispatch"
	probeStageHandshake = "handshake"
	probeStageRequest   = "request"
	probeStageResponse  = "response"
	probeStageBody      = "body"
	probeStageValidate  = "validate"
	probeStageParse     = "parse"

	probeReasonInvalidRequest      = "invalid_request"
	probeReasonUnexpectedStatus    = "unexpected_status"
	probeReasonInsufficientBytes   = "insufficient_bytes"
	probeReasonBaselineUnavailable = "baseline_unavailable"
	probeReasonIPUnchanged         = "ip_unchanged"
	probeReasonInvalidIPResponse   = "invalid_ip_response"
	probeReasonUnsupportedMethod   = "unsupported_method"
	probeReasonTimeout             = "timeout"
	probeReasonCanceled            = "canceled"
	probeReasonIOError             = "io_error"
	probeReasonFailed              = "probe_failed"

	policyReasonNotProbed           = "not_probed"
	policyReasonInsufficientSamples = "insufficient_samples"
	policyReasonFailureThreshold    = "failure_threshold"
	policyReasonRecoveryThreshold   = "recovery_threshold"
	policyReasonHysteresis          = "hysteresis"

	defaultSlidingWindowSize              uint32 = 10
	maximumSlidingWindowSize              uint32 = 1024
	defaultSlidingWindowMinimumSamples    uint32 = 3
	defaultSlidingWindowFailureThreshold         = 0.5
	defaultSlidingWindowRecoveryThreshold        = 0.2

	defaultProbeHistoryLimit = int(maximumSlidingWindowSize)
	staleIntervalMultiplier  = 2
	directBaselineTTL        = 10 * time.Minute
	// Refresh halfway through the fresh TTL. A per-cycle jitter is applied by
	// Manager so multiple Xray instances do not refresh in lockstep.
	directBaselineRefreshInterval      = 5 * time.Minute
	directBaselineRefreshJitterDivisor = 5
	// directBaselineMaxAge bounds how long a failed refresh may reuse the
	// last successful value, measured from when that value was fetched.
	directBaselineMaxAge       = 30 * time.Minute
	maxIPResponseBytes   int64 = 1024
	minimumDownloadBytes int64 = 1
	probeUserAgentHeader       = "User-Agent"
	probeUserAgent             = "xray-observatory/1"
)
