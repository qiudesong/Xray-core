package multiobservatory

import (
	"context"
	"crypto/tls"
	stderrors "errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/errors"
	v2net "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/extension"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport/internet/tagged"
)

type cachedBaseline struct {
	value     netip.Addr
	expiresAt time.Time
	staleAt   time.Time
}

type baselineKey struct {
	target     string
	parserType string
}

type checker struct {
	dispatcher routing.Dispatcher

	baselineMu sync.Mutex
	baselines  map[baselineKey]cachedBaseline
}

type probeChecker interface {
	check(ctx context.Context, outbound string, config *HealthConfig, parser ipResponseParser) extension.ProbeSample
}

type baselineMaintainer interface {
	prewarmBaseline(ctx context.Context, target string, parser ipResponseParser) error
	refreshBaseline(ctx context.Context, target string, parser ipResponseParser) error
}

func newChecker(dispatcher routing.Dispatcher) *checker {
	return &checker{dispatcher: dispatcher, baselines: make(map[baselineKey]cachedBaseline)}
}

func (c *checker) check(ctx context.Context, outbound string, config *HealthConfig, parser ipResponseParser) extension.ProbeSample {
	started := time.Now()
	checkCtx, cancel := context.WithTimeout(ctx, time.Duration(config.Timeout))
	defer cancel()
	target := config.Url
	if config.Method == ProbeMethodIP && config.IpProvider != nil {
		target = config.IpProvider.Url
	}

	var stage atomic.Value
	stage.Store(probeStageDispatch)
	var ttfbNanos atomic.Int64
	trace := &httptrace.ClientTrace{
		GotConn:           func(httptrace.GotConnInfo) { stage.Store(probeStageRequest) },
		TLSHandshakeStart: func() { stage.Store(probeStageHandshake) },
		TLSHandshakeDone: func(_ tls.ConnectionState, err error) {
			if err == nil {
				stage.Store(probeStageRequest)
			}
		},
		WroteRequest: func(httptrace.WroteRequestInfo) { stage.Store(probeStageResponse) },
		GotFirstResponseByte: func() {
			ttfbNanos.CompareAndSwap(0, time.Since(started).Nanoseconds())
			stage.Store(probeStageBody)
		},
	}
	checkCtx = httptrace.WithClientTrace(checkCtx, trace)
	req, err := http.NewRequestWithContext(checkCtx, http.MethodGet, target, nil)
	if err != nil {
		return failedSample(started, probeStagePrepare, probeReasonInvalidRequest)
	}
	req.Header.Set(probeUserAgentHeader, probeUserAgent)

	client := &http.Client{
		Transport: &http.Transport{
			Proxy: nil,
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				destination, err := v2net.ParseDestination(network + ":" + address)
				if err != nil {
					return nil, err
				}
				return tagged.Dialer(ctx, c.dispatcher, destination, outbound)
			},
			TLSHandshakeTimeout: time.Duration(config.Timeout),
			DisableKeepAlives:   true,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	response, err := client.Do(req)
	if err != nil {
		return failedSample(started, stage.Load().(string), classifyProbeError(err))
	}
	defer response.Body.Close()

	sample := extension.ProbeSample{
		Success: true, CheckedAt: time.Now(), Duration: time.Since(started),
		TTFB: time.Duration(ttfbNanos.Load()), HTTPStatus: response.StatusCode,
	}
	if response.StatusCode < int(config.AcceptedStatusMin) || response.StatusCode > int(config.AcceptedStatusMax) {
		sample.Success = false
		sample.Error = extension.ProbeError{Stage: probeStageValidate, Reason: probeReasonUnexpectedStatus}
		return sample
	}

	switch config.Method {
	case ProbeMethodHTTP:
		_, err := io.Copy(io.Discard, io.LimitReader(response.Body, MaxProbeBodyBytes))
		sample.CheckedAt = time.Now()
		sample.Duration = time.Since(started)
		if err != nil {
			sample.Success = false
			sample.Error = extension.ProbeError{Stage: probeStageBody, Reason: classifyProbeError(err)}
		}
		return sample
	case ProbeMethodDownload:
		minimum := config.MinBytes
		if minimum <= 0 {
			minimum = minimumDownloadBytes
		}
		count, err := io.Copy(io.Discard, io.LimitReader(response.Body, MaxProbeBodyBytes))
		sample.CheckedAt = time.Now()
		sample.Duration = time.Since(started)
		sample.Bytes = count
		if err != nil {
			sample.Success = false
			sample.Error = extension.ProbeError{Stage: probeStageBody, Reason: classifyProbeError(err)}
		} else if count < minimum {
			sample.Success = false
			sample.Error = extension.ProbeError{Stage: probeStageValidate, Reason: probeReasonInsufficientBytes}
		}
		return sample
	case ProbeMethodIP:
		body, err := io.ReadAll(io.LimitReader(response.Body, maxIPResponseBytes))
		sample.Bytes = int64(len(body))
		if err != nil {
			sample.Success = false
			sample.Error = extension.ProbeError{Stage: probeStageBody, Reason: classifyProbeError(err)}
			sample.CheckedAt = time.Now()
			sample.Duration = time.Since(started)
			return sample
		}
		if parser == nil {
			sample.Success = false
			sample.Error = extension.ProbeError{Stage: probeStageParse, Reason: probeReasonInvalidIPResponse}
			sample.CheckedAt = time.Now()
			sample.Duration = time.Since(started)
			return sample
		}
		observation, err := parser.Parse(body)
		if err != nil {
			sample.Success = false
			sample.Error = extension.ProbeError{Stage: probeStageParse, Reason: probeReasonInvalidIPResponse}
			sample.CheckedAt = time.Now()
			sample.Duration = time.Since(started)
			return sample
		}
		sample.Location = extension.ProbeLocation{
			Source: observation.source, Country: observation.country, ObservedAt: time.Now(),
		}
		// Baseline network access is owned by the Manager's maintenance task.
		// Probes only read the most recent bounded-age cache entry.
		baseline, err := c.directBaseline(checkCtx, target, parser)
		if err != nil {
			sample.Success = false
			sample.Error = extension.ProbeError{Stage: probeStageValidate, Reason: probeReasonBaselineUnavailable}
			sample.CheckedAt = time.Now()
			sample.Duration = time.Since(started)
			return sample
		}
		// The IP probe verifies that traffic actually traverses the selected
		// outbound. Matching the direct baseline means the public IP did not
		// change, so the outbound is not providing the expected proxy path.
		if observation.address == baseline {
			sample.Success = false
			sample.Error = extension.ProbeError{Stage: probeStageValidate, Reason: probeReasonIPUnchanged}
		}
		sample.CheckedAt = time.Now()
		sample.Duration = time.Since(started)
		return sample
	default:
		return failedSample(started, probeStagePrepare, probeReasonUnsupportedMethod)
	}
}

func (c *checker) prewarmBaseline(ctx context.Context, target string, parser ipResponseParser) error {
	return c.refreshBaseline(ctx, target, parser)
}

func (c *checker) refreshBaseline(ctx context.Context, target string, parser ipResponseParser) error {
	if parser == nil {
		return errors.New("IP response parser is required")
	}
	key := baselineKey{target: target, parserType: parser.Type()}
	value, err := c.fetchDirectBaseline(ctx, target, parser)
	if err != nil {
		return err
	}
	refreshedAt := time.Now()
	c.baselineMu.Lock()
	c.baselines[key] = cachedBaseline{
		value: value, expiresAt: refreshedAt.Add(directBaselineTTL),
		staleAt: refreshedAt.Add(directBaselineMaxAge),
	}
	c.baselineMu.Unlock()
	return nil
}

func (c *checker) directBaseline(ctx context.Context, target string, parser ipResponseParser) (netip.Addr, error) {
	if parser == nil {
		return netip.Addr{}, errors.New("IP response parser is required")
	}
	if err := ctx.Err(); err != nil {
		return netip.Addr{}, err
	}
	key := baselineKey{target: target, parserType: parser.Type()}
	now := time.Now()
	cached, found := c.loadBaseline(key)
	if !found {
		return netip.Addr{}, errors.New("IP baseline is unavailable")
	}
	if now.Before(cached.expiresAt) {
		return cached.value, nil
	}
	if now.Before(cached.staleAt) {
		return cached.value, nil
	}
	return netip.Addr{}, errors.New("IP baseline is expired")
}

func (c *checker) loadBaseline(key baselineKey) (cachedBaseline, bool) {
	c.baselineMu.Lock()
	defer c.baselineMu.Unlock()
	cached, found := c.baselines[key]
	return cached, found
}

func (c *checker) fetchDirectBaseline(ctx context.Context, target string, parser ipResponseParser) (netip.Addr, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return netip.Addr{}, err
	}
	client := &http.Client{
		// A baseline must represent the host's direct public IP. An explicit
		// nil proxy prevents HTTP_PROXY/HTTPS_PROXY from changing that path.
		Transport:     &http.Transport{Proxy: nil, DisableKeepAlives: true},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	response, err := client.Do(request)
	if err != nil {
		return netip.Addr{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusBadRequest {
		return netip.Addr{}, errors.New("unexpected IP baseline status")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxIPResponseBytes))
	if err != nil {
		return netip.Addr{}, err
	}
	observation, err := parser.Parse(body)
	if err != nil {
		return netip.Addr{}, errors.New("invalid IP baseline response").Base(err)
	}
	return observation.address, nil
}

func failedSample(started time.Time, stage, reason string) extension.ProbeSample {
	return extension.ProbeSample{
		Success: false, CheckedAt: time.Now(), Duration: time.Since(started),
		Error: extension.ProbeError{Stage: stage, Reason: reason},
	}
}

func classifyProbeError(err error) string {
	switch {
	case stderrors.Is(err, context.DeadlineExceeded):
		return probeReasonTimeout
	case stderrors.Is(err, context.Canceled):
		return probeReasonCanceled
	default:
		var netError net.Error
		if stderrors.As(err, &netError) && netError.Timeout() {
			return probeReasonTimeout
		}
		return probeReasonIOError
	}
}
