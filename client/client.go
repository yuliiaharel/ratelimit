// Package client is a small Go client for RateLimitService: a thin,
// Go-shaped wrapper around the generated connect client, so callers
// don't need to work with protobuf types directly.
//
// Every call gets its own deadline (Config.Timeout), and when the
// service is unreachable or doesn't answer in time the client decides
// locally according to Config.Fallback instead of returning an error.
// By default that fallback is fail-open, matching the server's default
// failure_mode. Decision.Reason and Decision.Fallback tell a real
// decision apart from either kind of failure-mode answer.
package client

import (
	"context"
	"errors"
	"net/http"
	"time"

	"connectrpc.com/connect"

	"github.com/yuliiaharel/ratelimit"
	ratelimitv1 "github.com/yuliiaharel/ratelimit/gen/ratelimit/v1"
	"github.com/yuliiaharel/ratelimit/gen/ratelimit/v1/ratelimitv1connect"
)

// Priority re-exports the proto enum so callers of this package don't
// need to import gen/... themselves.
type Priority = ratelimitv1.Priority

const (
	PriorityUnspecified = ratelimitv1.Priority_PRIORITY_UNSPECIFIED
	PriorityCritical    = ratelimitv1.Priority_PRIORITY_CRITICAL
	PriorityDefault     = ratelimitv1.Priority_PRIORITY_DEFAULT
	PrioritySheddable   = ratelimitv1.Priority_PRIORITY_SHEDDABLE
)

// Reason re-exports the proto enum explaining a Decision.
type Reason = ratelimitv1.ShouldAllowResponse_Reason

const (
	ReasonUnspecified = ratelimitv1.ShouldAllowResponse_REASON_UNSPECIFIED
	ReasonOK          = ratelimitv1.ShouldAllowResponse_REASON_OK
	ReasonOverLimit   = ratelimitv1.ShouldAllowResponse_REASON_OVER_LIMIT
	ReasonFailOpen    = ratelimitv1.ShouldAllowResponse_REASON_FAIL_OPEN
	ReasonFailClosed  = ratelimitv1.ShouldAllowResponse_REASON_FAIL_CLOSED
)

// FallbackMode is what the client does when RateLimitService itself is
// unreachable or doesn't answer within Config.Timeout.
type FallbackMode int

const (
	// FallbackFailOpen allows the request (Reason ReasonFailOpen). The
	// default: the zero value of FallbackMode.
	FallbackFailOpen FallbackMode = iota

	// FallbackFailClosed denies the request (Reason ReasonFailClosed),
	// with RetryAfter set to Config.FallbackRetryAfter.
	FallbackFailClosed

	// FallbackError returns the error to the caller, which then decides.
	FallbackError
)

const (
	// DefaultTimeout is the per-call deadline when Config.Timeout is
	// unset. It sits above the server's default 50ms Redis timeout, so
	// a slow Redis is normally answered by the server's failure mode
	// (which knows each domain's policy) before the client gives up.
	DefaultTimeout = 200 * time.Millisecond

	// DefaultFallbackRetryAfter is RetryAfter on a client-side
	// fail-closed denial when Config.FallbackRetryAfter is unset.
	DefaultFallbackRetryAfter = time.Second
)

// Config tunes per-call deadlines and client-side fallback. The zero
// value is valid: DefaultTimeout, fail-open fallback for every domain.
type Config struct {
	// Timeout bounds each call. Zero means DefaultTimeout. A caller's
	// own earlier context deadline still wins.
	Timeout time.Duration

	// Fallback applies to every domain not listed in DomainFallback.
	Fallback FallbackMode

	// DomainFallback overrides Fallback per domain, e.g. to fail
	// closed only for a domain guarding a paid third-party API. It
	// should usually mirror that domain's server-side failure_mode.
	DomainFallback map[string]FallbackMode

	// FallbackRetryAfter is RetryAfter on a client-side fail-closed
	// denial. Zero means DefaultFallbackRetryAfter.
	FallbackRetryAfter time.Duration
}

// Client talks to a RateLimitService over HTTP.
type Client struct {
	rpc ratelimitv1connect.RateLimitServiceClient
	cfg Config

	// clock sets each call's deadline (now + timeout). It's the system
	// clock outside tests; tests swap in a fake one to control whether
	// that deadline has already passed without sleeping.
	clock ratelimit.Clock
}

// New returns a Client for the RateLimitService at baseURL (e.g.
// "http://localhost:8080") with the default Config. A nil httpClient
// defaults to http.DefaultClient.
func New(baseURL string, httpClient *http.Client, opts ...connect.ClientOption) *Client {
	return NewWithConfig(baseURL, httpClient, Config{}, opts...)
}

// NewWithConfig is New with explicit timeout and fallback settings.
func NewWithConfig(baseURL string, httpClient *http.Client, cfg Config, opts ...connect.ClientOption) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.FallbackRetryAfter <= 0 {
		cfg.FallbackRetryAfter = DefaultFallbackRetryAfter
	}
	return &Client{
		rpc:   ratelimitv1connect.NewRateLimitServiceClient(httpClient, baseURL, opts...),
		cfg:   cfg,
		clock: ratelimit.SystemClock{},
	}
}

// Check is one rate-limit check: a plain-field mirror of
// ShouldAllowRequest. Cost and Priority default the same way
// ratelimit.Request's do: a zero Cost means 1, a zero Priority means
// Default.
type Check struct {
	Domain   string
	Key      string
	Cost     uint32
	Priority Priority
}

// Decision mirrors ShouldAllowResponse, with RetryAfter already
// converted to a time.Duration.
type Decision struct {
	Allowed    bool
	Reason     Reason
	Limit      uint32
	Remaining  uint32
	RetryAfter time.Duration
	PolicyID   string

	// Fallback is true when the client decided locally because the
	// service was unreachable; FallbackCause is the error that caused
	// it. A server-side failure-mode answer (Redis down, service up)
	// has Fallback false and Reason ReasonFailOpen/ReasonFailClosed.
	Fallback      bool
	FallbackCause error
}

func (c *Client) callContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithDeadline(ctx, c.clock.Now().Add(c.cfg.Timeout))
}

// ShouldAllow checks a single key.
func (c *Client) ShouldAllow(ctx context.Context, check Check) (Decision, error) {
	cctx, cancel := c.callContext(ctx)
	defer cancel()
	resp, err := c.rpc.ShouldAllow(cctx, connect.NewRequest(toProto(check)))
	if err != nil {
		if !c.shouldFallBack(ctx, err) || c.modeFor(check.Domain) == FallbackError {
			return Decision{}, err
		}
		return c.fallbackDecision(check.Domain, err), nil
	}
	return decisionFromProto(resp.Msg), nil
}

// ShouldAllowBatch checks every one of checks atomically together:
// the whole call is allowed only if every check is, and tokens are
// only consumed when all of them pass. decisions is in the same order
// as checks.
//
// On fallback each check gets its own domain's mode, and the batch is
// allowed only if all of them allow. If any domain's mode is
// FallbackError, the error is returned for the whole batch.
func (c *Client) ShouldAllowBatch(ctx context.Context, checks []Check) (allowed bool, decisions []Decision, err error) {
	reqs := make([]*ratelimitv1.ShouldAllowRequest, len(checks))
	for i, chk := range checks {
		reqs[i] = toProto(chk)
	}

	cctx, cancel := c.callContext(ctx)
	defer cancel()
	resp, err := c.rpc.ShouldAllowBatch(cctx, connect.NewRequest(&ratelimitv1.ShouldAllowBatchRequest{Checks: reqs}))
	if err != nil {
		if !c.shouldFallBack(ctx, err) {
			return false, nil, err
		}
		for _, chk := range checks {
			if c.modeFor(chk.Domain) == FallbackError {
				return false, nil, err
			}
		}
		allowed = true
		decisions = make([]Decision, len(checks))
		for i, chk := range checks {
			decisions[i] = c.fallbackDecision(chk.Domain, err)
			allowed = allowed && decisions[i].Allowed
		}
		return allowed, decisions, nil
	}

	results := resp.Msg.GetResults()
	decisions = make([]Decision, len(results))
	for i, r := range results {
		decisions[i] = decisionFromProto(r)
	}
	return resp.Msg.GetAllowed(), decisions, nil
}

// shouldFallBack reports whether err means the service couldn't be
// reached in time, as opposed to the service rejecting the request
// (e.g. InvalidArgument) or the caller giving up.
func (c *Client) shouldFallBack(parent context.Context, err error) bool {
	if parent.Err() != nil {
		return false
	}
	switch connect.CodeOf(err) {
	case connect.CodeUnavailable, connect.CodeDeadlineExceeded:
		return true
	default:
		// connect reports a context deadline hit before the request
		// was sent as a plain context error rather than a coded one.
		return errors.Is(err, context.DeadlineExceeded)
	}
}

func (c *Client) modeFor(domain string) FallbackMode {
	if m, ok := c.cfg.DomainFallback[domain]; ok {
		return m
	}
	return c.cfg.Fallback
}

func (c *Client) fallbackDecision(domain string, cause error) Decision {
	d := Decision{Fallback: true, FallbackCause: cause}
	if c.modeFor(domain) == FallbackFailClosed {
		d.Reason = ReasonFailClosed
		d.RetryAfter = c.cfg.FallbackRetryAfter
		return d
	}
	d.Allowed = true
	d.Reason = ReasonFailOpen
	return d
}

func toProto(chk Check) *ratelimitv1.ShouldAllowRequest {
	return &ratelimitv1.ShouldAllowRequest{
		Domain: chk.Domain, Key: chk.Key, Cost: chk.Cost, Priority: chk.Priority,
	}
}

func decisionFromProto(r *ratelimitv1.ShouldAllowResponse) Decision {
	return Decision{
		Allowed:    r.GetAllowed(),
		Reason:     r.GetReason(),
		Limit:      r.GetLimit(),
		Remaining:  r.GetRemaining(),
		RetryAfter: r.GetRetryAfter().AsDuration(),
		PolicyID:   r.GetPolicyId(),
	}
}
