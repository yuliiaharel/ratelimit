// Package client is a small Go client for RateLimitService: a thin,
// Go-shaped wrapper around the generated connect client, so callers
// don't need to work with protobuf types directly.
//
// It's a plain RPC client for now -- no local caching or fail-open/
// fail-closed fallback yet. Those come in roadmap step 9.
package client

import (
	"context"
	"net/http"
	"time"

	"connectrpc.com/connect"

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

// Client talks to a RateLimitService over HTTP.
type Client struct {
	rpc ratelimitv1connect.RateLimitServiceClient
}

// New returns a Client for the RateLimitService at baseURL (e.g.
// "http://localhost:8080"). A nil httpClient defaults to
// http.DefaultClient.
func New(baseURL string, httpClient *http.Client, opts ...connect.ClientOption) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{rpc: ratelimitv1connect.NewRateLimitServiceClient(httpClient, baseURL, opts...)}
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
	Limit      uint32
	Remaining  uint32
	RetryAfter time.Duration
	PolicyID   string
}

// ShouldAllow checks a single key.
func (c *Client) ShouldAllow(ctx context.Context, check Check) (Decision, error) {
	resp, err := c.rpc.ShouldAllow(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowRequest{
		Domain:   check.Domain,
		Key:      check.Key,
		Cost:     check.Cost,
		Priority: check.Priority,
	}))
	if err != nil {
		return Decision{}, err
	}
	return decisionFromProto(resp.Msg), nil
}

// ShouldAllowBatch checks every one of checks atomically together:
// the whole call is allowed only if every check is, and tokens are
// only consumed when all of them pass. decisions is in the same order
// as checks.
func (c *Client) ShouldAllowBatch(ctx context.Context, checks []Check) (allowed bool, decisions []Decision, err error) {
	reqs := make([]*ratelimitv1.ShouldAllowRequest, len(checks))
	for i, chk := range checks {
		reqs[i] = &ratelimitv1.ShouldAllowRequest{
			Domain: chk.Domain, Key: chk.Key, Cost: chk.Cost, Priority: chk.Priority,
		}
	}

	resp, err := c.rpc.ShouldAllowBatch(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowBatchRequest{Checks: reqs}))
	if err != nil {
		return false, nil, err
	}

	results := resp.Msg.GetResults()
	decisions = make([]Decision, len(results))
	for i, r := range results {
		decisions[i] = decisionFromProto(r)
	}
	return resp.Msg.GetAllowed(), decisions, nil
}

func decisionFromProto(r *ratelimitv1.ShouldAllowResponse) Decision {
	return Decision{
		Allowed:    r.GetAllowed(),
		Limit:      r.GetLimit(),
		Remaining:  r.GetRemaining(),
		RetryAfter: r.GetRetryAfter().AsDuration(),
		PolicyID:   r.GetPolicyId(),
	}
}
