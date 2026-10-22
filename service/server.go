// Package service implements RateLimitService, backed by Redis.
package service

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/yuliiaharel/ratelimit"
	ratelimitv1 "github.com/yuliiaharel/ratelimit/gen/ratelimit/v1"
	"github.com/yuliiaharel/ratelimit/gen/ratelimit/v1/ratelimitv1connect"
	"github.com/yuliiaharel/ratelimit/policy"
	"github.com/yuliiaharel/ratelimit/redisstore"
)

// Server implements ratelimitv1connect.RateLimitServiceHandler. Each
// domain's Rate/Burst/priority reservation comes from policies, looked
// up fresh on every request -- so a policy hot-reload takes effect on
// the very next call, with no per-domain state in Server itself to
// invalidate.
type Server struct {
	client    *redis.Client
	policies  *policy.Store
	keyPrefix string
}

var _ ratelimitv1connect.RateLimitServiceHandler = (*Server)(nil)

// NewServer returns a Server backed by client, looking up each
// request's policy from policies.
func NewServer(client *redis.Client, policies *policy.Store) *Server {
	return &Server{client: client, policies: policies, keyPrefix: "ratelimit:tb:"}
}

// ShouldAllow checks a single key.
func (s *Server) ShouldAllow(
	ctx context.Context, req *connect.Request[ratelimitv1.ShouldAllowRequest],
) (*connect.Response[ratelimitv1.ShouldAllowResponse], error) {
	msg := req.Msg
	if msg.GetKey() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("service: key is required"))
	}

	p, policyName := s.policies.Lookup(msg.GetDomain())
	tb, err := redisstore.NewTokenBucket(redisstore.TokenBucketConfig{
		Client:             s.client,
		Rate:               p.Rate,
		Burst:              p.Burst,
		ReserveForCritical: p.ReserveForCritical,
		ReserveForDefault:  p.ReserveForDefault,
		KeyPrefix:          s.keyPrefix,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("service: building limiter for policy %q: %w", policyName, err))
	}

	d, err := tb.Allow(ctx, ratelimit.Request{
		Key:      compositeKey(msg.GetDomain(), msg.GetKey()),
		Cost:     int(msg.GetCost()),
		Priority: priorityFromProto(msg.GetPriority()),
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(decisionToProto(d, policyName)), nil
}

// ShouldAllowBatch checks every key in req.Checks atomically together,
// even across domains with different policies: the request is allowed
// only if every check passes, and tokens are consumed only when all
// of them do.
func (s *Server) ShouldAllowBatch(
	ctx context.Context, req *connect.Request[ratelimitv1.ShouldAllowBatchRequest],
) (*connect.Response[ratelimitv1.ShouldAllowBatchResponse], error) {
	checks := req.Msg.GetChecks()
	if len(checks) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("service: at least one check is required"))
	}

	entries := make([]redisstore.PolicyCheck, len(checks))
	policyNames := make([]string, len(checks))
	for i, c := range checks {
		if c.GetKey() == "" {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("service: check %d: key is required", i))
		}
		p, policyName := s.policies.Lookup(c.GetDomain())
		policyNames[i] = policyName
		entries[i] = redisstore.PolicyCheck{
			Key:      compositeKey(c.GetDomain(), c.GetKey()),
			Cost:     int(c.GetCost()),
			Priority: priorityFromProto(c.GetPriority()),
			Policy: redisstore.Policy{
				Rate:               p.Rate,
				Burst:              p.Burst,
				ReserveForCritical: p.ReserveForCritical,
				ReserveForDefault:  p.ReserveForDefault,
			},
		}
	}

	decisions, allowed, err := redisstore.AllowBatchAcrossPolicies(ctx, s.client, s.keyPrefix, entries)
	if err != nil {
		return nil, toConnectError(err)
	}

	results := make([]*ratelimitv1.ShouldAllowResponse, len(decisions))
	for i, d := range decisions {
		results[i] = decisionToProto(d, policyNames[i])
	}

	return connect.NewResponse(&ratelimitv1.ShouldAllowBatchResponse{
		Allowed: allowed,
		Results: results,
	}), nil
}

// compositeKey folds a request's domain and key into the single string
// the underlying limiter keys off of, so the same key in two domains
// (e.g. "user:42" under "api" and under "send-message") gets
// independent budgets.
func compositeKey(domain, key string) string {
	return domain + ":" + key
}

// priorityFromProto maps the wire enum onto the core package's type.
// PRIORITY_UNSPECIFIED and PRIORITY_DEFAULT both map to
// ratelimit.PriorityDefault, matching Priority's own documented
// zero-value convention.
func priorityFromProto(p ratelimitv1.Priority) ratelimit.Priority {
	switch p {
	case ratelimitv1.Priority_PRIORITY_CRITICAL:
		return ratelimit.PriorityCritical
	case ratelimitv1.Priority_PRIORITY_SHEDDABLE:
		return ratelimit.PrioritySheddable
	default:
		return ratelimit.PriorityDefault
	}
}

func decisionToProto(d ratelimit.Decision, policyName string) *ratelimitv1.ShouldAllowResponse {
	reason := ratelimitv1.ShouldAllowResponse_REASON_OK
	if !d.Allowed {
		reason = ratelimitv1.ShouldAllowResponse_REASON_OVER_LIMIT
	}
	return &ratelimitv1.ShouldAllowResponse{
		Allowed:    d.Allowed,
		Reason:     reason,
		Limit:      uint32(d.Limit),
		Remaining:  uint32(d.Remaining),
		RetryAfter: durationpb.New(d.RetryAfter),
		PolicyId:   policyName,
	}
}

func toConnectError(err error) error {
	if errors.Is(err, ratelimit.ErrCostExceedsLimit) || errors.Is(err, ratelimit.ErrEmptyKey) ||
		errors.Is(err, ratelimit.ErrNegativeCost) {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	// Redis unreachable, a malformed script reply, etc. -- the limiter
	// backend itself is unavailable, not the caller's request.
	return connect.NewError(connect.CodeUnavailable, fmt.Errorf("service: %w", err))
}
