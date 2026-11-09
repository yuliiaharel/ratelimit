// Package service implements RateLimitService, backed by Redis.
package service

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/yuliiaharel/ratelimit"
	ratelimitv1 "github.com/yuliiaharel/ratelimit/gen/ratelimit/v1"
	"github.com/yuliiaharel/ratelimit/gen/ratelimit/v1/ratelimitv1connect"
	"github.com/yuliiaharel/ratelimit/redisstore"
)

// Server implements ratelimitv1connect.RateLimitServiceHandler.
//
// Every domain shares one token bucket policy for now (Rate/Burst
// configured on tb) -- there's no per-domain policy lookup yet; that's
// roadmap step 8. PolicyId on every response is a fixed placeholder
// ("default") until then.
type Server struct {
	tb *redisstore.TokenBucket
}

var _ ratelimitv1connect.RateLimitServiceHandler = (*Server)(nil)

// NewServer returns a Server backed by tb.
func NewServer(tb *redisstore.TokenBucket) *Server {
	return &Server{tb: tb}
}

// ShouldAllow checks a single key.
func (s *Server) ShouldAllow(
	ctx context.Context, req *connect.Request[ratelimitv1.ShouldAllowRequest],
) (*connect.Response[ratelimitv1.ShouldAllowResponse], error) {
	msg := req.Msg
	if msg.GetKey() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("service: key is required"))
	}

	d, err := s.tb.Allow(ctx, ratelimit.Request{
		Key:  compositeKey(msg.GetDomain(), msg.GetKey()),
		Cost: int(msg.GetCost()),
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(decisionToProto(d)), nil
}

// ShouldAllowBatch checks every key in req.Checks atomically together:
// the request is allowed only if every check passes, and tokens are
// consumed only when all of them do.
func (s *Server) ShouldAllowBatch(
	ctx context.Context, req *connect.Request[ratelimitv1.ShouldAllowBatchRequest],
) (*connect.Response[ratelimitv1.ShouldAllowBatchResponse], error) {
	checks := req.Msg.GetChecks()
	if len(checks) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("service: at least one check is required"))
	}

	reqs := make([]ratelimit.Request, len(checks))
	for i, c := range checks {
		if c.GetKey() == "" {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("service: check %d: key is required", i))
		}
		reqs[i] = ratelimit.Request{Key: compositeKey(c.GetDomain(), c.GetKey()), Cost: int(c.GetCost())}
	}

	decisions, allowed, err := s.tb.AllowBatch(ctx, reqs)
	if err != nil {
		return nil, toConnectError(err)
	}

	results := make([]*ratelimitv1.ShouldAllowResponse, len(decisions))
	for i, d := range decisions {
		results[i] = decisionToProto(d)
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

func decisionToProto(d ratelimit.Decision) *ratelimitv1.ShouldAllowResponse {
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
		PolicyId:   "default",
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
