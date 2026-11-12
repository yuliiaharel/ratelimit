// Package service implements RateLimitService, backed by Redis: the
// server other services call to ask whether a request may proceed.
//
// A request names a domain and a key. The domain selects a policy
// (rate, burst, priority reserves, failure mode) from a policy.Store,
// looked up on every request so a reloaded policy file applies
// immediately. The check itself is a redisstore token bucket keyed by
// domain and key.
//
// Server implements the generated connect handler, so it is mounted on
// an http.ServeMux and answers the Connect protocol, which includes
// plain HTTP/JSON POSTs. cmd/ratelimitd is the ready-made binary.
//
// When Redis fails or exceeds the backend timeout the server still
// answers, allowing or denying according to the domain's failure
// mode, and marks the response with REASON_FAIL_OPEN or
// REASON_FAIL_CLOSED.
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"connectrpc.com/connect"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/yuliiaharel/ratelimit"
	ratelimitv1 "github.com/yuliiaharel/ratelimit/gen/ratelimit/v1"
	"github.com/yuliiaharel/ratelimit/gen/ratelimit/v1/ratelimitv1connect"
	"github.com/yuliiaharel/ratelimit/policy"
	"github.com/yuliiaharel/ratelimit/redisstore"
)

// DefaultBackendTimeout bounds every Redis call when NewServer isn't
// given WithBackendTimeout. A healthy Redis answers these scripts in
// well under a millisecond; 50ms leaves room for a GC pause or a busy
// network without letting a stalled Redis hold callers for long.
const DefaultBackendTimeout = 50 * time.Millisecond

// backend is the Redis-facing half of Server, split out so tests can
// substitute a store that fails or stalls on demand.
type backend interface {
	allow(ctx context.Context, p redisstore.Policy, req ratelimit.Request) (ratelimit.Decision, error)
	allowBatch(ctx context.Context, entries []redisstore.PolicyCheck) ([]ratelimit.Decision, bool, error)
}

type redisBackend struct {
	client    *redis.Client
	keyPrefix string
}

func (b redisBackend) allow(ctx context.Context, p redisstore.Policy, req ratelimit.Request) (ratelimit.Decision, error) {
	tb, err := redisstore.NewTokenBucket(redisstore.TokenBucketConfig{
		Client:             b.client,
		Rate:               p.Rate,
		Burst:              p.Burst,
		ReserveForCritical: p.ReserveForCritical,
		ReserveForDefault:  p.ReserveForDefault,
		KeyPrefix:          b.keyPrefix,
	})
	if err != nil {
		return ratelimit.Decision{}, fmt.Errorf("building limiter: %w", err)
	}
	return tb.Allow(ctx, req)
}

func (b redisBackend) allowBatch(ctx context.Context, entries []redisstore.PolicyCheck) ([]ratelimit.Decision, bool, error) {
	return redisstore.AllowBatchAcrossPolicies(ctx, b.client, b.keyPrefix, entries)
}

// Server implements ratelimitv1connect.RateLimitServiceHandler. Each
// domain's Rate/Burst/priority reservation and failure mode come from
// policies, looked up fresh on every request -- so a policy hot-reload
// takes effect on the very next call, with no per-domain state in
// Server itself to invalidate.
//
// When Redis errors or doesn't answer within the backend timeout,
// Server doesn't return an error: it answers according to the
// domain's failure mode, with Reason REASON_FAIL_OPEN or
// REASON_FAIL_CLOSED so callers can tell that apart from a real
// decision.
type Server struct {
	backend  backend
	policies *policy.Store
	timeout  time.Duration
	logger   *slog.Logger

	// clock sets each backend call's deadline (now + timeout). It's the
	// system clock outside tests; tests swap in a fake one to control
	// whether that deadline has already passed without sleeping.
	clock ratelimit.Clock
}

var _ ratelimitv1connect.RateLimitServiceHandler = (*Server)(nil)

// Option configures a Server.
type Option func(*Server)

// WithBackendTimeout bounds every Redis call to d (default
// DefaultBackendTimeout). Non-positive values are ignored.
func WithBackendTimeout(d time.Duration) Option {
	return func(s *Server) {
		if d > 0 {
			s.timeout = d
		}
	}
}

// WithLogger sets where backend failures are logged (default
// slog.Default()).
func WithLogger(l *slog.Logger) Option {
	return func(s *Server) {
		if l != nil {
			s.logger = l
		}
	}
}

// NewServer returns a Server backed by client, looking up each
// request's policy from policies.
//
// client should have redis.Options.ContextTimeoutEnabled set. Without
// it, go-redis ignores context deadlines on socket reads and falls
// back to its own ReadTimeout (3s by default), so a stalled Redis
// could hold a request well past the backend timeout.
func NewServer(client *redis.Client, policies *policy.Store, opts ...Option) *Server {
	return newServer(redisBackend{client: client, keyPrefix: "ratelimit:tb:"}, policies, opts...)
}

func newServer(b backend, policies *policy.Store, opts ...Option) *Server {
	s := &Server{
		backend:  b,
		policies: policies,
		timeout:  DefaultBackendTimeout,
		logger:   slog.Default(),
		clock:    ratelimit.SystemClock{},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// backendContext derives the context for one backend call.
func (s *Server) backendContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithDeadline(ctx, s.clock.Now().Add(s.timeout))
}

// ShouldAllow checks a single key.
func (s *Server) ShouldAllow(
	ctx context.Context, req *connect.Request[ratelimitv1.ShouldAllowRequest],
) (*connect.Response[ratelimitv1.ShouldAllowResponse], error) {
	msg := req.Msg
	if msg.GetKey() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("service: key is required"))
	}

	p, mode, policyName := s.policies.Lookup(msg.GetDomain())

	bctx, cancel := s.backendContext(ctx)
	defer cancel()
	d, err := s.backend.allow(bctx, p, ratelimit.Request{
		Key:      compositeKey(msg.GetDomain(), msg.GetKey()),
		Cost:     int(msg.GetCost()),
		Priority: priorityFromProto(msg.GetPriority()),
	})
	if err != nil {
		if rerr := requestError(ctx, err); rerr != nil {
			return nil, rerr
		}
		s.logBackendFailure(ctx, err, msg.GetDomain(), policyName, mode)
		return connect.NewResponse(failureResponse(p, mode, policyName)), nil
	}

	return connect.NewResponse(decisionToProto(d, policyName)), nil
}

// ShouldAllowBatch checks every key in req.Checks atomically together,
// even across domains with different policies: the request is allowed
// only if every check passes, and tokens are consumed only when all
// of them do.
//
// If the backend fails, each check is answered by its own domain's
// failure mode, and the batch is allowed only if all of those allow --
// i.e. only if every domain in it is fail_open.
func (s *Server) ShouldAllowBatch(
	ctx context.Context, req *connect.Request[ratelimitv1.ShouldAllowBatchRequest],
) (*connect.Response[ratelimitv1.ShouldAllowBatchResponse], error) {
	checks := req.Msg.GetChecks()
	if len(checks) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("service: at least one check is required"))
	}

	entries := make([]redisstore.PolicyCheck, len(checks))
	policyNames := make([]string, len(checks))
	modes := make([]policy.FailureMode, len(checks))
	for i, c := range checks {
		if c.GetKey() == "" {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("service: check %d: key is required", i))
		}
		p, mode, policyName := s.policies.Lookup(c.GetDomain())
		policyNames[i] = policyName
		modes[i] = mode
		entries[i] = redisstore.PolicyCheck{
			Key:      compositeKey(c.GetDomain(), c.GetKey()),
			Cost:     int(c.GetCost()),
			Priority: priorityFromProto(c.GetPriority()),
			Policy:   p,
		}
	}

	bctx, cancel := s.backendContext(ctx)
	defer cancel()
	decisions, allowed, err := s.backend.allowBatch(bctx, entries)
	if err != nil {
		if rerr := requestError(ctx, err); rerr != nil {
			return nil, rerr
		}
		results := make([]*ratelimitv1.ShouldAllowResponse, len(entries))
		allowed := true
		for i, e := range entries {
			s.logBackendFailure(ctx, err, checks[i].GetDomain(), policyNames[i], modes[i])
			results[i] = failureResponse(e.Policy, modes[i], policyNames[i])
			allowed = allowed && results[i].GetAllowed()
		}
		return connect.NewResponse(&ratelimitv1.ShouldAllowBatchResponse{Allowed: allowed, Results: results}), nil
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

// requestError returns the RPC error to send when err is not a backend
// failure: the request itself is invalid, or the caller has gone away.
// It returns nil when err should be answered by the failure mode.
func requestError(ctx context.Context, err error) error {
	if errors.Is(err, ratelimit.ErrCostExceedsLimit) || errors.Is(err, ratelimit.ErrEmptyKey) ||
		errors.Is(err, ratelimit.ErrNegativeCost) {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	// The caller canceled or its own deadline passed: there's nobody to
	// answer, and a fail-open "allowed" would be misleading in logs.
	// Only our backend timeout (a child of ctx) falls through to the
	// failure mode.
	if cerr := ctx.Err(); cerr != nil {
		if errors.Is(cerr, context.DeadlineExceeded) {
			return connect.NewError(connect.CodeDeadlineExceeded, cerr)
		}
		return connect.NewError(connect.CodeCanceled, cerr)
	}
	return nil
}

func (s *Server) logBackendFailure(ctx context.Context, err error, domain, policyName string, mode policy.FailureMode) {
	// The key is deliberately not logged: keys are often user or
	// tenant identifiers.
	s.logger.WarnContext(ctx, "rate-limit backend unavailable, applying failure mode",
		slog.String("domain", domain),
		slog.String("policy", policyName),
		slog.String("failure_mode", string(mode)),
		slog.Bool("timeout", errors.Is(err, context.DeadlineExceeded)),
		slog.Any("error", err),
	)
}

// failureResponse is the answer for a check whose backend call failed.
// Remaining is 0 because the real count is unknown. A fail-closed
// denial suggests retrying after one token's refill time -- what an
// empty bucket would say -- so well-behaved callers back off at the
// policy's own pace instead of hammering a struggling backend.
func failureResponse(p redisstore.Policy, mode policy.FailureMode, policyName string) *ratelimitv1.ShouldAllowResponse {
	resp := &ratelimitv1.ShouldAllowResponse{
		Limit:      uint32(p.Burst),
		Remaining:  0,
		RetryAfter: durationpb.New(0),
		PolicyId:   policyName,
	}
	if mode == policy.FailClosed {
		resp.Allowed = false
		resp.Reason = ratelimitv1.ShouldAllowResponse_REASON_FAIL_CLOSED
		resp.RetryAfter = durationpb.New(time.Duration(float64(time.Second) / p.Rate))
		return resp
	}
	resp.Allowed = true
	resp.Reason = ratelimitv1.ShouldAllowResponse_REASON_FAIL_OPEN
	return resp
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
