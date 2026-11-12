// Package ratelimit is a rate limiter that can run in-process or as a
// shared service. This package defines the core types every
// implementation shares: the Limiter interface, the Request a caller
// submits, the Decision it gets back, and the Clock implementations
// read time from.
//
// A Request names a key (what is being limited, e.g. "user:42"), a
// cost (how many tokens it consumes) and a priority. A Decision says
// whether it may proceed and, if not, how long to wait.
//
// The implementations live in subpackages:
//
//   - memory: in-process fixed window, token bucket, sliding window
//     log and sliding window counter. They honor cost but treat every
//     priority alike.
//   - redisstore: a token bucket in Redis, atomic via a Lua script, so
//     one limit holds across many processes. Honors cost and priority.
//   - interceptor: gRPC unary and stream server interceptors around
//     any Limiter.
//   - service, policy, client: RateLimitService, a standalone server
//     with per-domain policies that other services call before doing
//     work, and its Go client. The binary is cmd/ratelimitd.
//   - clocktest: a fake Clock for deterministic tests.
package ratelimit

import (
	"context"
	"errors"
	"time"
)

// Limiter decides whether a request may proceed.
//
// Implementations must be safe for concurrent use.
type Limiter interface {
	Allow(ctx context.Context, req Request) (Decision, error)
}

// Priority describes how important a request is. Under pressure,
// Sheddable traffic should hit its limit before Critical traffic.
// (Implementations that don't support priorities treat all of them alike.)
type Priority int

const (
	PriorityDefault Priority = iota // zero value, so an unset Priority means Default
	PriorityCritical
	PrioritySheddable
)

func (p Priority) String() string {
	switch p {
	case PriorityCritical:
		return "critical"
	case PrioritySheddable:
		return "sheddable"
	default:
		return "default"
	}
}

// Request is a single admission check.
type Request struct {
	// Key identifies what is being limited, e.g. "user:42" or "tenant:acme".
	Key string

	// Cost is how many tokens this request consumes. Zero means 1.
	Cost int

	Priority Priority
}

// EffectiveCost returns the cost to charge, applying the zero-means-one default.
func (r Request) EffectiveCost() int {
	if r.Cost == 0 {
		return 1
	}
	return r.Cost
}

// Validate reports whether the request is well-formed.
func (r Request) Validate() error {
	if r.Key == "" {
		return ErrEmptyKey
	}
	if r.Cost < 0 {
		return ErrNegativeCost
	}
	return nil
}

// Decision is the outcome of an admission check.
type Decision struct {
	Allowed bool

	// Limit is the maximum the key may consume per window (or the bucket size).
	Limit int

	// Remaining is how much the key may still consume right now.
	Remaining int

	// RetryAfter is how long to wait before the same request could succeed.
	// Zero when Allowed is true.
	RetryAfter time.Duration
}

var (
	ErrEmptyKey     = errors.New("ratelimit: empty key")
	ErrNegativeCost = errors.New("ratelimit: negative cost")

	// ErrCostExceedsLimit means the request could never succeed, no matter
	// how long the caller waits. Returned as an error rather than a denial
	// so callers don't retry forever.
	ErrCostExceedsLimit = errors.New("ratelimit: cost exceeds limit")
)

// Clock is the source of time for every limiter, so tests can control it.
type Clock interface {
	Now() time.Time
}

// SystemClock reads the real wall clock.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now() }
