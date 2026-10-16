// Package ratelimit defines the core types shared by every limiter
// implementation: the Limiter interface, the Request a caller submits,
// the Decision it gets back, and the Clock all implementations read time from.
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
