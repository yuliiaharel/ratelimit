// Package memory contains in-process limiter implementations.
package memory

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/yuliiaharel/ratelimit"
)

// FixedWindowConfig configures a FixedWindow limiter.
type FixedWindowConfig struct {
	// Limit is the total cost allowed per key per window.
	Limit int

	// Window is the window length. Windows are aligned to the Unix epoch,
	// so every key's window starts at the same instant.
	Window time.Duration

	// Clock defaults to the system clock.
	Clock ratelimit.Clock
}

// FixedWindow counts usage per key in fixed, epoch-aligned windows.
//
// It is the simplest algorithm and the baseline the others are measured
// against. Its known weakness: a client can spend its full limit at the end
// of one window and again at the start of the next, getting up to 2× the
// limit in a span of one window length.
type FixedWindow struct {
	limit  int
	window time.Duration
	clock  ratelimit.Clock

	// A single mutex for now; sharding comes in roadmap step 4.
	mu      sync.Mutex
	entries map[string]*windowEntry
}

type windowEntry struct {
	start time.Time
	used  int
}

var _ ratelimit.Limiter = (*FixedWindow)(nil)

// NewFixedWindow validates cfg and returns a ready limiter.
func NewFixedWindow(cfg FixedWindowConfig) (*FixedWindow, error) {
	if cfg.Limit <= 0 {
		return nil, errors.New("memory: FixedWindow limit must be positive")
	}
	if cfg.Window <= 0 {
		return nil, errors.New("memory: FixedWindow window must be positive")
	}
	if cfg.Clock == nil {
		cfg.Clock = ratelimit.SystemClock{}
	}
	return &FixedWindow{
		limit:   cfg.Limit,
		window:  cfg.Window,
		clock:   cfg.Clock,
		entries: make(map[string]*windowEntry),
	}, nil
}

// Allow charges req's cost against the current window if it fits.
// A denied request consumes nothing.
func (f *FixedWindow) Allow(_ context.Context, req ratelimit.Request) (ratelimit.Decision, error) {
	if err := req.Validate(); err != nil {
		return ratelimit.Decision{}, err
	}
	cost := req.EffectiveCost()
	if cost > f.limit {
		return ratelimit.Decision{Limit: f.limit}, ratelimit.ErrCostExceedsLimit
	}

	now := f.clock.Now()
	start := now.Truncate(f.window)

	f.mu.Lock()
	defer f.mu.Unlock()

	e, ok := f.entries[req.Key]
	if !ok {
		e = &windowEntry{}
		f.entries[req.Key] = e
	}
	if !e.start.Equal(start) {
		// New window: reset. Stale entries for idle keys are reclaimed
		// the next time the key is seen; TODO(step 4): background sweep.
		e.start = start
		e.used = 0
	}

	if e.used+cost > f.limit {
		return ratelimit.Decision{
			Allowed:    false,
			Limit:      f.limit,
			Remaining:  f.limit - e.used,
			RetryAfter: start.Add(f.window).Sub(now),
		}, nil
	}

	e.used += cost
	return ratelimit.Decision{
		Allowed:   true,
		Limit:     f.limit,
		Remaining: f.limit - e.used,
	}, nil
}
