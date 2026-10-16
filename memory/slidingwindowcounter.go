package memory

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/yuliiaharel/ratelimit"
)

// windowEpsilon absorbs floating-point error in the weighted estimate
// below, mirroring tokenEpsilon in TokenBucket.
const windowEpsilon = 1e-9

// SlidingWindowCounterConfig configures a SlidingWindowCounter limiter.
type SlidingWindowCounterConfig struct {
	// Limit is the total cost allowed per key per window.
	Limit int

	// Window is the window length. Windows are aligned to the Unix
	// epoch, like FixedWindow.
	Window time.Duration

	// Clock defaults to the system clock.
	Clock ratelimit.Clock
}

// SlidingWindowCounter approximates a trailing window using two
// fixed-size counters per key -- the current epoch-aligned window and
// the one before it -- weighting the previous window's count by how
// much of it still overlaps the trailing Window ending now.
//
// It fixes FixedWindow's worst case (bursting up to 2x right at a
// window edge) for O(1) memory per key, at the cost of being an
// estimate rather than an exact count: see SlidingWindowLog for the
// exact, O(requests) alternative.
type SlidingWindowCounter struct {
	limit  int
	window time.Duration
	clock  ratelimit.Clock

	counters *shardedMap[counterEntry]
}

type counterEntry struct {
	start time.Time // start of the current window
	curr  int
	prev  int
}

var _ ratelimit.Limiter = (*SlidingWindowCounter)(nil)

// NewSlidingWindowCounter validates cfg and returns a ready limiter.
func NewSlidingWindowCounter(cfg SlidingWindowCounterConfig) (*SlidingWindowCounter, error) {
	if cfg.Limit <= 0 {
		return nil, errors.New("memory: SlidingWindowCounter limit must be positive")
	}
	if cfg.Window <= 0 {
		return nil, errors.New("memory: SlidingWindowCounter window must be positive")
	}
	if cfg.Clock == nil {
		cfg.Clock = ratelimit.SystemClock{}
	}
	return &SlidingWindowCounter{
		limit:    cfg.Limit,
		window:   cfg.Window,
		clock:    cfg.Clock,
		counters: newShardedMap[counterEntry](defaultShardCount),
	}, nil
}

// Allow charges req's cost against the weighted estimate if it fits.
// A denied request consumes nothing.
func (s *SlidingWindowCounter) Allow(_ context.Context, req ratelimit.Request) (ratelimit.Decision, error) {
	if err := req.Validate(); err != nil {
		return ratelimit.Decision{}, err
	}
	cost := req.EffectiveCost()
	if cost > s.limit {
		return ratelimit.Decision{Limit: s.limit}, ratelimit.ErrCostExceedsLimit
	}

	now := s.clock.Now()
	start := now.Truncate(s.window)

	var d ratelimit.Decision
	s.counters.withLock(req.Key, func() *counterEntry { return &counterEntry{} }, func(e *counterEntry) {
		if !e.start.Equal(start) {
			// A zero-value e.start (brand new key) never equals a real
			// start and never trails it by exactly one window, so this
			// also correctly treats a fresh key as having no history.
			if start.Sub(e.start) == s.window {
				// Consecutive window: today's current becomes
				// yesterday's previous.
				e.prev = e.curr
			} else {
				// Idle longer than a full window: no usable history.
				e.prev = 0
			}
			e.curr = 0
			e.start = start
		}

		elapsed := now.Sub(e.start)
		weight := float64(s.window-elapsed) / float64(s.window)
		estimated := float64(e.prev)*weight + float64(e.curr)

		if estimated+float64(cost) > float64(s.limit)+windowEpsilon {
			d = ratelimit.Decision{
				Allowed:    false,
				Limit:      s.limit,
				Remaining:  remainingTokens(s.limit, estimated),
				RetryAfter: retryAfterCounter(e, s.window, s.limit, cost, elapsed),
			}
			return
		}

		e.curr += cost
		d = ratelimit.Decision{
			Allowed:   true,
			Limit:     s.limit,
			Remaining: remainingTokens(s.limit, estimated+float64(cost)),
		}
	})
	return d, nil
}

func remainingTokens(limit int, estimated float64) int {
	remaining := limit - int(math.Ceil(estimated-windowEpsilon))
	if remaining < 0 {
		remaining = 0
	}
	return remaining
}

// retryAfterCounter estimates how long until the weighted estimate drops
// enough for a request of the given cost to fit.
//
// This is an approximation, same as the rest of this type: if prev's
// decay alone can't free enough room before the window rolls over, it
// falls back to the time remaining in the current window, since prev's
// weight resets favorably (to 1, against a fresh curr of 0) right at
// that boundary.
func retryAfterCounter(e *counterEntry, window time.Duration, limit, cost int, elapsed time.Duration) time.Duration {
	remain := window - elapsed
	if e.prev == 0 {
		return remain
	}

	// Solve prev*(window-elapsed-Δ)/window + curr + cost <= limit for
	// the smallest Δ >= 0:
	//   Δ >= [prev*(window-elapsed) - (limit-cost-curr)*window] / prev
	numerator := float64(e.prev)*float64(remain) - float64(limit-cost-e.curr)*float64(window)
	if numerator <= 0 {
		return 0
	}
	delta := time.Duration(numerator / float64(e.prev))
	if delta > remain {
		return remain
	}
	return delta
}
