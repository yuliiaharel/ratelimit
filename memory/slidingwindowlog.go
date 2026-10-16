package memory

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/yuliiaharel/ratelimit"
)

// SlidingWindowLogConfig configures a SlidingWindowLog limiter.
type SlidingWindowLogConfig struct {
	// Limit is the total cost allowed in any trailing window of length
	// Window.
	Limit int

	// Window is the length of the trailing window considered on every
	// check.
	Window time.Duration

	// Clock defaults to the system clock.
	Clock ratelimit.Clock
}

// SlidingWindowLog counts the exact cost spent by a key in the trailing
// Window ending now, by keeping a timestamped log of granted requests
// per key.
//
// Unlike FixedWindow, there is no window edge to straddle: the limit is
// exact over any Window-length interval, no matter when it's measured.
// The cost is O(requests) memory per active key -- bounded by Limit,
// since a denied request never grows the log -- which is why
// SlidingWindowCounter trades a little accuracy for O(1) memory.
type SlidingWindowLog struct {
	limit  int
	window time.Duration
	clock  ratelimit.Clock

	// A single mutex for now; sharding comes in roadmap step 4.
	mu   sync.Mutex
	logs map[string][]logEntry
}

type logEntry struct {
	at   time.Time
	cost int
}

var _ ratelimit.Limiter = (*SlidingWindowLog)(nil)

// NewSlidingWindowLog validates cfg and returns a ready limiter.
func NewSlidingWindowLog(cfg SlidingWindowLogConfig) (*SlidingWindowLog, error) {
	if cfg.Limit <= 0 {
		return nil, errors.New("memory: SlidingWindowLog limit must be positive")
	}
	if cfg.Window <= 0 {
		return nil, errors.New("memory: SlidingWindowLog window must be positive")
	}
	if cfg.Clock == nil {
		cfg.Clock = ratelimit.SystemClock{}
	}
	return &SlidingWindowLog{
		limit:  cfg.Limit,
		window: cfg.Window,
		clock:  cfg.Clock,
		logs:   make(map[string][]logEntry),
	}, nil
}

// Allow charges req's cost against the trailing window if it fits.
// A denied request consumes nothing.
func (s *SlidingWindowLog) Allow(_ context.Context, req ratelimit.Request) (ratelimit.Decision, error) {
	if err := req.Validate(); err != nil {
		return ratelimit.Decision{}, err
	}
	cost := req.EffectiveCost()
	if cost > s.limit {
		return ratelimit.Decision{Limit: s.limit}, ratelimit.ErrCostExceedsLimit
	}

	now := s.clock.Now()
	cutoff := now.Add(-s.window)

	s.mu.Lock()
	defer s.mu.Unlock()

	entries := s.logs[req.Key]
	// Evict everything that has aged out of the trailing window.
	// Entries are appended in increasing time order, so the stale ones
	// are always a prefix.
	i := 0
	for i < len(entries) && !entries[i].at.After(cutoff) {
		i++
	}
	entries = entries[i:]

	used := 0
	for _, e := range entries {
		used += e.cost
	}

	if used+cost > s.limit {
		s.logs[req.Key] = entries
		return ratelimit.Decision{
			Allowed:    false,
			Limit:      s.limit,
			Remaining:  s.limit - used,
			RetryAfter: retryAfterLog(entries, now, s.window, used, cost, s.limit),
		}, nil
	}

	entries = append(entries, logEntry{at: now, cost: cost})
	s.logs[req.Key] = entries
	used += cost
	return ratelimit.Decision{
		Allowed:   true,
		Limit:     s.limit,
		Remaining: s.limit - used,
	}, nil
}

// retryAfterLog returns how long until enough cost has aged out of the
// window for a request of the given cost to fit. cost <= limit is
// guaranteed by the caller, so evicting every remaining entry always
// frees enough room eventually.
func retryAfterLog(entries []logEntry, now time.Time, window time.Duration, used, cost, limit int) time.Duration {
	freed := 0
	for _, e := range entries {
		freed += e.cost
		if used-freed+cost <= limit {
			return e.at.Add(window).Sub(now)
		}
	}
	return 0 // unreachable given the cost <= limit precondition
}
