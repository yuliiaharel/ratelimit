package memory

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/yuliiaharel/ratelimit"
)

// tokenEpsilon absorbs floating-point error from repeated refill
// arithmetic, so a bucket that should read as exactly N tokens doesn't
// read as N-ε and get treated as short by one.
const tokenEpsilon = 1e-9

// TokenBucketConfig configures a TokenBucket limiter.
type TokenBucketConfig struct {
	// Rate is how many tokens are added to the bucket per second.
	Rate float64

	// Burst is the bucket's capacity: the most tokens a key can
	// accumulate, and so the largest burst it can spend in one instant.
	// A fresh key starts with a full bucket.
	Burst int

	// Clock defaults to the system clock.
	Clock ratelimit.Clock
}

// TokenBucket limits each key to Rate tokens/second on average, while
// allowing bursts of up to Burst tokens.
//
// Tokens accumulate continuously rather than in discrete windows, so
// unlike FixedWindow it has no window-edge doubling: the rate is exact
// over any interval, and Burst is the only source of burstiness.
type TokenBucket struct {
	rate  float64
	burst int
	clock ratelimit.Clock

	buckets *shardedMap[bucketEntry]
}

type bucketEntry struct {
	tokens float64
	last   time.Time
}

var _ ratelimit.Limiter = (*TokenBucket)(nil)

// NewTokenBucket validates cfg and returns a ready limiter.
func NewTokenBucket(cfg TokenBucketConfig) (*TokenBucket, error) {
	if cfg.Rate <= 0 {
		return nil, errors.New("memory: TokenBucket rate must be positive")
	}
	if cfg.Burst <= 0 {
		return nil, errors.New("memory: TokenBucket burst must be positive")
	}
	if cfg.Clock == nil {
		cfg.Clock = ratelimit.SystemClock{}
	}
	return &TokenBucket{
		rate:    cfg.Rate,
		burst:   cfg.Burst,
		clock:   cfg.Clock,
		buckets: newShardedMap[bucketEntry](defaultShardCount),
	}, nil
}

// Allow charges req's cost against the bucket if enough tokens have
// accumulated. A denied request consumes nothing.
func (tb *TokenBucket) Allow(_ context.Context, req ratelimit.Request) (ratelimit.Decision, error) {
	if err := req.Validate(); err != nil {
		return ratelimit.Decision{}, err
	}
	cost := req.EffectiveCost()
	if cost > tb.burst {
		return ratelimit.Decision{Limit: tb.burst}, ratelimit.ErrCostExceedsLimit
	}

	now := tb.clock.Now()

	var d ratelimit.Decision
	create := func() *bucketEntry { return &bucketEntry{tokens: float64(tb.burst), last: now} }
	tb.buckets.withLock(req.Key, create, func(e *bucketEntry) {
		if elapsed := now.Sub(e.last); elapsed > 0 {
			e.tokens = math.Min(float64(tb.burst), e.tokens+elapsed.Seconds()*tb.rate)
			e.last = now
		}

		if e.tokens+tokenEpsilon < float64(cost) {
			deficit := float64(cost) - e.tokens
			d = ratelimit.Decision{
				Allowed:    false,
				Limit:      tb.burst,
				Remaining:  flooredTokens(e.tokens),
				RetryAfter: time.Duration(deficit / tb.rate * float64(time.Second)),
			}
			return
		}

		e.tokens -= float64(cost)
		d = ratelimit.Decision{
			Allowed:   true,
			Limit:     tb.burst,
			Remaining: flooredTokens(e.tokens),
		}
	})
	return d, nil
}

func flooredTokens(tokens float64) int {
	return int(math.Floor(tokens + tokenEpsilon))
}
