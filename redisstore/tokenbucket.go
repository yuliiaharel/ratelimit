// Package redisstore backs a ratelimit.Limiter with Redis, so the limit
// holds across every instance of a service rather than per-process.
package redisstore

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yuliiaharel/ratelimit"
)

//go:embed tokenbucket.lua
var tokenBucketScriptSrc string

var tokenBucketScript = redis.NewScript(tokenBucketScriptSrc)

// tokenEpsilon mirrors memory.TokenBucket's: it absorbs floating-point
// error from the refill arithmetic (here done in Lua, not Go) so a
// bucket that should read as exactly N tokens doesn't read as N-ε.
const tokenEpsilon = 1e-9

// TokenBucketConfig configures a TokenBucket limiter.
type TokenBucketConfig struct {
	// Client is the Redis connection to use. Required.
	Client *redis.Client

	// Rate is how many tokens are added to the bucket per second.
	Rate float64

	// Burst is the bucket's capacity: the most tokens a key can
	// accumulate, and so the largest burst it can spend in one instant.
	// A fresh key starts with a full bucket.
	Burst int

	// KeyPrefix namespaces every Redis key this limiter touches, so it
	// can share one Redis instance with other data without clobbering
	// it. Defaults to "ratelimit:tb:".
	KeyPrefix string
}

// TokenBucket is the same algorithm as memory.TokenBucket, but backed by
// Redis: every Allow call runs a Lua script that reads, refills,
// checks, and writes back the bucket in one atomic round trip, so
// concurrent callers across many processes can't race each other the
// way separate GET/SET calls would.
type TokenBucket struct {
	client    *redis.Client
	rate      float64
	burst     int
	keyPrefix string
	ttl       time.Duration
}

var _ ratelimit.Limiter = (*TokenBucket)(nil)

// NewTokenBucket validates cfg and returns a ready limiter.
func NewTokenBucket(cfg TokenBucketConfig) (*TokenBucket, error) {
	if cfg.Client == nil {
		return nil, errors.New("redisstore: TokenBucket client must not be nil")
	}
	if cfg.Rate <= 0 {
		return nil, errors.New("redisstore: TokenBucket rate must be positive")
	}
	if cfg.Burst <= 0 {
		return nil, errors.New("redisstore: TokenBucket burst must be positive")
	}
	keyPrefix := cfg.KeyPrefix
	if keyPrefix == "" {
		keyPrefix = "ratelimit:tb:"
	}

	return &TokenBucket{
		client:    cfg.Client,
		rate:      cfg.Rate,
		burst:     cfg.Burst,
		keyPrefix: keyPrefix,
		ttl:       idleTTL(cfg.Rate, cfg.Burst),
	}, nil
}

// idleTTL is how long an untouched key is kept around: twice the time
// it'd take to refill the bucket from empty, with a floor so a fast
// rate doesn't expire a key that's merely idle for a second.
func idleTTL(rate float64, burst int) time.Duration {
	fillTime := time.Duration(float64(burst) / rate * float64(time.Second))
	ttl := 2 * fillTime
	if ttl < 60*time.Second {
		ttl = 60 * time.Second
	}
	return ttl
}

// Allow charges req's cost against the bucket if enough tokens have
// accumulated. A denied request consumes nothing.
func (tb *TokenBucket) Allow(ctx context.Context, req ratelimit.Request) (ratelimit.Decision, error) {
	if err := req.Validate(); err != nil {
		return ratelimit.Decision{}, err
	}
	cost := req.EffectiveCost()
	if cost > tb.burst {
		return ratelimit.Decision{Limit: tb.burst}, ratelimit.ErrCostExceedsLimit
	}

	key := tb.keyPrefix + req.Key
	res, err := tokenBucketScript.Run(ctx, tb.client, []string{key},
		tb.rate, tb.burst, cost, int(tb.ttl.Seconds()),
	).Result()
	if err != nil {
		return ratelimit.Decision{}, fmt.Errorf("redisstore: %w", err)
	}

	allowed, tokensRemaining, retryAfterSeconds, err := parseResult(res)
	if err != nil {
		return ratelimit.Decision{}, fmt.Errorf("redisstore: %w", err)
	}

	return ratelimit.Decision{
		Allowed:    allowed,
		Limit:      tb.burst,
		Remaining:  flooredTokens(tokensRemaining),
		RetryAfter: time.Duration(retryAfterSeconds * float64(time.Second)),
	}, nil
}

// parseResult unpacks the script's { allowed, tokens, retry_after, burst }
// reply. tokens and retry_after are returned as strings (see
// tokenbucket.lua for why) and parsed back into float64 here.
func parseResult(res any) (allowed bool, tokens, retryAfterSeconds float64, err error) {
	row, ok := res.([]any)
	if !ok || len(row) < 3 {
		return false, 0, 0, fmt.Errorf("unexpected script result: %#v", res)
	}

	allowedN, ok := row[0].(int64)
	if !ok {
		return false, 0, 0, fmt.Errorf("unexpected 'allowed' field: %#v", row[0])
	}

	tokensStr, ok := row[1].(string)
	if !ok {
		return false, 0, 0, fmt.Errorf("unexpected 'tokens' field: %#v", row[1])
	}
	tokens, err = strconv.ParseFloat(tokensStr, 64)
	if err != nil {
		return false, 0, 0, fmt.Errorf("parsing tokens: %w", err)
	}

	retryStr, ok := row[2].(string)
	if !ok {
		return false, 0, 0, fmt.Errorf("unexpected 'retry_after' field: %#v", row[2])
	}
	retryAfterSeconds, err = strconv.ParseFloat(retryStr, 64)
	if err != nil {
		return false, 0, 0, fmt.Errorf("parsing retry_after: %w", err)
	}

	return allowedN == 1, tokens, retryAfterSeconds, nil
}

func flooredTokens(tokens float64) int {
	return int(math.Floor(tokens + tokenEpsilon))
}
