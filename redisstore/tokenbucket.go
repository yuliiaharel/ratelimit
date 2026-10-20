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

//go:embed tokenbucketbatch.lua
var tokenBucketBatchScriptSrc string

var (
	tokenBucketScript      = redis.NewScript(tokenBucketScriptSrc)
	tokenBucketBatchScript = redis.NewScript(tokenBucketBatchScriptSrc)
)

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

	row, ok := res.([]any)
	if !ok || len(row) < 3 {
		return ratelimit.Decision{}, fmt.Errorf("redisstore: unexpected script result: %#v", res)
	}
	allowed, tokensRemaining, retryAfterSeconds, err := parseRow(row[0], row[1], row[2])
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

// AllowBatch checks every req atomically together: all of them succeed
// or none of them do, and tokens are only consumed when they all do.
// Returns one Decision per req, in the same order, plus whether the
// batch as a whole was allowed. A denied batch consumes nothing.
func (tb *TokenBucket) AllowBatch(ctx context.Context, reqs []ratelimit.Request) ([]ratelimit.Decision, bool, error) {
	if len(reqs) == 0 {
		return nil, true, nil
	}

	keys := make([]string, len(reqs))
	args := make([]any, 3, 3+len(reqs))
	args[0], args[1], args[2] = tb.rate, tb.burst, int(tb.ttl.Seconds())
	for i, req := range reqs {
		if err := req.Validate(); err != nil {
			return nil, false, err
		}
		cost := req.EffectiveCost()
		if cost > tb.burst {
			return nil, false, ratelimit.ErrCostExceedsLimit
		}
		keys[i] = tb.keyPrefix + req.Key
		args = append(args, cost)
	}

	res, err := tokenBucketBatchScript.Run(ctx, tb.client, keys, args...).Result()
	if err != nil {
		return nil, false, fmt.Errorf("redisstore: %w", err)
	}

	rows, ok := res.([]any)
	if !ok || len(rows) != len(reqs)*3 {
		return nil, false, fmt.Errorf("redisstore: unexpected batch script result: %#v", res)
	}

	decisions := make([]ratelimit.Decision, len(reqs))
	allAllowed := true
	for i := range reqs {
		allowed, tokens, retryAfterSeconds, perr := parseRow(rows[i*3], rows[i*3+1], rows[i*3+2])
		if perr != nil {
			return nil, false, fmt.Errorf("redisstore: %w", perr)
		}
		allAllowed = allAllowed && allowed
		decisions[i] = ratelimit.Decision{
			Allowed:    allowed,
			Limit:      tb.burst,
			Remaining:  flooredTokens(tokens),
			RetryAfter: time.Duration(retryAfterSeconds * float64(time.Second)),
		}
	}

	return decisions, allAllowed, nil
}

// parseRow unpacks one { allowed, tokens, retry_after } triple. tokens
// and retry_after are returned as strings (see tokenbucket.lua for why)
// and parsed back into float64 here.
func parseRow(allowedField, tokensField, retryField any) (allowed bool, tokens, retryAfterSeconds float64, err error) {
	allowedN, ok := allowedField.(int64)
	if !ok {
		return false, 0, 0, fmt.Errorf("unexpected 'allowed' field: %#v", allowedField)
	}

	tokensStr, ok := tokensField.(string)
	if !ok {
		return false, 0, 0, fmt.Errorf("unexpected 'tokens' field: %#v", tokensField)
	}
	tokens, err = strconv.ParseFloat(tokensStr, 64)
	if err != nil {
		return false, 0, 0, fmt.Errorf("parsing tokens: %w", err)
	}

	retryStr, ok := retryField.(string)
	if !ok {
		return false, 0, 0, fmt.Errorf("unexpected 'retry_after' field: %#v", retryField)
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
