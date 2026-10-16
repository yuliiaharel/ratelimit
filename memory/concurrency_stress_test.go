package memory

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yuliiaharel/ratelimit"
	"github.com/yuliiaharel/ratelimit/clocktest"
)

// TestConcurrentManyKeys hammers several distinct keys concurrently
// across every limiter type, with the clock frozen so the expected
// outcome is deterministic: each key should allow exactly `limit` of
// its requests and deny the rest, no matter how goroutines interleave.
//
// Unlike each type's own single-hot-key concurrency test (which only
// ever touches one shard), this spreads goroutines across many keys,
// so it's the test that actually exercises more than one shard lock at
// once. Run with -race.
func TestConcurrentManyKeys(t *testing.T) {
	const (
		limit                = 50
		numKeys              = 8
		goroutinesPerKey     = 10
		attemptsPerGoroutine = 20
	)

	tests := []struct {
		name       string
		newLimiter func(t *testing.T, clk ratelimit.Clock) ratelimit.Limiter
	}{
		{"FixedWindow", func(t *testing.T, clk ratelimit.Clock) ratelimit.Limiter {
			lim, err := NewFixedWindow(FixedWindowConfig{Limit: limit, Window: time.Minute, Clock: clk})
			if err != nil {
				t.Fatalf("NewFixedWindow: %v", err)
			}
			return lim
		}},
		{"TokenBucket", func(t *testing.T, clk ratelimit.Clock) ratelimit.Limiter {
			lim, err := NewTokenBucket(TokenBucketConfig{Rate: 1, Burst: limit, Clock: clk})
			if err != nil {
				t.Fatalf("NewTokenBucket: %v", err)
			}
			return lim
		}},
		{"SlidingWindowLog", func(t *testing.T, clk ratelimit.Clock) ratelimit.Limiter {
			lim, err := NewSlidingWindowLog(SlidingWindowLogConfig{Limit: limit, Window: time.Minute, Clock: clk})
			if err != nil {
				t.Fatalf("NewSlidingWindowLog: %v", err)
			}
			return lim
		}},
		{"SlidingWindowCounter", func(t *testing.T, clk ratelimit.Clock) ratelimit.Limiter {
			lim, err := NewSlidingWindowCounter(SlidingWindowCounterConfig{Limit: limit, Window: time.Minute, Clock: clk})
			if err != nil {
				t.Fatalf("NewSlidingWindowCounter: %v", err)
			}
			return lim
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := clocktest.New(epoch) // never advanced: fully deterministic
			lim := tt.newLimiter(t, clk)

			keys := make([]string, numKeys)
			for i := range keys {
				keys[i] = fmt.Sprintf("key-%d", i)
			}

			allowed := make([]atomic.Int64, numKeys)
			var wg sync.WaitGroup
			for k := range numKeys {
				for range goroutinesPerKey {
					wg.Add(1)
					go func(k int) {
						defer wg.Done()
						for range attemptsPerGoroutine {
							d, err := lim.Allow(context.Background(), ratelimit.Request{Key: keys[k]})
							if err == nil && d.Allowed {
								allowed[k].Add(1)
							}
						}
					}(k)
				}
			}
			wg.Wait()

			for k := range numKeys {
				if got := allowed[k].Load(); got != limit {
					t.Errorf("key %d: allowed = %d, want exactly %d", k, got, limit)
				}
			}
		})
	}
}
