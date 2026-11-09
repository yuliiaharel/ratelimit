package memory

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/yuliiaharel/ratelimit"
)

// newFixedWindowWithShards builds a FixedWindow with a specific shard
// count, bypassing NewFixedWindow's fixed defaultShardCount, so
// BenchmarkFixedWindow_ShardCount can compare shard counts directly.
func newFixedWindowWithShards(limit int, window time.Duration, clk ratelimit.Clock, shardCount int) *FixedWindow {
	return &FixedWindow{
		limit:   limit,
		window:  window,
		clock:   clk,
		entries: newShardedMap[windowEntry](shardCount),
	}
}

// BenchmarkFixedWindow_ShardCount is the comparison the roadmap calls
// out directly: one process-wide mutex (shards=1) against the default
// shard count, under parallel access to many distinct keys.
func BenchmarkFixedWindow_ShardCount(b *testing.B) {
	const numKeys = 1024
	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	for _, shardCount := range []int{1, defaultShardCount} {
		b.Run(fmt.Sprintf("shards=%d", shardCount), func(b *testing.B) {
			// Limit and window are generous enough that denials never
			// happen, so the benchmark measures lock/map overhead, not
			// the denial branch.
			lim := newFixedWindowWithShards(1_000_000, time.Hour, ratelimit.SystemClock{}, shardCount)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					_, _ = lim.Allow(context.Background(), ratelimit.Request{Key: keys[i%numKeys]})
					i++
				}
			})
		})
	}
}

func BenchmarkFixedWindow(b *testing.B) {
	benchmarkLimiter(b, func() ratelimit.Limiter {
		lim, _ := NewFixedWindow(FixedWindowConfig{Limit: 1_000_000, Window: time.Hour, Clock: ratelimit.SystemClock{}})
		return lim
	})
}

func BenchmarkTokenBucket(b *testing.B) {
	benchmarkLimiter(b, func() ratelimit.Limiter {
		lim, _ := NewTokenBucket(TokenBucketConfig{Rate: 1_000_000, Burst: 1_000_000, Clock: ratelimit.SystemClock{}})
		return lim
	})
}

func BenchmarkSlidingWindowLog(b *testing.B) {
	benchmarkLimiter(b, func() ratelimit.Limiter {
		lim, _ := NewSlidingWindowLog(SlidingWindowLogConfig{Limit: 1_000_000, Window: time.Hour, Clock: ratelimit.SystemClock{}})
		return lim
	})
}

func BenchmarkSlidingWindowCounter(b *testing.B) {
	benchmarkLimiter(b, func() ratelimit.Limiter {
		lim, _ := NewSlidingWindowCounter(SlidingWindowCounterConfig{Limit: 1_000_000, Window: time.Hour, Clock: ratelimit.SystemClock{}})
		return lim
	})
}

// benchmarkLimiter runs two sub-benchmarks against a freshly
// constructed limiter: a single hot key (maximum lock contention, one
// shard does all the work) and many distinct keys accessed in parallel
// (where sharding pays off).
func benchmarkLimiter(b *testing.B, newLimiter func() ratelimit.Limiter) {
	b.Run("SingleKey", func(b *testing.B) {
		lim := newLimiter()
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = lim.Allow(context.Background(), ratelimit.Request{Key: "hot"})
			}
		})
	})

	b.Run("ManyKeys", func(b *testing.B) {
		const numKeys = 1024
		keys := make([]string, numKeys)
		for i := range keys {
			keys[i] = fmt.Sprintf("key-%d", i)
		}
		lim := newLimiter()
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				_, _ = lim.Allow(context.Background(), ratelimit.Request{Key: keys[i%numKeys]})
				i++
			}
		})
	})
}
