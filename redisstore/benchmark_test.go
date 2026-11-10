package redisstore

import (
	"context"
	"fmt"
	"testing"

	"github.com/yuliiaharel/ratelimit"
)

// BenchmarkTokenBucket measures one Allow call (one Lua round trip)
// against a real Redis container. Skipped if Docker isn't available.
//
// Serial is one caller on one connection, so ns/op is the round-trip
// latency. SingleKey and ManyKeys mirror the in-memory benchmarks:
// parallel callers on one hot key or spread over 1024 keys, where
// ns/op is the inverse of throughput, not latency.
func BenchmarkTokenBucket(b *testing.B) {
	client := newTestClient(b)
	// Rate and burst are generous enough that denials never happen, so
	// every call takes the same script path.
	tb, err := NewTokenBucket(TokenBucketConfig{Client: client, Rate: 1_000_000, Burst: 1_000_000})
	if err != nil {
		b.Fatalf("NewTokenBucket: %v", err)
	}
	ctx := context.Background()

	const numKeys = 1024
	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	b.Run("Serial", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := tb.Allow(ctx, ratelimit.Request{Key: "serial"}); err != nil {
				b.Fatalf("Allow: %v", err)
			}
		}
	})

	b.Run("SingleKey", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := tb.Allow(ctx, ratelimit.Request{Key: "hot"}); err != nil {
					b.Errorf("Allow: %v", err)
					return
				}
			}
		})
	})

	b.Run("ManyKeys", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if _, err := tb.Allow(ctx, ratelimit.Request{Key: keys[i%numKeys]}); err != nil {
					b.Errorf("Allow: %v", err)
					return
				}
				i++
			}
		})
	})
}
