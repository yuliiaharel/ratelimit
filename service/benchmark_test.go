package service

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	ratelimitv1 "github.com/yuliiaharel/ratelimit/gen/ratelimit/v1"
	"github.com/yuliiaharel/ratelimit/gen/ratelimit/v1/ratelimitv1connect"
)

// BenchmarkShouldAllow measures the full service round trip: connect
// client -> HTTP (loopback) -> Server -> Lua -> Redis container, and
// back. Skipped if Docker isn't available.
//
// Besides ns/op it reports per-call p50 and p99 latency. In Serial
// (one caller) ns/op is itself the mean latency; in Parallel
// (GOMAXPROCS callers over 1024 keys) ns/op is the inverse of
// throughput and the percentiles are what a caller sees under that
// load.
func BenchmarkShouldAllow(b *testing.B) {
	// Generous enough that denials never happen, so every call takes
	// the same path.
	_, baseURL := newTestServer(b, 1_000_000, 1_000_000)

	// http.DefaultClient keeps only 2 idle connections per host; with
	// more parallel callers than that it would redial constantly and
	// the benchmark would measure connection setup.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 256
	b.Cleanup(transport.CloseIdleConnections)
	client := ratelimitv1connect.NewRateLimitServiceClient(&http.Client{Transport: transport}, baseURL)
	ctx := context.Background()

	const numKeys = 1024
	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	call := func(key string) (time.Duration, error) {
		start := time.Now()
		_, err := client.ShouldAllow(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowRequest{
			Domain: "bench", Key: key,
		}))
		return time.Since(start), err
	}

	b.Run("Serial", func(b *testing.B) {
		latencies := make([]time.Duration, 0, b.N)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			d, err := call(keys[i%numKeys])
			if err != nil {
				b.Fatalf("ShouldAllow: %v", err)
			}
			latencies = append(latencies, d)
		}
		b.StopTimer()
		reportPercentiles(b, latencies)
	})

	b.Run("Parallel", func(b *testing.B) {
		var mu sync.Mutex
		var latencies []time.Duration
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			var local []time.Duration
			i := 0
			for pb.Next() {
				d, err := call(keys[i%numKeys])
				if err != nil {
					b.Errorf("ShouldAllow: %v", err)
					break
				}
				local = append(local, d)
				i++
			}
			mu.Lock()
			latencies = append(latencies, local...)
			mu.Unlock()
		})
		b.StopTimer()
		reportPercentiles(b, latencies)
	})
}

// reportPercentiles adds nearest-rank p50 and p99 of latencies to the
// benchmark's output, in nanoseconds.
func reportPercentiles(b *testing.B, latencies []time.Duration) {
	b.Helper()
	if len(latencies) == 0 {
		return
	}
	slices.Sort(latencies)
	rank := func(p int) time.Duration {
		// Ceiling of p% of n, as a 1-based rank.
		return latencies[(p*len(latencies)+99)/100-1]
	}
	b.ReportMetric(float64(rank(50)), "p50-ns")
	b.ReportMetric(float64(rank(99)), "p99-ns")
}
