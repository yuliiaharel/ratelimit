package redisstore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/yuliiaharel/ratelimit"
)

func TestNewTokenBucketValidation(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:0"}) // never dialed
	t.Cleanup(func() { _ = client.Close() })

	tests := []struct {
		name string
		cfg  TokenBucketConfig
	}{
		{"nil client", TokenBucketConfig{Client: nil, Rate: 1, Burst: 1}},
		{"zero rate", TokenBucketConfig{Client: client, Rate: 0, Burst: 1}},
		{"negative rate", TokenBucketConfig{Client: client, Rate: -1, Burst: 1}},
		{"zero burst", TokenBucketConfig{Client: client, Rate: 1, Burst: 0}},
		{"negative burst", TokenBucketConfig{Client: client, Rate: 1, Burst: -1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewTokenBucket(tt.cfg); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

// newTestClient starts a real Redis in a container and returns a
// client connected to it. Tests using it are skipped (not failed) if
// Docker isn't available, so contributors without it aren't blocked.
func newTestClient(t *testing.T) *redis.Client {
	t.Helper()
	ctx := context.Background()

	container, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Skipf("skipping: couldn't start a Redis container (is Docker running?): %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminate container: %v", err)
		}
	})

	connStr, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("ConnectionString: %v", err)
	}
	opts, err := redis.ParseURL(connStr)
	if err != nil {
		t.Fatalf("ParseURL: %v", err)
	}

	client := redis.NewClient(opts)
	t.Cleanup(func() { _ = client.Close() })

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis: %v", err)
	}
	return client
}

func TestTokenBucketIntegration(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	tb, err := NewTokenBucket(TokenBucketConfig{Client: client, Rate: 10, Burst: 3})
	if err != nil {
		t.Fatalf("NewTokenBucket: %v", err)
	}

	t.Run("burst then deny", func(t *testing.T) {
		key := "burst-then-deny"
		for i := range 3 {
			d, err := tb.Allow(ctx, ratelimit.Request{Key: key})
			if err != nil || !d.Allowed {
				t.Fatalf("request %d: Allowed = %v, err = %v, want true, nil", i, d.Allowed, err)
			}
		}

		d, err := tb.Allow(ctx, ratelimit.Request{Key: key})
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		if d.Allowed {
			t.Fatal("4th request: want denied")
		}
		if d.RetryAfter <= 0 {
			t.Fatalf("RetryAfter = %v, want > 0", d.RetryAfter)
		}
	})

	t.Run("refills over real time", func(t *testing.T) {
		key := "refill"
		for i := range 3 {
			if _, err := tb.Allow(ctx, ratelimit.Request{Key: key}); err != nil {
				t.Fatalf("request %d: %v", i, err)
			}
		}
		// Drained. At 10 tokens/sec, 150ms buys back ~1.5.
		time.Sleep(150 * time.Millisecond)

		d, err := tb.Allow(ctx, ratelimit.Request{Key: key})
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		if !d.Allowed {
			t.Fatal("want allowed after refill")
		}
	})

	t.Run("keys are independent", func(t *testing.T) {
		a, err := tb.Allow(ctx, ratelimit.Request{Key: "indep-a", Cost: 3})
		if err != nil || !a.Allowed {
			t.Fatalf("key a: Allowed = %v, err = %v, want true, nil", a.Allowed, err)
		}
		b, err := tb.Allow(ctx, ratelimit.Request{Key: "indep-b", Cost: 3})
		if err != nil || !b.Allowed {
			t.Fatalf("key b: Allowed = %v, err = %v, want true, nil", b.Allowed, err)
		}
	})

	t.Run("cost greater than one", func(t *testing.T) {
		d, err := tb.Allow(ctx, ratelimit.Request{Key: "cost", Cost: 2})
		if err != nil || !d.Allowed || d.Remaining != 1 {
			t.Fatalf("Allowed = %v, Remaining = %d, err = %v, want true, 1, nil", d.Allowed, d.Remaining, err)
		}
	})

	t.Run("cost above limit errors without touching redis", func(t *testing.T) {
		_, err := tb.Allow(ctx, ratelimit.Request{Key: "too-big", Cost: 10})
		if !errors.Is(err, ratelimit.ErrCostExceedsLimit) {
			t.Fatalf("err = %v, want ErrCostExceedsLimit", err)
		}
	})

	// TestConcurrentRequestsAreAtomic hammers one key from many
	// goroutines, all hitting the same Redis instance over the
	// network, and checks that exactly `burst` win -- the thing the
	// Lua script buys over separate GET/SET calls: no lost updates
	// from a read-modify-write race.
	t.Run("concurrent requests to one key are atomic", func(t *testing.T) {
		key := "concurrent"
		const goroutines = 50

		var allowed atomic.Int64
		var wg sync.WaitGroup
		for range goroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				d, err := tb.Allow(ctx, ratelimit.Request{Key: key})
				if err == nil && d.Allowed {
					allowed.Add(1)
				}
			}()
		}
		wg.Wait()

		if got := allowed.Load(); got != 3 {
			t.Fatalf("allowed = %d, want exactly 3 (the burst) -- a race would over- or under-count", got)
		}
	})
}
