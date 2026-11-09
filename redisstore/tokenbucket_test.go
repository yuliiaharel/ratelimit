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

	t.Run("batch: all checks succeed together", func(t *testing.T) {
		decisions, allowed, err := tb.AllowBatch(ctx, []ratelimit.Request{
			{Key: "batch-ok-a"},
			{Key: "batch-ok-b"},
		})
		if err != nil {
			t.Fatalf("AllowBatch: %v", err)
		}
		if !allowed {
			t.Fatal("batch: want allowed")
		}
		for i, d := range decisions {
			if !d.Allowed {
				t.Fatalf("decision %d: want allowed", i)
			}
		}
	})

	t.Run("batch: one short key denies the whole batch and consumes nothing", func(t *testing.T) {
		keyA, keyB := "batch-deny-a", "batch-deny-b"
		// Drain keyB to 0 first.
		if _, err := tb.Allow(ctx, ratelimit.Request{Key: keyB, Cost: 3}); err != nil {
			t.Fatalf("draining keyB: %v", err)
		}

		decisions, allowed, err := tb.AllowBatch(ctx, []ratelimit.Request{
			{Key: keyA, Cost: 1}, // would succeed alone
			{Key: keyB, Cost: 1}, // would NOT succeed alone: already empty
		})
		if err != nil {
			t.Fatalf("AllowBatch: %v", err)
		}
		if allowed {
			t.Fatal("batch: want denied")
		}
		if decisions[0].Allowed || decisions[1].Allowed {
			t.Fatalf("decisions = %+v, want both denied -- the batch is all-or-nothing", decisions)
		}

		// keyA's own tokens must be untouched: a batch that fails
		// consumes nothing, anywhere.
		d, err := tb.Allow(ctx, ratelimit.Request{Key: keyA, Cost: 3})
		if err != nil || !d.Allowed {
			t.Fatalf("keyA after denied batch: Allowed = %v, err = %v, want true, nil (full 3 still available)", d.Allowed, err)
		}
	})

	t.Run("batch: cost above limit on any check errors without touching redis", func(t *testing.T) {
		_, _, err := tb.AllowBatch(ctx, []ratelimit.Request{
			{Key: "batch-too-big-a"},
			{Key: "batch-too-big-b", Cost: 10},
		})
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

func TestPolicyValidate(t *testing.T) {
	tests := []struct {
		name string
		p    Policy
		ok   bool
	}{
		{"valid, no reserve", Policy{Rate: 1, Burst: 1}, true},
		{"valid, with reserve", Policy{Rate: 1, Burst: 10, ReserveForCritical: 0.3, ReserveForDefault: 0.2}, true},
		{"zero rate", Policy{Rate: 0, Burst: 1}, false},
		{"negative rate", Policy{Rate: -1, Burst: 1}, false},
		{"zero burst", Policy{Rate: 1, Burst: 0}, false},
		{"reserve for critical too high", Policy{Rate: 1, Burst: 1, ReserveForCritical: 1.1}, false},
		{"reserve for critical negative", Policy{Rate: 1, Burst: 1, ReserveForCritical: -0.1}, false},
		{"reserve for default too high", Policy{Rate: 1, Burst: 1, ReserveForDefault: 1.1}, false},
		{"reserves sum above 1", Policy{Rate: 1, Burst: 1, ReserveForCritical: 0.6, ReserveForDefault: 0.6}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.p.Validate()
			if tt.ok && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tt.ok && err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

// TestTokenBucketPriorityShedding demonstrates the mechanism behind
// "shed sheddable traffic before critical traffic": with
// ReserveForCritical=0.3 and ReserveForDefault=0.2 on a Burst of 10,
// Default's floor is 3 and Sheddable's is 5. Once the bucket drains to
// 3, Default and Sheddable are both denied, but Critical -- whose
// floor is always 0 -- still gets through.
func TestTokenBucketPriorityShedding(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	// Rate is practically zero so refill during the test is
	// negligible: the test relies on exact token counts.
	tb, err := NewTokenBucket(TokenBucketConfig{
		Client: client, Rate: 0.0001, Burst: 10,
		ReserveForCritical: 0.3, ReserveForDefault: 0.2,
	})
	if err != nil {
		t.Fatalf("NewTokenBucket: %v", err)
	}

	const key = "shedding"
	allowedCount := 0
	for range 20 {
		d, err := tb.Allow(ctx, ratelimit.Request{Key: key, Priority: ratelimit.PriorityDefault})
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		if !d.Allowed {
			break
		}
		allowedCount++
	}
	// Burst 10, floor 3: Default can spend down to 3, i.e. 7 requests.
	if allowedCount != 7 {
		t.Fatalf("Default allowed %d requests before hitting its floor, want 7", allowedCount)
	}

	d, err := tb.Allow(ctx, ratelimit.Request{Key: key, Priority: ratelimit.PrioritySheddable})
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if d.Allowed {
		t.Fatal("Sheddable: want denied -- its floor (5) is above the 3 tokens left")
	}

	d, err = tb.Allow(ctx, ratelimit.Request{Key: key, Priority: ratelimit.PriorityCritical})
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !d.Allowed {
		t.Fatal("Critical: want allowed -- its floor is always 0, so it may drain below Default/Sheddable's reserve")
	}
}

// TestAllowBatchAcrossPolicies checks that a batch spanning two
// different domains' policies is still atomic end to end: a key that
// would succeed under its own policy is still denied, and left
// untouched, when a sibling check in the same batch fails under a
// different policy.
func TestAllowBatchAcrossPolicies(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	policyA := Policy{Rate: 10, Burst: 3}
	policyB := Policy{Rate: 10, Burst: 1}

	tbB, err := NewTokenBucket(TokenBucketConfig{Client: client, Rate: policyB.Rate, Burst: policyB.Burst})
	if err != nil {
		t.Fatalf("NewTokenBucket: %v", err)
	}
	if _, err := tbB.Allow(ctx, ratelimit.Request{Key: "cross-b"}); err != nil {
		t.Fatalf("draining b: %v", err)
	}

	decisions, allowed, err := AllowBatchAcrossPolicies(ctx, client, "ratelimit:tb:", []PolicyCheck{
		{Key: "cross-a", Cost: 1, Policy: policyA}, // would succeed alone
		{Key: "cross-b", Cost: 1, Policy: policyB}, // already empty
	})
	if err != nil {
		t.Fatalf("AllowBatchAcrossPolicies: %v", err)
	}
	if allowed {
		t.Fatal("batch: want denied")
	}
	if len(decisions) != 2 || decisions[0].Allowed || decisions[1].Allowed {
		t.Fatalf("decisions = %+v, want both denied", decisions)
	}

	tbA, err := NewTokenBucket(TokenBucketConfig{Client: client, Rate: policyA.Rate, Burst: policyA.Burst})
	if err != nil {
		t.Fatalf("NewTokenBucket: %v", err)
	}
	d, err := tbA.Allow(ctx, ratelimit.Request{Key: "cross-a", Cost: 3})
	if err != nil || !d.Allowed {
		t.Fatalf("key a after denied batch: Allowed = %v, err = %v, want true, nil (full 3 still available)", d.Allowed, err)
	}
}
