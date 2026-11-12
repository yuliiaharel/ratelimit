package memory_test

import (
	"context"
	"fmt"
	"time"

	"github.com/yuliiaharel/ratelimit"
	"github.com/yuliiaharel/ratelimit/clocktest"
	"github.com/yuliiaharel/ratelimit/memory"
)

// A request can cost more than one token, so expensive operations use
// up the budget faster than cheap ones.
func ExampleNewTokenBucket() {
	clock := clocktest.New(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	lim, err := memory.NewTokenBucket(memory.TokenBucketConfig{Rate: 10, Burst: 20, Clock: clock})
	if err != nil {
		fmt.Println(err)
		return
	}
	ctx := context.Background()

	d, _ := lim.Allow(ctx, ratelimit.Request{Key: "tenant:acme", Cost: 15})
	fmt.Printf("cost 15: allowed=%v remaining=%d\n", d.Allowed, d.Remaining)

	// 5 tokens are left; the missing 5 refill in half a second.
	d, _ = lim.Allow(ctx, ratelimit.Request{Key: "tenant:acme", Cost: 10})
	fmt.Printf("cost 10: allowed=%v remaining=%d retry_after=%s\n", d.Allowed, d.Remaining, d.RetryAfter)

	// Output:
	// cost 15: allowed=true remaining=5
	// cost 10: allowed=false remaining=5 retry_after=500ms
}

// Windows are aligned to the Unix epoch, so the budget resets at the
// window boundary no matter when the key was first seen.
func ExampleNewFixedWindow() {
	clock := clocktest.New(time.Date(2026, 1, 1, 12, 0, 50, 0, time.UTC))
	lim, err := memory.NewFixedWindow(memory.FixedWindowConfig{Limit: 2, Window: time.Minute, Clock: clock})
	if err != nil {
		fmt.Println(err)
		return
	}
	ctx := context.Background()

	for range 3 {
		d, _ := lim.Allow(ctx, ratelimit.Request{Key: "user:42"})
		fmt.Printf("12:00:50 allowed=%v retry_after=%s\n", d.Allowed, d.RetryAfter)
	}

	// The next window starts at 12:01:00 with a full budget.
	clock.Advance(10 * time.Second)
	d, _ := lim.Allow(ctx, ratelimit.Request{Key: "user:42"})
	fmt.Printf("12:01:00 allowed=%v remaining=%d\n", d.Allowed, d.Remaining)

	// Output:
	// 12:00:50 allowed=true retry_after=0s
	// 12:00:50 allowed=true retry_after=0s
	// 12:00:50 allowed=false retry_after=10s
	// 12:01:00 allowed=true remaining=1
}

// The log variant counts exactly what was granted in the trailing
// window, so RetryAfter is the moment the oldest entry ages out.
func ExampleNewSlidingWindowLog() {
	clock := clocktest.New(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	lim, err := memory.NewSlidingWindowLog(memory.SlidingWindowLogConfig{Limit: 2, Window: time.Minute, Clock: clock})
	if err != nil {
		fmt.Println(err)
		return
	}
	ctx := context.Background()

	d, _ := lim.Allow(ctx, ratelimit.Request{Key: "user:42"})
	fmt.Printf("+0s  allowed=%v\n", d.Allowed)

	clock.Advance(30 * time.Second)
	d, _ = lim.Allow(ctx, ratelimit.Request{Key: "user:42"})
	fmt.Printf("+30s allowed=%v\n", d.Allowed)

	clock.Advance(10 * time.Second)
	d, _ = lim.Allow(ctx, ratelimit.Request{Key: "user:42"})
	fmt.Printf("+40s allowed=%v retry_after=%s\n", d.Allowed, d.RetryAfter)

	clock.Advance(20 * time.Second)
	d, _ = lim.Allow(ctx, ratelimit.Request{Key: "user:42"})
	fmt.Printf("+60s allowed=%v\n", d.Allowed)

	// Output:
	// +0s  allowed=true
	// +30s allowed=true
	// +40s allowed=false retry_after=20s
	// +60s allowed=true
}

// The counter variant estimates the trailing window from two counters:
// halfway through a window, half of the previous window's usage still
// counts.
func ExampleNewSlidingWindowCounter() {
	clock := clocktest.New(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	lim, err := memory.NewSlidingWindowCounter(memory.SlidingWindowCounterConfig{Limit: 10, Window: time.Minute, Clock: clock})
	if err != nil {
		fmt.Println(err)
		return
	}
	ctx := context.Background()

	// Spend the whole limit in the 12:00 window.
	d, _ := lim.Allow(ctx, ratelimit.Request{Key: "user:42", Cost: 10})
	fmt.Printf("12:00:00 cost 10: allowed=%v\n", d.Allowed)

	// At 12:01:30 the estimate is 10 * 0.5 = 5, so 5 more fit and 6 don't.
	clock.Advance(90 * time.Second)
	d, _ = lim.Allow(ctx, ratelimit.Request{Key: "user:42", Cost: 6})
	fmt.Printf("12:01:30 cost 6:  allowed=%v remaining=%d\n", d.Allowed, d.Remaining)
	d, _ = lim.Allow(ctx, ratelimit.Request{Key: "user:42", Cost: 5})
	fmt.Printf("12:01:30 cost 5:  allowed=%v remaining=%d\n", d.Allowed, d.Remaining)

	// Output:
	// 12:00:00 cost 10: allowed=true
	// 12:01:30 cost 6:  allowed=false remaining=5
	// 12:01:30 cost 5:  allowed=true remaining=0
}
