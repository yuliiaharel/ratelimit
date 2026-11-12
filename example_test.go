package ratelimit_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yuliiaharel/ratelimit"
	"github.com/yuliiaharel/ratelimit/clocktest"
	"github.com/yuliiaharel/ratelimit/memory"
)

// Every limiter is used the same way: build one, then call Allow with
// a key before doing the work. The fake clock only makes the output
// deterministic; leave Clock unset to use the system clock.
func ExampleLimiter() {
	clock := clocktest.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	tb, err := memory.NewTokenBucket(memory.TokenBucketConfig{
		Rate:  1, // tokens per second
		Burst: 2, // bucket capacity
		Clock: clock,
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	var lim ratelimit.Limiter = tb
	ctx := context.Background()

	for range 3 {
		d, err := lim.Allow(ctx, ratelimit.Request{Key: "user:42"})
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("allowed=%v remaining=%d retry_after=%s\n", d.Allowed, d.Remaining, d.RetryAfter)
	}

	// One second later one token has been refilled.
	clock.Advance(time.Second)
	d, _ := lim.Allow(ctx, ratelimit.Request{Key: "user:42"})
	fmt.Printf("allowed=%v remaining=%d\n", d.Allowed, d.Remaining)

	// A request that could never fit is an error, not a denial, so
	// callers don't retry it forever.
	_, err = lim.Allow(ctx, ratelimit.Request{Key: "user:42", Cost: 3})
	fmt.Println(errors.Is(err, ratelimit.ErrCostExceedsLimit))

	// Output:
	// allowed=true remaining=1 retry_after=0s
	// allowed=true remaining=0 retry_after=0s
	// allowed=false remaining=0 retry_after=1s
	// allowed=true remaining=0
	// true
}
