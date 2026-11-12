package redisstore_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/yuliiaharel/ratelimit"
	"github.com/yuliiaharel/ratelimit/redisstore"
)

// A Redis-backed token bucket shared by every process that uses the
// same Redis. This example needs a running Redis (`docker compose up
// -d`), so it is compiled but not run by `go test`.
func ExampleNewTokenBucket() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer func() { _ = rdb.Close() }()

	lim, err := redisstore.NewTokenBucket(redisstore.TokenBucketConfig{
		Client: rdb,
		Rate:   10, // tokens per second
		Burst:  20, // bucket capacity
		// Keep the last 30% of the bucket for critical requests and a
		// further 20% for default ones.
		ReserveForCritical: 0.3,
		ReserveForDefault:  0.2,
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	d, err := lim.Allow(context.Background(), ratelimit.Request{
		Key:      "user:42",
		Cost:     5,
		Priority: ratelimit.PrioritySheddable,
	})
	if err != nil {
		fmt.Println(err) // Redis unreachable: the caller decides what to do
		return
	}
	fmt.Println(d.Allowed, d.Remaining, d.RetryAfter)
}

// Charge a per-user and a per-tenant bucket together: either both are
// charged or neither is. Like ExampleNewTokenBucket, this needs a
// running Redis and is compiled but not run.
func ExampleTokenBucket_AllowBatch() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer func() { _ = rdb.Close() }()

	lim, err := redisstore.NewTokenBucket(redisstore.TokenBucketConfig{Client: rdb, Rate: 10, Burst: 20})
	if err != nil {
		fmt.Println(err)
		return
	}

	decisions, allowed, err := lim.AllowBatch(context.Background(), []ratelimit.Request{
		{Key: "user:42"},
		{Key: "tenant:acme", Cost: 3},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(allowed, len(decisions))
}
