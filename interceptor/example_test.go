package interceptor_test

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/yuliiaharel/ratelimit/clocktest"
	"github.com/yuliiaharel/ratelimit/interceptor"
	"github.com/yuliiaharel/ratelimit/memory"
)

// Wiring both interceptors into a gRPC server. Each call is charged
// one token against the key found in the x-api-key metadata header.
func Example() {
	lim, err := memory.NewTokenBucket(memory.TokenBucketConfig{
		Rate:  10, // tokens per second
		Burst: 20, // bucket capacity
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	keyFunc := interceptor.KeyFromMetadata("x-api-key")
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(interceptor.Unary(lim, keyFunc)),
		grpc.StreamInterceptor(interceptor.Stream(lim, keyFunc, interceptor.PerMessage)),
	)
	defer srv.Stop()
	// Register services and call srv.Serve as usual.
}

// The interceptor is called directly here to show what a handler's
// caller sees; in a server, gRPC makes this call for every request.
func ExampleUnary() {
	clock := clocktest.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	lim, err := memory.NewTokenBucket(memory.TokenBucketConfig{Rate: 1, Burst: 1, Clock: clock})
	if err != nil {
		fmt.Println(err)
		return
	}
	unary := interceptor.Unary(lim, interceptor.KeyFromMetadata("x-api-key"))

	handler := func(context.Context, any) (any, error) { return "handled", nil }
	call := func(ctx context.Context) {
		resp, err := unary(ctx, nil, &grpc.UnaryServerInfo{}, handler)
		fmt.Printf("resp=%v code=%s\n", resp, status.Code(err))
	}

	alice := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", "alice"))
	call(alice)
	call(alice) // over the limit: the handler never runs

	// No key to limit on is the caller's mistake, not a denial.
	call(context.Background())

	// Output:
	// resp=handled code=OK
	// resp=<nil> code=ResourceExhausted
	// resp=<nil> code=InvalidArgument
}
