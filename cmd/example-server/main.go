// Command example-server runs a minimal gRPC server with this
// package's rate-limit interceptors in front of gRPC's standard health
// service, to show interceptor.Unary and interceptor.Stream wired into
// a real server without needing a custom .proto (that comes in roadmap
// step 7, once buf is part of the workflow).
//
// Run it:
//
//	go run ./cmd/example-server
//
// Then, in another terminal (needs grpcurl):
//
//	grpcurl -plaintext -H 'x-api-key: alice' localhost:50051 grpc.health.v1.Health/Check
//
// Repeat that quickly (more than 3 times in under a second) and the
// 4th+ call fails with RESOURCE_EXHAUSTED and a RetryInfo detail
// naming how long to back off. A different -H 'x-api-key: ...' value
// gets its own independent budget. Omitting the header entirely fails
// with INVALID_ARGUMENT instead -- the interceptor couldn't find a key
// to check.
//
// Health/Watch is server-streaming, so it exercises interceptor.Stream
// in PerStream mode: one check when the watch opens, limiting how many
// watches a key may start, rather than how many messages flow once one
// is open.
//
//	grpcurl -plaintext -H 'x-api-key: alice' -d '{}' localhost:50051 grpc.health.v1.Health/Watch
package main

import (
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/yuliiaharel/ratelimit/interceptor"
	"github.com/yuliiaharel/ratelimit/memory"
)

const addr = ":50051"

func main() {
	// 3 requests/sec, with bursts up to 5 -- generous enough to try by
	// hand, tight enough to hit quickly.
	lim, err := memory.NewTokenBucket(memory.TokenBucketConfig{Rate: 3, Burst: 5})
	if err != nil {
		log.Fatalf("NewTokenBucket: %v", err)
	}

	keyFunc := interceptor.KeyFromMetadata("x-api-key")
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(interceptor.Unary(lim, keyFunc)),
		grpc.StreamInterceptor(interceptor.Stream(lim, keyFunc, interceptor.PerStream)),
	)

	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, healthSrv)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen on %s: %v", addr, err)
	}

	log.Printf("example-server listening on %s (rate-limited: 3 req/s, burst 5, per x-api-key)", addr)
	log.Println(`try: grpcurl -plaintext -H "x-api-key: alice" localhost:50051 grpc.health.v1.Health/Check`)

	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
