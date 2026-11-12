package service_test

import (
	"log"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yuliiaharel/ratelimit/gen/ratelimit/v1/ratelimitv1connect"
	"github.com/yuliiaharel/ratelimit/policy"
	"github.com/yuliiaharel/ratelimit/service"
)

// Serving RateLimitService from your own binary; cmd/ratelimitd does
// the same with configuration from the environment. This example needs
// a running Redis and a policy file, so it is compiled but not run by
// `go test`.
func ExampleNewServer() {
	// ContextTimeoutEnabled makes go-redis honor the per-call deadline
	// the server sets; without it a stalled Redis holds each request
	// for go-redis's own 3s read timeout.
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", ContextTimeoutEnabled: true})
	defer func() { _ = rdb.Close() }()

	policies, err := policy.Load("policies.yaml")
	if err != nil {
		log.Printf("loading policies: %v", err)
		return
	}

	srv := service.NewServer(rdb, policies, service.WithBackendTimeout(50*time.Millisecond))

	mux := http.NewServeMux()
	mux.Handle(ratelimitv1connect.NewRateLimitServiceHandler(srv))
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Printf("serve: %v", err)
	}
}
