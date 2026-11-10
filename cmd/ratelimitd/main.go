// Command ratelimitd runs RateLimitService as a standalone HTTP
// server, backed by Redis. Per-domain limits come from a policy file,
// reloaded automatically when it changes -- no restart needed.
//
// Configure via environment variables:
//
//	REDIS_ADDR     Redis address (default "localhost:6379")
//	POLICIES_PATH  path to the policy YAML file (default "policies.yaml")
//	ADDR           address to listen on (default ":8080")
//	REDIS_TIMEOUT  deadline for each Redis call, as a Go duration
//	               (default service.DefaultBackendTimeout, 50ms). On
//	               timeout or error, each domain's failure_mode decides.
//
// Run it (with Redis up via `docker compose up -d`):
//
//	go run ./cmd/ratelimitd
//
// Then, since it's built with connect-go, plain HTTP/JSON works
// without any generated client:
//
//	curl -s localhost:8080/ratelimit.v1.RateLimitService/ShouldAllow \
//	  -H 'Content-Type: application/json' \
//	  -d '{"domain":"api","key":"user:42","cost":1}'
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yuliiaharel/ratelimit/gen/ratelimit/v1/ratelimitv1connect"
	"github.com/yuliiaharel/ratelimit/policy"
	"github.com/yuliiaharel/ratelimit/service"
)

func main() {
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	policiesPath := getEnv("POLICIES_PATH", "policies.yaml")
	addr := getEnv("ADDR", ":8080")
	redisTimeout := service.DefaultBackendTimeout
	if v := os.Getenv("REDIS_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			log.Fatalf("parsing REDIS_TIMEOUT=%q: %v", v, err)
		}
		if d <= 0 {
			log.Fatalf("REDIS_TIMEOUT=%q: must be positive", v)
		}
		redisTimeout = d
	}

	// ContextTimeoutEnabled makes go-redis honor the server's per-call
	// deadline on socket reads and writes; without it a stalled Redis
	// holds each call for go-redis's own ReadTimeout (3s) instead.
	client := redis.NewClient(&redis.Options{Addr: redisAddr, ContextTimeoutEnabled: true})
	defer func() { _ = client.Close() }()

	policies, err := policy.Load(policiesPath)
	if err != nil {
		log.Fatalf("loading policies from %s: %v", policiesPath, err)
	}

	watchCtx, cancelWatch := context.WithCancel(context.Background())
	defer cancelWatch()
	go func() {
		if err := policies.Watch(watchCtx); err != nil {
			log.Printf("policy watcher stopped: %v", err)
		}
	}()

	mux := http.NewServeMux()
	path, handler := ratelimitv1connect.NewRateLimitServiceHandler(service.NewServer(client, policies, service.WithBackendTimeout(redisTimeout)))
	mux.Handle(path, handler)

	log.Printf("ratelimitd listening on %s (redis=%s redis_timeout=%s policies=%s)", addr, redisAddr, redisTimeout, policiesPath)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
