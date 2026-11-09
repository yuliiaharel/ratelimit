// Command ratelimitd runs RateLimitService as a standalone HTTP
// server, backed by Redis. Per-domain limits come from a policy file,
// reloaded automatically when it changes -- no restart needed.
//
// Configure via environment variables:
//
//	REDIS_ADDR     Redis address (default "localhost:6379")
//	POLICIES_PATH  path to the policy YAML file (default "policies.yaml")
//	ADDR           address to listen on (default ":8080")
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

	"github.com/redis/go-redis/v9"

	"github.com/yuliiaharel/ratelimit/gen/ratelimit/v1/ratelimitv1connect"
	"github.com/yuliiaharel/ratelimit/policy"
	"github.com/yuliiaharel/ratelimit/service"
)

func main() {
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	policiesPath := getEnv("POLICIES_PATH", "policies.yaml")
	addr := getEnv("ADDR", ":8080")

	client := redis.NewClient(&redis.Options{Addr: redisAddr})
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
	path, handler := ratelimitv1connect.NewRateLimitServiceHandler(service.NewServer(client, policies))
	mux.Handle(path, handler)

	log.Printf("ratelimitd listening on %s (redis=%s policies=%s)", addr, redisAddr, policiesPath)
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
