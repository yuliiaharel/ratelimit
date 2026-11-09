// Command ratelimitd runs RateLimitService as a standalone HTTP
// server, backed by Redis.
//
// Configure via environment variables:
//
//	REDIS_ADDR   Redis address (default "localhost:6379")
//	RATE         tokens added per second, shared by every domain for now (default 10)
//	BURST        bucket capacity (default 20)
//	ADDR         address to listen on (default ":8080")
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
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/redis/go-redis/v9"

	"github.com/yuliiaharel/ratelimit/gen/ratelimit/v1/ratelimitv1connect"
	"github.com/yuliiaharel/ratelimit/redisstore"
	"github.com/yuliiaharel/ratelimit/service"
)

func main() {
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	rate := getEnvFloat("RATE", 10)
	burst := getEnvInt("BURST", 20)
	addr := getEnv("ADDR", ":8080")

	client := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() { _ = client.Close() }()

	tb, err := redisstore.NewTokenBucket(redisstore.TokenBucketConfig{Client: client, Rate: rate, Burst: burst})
	if err != nil {
		log.Fatalf("NewTokenBucket: %v", err)
	}

	mux := http.NewServeMux()
	path, handler := ratelimitv1connect.NewRateLimitServiceHandler(service.NewServer(tb))
	mux.Handle(path, handler)

	log.Printf("ratelimitd listening on %s (redis=%s rate=%v/s burst=%d)", addr, redisAddr, rate, burst)
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

func getEnvFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
