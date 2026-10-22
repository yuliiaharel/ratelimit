package client

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/yuliiaharel/ratelimit/gen/ratelimit/v1/ratelimitv1connect"
	"github.com/yuliiaharel/ratelimit/policy"
	"github.com/yuliiaharel/ratelimit/service"
)

// newTestClient starts a real Redis container and a RateLimitService
// on top of it (every domain sharing one policy of rate/burst), and
// returns a Client pointed at it. Skipped, not failed, if Docker isn't
// available.
func newTestClient(t *testing.T, rate float64, burst int) *Client {
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
	redisClient := redis.NewClient(opts)
	t.Cleanup(func() { _ = redisClient.Close() })

	policyPath := filepath.Join(t.TempDir(), "policies.yaml")
	content := fmt.Sprintf("default:\n  rate: %v\n  burst: %d\n", rate, burst)
	if err := os.WriteFile(policyPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	policies, err := policy.Load(policyPath)
	if err != nil {
		t.Fatalf("policy.Load: %v", err)
	}

	mux := http.NewServeMux()
	path, handler := ratelimitv1connect.NewRateLimitServiceHandler(service.NewServer(redisClient, policies))
	mux.Handle(path, handler)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return New(srv.URL, nil)
}

func TestClientShouldAllow(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(t, 10, 3)

	for i := range 3 {
		d, err := c.ShouldAllow(ctx, Check{Domain: "api", Key: "user:42"})
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if !d.Allowed {
			t.Fatalf("request %d: want allowed", i)
		}
	}

	d, err := c.ShouldAllow(ctx, Check{Domain: "api", Key: "user:42"})
	if err != nil {
		t.Fatalf("4th request: %v", err)
	}
	if d.Allowed {
		t.Fatal("4th request: want denied")
	}
	if d.RetryAfter <= 0 {
		t.Fatal("want RetryAfter > 0")
	}
	if d.Limit != 3 {
		t.Fatalf("Limit = %d, want 3", d.Limit)
	}
}

func TestClientShouldAllowBatch(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(t, 10, 1)

	// Drain "b" first, so the batch has one check that would succeed
	// alone and one that wouldn't.
	if _, err := c.ShouldAllow(ctx, Check{Domain: "api", Key: "b"}); err != nil {
		t.Fatalf("draining b: %v", err)
	}

	allowed, decisions, err := c.ShouldAllowBatch(ctx, []Check{
		{Domain: "api", Key: "a"},
		{Domain: "api", Key: "b"},
	})
	if err != nil {
		t.Fatalf("ShouldAllowBatch: %v", err)
	}
	if allowed {
		t.Fatal("batch: want denied")
	}
	if len(decisions) != 2 || decisions[0].Allowed || decisions[1].Allowed {
		t.Fatalf("decisions = %+v, want both denied -- the batch is all-or-nothing", decisions)
	}

	// "a" must be untouched: a denied batch consumes nothing.
	d, err := c.ShouldAllow(ctx, Check{Domain: "api", Key: "a"})
	if err != nil || !d.Allowed {
		t.Fatalf("key a after denied batch: Allowed = %v, err = %v, want true, nil", d.Allowed, err)
	}
}
