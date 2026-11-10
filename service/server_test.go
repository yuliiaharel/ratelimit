package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	ratelimitv1 "github.com/yuliiaharel/ratelimit/gen/ratelimit/v1"
	"github.com/yuliiaharel/ratelimit/gen/ratelimit/v1/ratelimitv1connect"
	"github.com/yuliiaharel/ratelimit/policy"
)

// newTestServer starts a real Redis container, a Server backed by it
// (every domain sharing one policy of rate/burst, via the file's
// fallback), and an httptest server exposing the generated connect
// handler. Returns a typed client plus the server's base URL (for the
// plain-JSON test). Skipped, not failed, if Docker isn't available.
func newTestServer(t testing.TB, rate float64, burst int) (ratelimitv1connect.RateLimitServiceClient, string) {
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

	policies := newTestPolicies(t, rate, burst)

	mux := http.NewServeMux()
	path, handler := ratelimitv1connect.NewRateLimitServiceHandler(NewServer(redisClient, policies))
	mux.Handle(path, handler)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return ratelimitv1connect.NewRateLimitServiceClient(http.DefaultClient, srv.URL), srv.URL
}

// newTestPolicies writes a policy file with just a fallback policy, so
// every domain shares the given rate/burst -- matching the uniform-
// policy behavior these tests exercise (per-domain policies get their
// own dedicated tests).
func newTestPolicies(t testing.TB, rate float64, burst int) *policy.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policies.yaml")
	content := fmt.Sprintf("default:\n  rate: %v\n  burst: %d\n", rate, burst)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	store, err := policy.Load(path)
	if err != nil {
		t.Fatalf("policy.Load: %v", err)
	}
	return store
}

func TestServerShouldAllow(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestServer(t, 10, 3)

	for i := range 3 {
		resp, err := client.ShouldAllow(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowRequest{
			Domain: "api", Key: "user:42",
		}))
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if !resp.Msg.GetAllowed() {
			t.Fatalf("request %d: want allowed", i)
		}
	}

	resp, err := client.ShouldAllow(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowRequest{
		Domain: "api", Key: "user:42",
	}))
	if err != nil {
		t.Fatalf("4th request: %v", err)
	}
	if resp.Msg.GetAllowed() {
		t.Fatal("4th request: want denied")
	}
	if resp.Msg.GetReason() != ratelimitv1.ShouldAllowResponse_REASON_OVER_LIMIT {
		t.Fatalf("Reason = %v, want REASON_OVER_LIMIT", resp.Msg.GetReason())
	}
	if resp.Msg.GetRetryAfter().AsDuration() <= 0 {
		t.Fatal("want RetryAfter > 0")
	}
	if resp.Msg.GetPolicyId() == "" {
		t.Fatal("want a non-empty PolicyId")
	}
}

func TestServerShouldAllowDomainsAreIndependent(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestServer(t, 10, 1)

	a, err := client.ShouldAllow(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowRequest{Domain: "api", Key: "user:42"}))
	if err != nil || !a.Msg.GetAllowed() {
		t.Fatalf("domain api: Allowed = %v, err = %v, want true, nil", a.Msg.GetAllowed(), err)
	}

	b, err := client.ShouldAllow(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowRequest{
		Domain: "send-message", Key: "user:42",
	}))
	if err != nil || !b.Msg.GetAllowed() {
		t.Fatalf("domain send-message: Allowed = %v, err = %v, want true, nil", b.Msg.GetAllowed(), err)
	}
}

func TestServerShouldAllowMissingKeyIsInvalidArgument(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestServer(t, 10, 3)

	_, err := client.ShouldAllow(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowRequest{Domain: "api"}))
	assertCode(t, err, connect.CodeInvalidArgument)
}

func TestServerShouldAllowCostAboveLimitIsInvalidArgument(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestServer(t, 10, 3)

	_, err := client.ShouldAllow(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowRequest{
		Domain: "api", Key: "k", Cost: 10,
	}))
	assertCode(t, err, connect.CodeInvalidArgument)
}

func TestServerShouldAllowBatchAllOrNothing(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestServer(t, 10, 1)

	// Drain key b first, so the batch has one key that would succeed
	// alone (a) and one that wouldn't (b).
	if _, err := client.ShouldAllow(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowRequest{Domain: "api", Key: "b"})); err != nil {
		t.Fatalf("draining b: %v", err)
	}

	resp, err := client.ShouldAllowBatch(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowBatchRequest{
		Checks: []*ratelimitv1.ShouldAllowRequest{
			{Domain: "api", Key: "a"},
			{Domain: "api", Key: "b"},
		},
	}))
	if err != nil {
		t.Fatalf("ShouldAllowBatch: %v", err)
	}
	if resp.Msg.GetAllowed() {
		t.Fatal("batch: want denied")
	}
	results := resp.Msg.GetResults()
	if len(results) != 2 || results[0].GetAllowed() || results[1].GetAllowed() {
		t.Fatalf("results = %+v, want both denied -- the batch is all-or-nothing", results)
	}

	// key a's token must be untouched: a denied batch consumes nothing.
	a, err := client.ShouldAllow(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowRequest{Domain: "api", Key: "a"}))
	if err != nil || !a.Msg.GetAllowed() {
		t.Fatalf("key a after denied batch: Allowed = %v, err = %v, want true, nil", a.Msg.GetAllowed(), err)
	}
}

func TestServerShouldAllowBatchEmptyChecksIsInvalidArgument(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestServer(t, 10, 3)

	_, err := client.ShouldAllowBatch(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowBatchRequest{}))
	assertCode(t, err, connect.CodeInvalidArgument)
}

// TestServerPlainJSON confirms the README's claim that connect-go lets
// the same handler answer plain HTTP/JSON, not just the generated
// client's wire protocol: it POSTs raw JSON with net/http instead.
func TestServerPlainJSON(t *testing.T) {
	_, baseURL := newTestServer(t, 10, 3)

	resp, err := http.Post(
		baseURL+"/ratelimit.v1.RateLimitService/ShouldAllow",
		"application/json",
		strings.NewReader(`{"domain":"api","key":"user:42","cost":1}`),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}

	var decoded struct {
		Allowed bool `json:"allowed"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !decoded.Allowed {
		t.Fatal("want allowed")
	}
}

// newTestServerWithPolicyFile is like newTestServer, but with a
// caller-supplied policy file, for tests that need more than one
// uniform policy (per-domain limits, priority reservations).
func newTestServerWithPolicyFile(t *testing.T, policyYAML string) ratelimitv1connect.RateLimitServiceClient {
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

	path := filepath.Join(t.TempDir(), "policies.yaml")
	if err := os.WriteFile(path, []byte(policyYAML), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	policies, err := policy.Load(path)
	if err != nil {
		t.Fatalf("policy.Load: %v", err)
	}

	mux := http.NewServeMux()
	handlerPath, handler := ratelimitv1connect.NewRateLimitServiceHandler(NewServer(redisClient, policies))
	mux.Handle(handlerPath, handler)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return ratelimitv1connect.NewRateLimitServiceClient(http.DefaultClient, srv.URL)
}

func TestServerPerDomainPoliciesAreIndependent(t *testing.T) {
	ctx := context.Background()
	client := newTestServerWithPolicyFile(t, `
policies:
  api:
    rate: 10
    burst: 3
  send-message:
    rate: 10
    burst: 1
`)

	// "api" allows 3, then denies.
	for i := range 3 {
		resp, err := client.ShouldAllow(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowRequest{Domain: "api", Key: "u"}))
		if err != nil || !resp.Msg.GetAllowed() {
			t.Fatalf("api request %d: Allowed = %v, err = %v, want true, nil", i, resp.Msg.GetAllowed(), err)
		}
	}
	resp, err := client.ShouldAllow(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowRequest{Domain: "api", Key: "u"}))
	if err != nil || resp.Msg.GetAllowed() {
		t.Fatalf("api 4th request: Allowed = %v, err = %v, want false, nil", resp.Msg.GetAllowed(), err)
	}
	if resp.Msg.GetPolicyId() != "api" {
		t.Fatalf("PolicyId = %q, want api", resp.Msg.GetPolicyId())
	}

	// "send-message" has its own, much smaller budget: allows 1, then denies.
	resp, err = client.ShouldAllow(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowRequest{Domain: "send-message", Key: "u"}))
	if err != nil || !resp.Msg.GetAllowed() {
		t.Fatalf("send-message 1st request: Allowed = %v, err = %v, want true, nil", resp.Msg.GetAllowed(), err)
	}
	resp, err = client.ShouldAllow(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowRequest{Domain: "send-message", Key: "u"}))
	if err != nil || resp.Msg.GetAllowed() {
		t.Fatalf("send-message 2nd request: Allowed = %v, err = %v, want false, nil", resp.Msg.GetAllowed(), err)
	}

	// An unlisted domain falls back to "default".
	resp, err = client.ShouldAllow(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowRequest{Domain: "unlisted", Key: "u"}))
	if err != nil {
		t.Fatalf("unlisted domain: %v", err)
	}
	if resp.Msg.GetPolicyId() != "default" {
		t.Fatalf("PolicyId = %q, want default", resp.Msg.GetPolicyId())
	}
}

// TestServerPriorityShedding exercises priority-aware shedding through
// the real proto/HTTP path: with Burst=10, ReserveForCritical=0.3,
// Default can only drain the bucket to 3, but Critical can still
// drain it to 0.
func TestServerPriorityShedding(t *testing.T) {
	ctx := context.Background()
	client := newTestServerWithPolicyFile(t, `
policies:
  api:
    rate: 0.0001
    burst: 10
    reserve_for_critical: 0.3
`)

	allowedCount := 0
	for range 20 {
		resp, err := client.ShouldAllow(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowRequest{
			Domain: "api", Key: "u", Priority: ratelimitv1.Priority_PRIORITY_DEFAULT,
		}))
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		if !resp.Msg.GetAllowed() {
			break
		}
		allowedCount++
	}
	if allowedCount != 7 {
		t.Fatalf("Default allowed %d requests before hitting its floor, want 7", allowedCount)
	}

	resp, err := client.ShouldAllow(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowRequest{
		Domain: "api", Key: "u", Priority: ratelimitv1.Priority_PRIORITY_CRITICAL,
	}))
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !resp.Msg.GetAllowed() {
		t.Fatal("Critical: want allowed -- it may drain below Default's reserved floor")
	}
}

// TestServerShouldAllowBatchAcrossDomains confirms a batch spanning
// domains with different policies is still atomic: a key that would
// succeed under its own (generous) policy is still denied, and left
// untouched, when a sibling check fails under a different (tighter)
// domain's policy.
func TestServerShouldAllowBatchAcrossDomains(t *testing.T) {
	ctx := context.Background()
	client := newTestServerWithPolicyFile(t, `
policies:
  api:
    rate: 10
    burst: 3
  send-message:
    rate: 10
    burst: 1
`)

	// Drain send-message's key first.
	if _, err := client.ShouldAllow(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowRequest{
		Domain: "send-message", Key: "u",
	})); err != nil {
		t.Fatalf("draining send-message: %v", err)
	}

	resp, err := client.ShouldAllowBatch(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowBatchRequest{
		Checks: []*ratelimitv1.ShouldAllowRequest{
			{Domain: "api", Key: "u"},          // would succeed alone (burst 3, untouched)
			{Domain: "send-message", Key: "u"}, // already empty
		},
	}))
	if err != nil {
		t.Fatalf("ShouldAllowBatch: %v", err)
	}
	if resp.Msg.GetAllowed() {
		t.Fatal("batch: want denied")
	}
	results := resp.Msg.GetResults()
	if len(results) != 2 || results[0].GetAllowed() || results[1].GetAllowed() {
		t.Fatalf("results = %+v, want both denied", results)
	}

	// api's key must be untouched: still has its full burst of 3.
	apiResp, err := client.ShouldAllow(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowRequest{
		Domain: "api", Key: "u", Cost: 3,
	}))
	if err != nil || !apiResp.Msg.GetAllowed() {
		t.Fatalf("api key after denied batch: Allowed = %v, err = %v, want true, nil", apiResp.Msg.GetAllowed(), err)
	}
}

func assertCode(t *testing.T, err error, want connect.Code) {
	t.Helper()
	var connErr *connect.Error
	if !errors.As(err, &connErr) {
		t.Fatalf("err = %v, want a *connect.Error", err)
	}
	if connErr.Code() != want {
		t.Fatalf("code = %v, want %v", connErr.Code(), want)
	}
}
