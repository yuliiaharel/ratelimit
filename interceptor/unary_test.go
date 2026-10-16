package interceptor

import (
	"context"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/yuliiaharel/ratelimit"
)

func TestUnaryAllowsAndInvokesHandler(t *testing.T) {
	lim := &fakeLimiter{decisions: []ratelimit.Decision{{Allowed: true, Limit: 10, Remaining: 9}}}
	intercept := Unary(lim, staticKey("alice"))

	handlerCalled := false
	handler := func(_ context.Context, _ any) (any, error) {
		handlerCalled = true
		return "ok", nil
	}

	resp, err := intercept(context.Background(), "req", &grpc.UnaryServerInfo{}, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handlerCalled {
		t.Fatal("handler was not invoked")
	}
	if resp != "ok" {
		t.Fatalf("resp = %v, want ok", resp)
	}
	if len(lim.calls) != 1 || lim.calls[0].Key != "alice" {
		t.Fatalf("calls = %+v, want exactly one call with key alice", lim.calls)
	}
}

func TestUnaryDeniedFailsWithResourceExhaustedAndRetryInfo(t *testing.T) {
	lim := &fakeLimiter{decisions: []ratelimit.Decision{
		{Allowed: false, Limit: 10, Remaining: 0, RetryAfter: 3 * time.Second},
	}}
	intercept := Unary(lim, staticKey("alice"))

	handlerCalled := false
	handler := func(_ context.Context, _ any) (any, error) {
		handlerCalled = true
		return "ok", nil
	}

	_, err := intercept(context.Background(), "req", &grpc.UnaryServerInfo{}, handler)
	if handlerCalled {
		t.Fatal("handler should not run when denied")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("err is not a gRPC status: %v", err)
	}
	if st.Code() != codes.ResourceExhausted {
		t.Fatalf("code = %v, want ResourceExhausted", st.Code())
	}

	var retryInfo *errdetails.RetryInfo
	for _, d := range st.Details() {
		if ri, ok := d.(*errdetails.RetryInfo); ok {
			retryInfo = ri
		}
	}
	if retryInfo == nil {
		t.Fatal("no RetryInfo detail on the status")
	}
	if got := retryInfo.GetRetryDelay().AsDuration(); got != 3*time.Second {
		t.Fatalf("RetryDelay = %v, want 3s", got)
	}
}

func TestUnaryMissingKeyFailsWithInvalidArgument(t *testing.T) {
	lim := &fakeLimiter{}
	missingKey := func(context.Context) (string, bool) { return "", false }
	intercept := Unary(lim, missingKey)

	handler := func(_ context.Context, _ any) (any, error) {
		t.Fatal("handler should not run")
		return nil, nil
	}

	_, err := intercept(context.Background(), "req", &grpc.UnaryServerInfo{}, handler)
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
	if len(lim.calls) != 0 {
		t.Fatalf("limiter should not be called when the key is missing, got %d calls", len(lim.calls))
	}
}

func TestUnaryLimiterErrorFailsWithInternal(t *testing.T) {
	lim := &fakeLimiter{
		decisions: []ratelimit.Decision{{}},
		errs:      []error{ratelimit.ErrCostExceedsLimit},
	}
	intercept := Unary(lim, staticKey("alice"))
	handler := func(_ context.Context, _ any) (any, error) { return "ok", nil }

	_, err := intercept(context.Background(), "req", &grpc.UnaryServerInfo{}, handler)
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Internal {
		t.Fatalf("err = %v, want Internal", err)
	}
}
