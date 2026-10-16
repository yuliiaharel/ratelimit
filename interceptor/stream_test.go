package interceptor

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/yuliiaharel/ratelimit"
)

func TestStreamPerStreamChecksOnceBeforeHandler(t *testing.T) {
	lim := &fakeLimiter{decisions: []ratelimit.Decision{{Allowed: true}}}
	intercept := Stream(lim, staticKey("alice"), PerStream)

	ss := &fakeServerStream{ctx: context.Background()}
	handlerCalled := false
	var gotStream grpc.ServerStream
	handler := func(_ any, stream grpc.ServerStream) error {
		handlerCalled = true
		gotStream = stream
		// PerStream doesn't wrap RecvMsg, so these shouldn't trigger
		// further Allow calls.
		_ = stream.RecvMsg(nil)
		_ = stream.RecvMsg(nil)
		return nil
	}

	if err := intercept(nil, ss, &grpc.StreamServerInfo{}, handler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handlerCalled {
		t.Fatal("handler was not invoked")
	}
	if len(lim.calls) != 1 {
		t.Fatalf("Allow called %d times, want exactly 1 (PerStream)", len(lim.calls))
	}
	if gotStream != ss {
		t.Fatal("PerStream should pass the original stream through unwrapped")
	}
}

func TestStreamPerStreamDeniedNeverInvokesHandler(t *testing.T) {
	lim := &fakeLimiter{decisions: []ratelimit.Decision{{Allowed: false, RetryAfter: time.Second}}}
	intercept := Stream(lim, staticKey("alice"), PerStream)

	ss := &fakeServerStream{ctx: context.Background()}
	handler := func(_ any, _ grpc.ServerStream) error {
		t.Fatal("handler should not run when denied")
		return nil
	}

	err := intercept(nil, ss, &grpc.StreamServerInfo{}, handler)
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		t.Fatalf("err = %v, want ResourceExhausted", err)
	}
}

func TestStreamPerMessageChecksEveryRecv(t *testing.T) {
	lim := &fakeLimiter{decisions: []ratelimit.Decision{
		{Allowed: true}, {Allowed: true}, {Allowed: false, RetryAfter: time.Second},
	}}
	intercept := Stream(lim, staticKey("alice"), PerMessage)

	ss := &fakeServerStream{ctx: context.Background()}
	var recvErrs []error
	handler := func(_ any, stream grpc.ServerStream) error {
		for range 3 {
			recvErrs = append(recvErrs, stream.RecvMsg(nil))
		}
		return nil
	}

	if err := intercept(nil, ss, &grpc.StreamServerInfo{}, handler); err != nil {
		t.Fatalf("unexpected error from the interceptor itself: %v", err)
	}
	if len(lim.calls) != 3 {
		t.Fatalf("Allow called %d times, want 3 (PerMessage)", len(lim.calls))
	}
	if recvErrs[0] != nil || recvErrs[1] != nil {
		t.Fatalf("first two RecvMsg calls should succeed, got %v", recvErrs[:2])
	}
	st, ok := status.FromError(recvErrs[2])
	if !ok || st.Code() != codes.ResourceExhausted {
		t.Fatalf("third RecvMsg = %v, want ResourceExhausted", recvErrs[2])
	}
	if ss.recvCalls != 2 {
		t.Fatalf("underlying RecvMsg called %d times, want 2 (the denied call must short-circuit)", ss.recvCalls)
	}
}

func TestStreamMissingKeyFailsWithInvalidArgument(t *testing.T) {
	lim := &fakeLimiter{}
	missingKey := func(context.Context) (string, bool) { return "", false }
	intercept := Stream(lim, missingKey, PerStream)

	ss := &fakeServerStream{ctx: context.Background()}
	handler := func(_ any, _ grpc.ServerStream) error {
		t.Fatal("handler should not run")
		return nil
	}

	err := intercept(nil, ss, &grpc.StreamServerInfo{}, handler)
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
}
