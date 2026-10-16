package interceptor

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/yuliiaharel/ratelimit"
)

// staticKey returns a KeyFunc that always resolves to key, ignoring
// the context -- a stand-in for KeyFromMetadata in tests that aren't
// about key extraction itself.
func staticKey(key string) KeyFunc {
	return func(context.Context) (string, bool) { return key, true }
}

// fakeLimiter returns a scripted sequence of decisions (and, optionally,
// errors) in call order, falling back to an always-allow Decision once
// the script runs out. It records every request it was asked about.
type fakeLimiter struct {
	decisions []ratelimit.Decision
	errs      []error // parallel to decisions; a nil entry means no error

	calls []ratelimit.Request
}

func (f *fakeLimiter) Allow(_ context.Context, req ratelimit.Request) (ratelimit.Decision, error) {
	i := len(f.calls)
	f.calls = append(f.calls, req)

	if i >= len(f.decisions) {
		return ratelimit.Decision{Allowed: true}, nil
	}
	var err error
	if i < len(f.errs) {
		err = f.errs[i]
	}
	return f.decisions[i], err
}

// fakeServerStream is a minimal grpc.ServerStream for testing: Context
// returns whatever was configured, RecvMsg just counts calls, and every
// other method is an unused no-op.
type fakeServerStream struct {
	ctx       context.Context
	recvCalls int
}

var _ grpc.ServerStream = (*fakeServerStream)(nil)

func (s *fakeServerStream) Context() context.Context     { return s.ctx }
func (s *fakeServerStream) RecvMsg(any) error            { s.recvCalls++; return nil }
func (s *fakeServerStream) SendMsg(any) error            { return nil }
func (s *fakeServerStream) SetHeader(metadata.MD) error  { return nil }
func (s *fakeServerStream) SendHeader(metadata.MD) error { return nil }
func (s *fakeServerStream) SetTrailer(metadata.MD)       {}
