package interceptor

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/yuliiaharel/ratelimit"
)

// StreamMode controls when Stream checks the limit over the life of one
// stream.
type StreamMode int

const (
	// PerStream checks the limit once, before the handler is invoked.
	// Good for limiting how many streams a key may open (e.g. one
	// long-lived subscription per key).
	PerStream StreamMode = iota

	// PerMessage checks the limit before every message the client
	// sends, inside RecvMsg. Good for limiting the rate of events a
	// client pushes over one long-lived stream.
	PerMessage
)

// Stream returns a grpc.StreamServerInterceptor that checks lim, using
// keyFunc to find the key, according to mode.
//
// A missing key fails with codes.InvalidArgument. An over-limit check
// fails with codes.ResourceExhausted and a RetryInfo detail carrying
// the decision's RetryAfter -- returned from the handler call (PerStream)
// or from the client's next RecvMsg (PerMessage).
func Stream(lim ratelimit.Limiter, keyFunc KeyFunc, mode StreamMode) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		key, ok := keyFunc(ss.Context())
		if !ok {
			return errMissingKey
		}

		check := func() error {
			d, err := lim.Allow(ss.Context(), ratelimit.Request{Key: key})
			if err != nil {
				return status.Errorf(codes.Internal, "ratelimit: %v", err)
			}
			if !d.Allowed {
				return resourceExhausted(d)
			}
			return nil
		}

		if mode == PerStream {
			if err := check(); err != nil {
				return err
			}
			return handler(srv, ss)
		}

		return handler(srv, &limitedServerStream{ServerStream: ss, check: check})
	}
}

// limitedServerStream wraps a grpc.ServerStream to run check before
// every message received from the client.
type limitedServerStream struct {
	grpc.ServerStream
	check func() error
}

func (s *limitedServerStream) RecvMsg(m any) error {
	if err := s.check(); err != nil {
		return err
	}
	return s.ServerStream.RecvMsg(m)
}
