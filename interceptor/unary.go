package interceptor

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/yuliiaharel/ratelimit"
)

// Unary returns a grpc.UnaryServerInterceptor that checks lim, using
// keyFunc to find the key, before invoking the handler.
//
// A missing key fails with codes.InvalidArgument. An over-limit call
// fails with codes.ResourceExhausted and a RetryInfo detail carrying
// the decision's RetryAfter.
func Unary(lim ratelimit.Limiter, keyFunc KeyFunc) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (any, error) {
		key, ok := keyFunc(ctx)
		if !ok {
			return nil, errMissingKey
		}

		d, err := lim.Allow(ctx, ratelimit.Request{Key: key})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "ratelimit: %v", err)
		}
		if !d.Allowed {
			return nil, resourceExhausted(d)
		}

		return handler(ctx, req)
	}
}
