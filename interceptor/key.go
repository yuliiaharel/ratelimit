// Package interceptor adapts a ratelimit.Limiter into gRPC unary and
// streaming server interceptors.
package interceptor

import (
	"context"

	"google.golang.org/grpc/metadata"
)

// KeyFunc extracts the rate-limit key from an incoming request's
// context. It returns ok=false when no key could be found (e.g. the
// expected metadata header is missing), in which case the interceptor
// rejects the call with codes.InvalidArgument rather than guessing.
type KeyFunc func(ctx context.Context) (key string, ok bool)

// KeyFromMetadata returns a KeyFunc that reads header from incoming
// gRPC metadata (case-insensitive, per gRPC's metadata convention). If
// the client set the header more than once, the first value is used.
func KeyFromMetadata(header string) KeyFunc {
	return func(ctx context.Context) (string, bool) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return "", false
		}
		vals := md.Get(header)
		if len(vals) == 0 || vals[0] == "" {
			return "", false
		}
		return vals[0], true
	}
}
