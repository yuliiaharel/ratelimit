// Package interceptor adapts a ratelimit.Limiter into gRPC unary and
// streaming server interceptors.
//
// A KeyFunc extracts the key to limit on from the incoming context,
// e.g. an API key header via KeyFromMetadata. Each check charges one
// token at default priority. A call over the limit fails with
// codes.ResourceExhausted and a RetryInfo detail saying how long to
// back off; a call with no key fails with codes.InvalidArgument; a
// limiter error fails with codes.Internal.
//
// Streams are checked once when they open (PerStream) or before every
// message the client sends (PerMessage).
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
