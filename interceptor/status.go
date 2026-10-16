package interceptor

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/yuliiaharel/ratelimit"
)

// errMissingKey is returned when keyFunc can't find a key to check.
// Distinct from a rate-limit denial: the caller's request is malformed
// (or missing required auth/metadata), not merely over budget.
var errMissingKey = status.Error(codes.InvalidArgument, "ratelimit: missing rate limit key")

// resourceExhausted builds the error a denied request fails with:
// codes.ResourceExhausted, with a RetryInfo detail carrying d's
// RetryAfter so well-behaved clients know how long to back off.
func resourceExhausted(d ratelimit.Decision) error {
	st := status.New(codes.ResourceExhausted,
		fmt.Sprintf("ratelimit: over limit (remaining %d/%d)", d.Remaining, d.Limit))

	withDetails, err := st.WithDetails(&errdetails.RetryInfo{
		RetryDelay: durationpb.New(d.RetryAfter),
	})
	if err != nil {
		// WithDetails only fails if a detail message doesn't satisfy
		// proto.Message, which RetryInfo always does. Fall back to the
		// plain status rather than let a denial panic or go unreported.
		return st.Err()
	}
	return withDetails.Err()
}
