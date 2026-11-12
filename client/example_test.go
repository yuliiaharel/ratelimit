package client_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"connectrpc.com/connect"

	"github.com/yuliiaharel/ratelimit/client"
	ratelimitv1 "github.com/yuliiaharel/ratelimit/gen/ratelimit/v1"
	"github.com/yuliiaharel/ratelimit/gen/ratelimit/v1/ratelimitv1connect"
)

// stubService stands in for a running ratelimitd so the examples need
// no Redis: it allows every request.
type stubService struct {
	ratelimitv1connect.UnimplementedRateLimitServiceHandler
}

func (stubService) ShouldAllow(
	context.Context, *connect.Request[ratelimitv1.ShouldAllowRequest],
) (*connect.Response[ratelimitv1.ShouldAllowResponse], error) {
	return connect.NewResponse(&ratelimitv1.ShouldAllowResponse{
		Allowed:   true,
		Reason:    ratelimitv1.ShouldAllowResponse_REASON_OK,
		Limit:     20,
		Remaining: 15,
		PolicyId:  "api",
	}), nil
}

func newStubServer() *httptest.Server {
	mux := http.NewServeMux()
	mux.Handle(ratelimitv1connect.NewRateLimitServiceHandler(stubService{}))
	return httptest.NewServer(mux)
}

// Ask the service before doing the work. In production the URL is
// where ratelimitd listens, e.g. "http://localhost:8080".
func ExampleClient_ShouldAllow() {
	srv := newStubServer()
	defer srv.Close()

	c := client.New(srv.URL, nil)
	d, err := c.ShouldAllow(context.Background(), client.Check{
		Domain:   "api",
		Key:      "user:42",
		Cost:     5,
		Priority: client.PrioritySheddable,
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("allowed=%v reason=%s remaining=%d/%d policy=%s\n",
		d.Allowed, d.Reason, d.Remaining, d.Limit, d.PolicyID)

	// Output:
	// allowed=true reason=REASON_OK remaining=15/20 policy=api
}

// When the service can't be reached the client decides locally. Here
// one domain fails closed while everything else keeps the default,
// fail-open.
func ExampleNewWithConfig() {
	srv := newStubServer()
	srv.Close() // nothing is listening any more

	c := client.NewWithConfig(srv.URL, nil, client.Config{
		Timeout:            100 * time.Millisecond,
		DomainFallback:     map[string]client.FallbackMode{"payments": client.FallbackFailClosed},
		FallbackRetryAfter: 2 * time.Second,
	})

	for _, domain := range []string{"api", "payments"} {
		d, err := c.ShouldAllow(context.Background(), client.Check{Domain: domain, Key: "user:42"})
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("%s: allowed=%v reason=%s fallback=%v retry_after=%s\n",
			domain, d.Allowed, d.Reason, d.Fallback, d.RetryAfter)
	}

	// Output:
	// api: allowed=true reason=REASON_FAIL_OPEN fallback=true retry_after=0s
	// payments: allowed=false reason=REASON_FAIL_CLOSED fallback=true retry_after=2s
}
