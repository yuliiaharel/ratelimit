package client

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/redis/go-redis/v9"

	"github.com/yuliiaharel/ratelimit/clocktest"
	"github.com/yuliiaharel/ratelimit/gen/ratelimit/v1/ratelimitv1connect"
	"github.com/yuliiaharel/ratelimit/policy"
	"github.com/yuliiaharel/ratelimit/service"
)

// closedAddr returns a loopback address nothing is listening on, so
// dialing it fails at once with "connection refused".
func closedAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return addr
}

// slowServer accepts requests and never answers them until the
// client's context ends.
func slowServer(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// The fake clock starts long before the real one, so the client's
// deadline (fake now + timeout) has already passed in real time: the
// call times out immediately and deterministically, exactly as if the
// service had stalled past the timeout.
var fakeEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

func withFakeClock(c *Client) *Client {
	c.clock = clocktest.New(fakeEpoch)
	return c
}

func TestClientFallback(t *testing.T) {
	tests := []struct {
		name        string
		url         func(t *testing.T) string
		cfg         Config
		wantAllowed bool
		wantReason  Reason
	}{
		{"unreachable/default is fail-open", func(t *testing.T) string { return "http://" + closedAddr(t) },
			Config{}, true, ReasonFailOpen},
		{"unreachable/fail-closed", func(t *testing.T) string { return "http://" + closedAddr(t) },
			Config{Fallback: FallbackFailClosed}, false, ReasonFailClosed},
		{"slow/fail-open", slowServer, Config{Fallback: FallbackFailOpen}, true, ReasonFailOpen},
		{"slow/fail-closed", slowServer, Config{Fallback: FallbackFailClosed}, false, ReasonFailClosed},
		{"unreachable/domain override", func(t *testing.T) string { return "http://" + closedAddr(t) },
			Config{Fallback: FallbackFailOpen, DomainFallback: map[string]FallbackMode{"api": FallbackFailClosed}},
			false, ReasonFailClosed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := withFakeClock(NewWithConfig(tt.url(t), nil, tt.cfg))

			d, err := c.ShouldAllow(context.Background(), Check{Domain: "api", Key: "user:1"})
			if err != nil {
				t.Fatalf("ShouldAllow: %v (want a fallback decision, not an error)", err)
			}
			if d.Allowed != tt.wantAllowed || d.Reason != tt.wantReason || !d.Fallback {
				t.Fatalf("got allowed=%v reason=%v fallback=%v, want allowed=%v reason=%v fallback=true",
					d.Allowed, d.Reason, d.Fallback, tt.wantAllowed, tt.wantReason)
			}
			if d.FallbackCause == nil {
				t.Fatal("FallbackCause = nil, want the transport error")
			}
			if !d.Allowed && d.RetryAfter != DefaultFallbackRetryAfter {
				t.Fatalf("RetryAfter = %v, want %v", d.RetryAfter, DefaultFallbackRetryAfter)
			}
		})
	}
}

func TestClientFallbackError(t *testing.T) {
	for name, url := range map[string]func(*testing.T) string{
		"unreachable": func(t *testing.T) string { return "http://" + closedAddr(t) },
		"slow":        slowServer,
	} {
		t.Run(name, func(t *testing.T) {
			c := withFakeClock(NewWithConfig(url(t), nil, Config{Fallback: FallbackError}))
			if _, err := c.ShouldAllow(context.Background(), Check{Domain: "api", Key: "user:1"}); err == nil {
				t.Fatal("want the error passed through with FallbackError, got nil")
			}
		})
	}
}

func TestClientBatchFallback(t *testing.T) {
	tests := []struct {
		name        string
		cfg         Config
		wantAllowed bool
		wantReasons []Reason
		wantErr     bool
	}{
		{"all fail-open", Config{}, true, []Reason{ReasonFailOpen, ReasonFailOpen}, false},
		{"one fail-closed", Config{DomainFallback: map[string]FallbackMode{"b": FallbackFailClosed}},
			false, []Reason{ReasonFailOpen, ReasonFailClosed}, false},
		{"one FallbackError", Config{DomainFallback: map[string]FallbackMode{"b": FallbackError}},
			false, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := withFakeClock(NewWithConfig("http://"+closedAddr(t), nil, tt.cfg))
			allowed, ds, err := c.ShouldAllowBatch(context.Background(), []Check{
				{Domain: "a", Key: "k"}, {Domain: "b", Key: "k"},
			})
			if tt.wantErr {
				if err == nil {
					t.Fatal("want error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("ShouldAllowBatch: %v", err)
			}
			if allowed != tt.wantAllowed {
				t.Fatalf("allowed = %v, want %v", allowed, tt.wantAllowed)
			}
			for i, d := range ds {
				if d.Reason != tt.wantReasons[i] || !d.Fallback {
					t.Fatalf("decision %d = %+v, want reason %v with fallback", i, d, tt.wantReasons[i])
				}
			}
		})
	}
}

// recordingTransport fails every request after recording its deadline.
type recordingTransport struct {
	mu        sync.Mutex
	deadlines []time.Time
}

func (rt *recordingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	dl, _ := r.Context().Deadline()
	rt.mu.Lock()
	rt.deadlines = append(rt.deadlines, dl)
	rt.mu.Unlock()
	return nil, errors.New("recordingTransport: refusing request")
}

func TestClientSetsPerCallDeadline(t *testing.T) {
	// A clock in the future, so the deadline hasn't passed and the
	// request actually reaches the transport.
	clock := clocktest.New(time.Now().Add(time.Hour))
	rt := &recordingTransport{}
	c := NewWithConfig("http://ratelimit.invalid", &http.Client{Transport: rt}, Config{Timeout: 75 * time.Millisecond})
	c.clock = clock

	if _, err := c.ShouldAllow(context.Background(), Check{Domain: "api", Key: "k"}); err != nil {
		t.Fatalf("ShouldAllow: %v", err)
	}
	if len(rt.deadlines) != 1 {
		t.Fatalf("transport called %d times, want 1", len(rt.deadlines))
	}
	if want := clock.Now().Add(75 * time.Millisecond); !rt.deadlines[0].Equal(want) {
		t.Fatalf("deadline = %v, want now+timeout = %v", rt.deadlines[0], want)
	}
}

// If the caller gave up, that's returned as an error: a fallback
// decision nobody asked for would hide the cancellation.
func TestClientCallerCanceledIsNotFallback(t *testing.T) {
	c := NewWithConfig(slowServer(t), nil, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.ShouldAllow(ctx, Check{Domain: "api", Key: "k"}); err == nil {
		t.Fatal("want error for a canceled caller, got nil")
	}
}

// newServiceWithDeadRedis runs a real RateLimitService whose Redis is
// unreachable, so the service is up but answers by failure mode.
func newServiceWithDeadRedis(t *testing.T) string {
	t.Helper()
	rc := redis.NewClient(&redis.Options{Addr: closedAddr(t), MaxRetries: -1, ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = rc.Close() })

	path := filepath.Join(t.TempDir(), "policies.yaml")
	content := `
policies:
  open:   {rate: 1, burst: 5, failure_mode: fail_open}
  closed: {rate: 1, burst: 5, failure_mode: fail_closed}
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	policies, err := policy.Load(path)
	if err != nil {
		t.Fatalf("policy.Load: %v", err)
	}

	mux := http.NewServeMux()
	p, h := ratelimitv1connect.NewRateLimitServiceHandler(service.NewServer(rc, policies,
		service.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))))
	mux.Handle(p, h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

// The service's own failure-mode answers come through as-is, with
// Fallback false, so callers can tell "Redis down, server decided"
// from "service unreachable, client decided".
func TestClientServerSideFailureModeIsNotFallback(t *testing.T) {
	c := New(newServiceWithDeadRedis(t), nil)
	for _, tt := range []struct {
		domain      string
		wantAllowed bool
		wantReason  Reason
	}{
		{"open", true, ReasonFailOpen},
		{"closed", false, ReasonFailClosed},
	} {
		d, err := c.ShouldAllow(context.Background(), Check{Domain: tt.domain, Key: "k"})
		if err != nil {
			t.Fatalf("%s: %v", tt.domain, err)
		}
		if d.Allowed != tt.wantAllowed || d.Reason != tt.wantReason || d.Fallback {
			t.Fatalf("%s: got %+v, want allowed=%v reason=%v fallback=false",
				tt.domain, d, tt.wantAllowed, tt.wantReason)
		}
	}
}

// A request the service rejects is an error even with fallback on:
// failing open on InvalidArgument would hide a caller bug.
func TestClientInvalidArgumentIsNotFallback(t *testing.T) {
	c := New(newServiceWithDeadRedis(t), nil)
	_, err := c.ShouldAllow(context.Background(), Check{Domain: "open", Key: ""})
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("code = %v (err %v), want InvalidArgument", connect.CodeOf(err), err)
	}
}
