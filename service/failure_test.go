package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/redis/go-redis/v9"

	"github.com/yuliiaharel/ratelimit"
	"github.com/yuliiaharel/ratelimit/clocktest"
	ratelimitv1 "github.com/yuliiaharel/ratelimit/gen/ratelimit/v1"
	"github.com/yuliiaharel/ratelimit/policy"
	"github.com/yuliiaharel/ratelimit/redisstore"
)

// fault is how fakeBackend misbehaves.
type fault int

const (
	healthy fault = iota
	down          // every call fails immediately, like a refused connection
	slow          // every call blocks until its context ends, like a stalled Redis
)

var errConnRefused = errors.New("dial tcp 127.0.0.1:6379: connect: connection refused")

// fakeBackend stands in for Redis. It records the deadline each call
// was given so tests can check the server set one.
type fakeBackend struct {
	fault     fault
	err       error // returned instead of errConnRefused when fault == down, if set
	deadlines []time.Time
}

func (f *fakeBackend) wait(ctx context.Context) error {
	dl, ok := ctx.Deadline()
	if !ok {
		return errors.New("fakeBackend: backend call has no deadline")
	}
	f.deadlines = append(f.deadlines, dl)
	switch f.fault {
	case down:
		if f.err != nil {
			return f.err
		}
		return errConnRefused
	case slow:
		<-ctx.Done()
		return ctx.Err()
	default:
		return nil
	}
}

func (f *fakeBackend) allow(ctx context.Context, p redisstore.Policy, _ ratelimit.Request) (ratelimit.Decision, error) {
	if err := f.wait(ctx); err != nil {
		return ratelimit.Decision{}, err
	}
	return ratelimit.Decision{Allowed: true, Limit: p.Burst, Remaining: p.Burst - 1}, nil
}

func (f *fakeBackend) allowBatch(ctx context.Context, entries []redisstore.PolicyCheck) ([]ratelimit.Decision, bool, error) {
	if err := f.wait(ctx); err != nil {
		return nil, false, err
	}
	ds := make([]ratelimit.Decision, len(entries))
	for i, e := range entries {
		ds[i] = ratelimit.Decision{Allowed: true, Limit: e.Policy.Burst, Remaining: e.Policy.Burst - 1}
	}
	return ds, true, nil
}

// failurePolicies has one domain per failure mode, plus "unset" to
// exercise the documented default.
func failurePolicies(t *testing.T) *policy.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policies.yaml")
	content := `
policies:
  open:
    rate: 4
    burst: 10
    failure_mode: fail_open
  closed:
    rate: 4
    burst: 10
    failure_mode: fail_closed
  unset:
    rate: 4
    burst: 10
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	store, err := policy.Load(path)
	if err != nil {
		t.Fatalf("policy.Load: %v", err)
	}
	return store
}

// The fake clock starts long before the real one, so every deadline the
// server derives from it (fake now + timeout) has already passed in
// real time: a slow backend's context ends immediately and
// deterministically, exactly as if Redis had stalled past the timeout.
var fakeEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

const testTimeout = 25 * time.Millisecond

func newFaultServer(t *testing.T, b backend) (*Server, *clocktest.Clock) {
	t.Helper()
	clock := clocktest.New(fakeEpoch)
	s := newServer(b, failurePolicies(t),
		WithBackendTimeout(testTimeout),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	s.clock = clock
	return s, clock
}

func TestServerFailureModes(t *testing.T) {
	tests := []struct {
		name        string
		fault       fault
		domain      string
		wantAllowed bool
		wantReason  ratelimitv1.ShouldAllowResponse_Reason
	}{
		{"healthy/open", healthy, "open", true, ratelimitv1.ShouldAllowResponse_REASON_OK},
		{"healthy/closed", healthy, "closed", true, ratelimitv1.ShouldAllowResponse_REASON_OK},
		{"redis down/open", down, "open", true, ratelimitv1.ShouldAllowResponse_REASON_FAIL_OPEN},
		{"redis down/closed", down, "closed", false, ratelimitv1.ShouldAllowResponse_REASON_FAIL_CLOSED},
		{"redis down/unset defaults to open", down, "unset", true, ratelimitv1.ShouldAllowResponse_REASON_FAIL_OPEN},
		{"redis slow/open", slow, "open", true, ratelimitv1.ShouldAllowResponse_REASON_FAIL_OPEN},
		{"redis slow/closed", slow, "closed", false, ratelimitv1.ShouldAllowResponse_REASON_FAIL_CLOSED},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fb := &fakeBackend{fault: tt.fault}
			s, clock := newFaultServer(t, fb)

			resp, err := s.ShouldAllow(context.Background(), connect.NewRequest(&ratelimitv1.ShouldAllowRequest{
				Domain: tt.domain, Key: "user:1",
			}))
			if err != nil {
				t.Fatalf("ShouldAllow: %v (a backend failure must be answered, not returned)", err)
			}
			got := resp.Msg
			if got.GetAllowed() != tt.wantAllowed || got.GetReason() != tt.wantReason {
				t.Fatalf("got allowed=%v reason=%v, want allowed=%v reason=%v",
					got.GetAllowed(), got.GetReason(), tt.wantAllowed, tt.wantReason)
			}
			if got.GetPolicyId() != tt.domain || got.GetLimit() != 10 {
				t.Fatalf("policy_id=%q limit=%d, want %q and 10", got.GetPolicyId(), got.GetLimit(), tt.domain)
			}
			if tt.wantReason == ratelimitv1.ShouldAllowResponse_REASON_FAIL_CLOSED {
				// One token's refill time at rate 4/s.
				if ra := got.GetRetryAfter().AsDuration(); ra != 250*time.Millisecond {
					t.Fatalf("retry_after = %v, want 250ms", ra)
				}
			}

			if len(fb.deadlines) != 1 {
				t.Fatalf("backend called %d times, want 1", len(fb.deadlines))
			}
			if want := clock.Now().Add(testTimeout); !fb.deadlines[0].Equal(want) {
				t.Fatalf("backend deadline = %v, want now+timeout = %v", fb.deadlines[0], want)
			}
		})
	}
}

func TestServerBatchFailureModes(t *testing.T) {
	tests := []struct {
		name        string
		fault       fault
		domains     []string
		wantAllowed bool
		wantReasons []ratelimitv1.ShouldAllowResponse_Reason
	}{
		{
			"redis down/all open", down, []string{"open", "unset"}, true,
			[]ratelimitv1.ShouldAllowResponse_Reason{
				ratelimitv1.ShouldAllowResponse_REASON_FAIL_OPEN, ratelimitv1.ShouldAllowResponse_REASON_FAIL_OPEN,
			},
		},
		{
			"redis down/mixed", down, []string{"open", "closed"}, false,
			[]ratelimitv1.ShouldAllowResponse_Reason{
				ratelimitv1.ShouldAllowResponse_REASON_FAIL_OPEN, ratelimitv1.ShouldAllowResponse_REASON_FAIL_CLOSED,
			},
		},
		{
			"redis slow/all closed", slow, []string{"closed", "closed"}, false,
			[]ratelimitv1.ShouldAllowResponse_Reason{
				ratelimitv1.ShouldAllowResponse_REASON_FAIL_CLOSED, ratelimitv1.ShouldAllowResponse_REASON_FAIL_CLOSED,
			},
		},
		{
			"redis slow/mixed", slow, []string{"closed", "open"}, false,
			[]ratelimitv1.ShouldAllowResponse_Reason{
				ratelimitv1.ShouldAllowResponse_REASON_FAIL_CLOSED, ratelimitv1.ShouldAllowResponse_REASON_FAIL_OPEN,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fb := &fakeBackend{fault: tt.fault}
			s, _ := newFaultServer(t, fb)

			checks := make([]*ratelimitv1.ShouldAllowRequest, len(tt.domains))
			for i, d := range tt.domains {
				checks[i] = &ratelimitv1.ShouldAllowRequest{Domain: d, Key: "user:1"}
			}
			resp, err := s.ShouldAllowBatch(context.Background(),
				connect.NewRequest(&ratelimitv1.ShouldAllowBatchRequest{Checks: checks}))
			if err != nil {
				t.Fatalf("ShouldAllowBatch: %v", err)
			}
			if resp.Msg.GetAllowed() != tt.wantAllowed {
				t.Fatalf("batch allowed = %v, want %v", resp.Msg.GetAllowed(), tt.wantAllowed)
			}
			for i, r := range resp.Msg.GetResults() {
				if r.GetReason() != tt.wantReasons[i] {
					t.Fatalf("result %d reason = %v, want %v", i, r.GetReason(), tt.wantReasons[i])
				}
			}
		})
	}
}

// A request error the backend reports (here, cost above burst) must
// still be InvalidArgument, never masked by a fail-open allow.
func TestServerFailureModeKeepsInvalidArgument(t *testing.T) {
	fb := &fakeBackend{fault: down, err: ratelimit.ErrCostExceedsLimit}
	s, _ := newFaultServer(t, fb)

	_, err := s.ShouldAllow(context.Background(), connect.NewRequest(&ratelimitv1.ShouldAllowRequest{
		Domain: "open", Key: "user:1", Cost: 99,
	}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", connect.CodeOf(err))
	}
}

// If the caller itself has gone away, the server reports that rather
// than inventing a fail-open "allowed" nobody will read.
func TestServerCallerCanceledIsNotFailOpen(t *testing.T) {
	fb := &fakeBackend{fault: slow}
	s, _ := newFaultServer(t, fb)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.ShouldAllow(ctx, connect.NewRequest(&ratelimitv1.ShouldAllowRequest{Domain: "open", Key: "user:1"}))
	if connect.CodeOf(err) != connect.CodeCanceled {
		t.Fatalf("code = %v, want Canceled", connect.CodeOf(err))
	}
}

// End to end through the real go-redis client, pointed at a port
// nothing listens on: the real error path must also reach the failure
// mode. Needs no Docker.
func TestServerRealRedisUnreachable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	rc := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1, ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = rc.Close() })

	s := NewServer(rc, failurePolicies(t), WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))

	for _, tt := range []struct {
		domain     string
		wantReason ratelimitv1.ShouldAllowResponse_Reason
	}{
		{"open", ratelimitv1.ShouldAllowResponse_REASON_FAIL_OPEN},
		{"closed", ratelimitv1.ShouldAllowResponse_REASON_FAIL_CLOSED},
	} {
		resp, err := s.ShouldAllow(context.Background(),
			connect.NewRequest(&ratelimitv1.ShouldAllowRequest{Domain: tt.domain, Key: "user:1"}))
		if err != nil {
			t.Fatalf("%s: ShouldAllow: %v", tt.domain, err)
		}
		if resp.Msg.GetReason() != tt.wantReason {
			t.Fatalf("%s: reason = %v, want %v", tt.domain, resp.Msg.GetReason(), tt.wantReason)
		}
	}
}
