package memory

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yourname/ratelimit"
	"github.com/yourname/ratelimit/clocktest"
)

// Aligned to a minute boundary so window math is easy to read.
var epoch = time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)

func newFixed(t *testing.T, limit int, window time.Duration) (*FixedWindow, *clocktest.Clock) {
	t.Helper()
	clk := clocktest.New(epoch)
	fw, err := NewFixedWindow(FixedWindowConfig{Limit: limit, Window: window, Clock: clk})
	if err != nil {
		t.Fatalf("NewFixedWindow: %v", err)
	}
	return fw, clk
}

func TestNewFixedWindowValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  FixedWindowConfig
	}{
		{"zero limit", FixedWindowConfig{Limit: 0, Window: time.Second}},
		{"negative limit", FixedWindowConfig{Limit: -1, Window: time.Second}},
		{"zero window", FixedWindowConfig{Limit: 1, Window: 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewFixedWindow(tt.cfg); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

// step is one call in a scripted scenario: optionally advance the clock,
// make a request, and check the decision.
type step struct {
	advance       time.Duration
	key           string
	cost          int
	wantAllowed   bool
	wantRemaining int
	wantRetry     time.Duration
}

func TestFixedWindowScenarios(t *testing.T) {
	tests := []struct {
		name   string
		limit  int
		window time.Duration
		steps  []step
	}{
		{
			name: "allows up to limit then denies", limit: 3, window: time.Second,
			steps: []step{
				{key: "a", wantAllowed: true, wantRemaining: 2},
				{key: "a", wantAllowed: true, wantRemaining: 1},
				{key: "a", wantAllowed: true, wantRemaining: 0},
				{key: "a", wantAllowed: false, wantRemaining: 0, wantRetry: time.Second},
			},
		},
		{
			name: "retry-after counts down to window end", limit: 1, window: time.Second,
			steps: []step{
				{key: "a", wantAllowed: true, wantRemaining: 0},
				{advance: 300 * time.Millisecond, key: "a", wantAllowed: false, wantRetry: 700 * time.Millisecond},
			},
		},
		{
			name: "resets at next window", limit: 2, window: time.Second,
			steps: []step{
				{key: "a", wantAllowed: true, wantRemaining: 1},
				{key: "a", wantAllowed: true, wantRemaining: 0},
				{key: "a", wantAllowed: false, wantRetry: time.Second},
				{advance: time.Second, key: "a", wantAllowed: true, wantRemaining: 1},
			},
		},
		{
			name: "keys are independent", limit: 1, window: time.Second,
			steps: []step{
				{key: "a", wantAllowed: true},
				{key: "b", wantAllowed: true},
				{key: "a", wantAllowed: false, wantRetry: time.Second},
			},
		},
		{
			name: "cost greater than one", limit: 10, window: time.Second,
			steps: []step{
				{key: "a", cost: 4, wantAllowed: true, wantRemaining: 6},
				{key: "a", cost: 4, wantAllowed: true, wantRemaining: 2},
				// Doesn't fit, and a denial consumes nothing...
				{key: "a", cost: 3, wantAllowed: false, wantRemaining: 2, wantRetry: time.Second},
				// ...so a smaller request still fits.
				{key: "a", cost: 2, wantAllowed: true, wantRemaining: 0},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fw, clk := newFixed(t, tt.limit, tt.window)
			for i, s := range tt.steps {
				clk.Advance(s.advance)
				d, err := fw.Allow(context.Background(), ratelimit.Request{Key: s.key, Cost: s.cost})
				if err != nil {
					t.Fatalf("step %d: unexpected error: %v", i, err)
				}
				if d.Allowed != s.wantAllowed {
					t.Errorf("step %d: Allowed = %v, want %v", i, d.Allowed, s.wantAllowed)
				}
				if d.Remaining != s.wantRemaining {
					t.Errorf("step %d: Remaining = %d, want %d", i, d.Remaining, s.wantRemaining)
				}
				if d.RetryAfter != s.wantRetry {
					t.Errorf("step %d: RetryAfter = %v, want %v", i, d.RetryAfter, s.wantRetry)
				}
				if d.Limit != tt.limit {
					t.Errorf("step %d: Limit = %d, want %d", i, d.Limit, tt.limit)
				}
			}
		})
	}
}

func TestFixedWindowInvalidRequests(t *testing.T) {
	fw, _ := newFixed(t, 5, time.Second)
	tests := []struct {
		name string
		req  ratelimit.Request
		want error
	}{
		{"empty key", ratelimit.Request{Key: ""}, ratelimit.ErrEmptyKey},
		{"negative cost", ratelimit.Request{Key: "a", Cost: -1}, ratelimit.ErrNegativeCost},
		{"cost above limit", ratelimit.Request{Key: "a", Cost: 6}, ratelimit.ErrCostExceedsLimit},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := fw.Allow(context.Background(), tt.req)
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
		})
	}
}

// TestFixedWindowBoundaryBurst documents the algorithm's known weakness:
// with a limit of 10/second, a client can get 20 requests through in a
// span of a few milliseconds by straddling a window boundary.
// The sliding-window algorithms (step 3) exist to fix exactly this.
func TestFixedWindowBoundaryBurst(t *testing.T) {
	const limit = 10
	fw, clk := newFixed(t, limit, time.Second)

	clk.Advance(999 * time.Millisecond) // last millisecond of window 1
	allowed := 0
	for range limit {
		if d, _ := fw.Allow(context.Background(), ratelimit.Request{Key: "a"}); d.Allowed {
			allowed++
		}
	}

	clk.Advance(2 * time.Millisecond) // first millisecond of window 2
	for range limit {
		if d, _ := fw.Allow(context.Background(), ratelimit.Request{Key: "a"}); d.Allowed {
			allowed++
		}
	}

	if allowed != 2*limit {
		t.Fatalf("allowed %d requests across the boundary, want %d (2× limit)", allowed, 2*limit)
	}
}

// TestFixedWindowConcurrent hammers one key from many goroutines and checks
// that exactly `limit` requests win. Run with -race.
func TestFixedWindowConcurrent(t *testing.T) {
	const limit = 100
	fw, _ := newFixed(t, limit, time.Minute)

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				if d, err := fw.Allow(context.Background(), ratelimit.Request{Key: "hot"}); err == nil && d.Allowed {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if got := allowed.Load(); got != limit {
		t.Fatalf("allowed = %d, want exactly %d", got, limit)
	}
}
