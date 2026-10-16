package memory

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yuliiaharel/ratelimit"
	"github.com/yuliiaharel/ratelimit/clocktest"
)

func newLog(t *testing.T, limit int, window time.Duration) (*SlidingWindowLog, *clocktest.Clock) {
	t.Helper()
	clk := clocktest.New(epoch)
	sw, err := NewSlidingWindowLog(SlidingWindowLogConfig{Limit: limit, Window: window, Clock: clk})
	if err != nil {
		t.Fatalf("NewSlidingWindowLog: %v", err)
	}
	return sw, clk
}

func TestNewSlidingWindowLogValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  SlidingWindowLogConfig
	}{
		{"zero limit", SlidingWindowLogConfig{Limit: 0, Window: time.Second}},
		{"negative limit", SlidingWindowLogConfig{Limit: -1, Window: time.Second}},
		{"zero window", SlidingWindowLogConfig{Limit: 1, Window: 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewSlidingWindowLog(tt.cfg); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestSlidingWindowLogScenarios(t *testing.T) {
	// Within a single window (no boundary crossing), an exact sliding
	// log behaves identically to a fixed window -- these numbers match
	// the equivalent FixedWindow scenarios. What differs is what
	// happens *at* a boundary; see TestWindowEdgeAccuracy.
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
		{
			// Aging out is gradual (one entry at a time), never a full
			// reset to zero the way FixedWindow resets at its boundary.
			name: "aging out is gradual, not a full reset", limit: 3, window: time.Second,
			steps: []step{
				{key: "a", wantAllowed: true, wantRemaining: 2},
				{advance: 500 * time.Millisecond, key: "a", wantAllowed: true, wantRemaining: 1},
				// The first entry (now 1s old) ages out right as this
				// request lands, freeing exactly one slot -- not three.
				{advance: 500 * time.Millisecond, key: "a", wantAllowed: true, wantRemaining: 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sw, clk := newLog(t, tt.limit, tt.window)
			for i, s := range tt.steps {
				clk.Advance(s.advance)
				d, err := sw.Allow(context.Background(), ratelimit.Request{Key: s.key, Cost: s.cost})
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

func TestSlidingWindowLogInvalidRequests(t *testing.T) {
	sw, _ := newLog(t, 5, time.Second)
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
			_, err := sw.Allow(context.Background(), tt.req)
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
		})
	}
}

// TestSlidingWindowLogConcurrent hammers one key from many goroutines
// and checks that exactly `limit` requests win. Run with -race.
func TestSlidingWindowLogConcurrent(t *testing.T) {
	const limit = 100
	sw, _ := newLog(t, limit, time.Minute)

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				if d, err := sw.Allow(context.Background(), ratelimit.Request{Key: "hot"}); err == nil && d.Allowed {
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
