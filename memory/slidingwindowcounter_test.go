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

func newCounter(t *testing.T, limit int, window time.Duration) (*SlidingWindowCounter, *clocktest.Clock) {
	t.Helper()
	clk := clocktest.New(epoch)
	sw, err := NewSlidingWindowCounter(SlidingWindowCounterConfig{Limit: limit, Window: window, Clock: clk})
	if err != nil {
		t.Fatalf("NewSlidingWindowCounter: %v", err)
	}
	return sw, clk
}

func TestNewSlidingWindowCounterValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  SlidingWindowCounterConfig
	}{
		{"zero limit", SlidingWindowCounterConfig{Limit: 0, Window: time.Second}},
		{"negative limit", SlidingWindowCounterConfig{Limit: -1, Window: time.Second}},
		{"zero window", SlidingWindowCounterConfig{Limit: 1, Window: 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewSlidingWindowCounter(tt.cfg); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestSlidingWindowCounterScenarios(t *testing.T) {
	// These scenarios never cross a window boundary, so prev is always
	// 0 and the weighted estimate reduces to plain curr -- identical to
	// FixedWindow's equivalent scenarios. The weighted carryover across
	// a boundary is covered separately below and in
	// TestWindowEdgeAccuracy.
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sw, clk := newCounter(t, tt.limit, tt.window)
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

func TestSlidingWindowCounterInvalidRequests(t *testing.T) {
	sw, _ := newCounter(t, 5, time.Second)
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

// TestSlidingWindowCounterWeightedCarryover pins the weighted-estimate
// math right at a window boundary: with only 1ms elapsed into the new
// window, essentially all of the previous window's usage still weighs
// against the trailing estimate, so a key that filled window 1 stays
// denied a millisecond into window 2 -- unlike FixedWindow, which would
// hand it a brand new, fully-allowed burst.
func TestSlidingWindowCounterWeightedCarryover(t *testing.T) {
	const limit = 10
	sw, clk := newCounter(t, limit, time.Second)

	clk.Advance(999 * time.Millisecond)
	for i := range limit {
		d, err := sw.Allow(context.Background(), ratelimit.Request{Key: "a"})
		if err != nil || !d.Allowed {
			t.Fatalf("request %d in window 1: Allowed = %v, err = %v, want true, nil", i, d.Allowed, err)
		}
	}

	clk.Advance(2 * time.Millisecond) // 1ms into window 2
	d, err := sw.Allow(context.Background(), ratelimit.Request{Key: "a"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Allowed {
		t.Fatal("want denied: the previous window's usage should still weigh almost fully")
	}
	if d.Remaining != 0 {
		t.Fatalf("Remaining = %d, want 0", d.Remaining)
	}
	if d.RetryAfter <= 0 {
		t.Fatalf("RetryAfter = %v, want > 0", d.RetryAfter)
	}
}

// TestSlidingWindowCounterIdleLosesHistory documents the flip side of
// the approximation: idle for a full window or more, and the previous
// window's usage is dropped entirely rather than partially decayed.
func TestSlidingWindowCounterIdleLosesHistory(t *testing.T) {
	const limit = 10
	sw, clk := newCounter(t, limit, time.Second)

	d, err := sw.Allow(context.Background(), ratelimit.Request{Key: "a", Cost: limit})
	if err != nil || !d.Allowed {
		t.Fatalf("initial drain: Allowed = %v, err = %v, want true, nil", d.Allowed, err)
	}

	clk.Advance(2 * time.Second) // two full windows idle

	d, err = sw.Allow(context.Background(), ratelimit.Request{Key: "a", Cost: limit})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("Allowed = %v, Remaining = %d, want true, 0", d.Allowed, d.Remaining)
	}
}

// TestSlidingWindowCounterConcurrent hammers one key from many
// goroutines, with the clock frozen so no window shift happens, and
// checks that exactly `limit` requests win. Run with -race.
func TestSlidingWindowCounterConcurrent(t *testing.T) {
	const limit = 100
	sw, _ := newCounter(t, limit, time.Minute)

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
