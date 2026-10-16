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

func newBucket(t *testing.T, rate float64, burst int) (*TokenBucket, *clocktest.Clock) {
	t.Helper()
	clk := clocktest.New(epoch)
	tb, err := NewTokenBucket(TokenBucketConfig{Rate: rate, Burst: burst, Clock: clk})
	if err != nil {
		t.Fatalf("NewTokenBucket: %v", err)
	}
	return tb, clk
}

func TestNewTokenBucketValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  TokenBucketConfig
	}{
		{"zero rate", TokenBucketConfig{Rate: 0, Burst: 1}},
		{"negative rate", TokenBucketConfig{Rate: -1, Burst: 1}},
		{"zero burst", TokenBucketConfig{Rate: 1, Burst: 0}},
		{"negative burst", TokenBucketConfig{Rate: 1, Burst: -1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewTokenBucket(tt.cfg); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

// step is declared in fixedwindow_test.go and reused here: both limiters
// are driven by the same scripted-scenario shape.

func TestTokenBucketScenarios(t *testing.T) {
	tests := []struct {
		name  string
		rate  float64
		burst int
		steps []step
	}{
		{
			name: "drains from full then denies", rate: 1, burst: 3,
			steps: []step{
				{key: "a", wantAllowed: true, wantRemaining: 2},
				{key: "a", wantAllowed: true, wantRemaining: 1},
				{key: "a", wantAllowed: true, wantRemaining: 0},
				{key: "a", wantAllowed: false, wantRemaining: 0, wantRetry: time.Second},
			},
		},
		{
			// Documents continuous (sub-second) refill: each partial
			// advance adds a fractional token, and RetryAfter counts
			// down accordingly, unlike FixedWindow's all-or-nothing
			// window reset.
			name: "fractional refill, retry-after counts down", rate: 1, burst: 5,
			steps: []step{
				{key: "a", cost: 5, wantAllowed: true, wantRemaining: 0},
				{advance: 3 * time.Second, key: "a", cost: 3, wantAllowed: true, wantRemaining: 0},
				{key: "a", cost: 1, wantAllowed: false, wantRemaining: 0, wantRetry: time.Second},
				{
					advance: 500 * time.Millisecond, key: "a", cost: 1,
					wantAllowed: false, wantRemaining: 0, wantRetry: 500 * time.Millisecond,
				},
				{advance: 500 * time.Millisecond, key: "a", cost: 1, wantAllowed: true, wantRemaining: 0},
			},
		},
		{
			name: "keys are independent", rate: 1, burst: 1,
			steps: []step{
				{key: "a", wantAllowed: true},
				{key: "b", wantAllowed: true},
				{key: "a", wantAllowed: false, wantRetry: time.Second},
			},
		},
		{
			name: "cost greater than one", rate: 10, burst: 10,
			steps: []step{
				{key: "a", cost: 4, wantAllowed: true, wantRemaining: 6},
				{key: "a", cost: 4, wantAllowed: true, wantRemaining: 2},
				// Doesn't fit, and a denial consumes nothing...
				{key: "a", cost: 3, wantAllowed: false, wantRemaining: 2, wantRetry: 100 * time.Millisecond},
				// ...so a smaller request still fits.
				{key: "a", cost: 2, wantAllowed: true, wantRemaining: 0},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tb, clk := newBucket(t, tt.rate, tt.burst)
			for i, s := range tt.steps {
				clk.Advance(s.advance)
				d, err := tb.Allow(context.Background(), ratelimit.Request{Key: s.key, Cost: s.cost})
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
				if d.Limit != tt.burst {
					t.Errorf("step %d: Limit = %d, want %d", i, d.Limit, tt.burst)
				}
			}
		})
	}
}

func TestTokenBucketInvalidRequests(t *testing.T) {
	tb, _ := newBucket(t, 1, 5)
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
			_, err := tb.Allow(context.Background(), tt.req)
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
		})
	}
}

// TestTokenBucketRefillCapsAtBurst documents that idle time never lets a
// key accumulate more than Burst tokens, no matter how long it waits.
func TestTokenBucketRefillCapsAtBurst(t *testing.T) {
	const burst = 5
	tb, clk := newBucket(t, 10, burst)

	if d, _ := tb.Allow(context.Background(), ratelimit.Request{Key: "a", Cost: burst}); !d.Allowed {
		t.Fatal("initial drain: want allowed")
	}

	clk.Advance(1000 * time.Second) // would be 10,000 tokens if uncapped

	d, err := tb.Allow(context.Background(), ratelimit.Request{Key: "a", Cost: burst})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("Allowed = %v, Remaining = %d, want true, 0", d.Allowed, d.Remaining)
	}

	// If refill had overflowed Burst, there would be tokens left over
	// for a further request; there shouldn't be.
	if d2, _ := tb.Allow(context.Background(), ratelimit.Request{Key: "a", Cost: 1}); d2.Allowed {
		t.Fatal("expected the extra request to be denied")
	}
}

// TestTokenBucketConcurrent hammers one key from many goroutines, with
// the clock frozen so no refill happens, and checks that exactly `burst`
// requests win. Run with -race.
func TestTokenBucketConcurrent(t *testing.T) {
	const burst = 100
	tb, _ := newBucket(t, 1, burst)

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				if d, err := tb.Allow(context.Background(), ratelimit.Request{Key: "hot"}); err == nil && d.Allowed {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if got := allowed.Load(); got != burst {
		t.Fatalf("allowed = %d, want exactly %d", got, burst)
	}
}
