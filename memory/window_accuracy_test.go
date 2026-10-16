package memory

import (
	"context"
	"testing"
	"time"

	"github.com/yuliiaharel/ratelimit"
	"github.com/yuliiaharel/ratelimit/clocktest"
)

// TestWindowEdgeAccuracy drives the same window-edge burst scenario
// (from TestFixedWindowBoundaryBurst) through all three windowed
// algorithms and checks how many requests each one actually allows.
//
// FixedWindow resets hard at the boundary, so a client can spend the
// full limit twice within a couple of milliseconds -- its documented
// weakness. SlidingWindowLog is exact and allows exactly the limit.
// SlidingWindowCounter approximates the trailing window and, this
// close to the edge, lands on the exact answer too -- but it's still
// only an estimate (see TestSlidingWindowCounterIdleLosesHistory for a
// case where it isn't exact).
func TestWindowEdgeAccuracy(t *testing.T) {
	const limit = 10
	const window = time.Second

	tests := []struct {
		name        string
		newLimiter  func(t *testing.T, clk ratelimit.Clock) ratelimit.Limiter
		wantAllowed int
	}{
		{
			name: "FixedWindow",
			newLimiter: func(t *testing.T, clk ratelimit.Clock) ratelimit.Limiter {
				fw, err := NewFixedWindow(FixedWindowConfig{Limit: limit, Window: window, Clock: clk})
				if err != nil {
					t.Fatalf("NewFixedWindow: %v", err)
				}
				return fw
			},
			wantAllowed: 2 * limit, // the known weakness
		},
		{
			name: "SlidingWindowLog",
			newLimiter: func(t *testing.T, clk ratelimit.Clock) ratelimit.Limiter {
				sw, err := NewSlidingWindowLog(SlidingWindowLogConfig{Limit: limit, Window: window, Clock: clk})
				if err != nil {
					t.Fatalf("NewSlidingWindowLog: %v", err)
				}
				return sw
			},
			wantAllowed: limit, // exact
		},
		{
			name: "SlidingWindowCounter",
			newLimiter: func(t *testing.T, clk ratelimit.Clock) ratelimit.Limiter {
				sw, err := NewSlidingWindowCounter(SlidingWindowCounterConfig{Limit: limit, Window: window, Clock: clk})
				if err != nil {
					t.Fatalf("NewSlidingWindowCounter: %v", err)
				}
				return sw
			},
			wantAllowed: limit, // this close to the edge, the estimate matches exactly
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := clocktest.New(epoch)
			lim := tt.newLimiter(t, clk)

			clk.Advance(999 * time.Millisecond) // last millisecond of window 1
			allowed := 0
			for range limit {
				if d, _ := lim.Allow(context.Background(), ratelimit.Request{Key: "a"}); d.Allowed {
					allowed++
				}
			}

			clk.Advance(2 * time.Millisecond) // first millisecond of window 2
			for range limit {
				if d, _ := lim.Allow(context.Background(), ratelimit.Request{Key: "a"}); d.Allowed {
					allowed++
				}
			}

			if allowed != tt.wantAllowed {
				t.Fatalf("allowed = %d, want %d", allowed, tt.wantAllowed)
			}
		})
	}
}
