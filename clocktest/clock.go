// Package clocktest provides a manually advanced clock for deterministic tests.
package clocktest

import (
	"sync"
	"time"
)

// Clock is a fake clock that only moves when told to. Safe for concurrent use.
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

// New returns a Clock set to start.
func New(start time.Time) *Clock {
	return &Clock{now: start}
}

func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Set moves the clock to t.
func (c *Clock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}
