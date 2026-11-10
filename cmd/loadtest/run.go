package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"connectrpc.com/connect"

	"github.com/yuliiaharel/ratelimit/client"
)

// checker is the slice of client.Client the harness uses, so tests
// can run the scheduler against a fake.
type checker interface {
	ShouldAllow(ctx context.Context, check client.Check) (client.Decision, error)
}

// result is the outcome of one scheduled request.
type result struct {
	key      int
	priority string
	cost     uint32

	// dropped means the request was never sent: -concurrency requests
	// were already in flight at its scheduled time.
	dropped bool

	// latency runs from the request's scheduled send time, not from
	// when it was actually sent, so a backlog in the harness or a slow
	// server shows up in the numbers instead of quietly lowering the
	// offered rate (coordinated omission).
	latency time.Duration

	err      error
	errCode  string
	decision client.Decision
}

// run sends cfg.RPS requests per second for cfg.Duration on a fixed
// schedule (open loop: send times don't wait for responses) and
// returns every request's result, plus the wall time the run took
// including draining in-flight requests.
func run(ctx context.Context, cfg config, c checker) ([]result, time.Duration) {
	n := int(cfg.RPS * cfg.Duration.Seconds())
	if n < 1 {
		n = 1
	}
	interval := time.Duration(float64(time.Second) / cfg.RPS)
	rng := rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15))

	// Each goroutine writes only its own index; wg.Wait below orders
	// those writes before the caller reads the slice.
	results := make([]result, n)
	sem := make(chan struct{}, cfg.Concurrency)
	var wg sync.WaitGroup

	start := time.Now()
	timer := time.NewTimer(0)
	defer timer.Stop()
	<-timer.C

	scheduledCount := n
schedule:
	for i := range n {
		scheduled := start.Add(time.Duration(i) * interval)
		if wait := time.Until(scheduled); wait > 0 {
			timer.Reset(wait)
			select {
			case <-ctx.Done():
				// Still wait for in-flight requests below: they write
				// into results.
				scheduledCount = i
				break schedule
			case <-timer.C:
			}
		}

		// Sampled here, on the scheduler goroutine, so rng needs no lock
		// and a given -seed always produces the same request sequence.
		r := &results[i]
		r.key = rng.IntN(cfg.Keys)
		r.priority = cfg.Mix.sample(rng)
		r.cost = cfg.Cost.sample(rng)

		select {
		case sem <- struct{}{}:
		default:
			r.dropped = true
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			d, err := c.ShouldAllow(ctx, client.Check{
				Domain:   cfg.Domain,
				Key:      cfg.KeyPrefix + strconv.Itoa(r.key),
				Cost:     r.cost,
				Priority: priorityByName[r.priority],
			})
			r.latency = time.Since(scheduled)
			r.decision = d
			if err != nil {
				r.err = err
				r.errCode = connect.CodeOf(err).String()
			}
		}()
	}
	wg.Wait()
	return results[:scheduledCount], time.Since(start)
}

// defaultKeyPrefix makes each run's keys unique, so buckets left
// drained by a previous run don't skew this one's accuracy.
func defaultKeyPrefix(now time.Time) string {
	return fmt.Sprintf("loadtest-%d:", now.UnixNano())
}
