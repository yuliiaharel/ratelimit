package main

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yuliiaharel/ratelimit/client"
)

// config is one load-test run's parameters, filled from flags.
type config struct {
	Target      string        `json:"target"`
	Domain      string        `json:"domain"`
	RPS         float64       `json:"rps"`
	Duration    time.Duration `json:"-"`
	Keys        int           `json:"keys"`
	KeyPrefix   string        `json:"key_prefix"`
	Cost        costDist      `json:"-"`
	Mix         priorityMix   `json:"-"`
	Concurrency int           `json:"concurrency"`
	Timeout     time.Duration `json:"-"`
	Seed        uint64        `json:"seed"`

	// Rate and Burst are the limits configured for Domain on the
	// server, used only to compute expected admissions. Zero Rate
	// disables the accuracy report.
	Rate  float64 `json:"rate"`
	Burst int     `json:"burst"`
}

func (c config) validate() error {
	switch {
	case c.Target == "":
		return errors.New("-target is required")
	case c.RPS <= 0:
		return errors.New("-rps must be positive")
	case c.Duration <= 0:
		return errors.New("-duration must be positive")
	case c.Keys <= 0:
		return errors.New("-keys must be positive")
	case c.Concurrency <= 0:
		return errors.New("-concurrency must be positive")
	case c.Rate < 0 || c.Burst < 0:
		return errors.New("-rate and -burst must not be negative")
	case c.Rate > 0 && c.Burst == 0:
		return errors.New("-burst is required when -rate is set")
	}
	return nil
}

// weighted is one option of a discrete distribution.
type weighted[T any] struct {
	value  T
	weight float64
}

// pick returns the option r (uniform in [0,1)) falls on.
func pick[T any](opts []weighted[T], total, r float64) T {
	x := r * total
	for _, o := range opts {
		if x < o.weight {
			return o.value
		}
		x -= o.weight
	}
	return opts[len(opts)-1].value // only reachable via float rounding
}

// costDist is a discrete distribution of request costs.
type costDist struct {
	opts  []weighted[uint32]
	total float64
	spec  string
}

// parseCostDist parses "N" (every request costs N) or
// "cost:weight,cost:weight,..." (e.g. "1:80,5:20": 80% cost 1, 20% cost 5).
func parseCostDist(spec string) (costDist, error) {
	d := costDist{spec: spec}
	for part := range strings.SplitSeq(spec, ",") {
		part = strings.TrimSpace(part)
		costStr, weightStr, hasWeight := strings.Cut(part, ":")
		cost, err := strconv.ParseUint(costStr, 10, 32)
		if err != nil || cost == 0 {
			return costDist{}, fmt.Errorf("cost %q: want a positive integer", costStr)
		}
		weight := 1.0
		if hasWeight {
			weight, err = strconv.ParseFloat(weightStr, 64)
			if err != nil || weight <= 0 {
				return costDist{}, fmt.Errorf("weight %q for cost %d: want a positive number", weightStr, cost)
			}
		}
		d.opts = append(d.opts, weighted[uint32]{uint32(cost), weight})
		d.total += weight
	}
	if len(d.opts) == 0 {
		return costDist{}, errors.New("empty cost distribution")
	}
	return d, nil
}

func (d costDist) sample(rng *rand.Rand) uint32 { return pick(d.opts, d.total, rng.Float64()) }

// priorityNames orders priorities for reports, most important first.
var priorityNames = []string{"critical", "default", "sheddable"}

var priorityByName = map[string]client.Priority{
	"critical":  client.PriorityCritical,
	"default":   client.PriorityDefault,
	"sheddable": client.PrioritySheddable,
}

// priorityMix is the share of requests sent at each priority.
type priorityMix struct {
	opts  []weighted[string]
	total float64
	spec  string
}

// parsePriorityMix parses "critical=10,default=60,sheddable=30".
// Weights are relative and need not sum to 100; omitted priorities get
// no traffic.
func parsePriorityMix(spec string) (priorityMix, error) {
	m := priorityMix{spec: spec}
	seen := map[string]bool{}
	for part := range strings.SplitSeq(spec, ",") {
		name, weightStr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return priorityMix{}, fmt.Errorf("%q: want name=weight", part)
		}
		if _, known := priorityByName[name]; !known {
			return priorityMix{}, fmt.Errorf("unknown priority %q (want critical, default or sheddable)", name)
		}
		if seen[name] {
			return priorityMix{}, fmt.Errorf("priority %q listed twice", name)
		}
		seen[name] = true
		weight, err := strconv.ParseFloat(weightStr, 64)
		if err != nil || weight < 0 {
			return priorityMix{}, fmt.Errorf("weight %q for %s: want a non-negative number", weightStr, name)
		}
		if weight > 0 {
			m.opts = append(m.opts, weighted[string]{name, weight})
			m.total += weight
		}
	}
	if m.total == 0 {
		return priorityMix{}, errors.New("priority mix has no traffic")
	}
	// Stable report order regardless of how the flag was written.
	sort.Slice(m.opts, func(i, j int) bool {
		return priorityRank(m.opts[i].value) < priorityRank(m.opts[j].value)
	})
	return m, nil
}

func priorityRank(name string) int {
	for i, n := range priorityNames {
		if n == name {
			return i
		}
	}
	return len(priorityNames)
}

func (m priorityMix) sample(rng *rand.Rand) string { return pick(m.opts, m.total, rng.Float64()) }
