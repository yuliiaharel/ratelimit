package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/yuliiaharel/ratelimit/client"
)

func TestParseCostDist(t *testing.T) {
	tests := []struct {
		spec    string
		want    []uint32 // distinct costs, in order
		wantErr bool
	}{
		{"1", []uint32{1}, false},
		{"5", []uint32{5}, false},
		{"1:80,5:20", []uint32{1, 5}, false},
		{" 1:1 , 2:1 ", []uint32{1, 2}, false},
		{"", nil, true},
		{"0", nil, true},
		{"-1", nil, true},
		{"1:0", nil, true},
		{"1:x", nil, true},
		{"a:1", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.spec, func(t *testing.T) {
			d, err := parseCostDist(tt.spec)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("want error, got %+v", d)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseCostDist: %v", err)
			}
			if len(d.opts) != len(tt.want) {
				t.Fatalf("got %d options, want %d", len(d.opts), len(tt.want))
			}
			for i, o := range d.opts {
				if o.value != tt.want[i] {
					t.Fatalf("option %d = %d, want %d", i, o.value, tt.want[i])
				}
			}
		})
	}
}

func TestParsePriorityMix(t *testing.T) {
	tests := []struct {
		spec    string
		want    []string
		wantErr bool
	}{
		{"critical=10,default=60,sheddable=30", []string{"critical", "default", "sheddable"}, false},
		{"sheddable=1,critical=1", []string{"critical", "sheddable"}, false}, // report order, not flag order
		{"default=1,sheddable=0", []string{"default"}, false},
		{"critical=0", nil, true},
		{"urgent=1", nil, true},
		{"critical", nil, true},
		{"critical=-1", nil, true},
		{"critical=1,critical=2", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.spec, func(t *testing.T) {
			m, err := parsePriorityMix(tt.spec)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("want error, got %+v", m)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePriorityMix: %v", err)
			}
			var got []string
			for _, o := range m.opts {
				got = append(got, o.value)
			}
			if strings.Join(got, ",") != strings.Join(tt.want, ",") {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// Sampling must follow the weights. With a fixed seed and many draws
// the shares land well within a couple of percentage points.
func TestSamplingFollowsWeights(t *testing.T) {
	m, err := parsePriorityMix("critical=10,default=60,sheddable=30")
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(1, 2))
	counts := map[string]int{}
	const n = 100_000
	for range n {
		counts[m.sample(rng)]++
	}
	for name, want := range map[string]float64{"critical": 0.10, "default": 0.60, "sheddable": 0.30} {
		got := float64(counts[name]) / n
		if got < want-0.02 || got > want+0.02 {
			t.Fatalf("%s share = %.3f, want %.2f±0.02", name, got, want)
		}
	}
}

func TestPercentile(t *testing.T) {
	ls := make([]time.Duration, 100)
	for i := range ls {
		ls[i] = time.Duration(i+1) * time.Millisecond
	}
	tests := []struct {
		p    float64
		want time.Duration
	}{
		{50, 50 * time.Millisecond},
		{99, 99 * time.Millisecond},
		{100, 100 * time.Millisecond},
		{0, 1 * time.Millisecond},
	}
	for _, tt := range tests {
		if got := percentile(ls, tt.p); got != tt.want {
			t.Fatalf("p%v = %v, want %v", tt.p, got, tt.want)
		}
	}
	if got := percentile(nil, 50); got != 0 {
		t.Fatalf("percentile(nil) = %v, want 0", got)
	}
	if got := percentile([]time.Duration{7}, 99); got != 7 {
		t.Fatalf("single-element p99 = %v, want 7", got)
	}
}

func mustCfg(t *testing.T) config {
	t.Helper()
	cost, err := parseCostDist("1")
	if err != nil {
		t.Fatal(err)
	}
	mix, err := parsePriorityMix("critical=1,default=1,sheddable=1")
	if err != nil {
		t.Fatal(err)
	}
	return config{
		Target: "http://x", Domain: "api", RPS: 100, Duration: 2 * time.Second,
		Keys: 2, Cost: cost, Mix: mix, Concurrency: 10, Rate: 1, Burst: 3,
	}
}

func ok(cost uint32, key int, pri string) result {
	return result{key: key, priority: pri, cost: cost, latency: time.Millisecond,
		decision: client.Decision{Allowed: true, Reason: client.ReasonOK}}
}

func denied(cost uint32, key int, pri string) result {
	return result{key: key, priority: pri, cost: cost, latency: time.Millisecond,
		decision: client.Decision{Reason: client.ReasonOverLimit}}
}

func TestSummarize(t *testing.T) {
	cfg := mustCfg(t) // capacity per key: burst 3 + rate 1 * 2s = 5 tokens

	results := []result{
		// key 0: 6 tokens requested, 5 admitted -- exactly at capacity.
		ok(1, 0, "critical"), ok(1, 0, "critical"), ok(1, 0, "default"),
		ok(1, 0, "default"), ok(1, 0, "sheddable"), denied(1, 0, "sheddable"),
		// key 1: 2 requested, 1 admitted, 1 admitted by fail-open (not counted as accurate).
		ok(1, 1, "critical"),
		{key: 1, priority: "default", cost: 1, decision: client.Decision{Allowed: true, Reason: client.ReasonFailOpen}},
		// Not sent, and an error.
		{key: 1, priority: "sheddable", cost: 1, dropped: true},
		{key: 1, priority: "sheddable", cost: 1, err: errors.New("boom"), errCode: "unavailable"},
	}

	s := summarize(cfg, results, 4*time.Second)

	if s.Scheduled != 10 || s.Sent != 9 || s.Dropped != 1 || s.Errors != 1 {
		t.Fatalf("scheduled/sent/dropped/errors = %d/%d/%d/%d, want 10/9/1/1",
			s.Scheduled, s.Sent, s.Dropped, s.Errors)
	}
	if s.ThroughputRPS != 9.0/4 {
		t.Fatalf("throughput = %v, want %v", s.ThroughputRPS, 9.0/4)
	}
	if s.Reasons["ok"] != 6 || s.Reasons["over_limit"] != 1 || s.Reasons["fail_open"] != 1 {
		t.Fatalf("reasons = %v", s.Reasons)
	}
	if s.ErrorCodes["unavailable"] != 1 {
		t.Fatalf("error codes = %v", s.ErrorCodes)
	}

	wantPri := map[string][2]int{"critical": {3, 3}, "default": {3, 3}, "sheddable": {3, 1}}
	for _, p := range s.ByPriority {
		w := wantPri[p.Priority]
		if p.Sent != w[0] || p.Allowed != w[1] {
			t.Fatalf("%s sent/allowed = %d/%d, want %d/%d", p.Priority, p.Sent, p.Allowed, w[0], w[1])
		}
	}

	a := s.Accuracy
	if a == nil {
		t.Fatal("accuracy missing with -rate set")
	}
	// key 0: min(6, 5) = 5 expected, 5 admitted. key 1: min(2, 5) = 2 expected, 1 admitted.
	if a.ExpectedTokens != 7 || a.AdmittedTokens != 6 || a.OverAdmittedTokens != 0 {
		t.Fatalf("accuracy = %+v, want expected 7 admitted 6 over 0", a)
	}
}

func TestSummarizeFlagsOverAdmission(t *testing.T) {
	cfg := mustCfg(t) // capacity 5 per key
	var results []result
	for range 7 {
		results = append(results, ok(1, 0, "default"))
	}
	s := summarize(cfg, results, time.Second)
	if s.Accuracy.OverAdmittedTokens != 2 {
		t.Fatalf("over-admitted = %v, want 2", s.Accuracy.OverAdmittedTokens)
	}
}

func TestSummarizeWithoutRateSkipsAccuracy(t *testing.T) {
	cfg := mustCfg(t)
	cfg.Rate = 0
	if s := summarize(cfg, []result{ok(1, 0, "default")}, time.Second); s.Accuracy != nil {
		t.Fatalf("accuracy = %+v, want nil when -rate is 0", s.Accuracy)
	}
}

func TestOutputs(t *testing.T) {
	cfg := mustCfg(t)
	s := summarize(cfg, []result{ok(1, 0, "critical"), denied(1, 0, "sheddable")}, time.Second)

	var js bytes.Buffer
	if err := writeJSON(&js, s); err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(js.Bytes(), &decoded); err != nil {
		t.Fatalf("JSON summary doesn't parse: %v", err)
	}
	for _, k := range []string{"throughput_rps", "latency_ms", "accuracy", "by_priority", "reasons"} {
		if _, ok := decoded[k]; !ok {
			t.Fatalf("JSON summary missing %q", k)
		}
	}

	var cs bytes.Buffer
	if err := writeCSV(&cs, s); err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(&cs).ReadAll()
	if err != nil {
		t.Fatalf("CSV summary doesn't parse: %v", err)
	}
	got := map[string]string{}
	for _, r := range rows {
		got[r[0]] = r[1]
	}
	if got["critical_success_rate"] != "1" || got["sheddable_success_rate"] != "0" || got["latency_p99_ms"] != "1" {
		t.Fatalf("CSV rows = %v", got)
	}

	var tbl bytes.Buffer
	if err := writeTable(&tbl, s); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"throughput", "p50 / p99", "accuracy", "critical", "sheddable"} {
		if !strings.Contains(tbl.String(), want) {
			t.Fatalf("table missing %q:\n%s", want, tbl.String())
		}
	}
}

// fakeChecker allows the first n calls per key and denies the rest.
type fakeChecker struct {
	mu      sync.Mutex
	n       int
	seen    map[string]int
	block   chan struct{} // if non-nil, every call waits on it
	entered chan struct{}
}

func (f *fakeChecker) ShouldAllow(ctx context.Context, chk client.Check) (client.Decision, error) {
	if f.block != nil {
		f.entered <- struct{}{}
		select {
		case <-f.block:
		case <-ctx.Done():
			return client.Decision{}, connect.NewError(connect.CodeCanceled, ctx.Err())
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen[chk.Key]++
	if f.seen[chk.Key] <= f.n {
		return client.Decision{Allowed: true, Reason: client.ReasonOK}, nil
	}
	return client.Decision{Reason: client.ReasonOverLimit}, nil
}

// run's pacing is wall-clock by nature; a high rate over a few
// milliseconds keeps this fast while still exercising the scheduler.
func TestRunSendsScheduleAgainstChecker(t *testing.T) {
	cfg := mustCfg(t)
	cfg.RPS, cfg.Duration, cfg.Keys, cfg.KeyPrefix = 10_000, 5*time.Millisecond, 3, "t:"
	fc := &fakeChecker{n: 2, seen: map[string]int{}}

	results, _ := run(context.Background(), cfg, fc)
	if len(results) != 50 {
		t.Fatalf("scheduled %d requests, want rps*duration = 50", len(results))
	}
	s := summarize(cfg, results, time.Second)
	if s.Sent+s.Dropped != 50 || s.Errors != 0 {
		t.Fatalf("sent %d dropped %d errors %d", s.Sent, s.Dropped, s.Errors)
	}
	// 3 keys x 2 allowed each.
	if s.Reasons["ok"] != 6 {
		t.Fatalf("ok = %d, want 6", s.Reasons["ok"])
	}
	for k := range fc.seen {
		if !strings.HasPrefix(k, "t:") {
			t.Fatalf("key %q missing -key-prefix", k)
		}
	}
}

// With every in-flight slot held, further requests are dropped rather
// than queued behind them.
func TestRunDropsWhenConcurrencyExhausted(t *testing.T) {
	cfg := mustCfg(t)
	cfg.RPS, cfg.Duration, cfg.Concurrency = 10_000, 2*time.Millisecond, 1
	fc := &fakeChecker{n: 100, seen: map[string]int{}, block: make(chan struct{}), entered: make(chan struct{}, 100)}

	done := make(chan []result)
	go func() {
		rs, _ := run(context.Background(), cfg, fc)
		done <- rs
	}()
	// The first request holds the only slot. Keep it held until the
	// whole send window has elapsed, so every later slot comes up while
	// it's still in flight; then release it so run can return.
	<-fc.entered
	<-time.After(cfg.Duration)
	close(fc.block)
	rs := <-done
	s := summarize(cfg, rs, time.Second)
	if s.Sent < 1 || s.Dropped < 1 || s.Sent+s.Dropped != 20 {
		t.Fatalf("sent %d dropped %d, want some of each totalling 20", s.Sent, s.Dropped)
	}
}

func TestMainErrRejectsBadFlags(t *testing.T) {
	for _, args := range [][]string{
		{"-rps", "0"},
		{"-cost", "0"},
		{"-mix", "urgent=1"},
		{"-format", "xml"},
		{"-rate", "5"}, // -burst missing
		{"-keys", "0"},
	} {
		if err := mainErr(args, io.Discard, io.Discard); err == nil {
			t.Fatalf("mainErr(%v) = nil, want error", args)
		}
	}
}
