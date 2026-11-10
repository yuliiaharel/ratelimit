package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/yuliiaharel/ratelimit/client"
)

// summary is the machine-readable report of one run.
type summary struct {
	Config          config  `json:"config"`
	DurationSeconds float64 `json:"duration_seconds"` // configured send window
	ElapsedSeconds  float64 `json:"elapsed_seconds"`  // wall time incl. draining in-flight requests
	CostSpec        string  `json:"cost"`
	MixSpec         string  `json:"priority_mix"`

	Scheduled int `json:"scheduled"`
	Sent      int `json:"sent"`
	Dropped   int `json:"dropped"` // not sent: -concurrency already in flight
	Errors    int `json:"errors"`

	OfferedRPS    float64 `json:"offered_rps"`
	ThroughputRPS float64 `json:"throughput_rps"` // sent requests completed per second of elapsed time

	Latency latencySummary `json:"latency_ms"`

	// Reasons counts decisions by reason; errors by connect code.
	Reasons    map[string]int `json:"reasons"`
	ErrorCodes map[string]int `json:"error_codes,omitempty"`

	ByPriority []priorityStats `json:"by_priority"`
	Accuracy   *accuracy       `json:"accuracy,omitempty"`
}

type latencySummary struct {
	P50 float64 `json:"p50"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

type priorityStats struct {
	Priority string `json:"priority"`
	Sent     int    `json:"sent"`
	Allowed  int    `json:"allowed"`
	// SuccessRate is Allowed/Sent: under overload, critical should stay
	// near 1 while sheddable falls first.
	SuccessRate float64 `json:"success_rate"`
}

// accuracy compares what the service admitted against what the
// configured token bucket allows. For each key, expected admissions
// are min(tokens requested, burst + rate*duration): the most a correct
// limiter may admit. Admitted counts only REASON_OK decisions, so
// fail-open answers don't count as the limiter being accurate.
//
// Ratio below 1 is expected and not by itself a bug: integer costs
// leave fractional tokens unused, and priority reserves keep lower
// priorities out of tokens that no higher-priority traffic claimed.
// OverAdmittedTokens above 0 means the limit itself was exceeded.
type accuracy struct {
	Rate               float64 `json:"rate"`
	Burst              int     `json:"burst"`
	ExpectedTokens     float64 `json:"expected_tokens"`
	AdmittedTokens     float64 `json:"admitted_tokens"`
	Ratio              float64 `json:"ratio"`
	OverAdmittedTokens float64 `json:"over_admitted_tokens"`
}

func summarize(cfg config, results []result, elapsed time.Duration) summary {
	s := summary{
		Config:          cfg,
		DurationSeconds: cfg.Duration.Seconds(),
		ElapsedSeconds:  elapsed.Seconds(),
		CostSpec:        cfg.Cost.spec,
		MixSpec:         cfg.Mix.spec,
		Scheduled:       len(results),
		OfferedRPS:      cfg.RPS,
		Reasons:         map[string]int{},
		ErrorCodes:      map[string]int{},
	}

	byPri := map[string]*priorityStats{}
	for _, name := range priorityNames {
		byPri[name] = &priorityStats{Priority: name}
	}
	requested := map[int]float64{}
	admitted := map[int]float64{}
	var latencies []time.Duration

	for _, r := range results {
		if r.dropped {
			s.Dropped++
			continue
		}
		s.Sent++
		latencies = append(latencies, r.latency)
		ps := byPri[r.priority]
		ps.Sent++
		if r.err != nil {
			s.Errors++
			s.ErrorCodes[r.errCode]++
			continue
		}
		s.Reasons[reasonName(r.decision.Reason)]++
		if r.decision.Allowed {
			ps.Allowed++
		}
		requested[r.key] += float64(r.cost)
		if r.decision.Reason == client.ReasonOK {
			admitted[r.key] += float64(r.cost)
		}
	}
	if len(s.ErrorCodes) == 0 {
		s.ErrorCodes = nil
	}

	if elapsed > 0 {
		s.ThroughputRPS = float64(s.Sent) / elapsed.Seconds()
	}
	s.Latency = summarizeLatency(latencies)

	for _, name := range priorityNames {
		ps := byPri[name]
		if ps.Sent == 0 {
			continue
		}
		ps.SuccessRate = float64(ps.Allowed) / float64(ps.Sent)
		s.ByPriority = append(s.ByPriority, *ps)
	}

	if cfg.Rate > 0 {
		capacity := float64(cfg.Burst) + cfg.Rate*cfg.Duration.Seconds()
		a := &accuracy{Rate: cfg.Rate, Burst: cfg.Burst}
		for key, req := range requested {
			a.ExpectedTokens += math.Min(req, capacity)
			a.AdmittedTokens += admitted[key]
			a.OverAdmittedTokens += math.Max(0, admitted[key]-capacity)
		}
		if a.ExpectedTokens > 0 {
			a.Ratio = a.AdmittedTokens / a.ExpectedTokens
		}
		s.Accuracy = a
	}
	return s
}

func summarizeLatency(ls []time.Duration) latencySummary {
	if len(ls) == 0 {
		return latencySummary{}
	}
	slices.Sort(ls)
	return latencySummary{
		P50: ms(percentile(ls, 50)),
		P99: ms(percentile(ls, 99)),
		Max: ms(ls[len(ls)-1]),
	}
}

// percentile returns the nearest-rank p-th percentile of sorted.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	rank = max(1, min(rank, len(sorted)))
	return sorted[rank-1]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func reasonName(r client.Reason) string {
	switch r {
	case client.ReasonOK:
		return "ok"
	case client.ReasonOverLimit:
		return "over_limit"
	case client.ReasonFailOpen:
		return "fail_open"
	case client.ReasonFailClosed:
		return "fail_closed"
	default:
		return "unspecified"
	}
}

func writeJSON(w io.Writer, s summary) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(s); err != nil {
		return fmt.Errorf("encoding JSON summary: %w", err)
	}
	return nil
}

// writeCSV writes the summary as metric,value rows, one metric per
// line, so runs can be appended to and compared in a spreadsheet.
func writeCSV(w io.Writer, s summary) error {
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
	i := strconv.Itoa
	rows := [][]string{
		{"metric", "value"},
		{"target", s.Config.Target},
		{"domain", s.Config.Domain},
		{"offered_rps", f(s.OfferedRPS)},
		{"duration_seconds", f(s.DurationSeconds)},
		{"keys", i(s.Config.Keys)},
		{"cost", s.CostSpec},
		{"priority_mix", s.MixSpec},
		{"scheduled", i(s.Scheduled)},
		{"sent", i(s.Sent)},
		{"dropped", i(s.Dropped)},
		{"errors", i(s.Errors)},
		{"throughput_rps", f(s.ThroughputRPS)},
		{"latency_p50_ms", f(s.Latency.P50)},
		{"latency_p99_ms", f(s.Latency.P99)},
		{"latency_max_ms", f(s.Latency.Max)},
	}
	for _, name := range []string{"ok", "over_limit", "fail_open", "fail_closed", "unspecified"} {
		rows = append(rows, []string{"reason_" + name, i(s.Reasons[name])})
	}
	for _, p := range s.ByPriority {
		rows = append(rows,
			[]string{p.Priority + "_sent", i(p.Sent)},
			[]string{p.Priority + "_allowed", i(p.Allowed)},
			[]string{p.Priority + "_success_rate", f(p.SuccessRate)},
		)
	}
	if a := s.Accuracy; a != nil {
		rows = append(rows,
			[]string{"accuracy_expected_tokens", f(a.ExpectedTokens)},
			[]string{"accuracy_admitted_tokens", f(a.AdmittedTokens)},
			[]string{"accuracy_ratio", f(a.Ratio)},
			[]string{"accuracy_over_admitted_tokens", f(a.OverAdmittedTokens)},
		)
	}
	cw := csv.NewWriter(w)
	if err := cw.WriteAll(rows); err != nil {
		return fmt.Errorf("writing CSV summary: %w", err)
	}
	return nil
}

// writeTable writes the short human-readable report.
func writeTable(w io.Writer, s summary) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	p := func(format string, args ...any) { _, _ = fmt.Fprintf(tw, format, args...) }

	p("target\t%s (domain %q)\n", s.Config.Target, s.Config.Domain)
	p("offered\t%.0f rps for %.1fs, %d keys, cost %s, mix %s\n",
		s.OfferedRPS, s.DurationSeconds, s.Config.Keys, s.CostSpec, s.MixSpec)
	p("sent / dropped / errors\t%d / %d / %d\n", s.Sent, s.Dropped, s.Errors)
	p("throughput\t%.1f rps\n", s.ThroughputRPS)
	p("latency p50 / p99 / max\t%.2f / %.2f / %.2f ms\n", s.Latency.P50, s.Latency.P99, s.Latency.Max)
	p("reasons\tok %d, over_limit %d, fail_open %d, fail_closed %d\n",
		s.Reasons["ok"], s.Reasons["over_limit"], s.Reasons["fail_open"], s.Reasons["fail_closed"])
	if a := s.Accuracy; a != nil {
		p("accuracy\t%.3f (admitted %.0f of %.0f expected tokens; over-admitted %.0f)\n",
			a.Ratio, a.AdmittedTokens, a.ExpectedTokens, a.OverAdmittedTokens)
	}
	p("\npriority\tsent\tallowed\tsuccess\n")
	for _, ps := range s.ByPriority {
		p("%s\t%d\t%d\t%.1f%%\n", ps.Priority, ps.Sent, ps.Allowed, 100*ps.SuccessRate)
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("writing table: %w", err)
	}
	return nil
}
