// Command loadtest drives a running RateLimitService at a fixed request
// rate and reports throughput, latency, accuracy against the
// configured limit, and per-priority success rate.
//
// Requests are sent open-loop: on a fixed schedule that doesn't wait
// for responses, with latency measured from each request's scheduled
// time. A request whose slot comes up while -concurrency requests are
// already in flight is counted as dropped rather than delayed.
//
// Example, against `go run ./cmd/ratelimitd` with the repo's
// policies.yaml (domain "api": rate 10, burst 20), at 20x overload:
//
//	go run ./cmd/loadtest -domain api -rate 10 -burst 20 \
//	  -rps 2000 -duration 10s -keys 10 -cost 1:90,5:10 \
//	  -mix critical=10,default=60,sheddable=30 -out summary.json
//
// The client is configured with FallbackError, so an unreachable
// service shows up as errors rather than as client-side fallback
// decisions. Server-side fail_open/fail_closed answers are counted
// under "reasons".
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/yuliiaharel/ratelimit/client"
)

func main() {
	if err := mainErr(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		log.Fatalf("loadtest: %v", err)
	}
}

func mainErr(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("loadtest", flag.ContinueOnError)
	fs.SetOutput(stderr)

	cfg := config{}
	var costSpec, mixSpec, out, format string
	fs.StringVar(&cfg.Target, "target", "http://localhost:8080", "RateLimitService base URL")
	fs.StringVar(&cfg.Domain, "domain", "api", "policy domain to send requests under")
	fs.Float64Var(&cfg.RPS, "rps", 200, "target requests per second")
	fs.DurationVar(&cfg.Duration, "duration", 10*time.Second, "how long to send for")
	fs.IntVar(&cfg.Keys, "keys", 100, "key cardinality: requests are spread uniformly over this many keys")
	fs.StringVar(&cfg.KeyPrefix, "key-prefix", "", "key prefix (default: unique per run, so earlier runs' buckets don't interfere)")
	fs.StringVar(&costSpec, "cost", "1", `cost distribution: "N", or "cost:weight,..." e.g. "1:90,5:10"`)
	fs.StringVar(&mixSpec, "mix", "critical=10,default=60,sheddable=30", "priority mix as relative weights")
	fs.IntVar(&cfg.Concurrency, "concurrency", 512, "max requests in flight")
	fs.DurationVar(&cfg.Timeout, "timeout", client.DefaultTimeout, "per-request client deadline")
	fs.Uint64Var(&cfg.Seed, "seed", 1, "seed for key, cost and priority sampling")
	fs.Float64Var(&cfg.Rate, "rate", 0, "the domain's configured rate, for the accuracy report (0 = skip)")
	fs.IntVar(&cfg.Burst, "burst", 0, "the domain's configured burst, for the accuracy report")
	fs.StringVar(&out, "out", "loadtest-summary.json", `machine-readable summary path ("-" for stdout)`)
	fs.StringVar(&format, "format", "json", "summary format: json or csv")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var err error
	if cfg.Cost, err = parseCostDist(costSpec); err != nil {
		return fmt.Errorf("-cost: %w", err)
	}
	if cfg.Mix, err = parsePriorityMix(mixSpec); err != nil {
		return fmt.Errorf("-mix: %w", err)
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = defaultKeyPrefix(time.Now())
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	write := writeJSON
	switch format {
	case "json":
	case "csv":
		write = writeCSV
	default:
		return fmt.Errorf("-format %q: want json or csv", format)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	c := client.NewWithConfig(cfg.Target, nil, client.Config{Timeout: cfg.Timeout, Fallback: client.FallbackError})
	results, elapsed := run(ctx, cfg, c)
	s := summarize(cfg, results, elapsed)

	// The table goes to stdout unless the summary does; then stderr,
	// so `-out -` output stays parseable.
	table := stdout
	if out == "-" {
		table = stderr
		if err := write(stdout, s); err != nil {
			return err
		}
	} else {
		f, err := os.Create(out)
		if err != nil {
			return fmt.Errorf("creating %s: %w", out, err)
		}
		if err := write(f, s); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("closing %s: %w", out, err)
		}
	}
	return writeTable(table, s)
}
