# ratelimit

A distributed rate limiter in Go. It ships as a library (pluggable algorithms, in-memory or Redis backends, gRPC interceptors) and as a standalone **RateLimitService** that other services call over RPC before doing expensive work.

> Status: 🚧 in progress. See [Roadmap](#roadmap).

---

## Why

This project is about **application-aware rate limiting**: limits that key off caller identity, tenant, request cost, and priority — so unfair or abusive use, expensive requests, and low-priority floods can be throttled without blunt, one-size-fits-all knobs.

## Goals

- Clear, swappable algorithms behind one interface
- Correct under concurrency (`go test -race` clean)
- Atomic distributed limiting via Redis + Lua (no read-modify-write races)
- **Cost-aware**: a request can consume more than one token
- **Priority-aware**: under pressure, shed sheddable traffic before critical traffic
- Measured, not guessed: benchmarks and load-test results in this README
- Explicit failure behavior (fail-open vs. fail-closed, configurable)

---

## Two ways to use it

### 1. As a library: gRPC interceptors

The limiter runs in-process. Requests over the limit fail fast with `RESOURCE_EXHAUSTED` and a `RetryInfo` detail.

```go
lim := memory.NewTokenBucket(memory.TokenBucketConfig{
    Rate:  10,   // tokens per second
    Burst: 20,   // bucket capacity
})

keyFunc := interceptor.KeyFromMetadata("x-api-key")
srv := grpc.NewServer(
    grpc.UnaryInterceptor(interceptor.Unary(lim, keyFunc)),
    grpc.StreamInterceptor(interceptor.Stream(lim, keyFunc, interceptor.PerMessage)),
)
```

### 2. As a service: RateLimitService

Services ask a shared limiter before doing work. Redis holds the counters, so limits hold across the whole fleet.

```
client service ──ShouldAllow(domain, key, cost, priority)──▶ RateLimitService ──Lua──▶ Redis
               ◀──────── allowed / remaining / retry_after ─────────┘
```

API: [`proto/ratelimit/v1/ratelimit.proto`](proto/ratelimit/v1/ratelimit.proto). Built with `connect-go`, so the same service also answers plain HTTP/JSON:

```bash
docker compose up -d
curl -s localhost:8080/ratelimit.v1.RateLimitService/ShouldAllow \
  -H 'Content-Type: application/json' \
  -d '{"domain":"api","key":"user:42","cost":1}'
```

---

## Core interface

```go
type Limiter interface {
    Allow(ctx context.Context, req Request) (Decision, error)
}

type Request struct {
    Key      string
    Cost     int        // defaults to 1
    Priority Priority   // Critical, Default, Sheddable
}

type Decision struct {
    Allowed    bool
    Limit      int
    Remaining  int
    RetryAfter time.Duration
}
```

All time comes through an injectable `Clock`, so tests are deterministic.

---

## Algorithms

| Algorithm | Memory per key | Accuracy | Burst handling | Notes |
|---|---|---|---|---|
| Fixed window | O(1) | Allows up to 2× at window edges | Poor | Simplest baseline |
| Token bucket | O(1) | Exact average rate | Explicit `Burst` | Good default |
| Sliding window log | O(requests) | Exact | Good | Memory-heavy at high rates |
| Sliding window counter | O(1) | Approximate (weighted) | Good | Best accuracy/memory trade-off |

_TODO: one paragraph + diagram per algorithm._

---

## Design

### Atomicity in Redis

`redisstore.TokenBucket` runs the whole read-refill-check-write sequence as one Lua
script (`redisstore/tokenbucket.lua`), not separate GET/SET calls. Redis runs a
script to completion before serving any other command, so two clients racing for
the last token can't both win the way they could with a read-modify-write over
separate round trips. `TestTokenBucketIntegration`'s concurrent-key test exercises
this directly: 50 goroutines hit one key against a real Redis container, and
exactly `Burst` of them win, every time.

### Time source

Redis's own `TIME` command, not a timestamp the caller computed. Every app server
calling the same Redis agrees on "now" from one source, so their clocks don't need
to agree with each other -- only with Redis's. The cost is one more thing a client
can't fake or inject for testing (the in-memory limiters' `Clock` interface doesn't
apply here); the alternative, trusting each caller's own clock, would let clock
skew between app servers directly skew the limit.

### Cost and priority

**Cost**: `ShouldAllowRequest.cost` flows straight into the same `Request.Cost` /
`EffectiveCost()` every limiter already uses -- a cost-N request consumes N tokens
in one atomic check, the same whether it's checked via the Go `Limiter` interface,
a gRPC interceptor, or RateLimitService over HTTP/JSON.

**Priority**: each `redisstore.Policy` reserves a fraction of its bucket for
higher-priority traffic as it drains, via `ReserveForCritical` and
`ReserveForDefault` (both fractions of `Burst`, 0..1). A request's *floor* --
the minimum tokens its priority must leave behind -- is 0 for Critical,
`Burst*ReserveForCritical` for Default, and `Burst*(ReserveForCritical+ReserveForDefault)`
for Sheddable. Once the bucket drains below a priority's floor, only strictly
higher-priority traffic keeps getting through: that's the whole mechanism behind
"shed sheddable traffic before critical traffic." It's enforced inside the same
atomic Lua script as the refill-and-check (`floor` is just one more parameter),
so there's no separate accounting to keep in sync. Leave both reserves at 0 (the
default) for no reservation -- every priority can drain the bucket fully, same as
if Priority didn't exist.

### Streaming RPCs

`interceptor.Stream` supports both, via a `StreamMode`:

- **`PerStream`**: checks the limit once, before the handler runs. Limits how many
  streams a key may open (e.g. one long-lived subscription per key) -- cheap, and
  the natural choice when a stream's cost is in opening it, not in what flows after.
- **`PerMessage`**: checks before every message the client sends, by wrapping
  `RecvMsg`. Limits the rate of events a client pushes over one long-lived stream.

A stream hit mid-flight doesn't get torn down out of band: in `PerMessage` mode, the
*next* `RecvMsg` call returns the `ResourceExhausted` error instead of the client's
message, and it's up to the handler's own loop to notice that error and return (which
ends the RPC with that status) -- the interceptor can't unilaterally stop a handler
that's already running. `PerStream` mode never faces this: the only check happens
before the handler starts, so a denial there means the handler never runs at all.

### The extra hop

`ShouldAllowBatch` lets a caller check several keys in one round trip,
atomically: the whole batch is allowed only if every check in it is, and tokens
are only consumed when all of them pass. `redisstore/tokenbucketbatch.lua` does
that all-or-nothing check-and-commit server-side, in one script, rather than the
client making N separate calls and hoping nothing changes between them.

_TODO: latency budget for the extra network hop; a client-side local token cache
to avoid a call per request entirely. Step 9 added per-call deadlines and
client-side fallback (see [Failure modes](#failure-modes)), but not a local cache._

### Policies

`policies.yaml`, loaded by `policy.Load` and looked up fresh on every request --
not cached per-domain in `Server` -- so a hot reload takes effect on the very
next call, with no server-side state to invalidate:

```yaml
policies:
  api:
    rate: 10
    burst: 20
    reserve_for_critical: 0.3
    reserve_for_default: 0.2
  send-message:
    rate: 2
    burst: 5
    reserve_for_critical: 0.5

default:            # applied to any domain with no entry above
  rate: 5
  burst: 10
```

`policy.Store.Watch` reloads on any write to the file (watching its parent
directory, not the file itself, since many editors and config-management tools
replace a file via rename rather than writing it in place -- a watch on the file
alone can miss that). A reload that fails to parse or validate is logged and
discarded: the `Store` keeps serving its last good snapshot rather than falling
back to a placeholder policy for every domain over one bad edit.

**What happens to in-flight counters when a limit changes**: nothing has to,
by design. Redis holds each key's raw token count, not "tokens as a fraction
of the configured burst" -- so a policy change just changes what that count
means going forward, with no migration needed. The one sharp edge: the Lua
script clamps `tokens` to `burst` on every access (`math.min(burst, ...)`), so
lowering `Burst` below a key's current count caps it down to the new ceiling
immediately, on its very next request -- not gradually. A caller that had
built up a large burst loses that headroom in one step the moment the new
policy takes effect, rather than spending it down first.

### Failure modes

A rate limiter sits in front of other services, so when it breaks, the
question is what it does to them. There are two places it can break, and each
is handled where the most information is available.

**Redis unavailable (service up).** Every Redis call runs under a deadline
(`REDIS_TIMEOUT`, default 50ms, `service.WithBackendTimeout`). If the call
errors or times out, the server doesn't return an error. It answers using the
domain's `failure_mode` from the policy file:

```yaml
policies:
  send-message:
    rate: 2
    burst: 5
    failure_mode: fail_closed   # or fail_open; default fail_open
```

| `failure_mode` | Answer | `reason` |
|---|---|---|
| `fail_open` (default) | allowed, `remaining: 0` | `REASON_FAIL_OPEN` |
| `fail_closed` | denied, `retry_after` = one token's refill time (`1/rate`) | `REASON_FAIL_CLOSED` |

**The default is `fail_open`.** For most domains a limiter outage shouldn't
also take down every service that calls it. Use `fail_closed` for domains
where overrunning the limit is worse than refusing traffic, such as a paid
third-party API or a fragile downstream. `failure_mode` hot-reloads like every
other policy field. An unknown value (`fail-closed`, `open`) fails the reload,
so a typo can't silently fail open. A batch with checks in several domains
answers each check by its own domain's mode, and is allowed only if all of
them are, i.e. only if every domain in it is `fail_open`.

The `reason` is what lets callers and dashboards tell an admission under
failure from a real one. `REASON_OK` means Redis counted the request.
`REASON_FAIL_OPEN` means nobody did. Two cases are never answered by the
failure mode:
- **Invalid requests** (empty key, cost above burst) are still
  `InvalidArgument`, so a fail-open allow can't hide a caller bug.
- **A caller that cancelled or ran past its own deadline** gets `Canceled` or
  `DeadlineExceeded`. Only the server's own Redis timeout triggers the
  failure mode.

The server is only as good as go-redis's deadline handling.
`redis.Options.ContextTimeoutEnabled` must be set (`ratelimitd` sets it).
Without it, go-redis ignores context deadlines on socket reads, so a stalled
Redis holds each call for its own 3s `ReadTimeout`.

**RateLimitService unreachable (client side).** The Go client puts a deadline
on every call (`client.Config.Timeout`, default 200ms). If the service can't
be reached (`Unavailable`) or doesn't answer in time, the client decides
locally using `Config.Fallback`:

| `client.FallbackMode` | Result |
|---|---|
| `FallbackFailOpen` (default) | allowed, `Reason: ReasonFailOpen` |
| `FallbackFailClosed` | denied, `Reason: ReasonFailClosed`, `RetryAfter: Config.FallbackRetryAfter` (default 1s) |
| `FallbackError` | the error is returned; the caller decides |

`Config.DomainFallback` overrides the mode per domain and should normally
mirror the server's `failure_mode` for that domain. The client can't read the
policy file, so the two must be configured separately. A local decision sets
`Decision.Fallback = true` and keeps the cause in `Decision.FallbackCause`. A
server-side failure-mode answer arrives with `Fallback = false`, so callers can
tell "Redis was down, the server decided" from "the service was down, the
client decided". The client's 200ms default sits above the server's 50ms Redis
timeout on purpose. When only Redis is slow, the server answers first with the
domain's configured mode, which the client may not know.

**No circuit breaker (yet).** During an outage, every request waits out the
full timeout before the failure mode applies, so while Redis hangs, request
latency rises to about `REDIS_TIMEOUT`. A breaker would stop calling Redis
after repeated failures and answer immediately. It was left out of step 9 to
keep the change small: done properly it needs per-instance failure tracking, a
half-open probe to detect recovery, and a decision about whether batches
count as one failure or many. It's future work. Until then, `REDIS_TIMEOUT`
caps the added latency.

### Concurrency (in-memory backend)

Each in-memory limiter keeps its per-key state in a `shardedMap`: the
key space is split across a fixed number of shards (32 by default),
each with its own mutex, instead of one mutex for the whole limiter. A
single mutex serializes every key, even ones that never contend with
each other; sharding by key hash removes that false contention. It does
nothing for one hot key, which still serializes on that key's shard.

Measured with `BenchmarkFixedWindow_ShardCount`: parallel callers spread
over 1024 keys, one mutex for everything (`shards=1`) against the default
(`shards=32`), at several `GOMAXPROCS` values. Median ns/op of 5 runs;
lower is better.

| GOMAXPROCS | 1 shard (single mutex) | 32 shards (default) | Sharded is faster by |
|---|---|---|---|
| 1 | 47.1 | 48.2 | 0.98x |
| 2 | 92.5 | 47.7 | 1.9x |
| 4 | 122.4 | 33.5 | 3.7x |
| 8 | 159.2 | 38.5 | 4.1x |
| 10 | 171.9 | 41.0 | 4.2x |

`go test -run '^$' -bench BenchmarkFixedWindow_ShardCount -benchmem -count=5 -cpu 1,2,4,8,10 ./memory/`
on the machine described under [Benchmarks](#benchmarks); raw output in
[`bench/raw/shards-gomaxprocs.txt`](bench/raw/shards-gomaxprocs.txt).

What the numbers say:

- **With one core there is nothing to win.** Both variants cost about 47-48
  ns per call; sharding adds no measurable overhead and no benefit.
- **The single mutex gets slower with every core added.** Per-call cost
  rises from 47.1 ns to 171.9 ns between 1 and 10 cores, so total
  throughput *falls* by about 3.6x: more cores means more time spent
  handing the lock around, not more work done.
- **Sharding stops that collapse, but it does not scale linearly.** With 32
  shards the per-call cost stays between 33.5 and 48.2 ns at every core
  count. Ten cores do about 1.2x the work of one, not 10x. The 4.2x gap at
  10 cores is mostly the single mutex getting worse, not the sharded map
  getting faster.
- **Best throughput is at 4 cores**, and it drops slightly at 8 and 10. This
  machine has 4 performance and 6 efficiency cores, so the extra cores are
  slower ones; that is a likely contributor, but it was not isolated here.

Why sharded throughput stays nearly flat was not investigated in this pass.
A plausible cause is the benchmark's shape: every goroutine walks the same
1024 keys in the same order, so every core keeps touching every shard's
mutex and entries, and those cache lines bounce between cores even when no
goroutine actually waits for a lock. That is a hypothesis, not a
measurement.

A single hot key gets no help from sharding, as expected: `SingleKey` in the
table below (every call on one key) costs 174-181 ns at 10 cores, close to
the single-mutex figure (171.9 ns).

---

## Benchmarks

Machine: Apple M5 (`Mac17,3`), 10 cores (4 performance + 6 efficiency), 24 GB
RAM, macOS 26.6.2, go1.27.2 darwin/arm64. Redis 7.4.11 (`redis:7-alpine`) in
Docker Desktop (engine 29.8.2). Run on 2026-11-10; the machine was not idle
(load average 2.8-4.6). Every figure is the median of 5 runs
(`-count=5`); raw output, per-benchmark min/max and the exact commands are in
[`bench/`](bench/README.md).

### In-memory limiters

`go test -run '^$' -bench . -benchmem -count=5 ./memory/`, `GOMAXPROCS=10`.
`SingleKey` is every call hitting the same key (worst case: one shard does all the work);
`ManyKeys` spreads calls over 1024 distinct keys in parallel (where sharding pays off).

| Algorithm | ns/op (SingleKey) | ns/op (ManyKeys) | allocs/op |
|---|---|---|---|
| fixed window | 175.8 | 40.6 | 0 |
| token bucket | 173.7 | 43.4 | 0 |
| sliding window log | 177.2 | 67.6 | 0 (small heap growth, see below) |
| sliding window counter | 181.2 | 47.4 | 0 |

Sliding window log reports 0 allocs/op but nonzero B/op (26-30 for `SingleKey`, 155-177
for `ManyKeys`) under `-benchmem`: its per-key log occasionally grows its backing slice
via `append`, which `go test` attributes to the slice's bytes rather than a separate
allocation count. It's also the one algorithm
whose *time* per call isn't O(1): Allow is O(stale entries evicted) amortized, which an
earlier version of this benchmark caught failing at -- it was resumming the entire log by
hand on every call instead of tracking total cost incrementally, making each call O(log
length). Fixed before these numbers were taken; see `logState` in `slidingwindowlog.go`.

### Redis backend and service round trip

`go test -run '^$' -bench . -benchmem -count=5 ./redisstore/ ./service/`. Both start a
Redis container through testcontainers. `Serial` is one caller, so ns/op is the latency
of one call. `Parallel` is `GOMAXPROCS=10` concurrent callers, so ns/op there is
wall time divided by calls (the inverse of throughput), not latency.

| Benchmark | ns/op (Serial) | ns/op (Parallel) | allocs/op | p50 / p99 |
|---|---|---|---|---|
| redis / token bucket, one hot key | 102,810 | 33,549 | 20 | not measured |
| redis / token bucket, 1024 keys | (same benchmark as above) | 33,613 | 20 | not measured |
| service round-trip, serial | 144,624 | | 175 | 141 µs / 207 µs |
| service round-trip, parallel | | 47,837 | 174 | 473 µs / 848 µs |

- One Redis `Allow` (one Lua round trip) takes about 103 µs for a single caller, and
  ten parallel callers get about 29,800 calls/s in total. Hot key or 1024 keys makes no
  difference (33,549 vs 33,613 ns/op): Redis runs scripts one at a time either way.
- The service adds about 42 µs on top of the Redis call for a single caller
  (144.6 µs vs 102.8 µs), and 155 allocations per request (175 vs 20), from HTTP and
  connect. Ten parallel callers get about 20,900 requests/s.
- The Redis call is roughly 2,400x the cost of the in-memory token bucket
  (102,810 ns vs 43.4 ns). The network round trip dominates, and here it crosses
  Docker Desktop's macOS-to-VM boundary, so these numbers are specific to this setup.
- The parallel p99 is the least stable figure: 0.70 ms to 1.48 ms across the five runs.

## Load test

**Harness.** [`cmd/loadtest`](cmd/loadtest) drives a running `ratelimitd` through the Go
client. It is open-loop: requests go out on a fixed schedule that does not wait for
responses, and latency is measured from each request's *scheduled* send time, so a slow
server shows up as latency instead of quietly lowering the offered rate. Keys, costs and
priorities are sampled from a seeded generator, so a run is repeatable. Accuracy compares
admitted tokens with the most a correct token bucket may admit per key,
`min(requested, burst + rate × duration)`.

**Setup.** Redis 7.4.11 from `docker compose`; `ratelimitd` and the harness as host
processes on the machine above, over loopback. Domain `api` from
[`policies.yaml`](policies.yaml): 10 tokens/s and burst 20 per key, 30% of burst reserved
for critical and a further 20% for default. 100 keys, so the sustained limit is 1000
requests/s in aggregate. Cost 1, priority mix 10% critical / 60% default / 30% sheddable,
30 s per scenario:

```bash
docker compose up -d && go run ./cmd/ratelimitd &
go run ./cmd/loadtest -domain api -rate 10 -burst 20 -rps <500|1000|3000> \
  -duration 30s -keys 100 -cost 1 -mix critical=10,default=60,sheddable=30 -seed 1
```

**Throughput and latency**

| Scenario | Offered | Throughput | p50 | p99 | max | Errors / dropped |
|---|---|---|---|---|---|---|
| Under the limit (0.5x) | 500 rps | 500.0 rps | 0.99 ms | 1.52 ms | 19.96 ms | 0 / 0 |
| At the limit (1x) | 1000 rps | 1000.0 rps | 0.75 ms | 1.33 ms | 11.68 ms | 0 / 0 |
| Overload (3x) | 3000 rps | 3000.0 rps | 0.20 ms | 0.43 ms | 37.81 ms | 0 / 0 |

**Accuracy**

| Scenario | Admitted | Most a correct limiter may admit | Ratio | Over-admitted |
|---|---|---|---|---|
| Under the limit | 15,000 | 15,000 | 1.000 | 0 |
| At the limit | 28,846 | 29,915 | 0.964 | 0 |
| Overload (3x) | 31,267 | 32,000 | 0.977 | 0 |

**Success rate by priority** (allowed / sent)

| Scenario | Critical | Default | Sheddable |
|---|---|---|---|
| Under the limit | 100.0% (1,480 / 1,480) | 100.0% (8,954 / 8,954) | 100.0% (4,566 / 4,566) |
| At the limit | 100.0% (2,925 / 2,925) | 99.7% (18,048 / 18,096) | 87.7% (7,873 / 8,979) |
| Overload (3x) | 100.0% (8,924 / 8,924) | 40.4% (21,918 / 54,296) | 1.6% (425 / 26,780) |

What the results show:

- **The limit was never exceeded.** Over-admitted tokens are 0 in all three runs.
- **Shedding follows priority.** At 3x overload every critical request got through,
  default kept 40.4%, and sheddable was cut to 1.6%.
- **Sheddable traffic is shed before the limit is reached.** At exactly 1x, 12.3% of
  sheddable requests were denied and 3.6% of the allowance went unused. Keys are picked
  at random, so a key regularly gets a short burst above its rate; its bucket then dips
  below the sheddable floor (half the burst) and sheddable requests are refused although
  tokens remain. This is the reserve working as configured, and it is the price of it.
- **Under overload about 2.3% of the allowance stays unused** (733 of 32,000 tokens). That
  is consistent with each of the 100 buckets ending the run near the critical reserve
  (6 tokens) that default and sheddable traffic may not spend, though no per-key trace
  was taken to confirm it.
- **Latency went down as load went up** (p50 0.99 ms at 500 rps, 0.20 ms at 3000 rps),
  the opposite of what a loaded server normally does. The cause was not investigated. At
  low rates each request probably finds idle connections and sleeping threads, on a
  path that includes Docker Desktop's network proxy, and pays to wake them up; treat
  the low-rate latencies as a property of this setup, not of the limiter.
- **These runs overload the limit, not the service.** 3000 rps is well below the roughly
  20,900 requests/s the round-trip benchmark reached, so throughput equals the offered
  rate in every scenario. The service's saturation point was not load-tested.

Raw harness output and JSON summaries: [`bench/raw/loadtest/`](bench/raw/loadtest).

---

## Scaling further

_TODO: what changes at 100× — local pre-aggregation, Redis Cluster key hashing, hot keys, regional vs. global limits._

---

## Roadmap

Each step is sized for one ~45-minute session.

**Library**
- [x] **1. Scaffold**: `go mod init`, `Limiter` interface, `Clock` abstraction, fixed window (in-memory), table-driven tests, GitHub Actions (`go test -race`, `golangci-lint`)
- [x] **2. Token bucket**: in-memory implementation, fake-clock tests for refill, burst, and cost > 1
- [x] **3. Sliding windows**: log and counter variants; a test comparing accuracy against fixed window at window edges
- [x] **4. Concurrency**: sharded locks, `-race` stress test, first benchmarks
- [x] **5. gRPC interceptors**: unary + streaming, `RESOURCE_EXHAUSTED` with `RetryInfo`, key extraction from metadata, example server
- [x] **6. Redis backend**: token bucket in Lua, `docker-compose`, integration tests (testcontainers)

**Service**
- [x] **7. RateLimitService**: `.proto`, `buf generate`, connect-go server backed by Redis, Go client
- [x] **8. Priority + policies**: criticality-aware shedding, YAML policies per domain with hot reload
- [x] **9. Failure modes + load test**: fail-open/closed per domain, timeouts, client-side fallback, load harness
- [ ] **10. Polish**: load-test results in README, fill in Design sections, diagrams, release `v0.1.0`, write the post

---

## Project layout

```
.
├── limiter.go               # Limiter, Request, Decision, Clock
├── clocktest/               # fake clock for deterministic tests
├── memory/                  # in-memory algorithms
├── redisstore/              # Redis backend + Lua scripts
├── interceptor/             # gRPC unary + stream interceptors
├── proto/ratelimit/v1/      # service API
├── gen/                     # generated code (buf)
├── service/                 # RateLimitService server
├── policy/                  # YAML policy loading + hot reload
├── client/                  # Go client with local cache + fallback
├── cmd/
│   ├── ratelimitd/          # the service binary
│   ├── example-server/
│   └── loadtest/
├── bench/                   # raw benchmark + load-test output behind the README numbers
├── policies.yaml
├── buf.yaml
├── docker-compose.yml
└── .github/workflows/ci.yml
```

## License

MIT
