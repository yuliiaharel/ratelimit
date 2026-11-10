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

The effect, measured directly (`BenchmarkFixedWindow_ShardCount`, Apple
M5, `go test -bench . -benchmem`, parallel access across 1024 keys):

| Shards | ns/op |
|---|---|
| 1 (one mutex for everything) | ~153 |
| 32 (default) | ~39 |

About a 4x improvement at this core count and key spread, from a
one-line change at each limiter's construction site.

---

## Benchmarks

`go test -bench . -benchmem ./memory/...`, Apple M5 (10 cores), go1.27.2 darwin/arm64.
`SingleKey` is every call hitting the same key (worst case: one shard does all the work);
`ManyKeys` spreads calls over 1024 distinct keys in parallel (where sharding pays off).

| Backend / algorithm | ns/op (SingleKey) | ns/op (ManyKeys) | allocs/op |
|---|---|---|---|
| memory / fixed window | ~163 | ~38 | 0 |
| memory / token bucket | ~168 | ~40 | 0 |
| memory / sliding window log | ~175 | ~55 | 0 (small heap growth, see below) |
| memory / sliding window counter | ~159 | ~42 | 0 |
| redis / token bucket | | | |
| service round-trip (p50 / p99) | | | |

Sliding window log reports 0 allocs/op but nonzero B/op (~26-150) under `-benchmem`: its
per-key log occasionally grows its backing slice via `append`, which `go test` attributes
to the slice's bytes rather than a separate allocation count. It's also the one algorithm
whose *time* per call isn't O(1): Allow is O(stale entries evicted) amortized, which an
earlier version of this benchmark caught failing at -- it was resumming the entire log by
hand on every call instead of tracking total cost incrementally, making each call O(log
length). Fixed before these numbers were taken; see `logState` in `slidingwindowlog.go`.

## Load test

_TODO: harness description, throughput/latency (p50/p99) chart, accuracy (allowed vs. expected), critical vs. sheddable success rate under overload._

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
├── policies.yaml
├── buf.yaml
├── docker-compose.yml
└── .github/workflows/ci.yml
```

## License

MIT
