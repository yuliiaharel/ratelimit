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

srv := grpc.NewServer(
    grpc.UnaryInterceptor(interceptor.Unary(lim, interceptor.KeyFromMetadata("x-api-key"))),
    grpc.StreamInterceptor(interceptor.Stream(lim, interceptor.PerMessage)),
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
_TODO: why a Lua script instead of GET/SET, and what the script does._

### Time source
_TODO: whose clock wins — app server or Redis `TIME`? Trade-offs with clock skew._

### Cost and priority
_TODO: how cost consumes tokens; how sheddable traffic gets a lower effective limit (reserved headroom for critical)._

### Streaming RPCs
_TODO: limit per stream, per message, or both? What happens to a stream mid-flight when the limit is hit?_

### The extra hop
_TODO: latency budget of calling RateLimitService; batching (`ShouldAllowBatch`); client-side local token cache to avoid a call per request._

### Policies
_TODO: YAML policy config keyed by domain; hot reload without restart; what happens to in-flight counters when a limit changes._

### Failure modes
_TODO: Redis or RateLimitService unavailable → fail-open (availability) vs. fail-closed (protection), per domain. Timeouts, circuit breaking._

### Concurrency (in-memory backend)
_TODO: single mutex vs. sharded locks; benchmark numbers._

---

## Benchmarks

_TODO: `go test -bench` results, machine spec._

| Backend / algorithm | ns/op | allocs/op |
|---|---|---|
| memory / token bucket | | |
| memory / sliding counter | | |
| redis / token bucket | | |
| service round-trip (p50 / p99) | | |

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
- [ ] **4. Concurrency**: sharded locks, `-race` stress test, first benchmarks
- [ ] **5. gRPC interceptors**: unary + streaming, `RESOURCE_EXHAUSTED` with `RetryInfo`, key extraction from metadata, example server
- [ ] **6. Redis backend**: token bucket in Lua, `docker-compose`, integration tests (testcontainers)

**Service**
- [ ] **7. RateLimitService**: `.proto`, `buf generate`, connect-go server backed by Redis, Go client
- [ ] **8. Priority + policies**: criticality-aware shedding, YAML policies per domain with hot reload
- [ ] **9. Failure modes + load test**: fail-open/closed per domain, timeouts, client-side fallback, load harness, results in README
- [ ] **10. Polish**: fill in Design sections, diagrams, release `v0.1.0`, write the post

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
