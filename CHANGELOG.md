# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). Until 1.0 the API
may change between minor versions.

## [0.1.0] - 2026-11-12

First release. Requires Go 1.26 or later; the Redis backend and the service
were tested against Redis 7.4.

### Added

- **Core API** (`ratelimit`): the `Limiter` interface, `Request` (key, cost,
  priority), `Decision` (allowed, limit, remaining, retry-after) and an
  injectable `Clock`. `clocktest` provides a fake clock for tests.
- **In-memory limiters** (`memory`): fixed window, token bucket, sliding
  window log and sliding window counter. All are safe for concurrent use, with
  per-key state behind sharded locks, and all support request cost.
- **Redis backend** (`redisstore`): a token bucket implemented as a Lua script,
  so each check is atomic and uses Redis's clock. Supports request cost,
  priority reserves (critical / default / sheddable), and all-or-nothing batch
  checks, including batches that span policies.
- **gRPC interceptors** (`interceptor`): unary and stream server interceptors
  for any `Limiter`, with per-stream or per-message checking. Denials return
  `RESOURCE_EXHAUSTED` with a `RetryInfo` detail.
- **RateLimitService** (`service`, `cmd/ratelimitd`): a standalone server
  built with connect-go, with `ShouldAllow` and `ShouldAllowBatch`. Also
  answers plain HTTP/JSON.
- **Policies** (`policy`): per-domain rate, burst, priority reserves and
  failure mode in a YAML file, reloaded automatically when the file changes.
- **Failure handling**: a deadline on every Redis call, `fail_open` or
  `fail_closed` per domain when Redis is unavailable, and a reason on every
  response that tells a real decision from a failure-mode answer.
- **Go client** (`client`): per-call deadlines and a configurable local
  fallback (fail open, fail closed, or return the error) for when the service
  cannot be reached.
- **Load harness** (`cmd/loadtest`): open-loop load generator reporting
  throughput, latency, accuracy against the configured limit and success rate
  by priority.
- **Benchmarks and measured results**: benchmarks for every backend and the
  service round trip; results, raw output and reproduction commands in the
  README and `bench/`.
- **Documentation**: package documentation and runnable examples for every
  public package, an example gRPC server (`cmd/example-server`), and design
  notes in the README.

### Known limitations

- The in-memory limiters ignore request priority and never evict idle keys.
- The gRPC interceptors always charge one token at default priority.
- The Redis backend targets a single Redis instance; Redis Cluster is not
  supported.
- There is no circuit breaker: while Redis is down, each request waits for the
  Redis timeout before the failure mode applies.
- There is no client-side token cache: every check through the service is one
  network round trip.

[0.1.0]: https://github.com/yuliiaharel/ratelimit/releases/tag/v0.1.0
