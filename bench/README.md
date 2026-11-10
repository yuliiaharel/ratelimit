# Benchmark and load-test results

Raw output behind every number in the top-level README. All of it was
produced on 2026-11-10 from commit `29dfd9c` (main) plus the two benchmark
files added alongside this directory.

## Environment

| | |
|---|---|
| Machine | Apple M5 (`Mac17,3`), 10 cores (4 performance + 6 efficiency), 24 GB RAM, on AC power |
| OS | macOS 26.6.2 (25G83), darwin/arm64 |
| Go | go1.27.2 darwin/arm64 |
| Redis | 7.4.11 (`redis:7-alpine`, linux/arm64), in a container |
| Docker | Docker Desktop, engine 29.8.2, VM with 10 CPUs / 7.9 GB |

The machine was not idle: `uptime` reported 1-minute load averages between
2.8 and 4.6 around the runs. Redis runs inside Docker Desktop's Linux VM, so
every Redis round trip crosses the macOS-to-VM network path; the Redis and
service numbers include that and would differ on Linux with native
containers.

## Benchmarks

```bash
go test -run '^$' -bench . -benchmem -count=5 ./memory/      > bench/raw/memory.txt
go test -run '^$' -bench 'BenchmarkFixedWindow_ShardCount' -benchmem -count=5 \
  -cpu 1,2,4,8,10 ./memory/                                  > bench/raw/shards-gomaxprocs.txt
go test -run '^$' -bench . -benchmem -count=5 ./redisstore/  > bench/raw/redis.txt
go test -run '^$' -bench . -benchmem -count=5 ./service/     > bench/raw/service.txt
```

The Redis and service benchmarks start their own `redis:7-alpine` container
through testcontainers (and are skipped without Docker), which is why those
two files also contain container start-up logs.

`summary.txt` holds the median, minimum and maximum of the five runs of each
benchmark. `benchstat` was not installed on this machine, so it is not a
benchstat report; to get one:

```bash
go install golang.org/x/perf/cmd/benchstat@latest
benchstat bench/raw/memory.txt
benchstat bench/raw/shards-gomaxprocs.txt
benchstat bench/raw/redis.txt
benchstat bench/raw/service.txt
```

## Load test

Redis from `docker compose`, `ratelimitd` and the harness as host processes
on the same machine, talking over loopback. Policy: the repo's
`policies.yaml`, domain `api` (rate 10/s, burst 20 per key,
`reserve_for_critical: 0.3`, `reserve_for_default: 0.2`). With 100 keys the
aggregate sustained limit is 1000 tokens/s.

```bash
docker compose up -d
go build -o /tmp/ratelimitd ./cmd/ratelimitd && go build -o /tmp/loadtest ./cmd/loadtest
/tmp/ratelimitd > bench/raw/loadtest/ratelimitd.log 2>&1 &

cd bench/raw/loadtest
MIX=critical=10,default=60,sheddable=30
/tmp/loadtest -domain api -rate 10 -burst 20 -rps 500  -duration 30s -keys 100 -cost 1 -mix $MIX -seed 1 -out under-limit.json | tee under-limit.txt
/tmp/loadtest -domain api -rate 10 -burst 20 -rps 1000 -duration 30s -keys 100 -cost 1 -mix $MIX -seed 1 -out at-limit.json    | tee at-limit.txt
/tmp/loadtest -domain api -rate 10 -burst 20 -rps 3000 -duration 30s -keys 100 -cost 1 -mix $MIX -seed 1 -out overload-3x.json | tee overload-3x.txt
```

Runs were back to back with a 3 s pause; each run uses its own key prefix, so
buckets are fresh. `*.txt` is the harness's table, `*.json` its
machine-readable summary, `ratelimitd.log` the service's log for the whole
session (one start-up line: no backend failures were logged).

## Files

```
summary.txt                  median / min / max per benchmark over the 5 runs
raw/memory.txt               in-memory limiters, GOMAXPROCS=10
raw/shards-gomaxprocs.txt    1 shard vs 32 shards at GOMAXPROCS 1, 2, 4, 8, 10
raw/redis.txt                redisstore.TokenBucket against a Redis container
raw/service.txt              RateLimitService round trip (connect client -> server -> Redis)
raw/loadtest/                three cmd/loadtest scenarios + service log
```
