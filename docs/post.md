# Building an application-aware rate limiter in Go

> **Draft.** Written from the code, the README and the measured results in this
> repository, as raw material to rewrite. The section "What I would do
> differently" lists candidates that follow from the results; keep the ones
> that match your own view and drop the rest. All numbers are from
> [`bench/`](../bench/README.md) and were measured on one laptop.

## The problem

Most systems already have rate limiting somewhere: at the load balancer, the
API gateway, or the proxy in front of each service. Those limits are cheap to
turn on and they stop the crudest kind of overload. They are also blunt,
because the infrastructure only sees what is on the outside of a request: an
IP address, a path, maybe a header.

That leaves it blind to the things that decide whether a request is a problem:

- **Who is asking.** One tenant sending ten times its share looks the same as
  ten tenants sending their fair share.
- **What the request costs.** A search over a year of data and a lookup by ID
  each count as one request.
- **How much it matters.** Under pressure, a background sync and a user
  pressing "send" are throttled alike.

The result is limits that are either too loose to protect anything or tight
enough to hurt legitimate traffic. The alternative is to move the decision to
where the knowledge is. The application knows the caller, the tenant, the cost
and the priority, so it should be the one asking "may I do this?", with all of
that in the question.

This project is that idea built end to end: a Go library with several
algorithms behind one interface, a Redis backend so a limit holds across a
fleet, and a small service that other services call before doing expensive
work.

## Design decisions and their trade-offs

**One interface, several algorithms.** Everything implements
`Allow(ctx, Request) (Decision, error)`, where a request carries a key, a cost
and a priority. Fixed window, token bucket, sliding window log and sliding
window counter are all there, mostly so their differences can be shown and
tested side by side: the fixed window really does admit twice its limit across
a window boundary, the log is exact but stores every request, and the counter
approximates it with two numbers. Only the token bucket made it to the Redis
backend. It is the one I would pick by default: constant memory, an exact
long-run rate, and bursts that are a setting, not an accident.

**Atomicity through a Lua script.** The distributed version of a token bucket
is a read, a refill, a comparison and a write. Done as separate commands, two
callers can both read the last token. Done as one Lua script, Redis runs the
whole sequence before anything else. The script also reads the time from Redis
itself, so application servers do not need synchronized clocks. The trade-off
is that everything goes through one single-threaded Redis, and the time source
can no longer be faked in tests the way the in-memory clock can.

**Cost is part of the request.** A request can take more than one token. This
is the smallest feature in the code and probably the most useful one: it lets
one limit cover cheap and expensive operations without a separate bucket for
each.

**Priority as a reserve.** Each policy can hold back a fraction of the bucket
for higher-priority traffic. Sheddable requests are refused once the bucket
falls below one threshold, default requests below a lower one, and critical
requests can drain it. It is one extra parameter in the same script, so there
is no second set of counters to keep consistent. The trade-off showed up in
the load test and is discussed below: the reserve costs some capacity even
when there is no overload.

**A service, and the extra hop it adds.** Putting the limiter behind an RPC
gives every service one shared answer and one place for policy. It also puts a
network round trip in front of every guarded request. Batching several checks
into one atomic call is the only mitigation built so far. Policies live in a
YAML file that is reloaded on change, and a bad edit is rejected while the
last good version keeps serving.

**Deciding what happens when it breaks.** A rate limiter sits in front of
other services, so its own failure mode matters more than most. Each domain
chooses whether to fail open or fail closed when Redis is unavailable, every
Redis call has a deadline, and every response says whether it was a real
decision or a failure-mode answer. The client makes the same choice locally
when the service itself cannot be reached. The default is to fail open,
because a limiter outage should not become an outage of everything behind it.
There is no circuit breaker yet, so during a Redis outage each request still
waits out the timeout.

**Sharded locks in memory.** The in-memory limiters keep per-key state in a
map split into 32 shards, each with its own mutex, instead of one mutex for
everything.

## Measured results

Everything here was run on one machine (Apple M5, 10 cores, Redis 7.4 in a
container), so treat the absolute numbers as specific to that setup.

**In memory**, a check costs about 41 to 68 ns when calls are spread over many
keys and 174 to 181 ns when every call hits one key, with no allocations.

**Sharding helped less than the headline suggests.** At 10 cores the sharded
map is 4.2x faster than a single mutex. But that gap comes mostly from the
single mutex getting worse as cores are added (47 ns per call at one core, 172
ns at ten), not from sharding getting faster: with 32 shards, ten cores do
only about 1.2x the work of one. Sharding prevents a collapse; it does not
deliver linear scaling. The cause was not investigated.

**Through Redis**, one check takes about 103 µs for a single caller and the
instance tops out near 29,800 checks per second, with no difference between
one hot key and a thousand keys. That is roughly 2,400 times the in-memory
cost, and almost all of it is the round trip.

**Through the service**, a single caller sees p50 141 µs and p99 207 µs, and
ten parallel callers get about 20,900 requests per second.

**Under load**, with 100 keys and a combined limit of 1000 requests per second:

| Offered load | Critical | Default | Sheddable | Limit exceeded? |
|---|---|---|---|---|
| 0.5x | 100% | 100% | 100% | no |
| 1x | 100% | 99.7% | 87.7% | no |
| 3x | 100% | 40.4% | 1.6% | no |

The limit was never exceeded and shedding followed priority exactly as
intended: at three times the limit, every critical request got through.

Two results were not what I expected. At exactly the limit, 12.3% of sheddable
requests were already being refused and 3.6% of the allowance went unused,
because random arrivals push individual buckets below the sheddable threshold
for short periods. That is the reserve working as configured, and it is a real
cost of the design. And latency went *down* as load went up (p50 0.99 ms at
500 requests/s, 0.20 ms at 3000), which is most likely an artifact of an idle
local setup and not a property of the limiter.

## What I would do differently

- **Measure before explaining.** The first version of the concurrency section
  claimed "about 4x from sharding" from a single run at one core count. The
  sweep across core counts told a different and more useful story. I would run
  the sweep first.
- **Find out why sharding does not scale.** The benchmark has every goroutine
  walk the same keys in the same order, which may be measuring cache-line
  traffic more than lock contention. A benchmark with per-goroutine key sets,
  and a profile, would settle it.
- **Test the service to saturation.** The load test overloads the *limit*,
  not the *service*: 3000 requests/s is far below what the service can handle.
  The point where the service itself falls over is still unknown.
- **Benchmark on a realistic network.** Redis in a container on a laptop adds
  a virtual-machine boundary to every call and hides real network latency. The
  numbers that matter are the ones from separate hosts.
- **Build the client-side token cache sooner.** One round trip per request is
  the main cost of the service design, and leasing tokens to clients is the
  known answer. It was left as future work; it probably deserved a roadmap
  step ahead of some of the polish.
- **Let interceptors carry cost and priority.** The gRPC interceptors always
  charge one token at default priority, so the two features that make the
  limiter application-aware are not reachable through the easiest way to
  adopt it.
- **Reconsider how reserves are expressed.** A reserve defined as a fraction
  of the burst is simple, but it sheds sheddable traffic at 1x load. A reserve
  tied to the refill rate, or one that only applies under sustained pressure,
  might waste less.
- **Evict idle keys in memory.** Redis buckets expire; the in-memory limiters
  keep every key forever. Fine for a demo, not for a long-running process with
  unbounded keys.
- **Add a circuit breaker.** Failing open after waiting 50 ms on every request
  is still a latency incident.

## Where it stands

The code, the benchmarks and the raw results are in the repository. The parts
that are designed but not built (client-side caching, Redis Cluster support,
regional limits) are described in the README as future work.
