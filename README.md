# Dependency Cache Layer for Upstream Data Sources

A cache access layer that sits in front of an upstream data source, absorbs
upstream latency/jitter, and keeps cached results consistent with the real
upstream data. The upstream is a local, injectable mock — the whole chain
runs without any external service and is fully reproducible locally.

## Why

- Every read used to hit the upstream directly; upstream slowdowns were
  amplified into request latency, and repeated reads added extra pressure.
- The cache layer returns fresh data from memory, serves stale data while
  revalidating in the background, and only waits when data is expired.
- Derived results depend on multiple upstream entities; when one entity
  changes, only the cache entries that truly depend on it are invalidated.
- Failures (timeout, upstream error, malformed payload) are never cached as
  valid data and are reported with distinguishable reasons.

## Freshness semantics

Every cached entry has a write timestamp. Its age is compared against three
configurable thresholds. Boundary behavior is deterministic (`age` is the
time since the last successful write):

| Condition | Freshness | Behavior |
| --- | --- | --- |
| `age <= fresh_ttl` | `fresh` | Return the cached result directly. |
| `fresh_ttl < age <= fresh_ttl + stale_ttl` | `stale` | Return the previous complete result **immediately** and trigger one background refresh (stale-while-revalidate). |
| `age > fresh_ttl + stale_ttl` | `expired` | **Wait** for a synchronous refresh; on success return the new result. |

Boundary values are inclusive: at `age == fresh_ttl` the entry is still
fresh; at `age == fresh_ttl + stale_ttl` it is still stale.

A read result carries one of these statuses:

- `hit` — fresh value served from cache.
- `stale` — stale value served, background refresh in flight.
- `degraded` — previous complete value served, but the refresh failed (the
  failure reason is attached). The failure is **not** cached as data.
- `miss` — no usable value (first load failed, or a not-found negative entry
  expired and its refresh failed).
- `negative` — a cached upstream not-found within its negative TTL.

## Stale fallback & background refresh

- In the stale window the caller never blocks on the upstream: the previous
  complete result is returned and a single background refresh is started
  (deduplicated per key; concurrent reads share it).
- Beyond the stale window the caller waits for the refresh. If the refresh
  fails, the previous complete result is kept and returned with a `degraded`
  marker and a distinguishable `failure_reason` (`timeout`, `upstream_error`,
  `malformed`). A failed refresh never overwrites a good entry.
- When a background refresh succeeds, the entry is replaced atomically with a
  new version. Reads never observe a half-written result: each entry is a
  fully constructed object swapped in under a per-key lock.

## Missing vs fault

- Upstream **not-found** is cached as a negative entry with a shorter TTL
  (`negative_ttl`). Reads within that TTL return `negative` without touching
  the upstream.
- Upstream **faults** (timeout, error, malformed payload) are **never**
  cached. They are returned as `miss`/`degraded` with the exact reason, and
  the next read retries the upstream.

## Dependency-based precise invalidation

A loader returns the set of upstream entities its derived result depends on.
`invalidate(entity)` removes only the entries whose dependency set contains
that entity; unrelated entries are untouched. `invalidate_key(key)` removes
a single entry directly.

## Bounded capacity & eviction

- Capacity is configurable and hard-capped (`1..100_000`) to prevent
  misconfiguration from causing unbounded memory growth.
- When capacity is exceeded, the least-recently-used entry is evicted
  (LRU). Entries with a refresh in flight are never chosen as victims.
- Evicted entries are reloaded from the upstream on the next read.

## Observability

`GET /admin/metrics` (or `cache.metrics()`) exposes counters:

- `hits`, `misses`, `negative_hits`
- `stale_returns`, `degraded_returns`
- `refresh_success`, `refresh_failures`
- `background_refresh_started`
- `evictions`, `invalidations`
- `shutdown_cancelled`

## Graceful shutdown

On shutdown, in-flight background refreshes are cancelled and awaited up to
`shutdown_timeout`. Cancelled refreshes never write entries (the swap happens
only after a loader completes), so no half-written entry is left behind and
shutdown never blocks past the timeout. The cache refuses new reads after
shutdown.

## Configuration

All thresholds are configurable (environment variables for the demo app):

| Env var | Default | Meaning |
| --- | --- | --- |
| `CACHE_FRESH_TTL` | `10` | Seconds an entry stays fresh. |
| `CACHE_STALE_TTL` | `60` | Seconds after freshness during which stale results are served while revalidating. Set to `0` to disable the stale window. |
| `CACHE_NEGATIVE_TTL` | `5` | Seconds a not-found result is cached. |
| `CACHE_CAPACITY` | `1000` | Max number of cached entries (hard cap `100_000`). |
| `CACHE_MAX_REFRESH_CONCURRENCY` | `4` | Max concurrent background refreshes. |

Programmatically, pass a `CacheConfig` to `CacheLayer(config, clock=...)`.
A `Clock` protocol is injected so tests can advance time deterministically
with `FakeClock`.

## HTTP API

- `GET /` — service info.
- `GET /users/{user_id}/summary` — cached derived result (depends on
  `user:{id}` and `profile:{id}`).
- `PUT /admin/users/{user_id}` — set the user entity and invalidate its
  dependents.
- `PUT /admin/profiles/{user_id}` — set the profile entity and invalidate.
- `DELETE /admin/users/{user_id}` — delete the user entity and invalidate.
- `GET /admin/metrics` — cache counters and entry count.

## Upstream mock

`MockUpstream` is an in-memory upstream with injectable behavior:

- `set(key, value)` / `delete(key)` — mutate the data.
- `configure(key, fail_times=..., reason=..., delay=..., malformed=...)` —
  inject failures, latency or malformed payloads.
- `calls` — log of every fetch for assertions.

## Tests

```bash
python -m pytest tests/ -q
```

The test suite covers: threshold boundaries, stale-while-revalidate,
background and synchronous refresh failure with degradation, not-found vs
fault distinction (timeout/error/malformed), precise dependency
invalidation, concurrent reads sharing a single refresh without mixed
versions, background refresh concurrency limit, LRU eviction and reload,
metrics, and graceful shutdown (no half-written entries, no blocking).
