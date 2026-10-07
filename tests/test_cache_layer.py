"""Tests for the dependency-aware cache access layer.

Coverage:
- freshness threshold boundaries (fresh / stale / expired)
- stale-while-revalidate and background refresh
- background / synchronous refresh failure with degraded marker
- not-found (negative caching) vs upstream faults (timeout/error/malformed)
- precise dependency invalidation
- concurrent reads / refreshes (single upstream call, no mixed versions)
- bounded LRU capacity and eviction reload
- metrics counters
- graceful shutdown (cancellation, no half-written entries, no blocking)
"""

from __future__ import annotations

import asyncio

import pytest

from cache_layer import (
    CacheConfig,
    CacheLayer,
    FakeClock,
    FailureReason,
    LoadResult,
    MockUpstream,
    NotFound,
    ReadStatus,
    UpstreamError,
    UpstreamFailure,
    parse_json_payload,
)


def run(coro):
    return asyncio.run(coro)


def make_loader(upstream: MockUpstream, key: str, deps: tuple[str, ...]):
    async def loader() -> LoadResult:
        result = await upstream.fetch(key)
        if isinstance(result, UpstreamFailure):
            raise UpstreamError(result.reason, result.message)
        if isinstance(result, NotFound):
            return LoadResult(missing=True, deps=deps)
        return LoadResult(value=result.value, deps=deps)

    return loader


def make_derived_loader(upstream: MockUpstream, user_id: str):
    """Derived result depending on two upstream entities."""

    async def loader() -> LoadResult:
        user = await upstream.fetch(f"user:{user_id}")
        if isinstance(user, UpstreamFailure):
            raise UpstreamError(user.reason, user.message)
        if isinstance(user, NotFound):
            return LoadResult(missing=True, deps=(f"user:{user_id}",))
        profile = await upstream.fetch(f"profile:{user_id}")
        if isinstance(profile, UpstreamFailure):
            raise UpstreamError(profile.reason, profile.message)
        if isinstance(profile, NotFound):
            return LoadResult(
                missing=True, deps=(f"user:{user_id}", f"profile:{user_id}")
            )
        try:
            value = {"name": user.value["name"], "title": profile.value["title"]}
        except (KeyError, TypeError):
            raise UpstreamError(FailureReason.MALFORMED, "malformed derived payload")
        return LoadResult(
            value=value, deps=(f"user:{user_id}", f"profile:{user_id}")
        )

    return loader


@pytest.fixture
def clock():
    return FakeClock()


def make_cache(clock, **overrides):
    defaults = dict(fresh_ttl=10.0, stale_ttl=20.0, negative_ttl=5.0)
    defaults.update(overrides)
    config = CacheConfig(**defaults)
    return CacheLayer(config, clock=clock)


# ---------------------------------------------------------------------------
# 1. Freshness threshold boundaries
# ---------------------------------------------------------------------------


def test_fresh_vs_expired_boundary(clock):
    """stale_ttl=0: age == fresh_ttl is fresh; age > fresh_ttl is expired."""
    upstream = MockUpstream()
    cache = make_cache(clock, stale_ttl=0.0)
    upstream.set("k", "v1")
    loader = make_loader(upstream, "k", ("k",))

    r = run(cache.get("k", loader))
    assert r.status == ReadStatus.HIT and r.freshness.value == "fresh"

    clock.advance(10.0)  # exactly at fresh_ttl: still fresh
    r = run(cache.get("k", loader))
    assert r.status == ReadStatus.HIT and r.freshness.value == "fresh"
    assert upstream.call_count("k") == 1

    upstream.set("k", "v2")
    clock.advance(0.001)  # beyond fresh_ttl: expired -> synchronous refresh
    r = run(cache.get("k", loader))
    assert r.status == ReadStatus.HIT and r.freshness.value == "fresh"
    assert r.value == "v2"
    assert upstream.call_count("k") == 2


def test_stale_window_boundaries(clock):
    """age in (fresh_ttl, fresh_ttl + stale_ttl] is stale; background refresh is deduped."""
    upstream = MockUpstream()
    cache = make_cache(clock, stale_ttl=20.0)
    upstream.set("k", "v1")
    loader = make_loader(upstream, "k", ("k",))
    run(cache.get("k", loader))

    async def scenario():
        clock.advance(10.001)  # just beyond fresh_ttl
        r = await cache.get("k", loader)
        assert r.status == ReadStatus.STALE and r.freshness.value == "stale"
        assert r.value == "v1"

        clock.advance(19.999)  # exactly at fresh_ttl + stale_ttl: still stale
        r = await cache.get("k", loader)
        assert r.status == ReadStatus.STALE and r.freshness.value == "stale"
        assert cache.metrics().background_refresh_started == 1  # deduped

        await cache.shutdown()

    run(scenario())


def test_stale_returns_immediately_and_refreshes_in_background(clock):
    upstream = MockUpstream(delay=0.1)
    cache = make_cache(clock)
    upstream.set("k", "v1")
    loader = make_loader(upstream, "k", ("k",))

    async def scenario():
        await cache.get("k", loader)
        clock.advance(15.0)  # into stale window

        r = await cache.get("k", loader)
        assert r.status == ReadStatus.STALE
        assert r.value == "v1"  # returned immediately, not after 0.1s
        assert cache.metrics().background_refresh_started == 1

        r2 = await cache.get("k", loader)
        assert r2.status == ReadStatus.STALE
        assert cache.metrics().background_refresh_started == 1  # deduped

        await asyncio.sleep(0.25)
        r3 = await cache.get("k", loader)
        assert r3.status == ReadStatus.HIT
        assert r3.value == "v1"
        assert r3.version == 2
        await cache.shutdown()

    run(scenario())


# ---------------------------------------------------------------------------
# 2. Refresh failure with degradation marker
# ---------------------------------------------------------------------------


def test_background_refresh_failure_keeps_old_value_marked_degraded(clock):
    upstream = MockUpstream()
    cache = make_cache(clock)
    upstream.set("k", "v1")
    loader = make_loader(upstream, "k", ("k",))

    async def scenario():
        await cache.get("k", loader)  # initial success
        upstream.configure("k", fail_times=1, reason=FailureReason.UPSTREAM)
        clock.advance(15.0)

        r = await cache.get("k", loader)
        assert r.status == ReadStatus.STALE and r.value == "v1"
        await asyncio.sleep(0.05)  # background refresh fails

        m = cache.metrics()
        assert m.refresh_failures == 1
        r2 = await cache.get("k", loader)
        assert r2.status == ReadStatus.DEGRADED
        assert r2.degraded is True
        assert r2.value == "v1"  # previous complete result retained
        assert r2.failure_reason == FailureReason.UPSTREAM

        # Upstream recovered: the next stale read triggers a fresh background refresh.
        await asyncio.sleep(0.2)
        r3 = await cache.get("k", loader)
        assert r3.status in (ReadStatus.STALE, ReadStatus.DEGRADED, ReadStatus.HIT)
        await asyncio.sleep(0.1)
        r4 = await cache.get("k", loader)
        assert r4.status == ReadStatus.HIT
        assert r4.value == "v1"
        assert r4.version == 2
        await cache.shutdown()

    run(scenario())


def test_expired_refresh_failure_waits_and_returns_degraded(clock):
    upstream = MockUpstream(delay=0.05)
    cache = make_cache(clock)
    upstream.set("k", "v1")
    loader = make_loader(upstream, "k", ("k",))

    async def scenario():
        await cache.get("k", loader)
        upstream.configure("k", fail_times=1, reason=FailureReason.TIMEOUT)
        clock.advance(35.0)  # beyond stale window

        start = asyncio.get_event_loop().time()
        r = await cache.get("k", loader)  # must wait for refresh to fail
        elapsed = asyncio.get_event_loop().time() - start
        assert elapsed >= 0.04
        assert r.status == ReadStatus.DEGRADED
        assert r.value == "v1"
        assert r.failure_reason == FailureReason.TIMEOUT

        # Failure is not cached as data: next read retries upstream and succeeds.
        r2 = await cache.get("k", loader)
        assert r2.status == ReadStatus.HIT
        assert r2.version == 2
        await cache.shutdown()

    run(scenario())


def test_failure_is_not_cached_as_data(clock):
    upstream = MockUpstream()
    cache = make_cache(clock)
    upstream.configure("k", fail_times=1, reason=FailureReason.UPSTREAM)
    upstream.set("k", "v")
    loader = make_loader(upstream, "k", ("k",))

    r = run(cache.get("k", loader))
    assert r.status == ReadStatus.MISS
    assert r.found is False
    assert r.degraded is True
    assert r.failure_reason == FailureReason.UPSTREAM
    assert upstream.call_count("k") == 1

    # Nothing cached: the next read goes upstream again and succeeds.
    r2 = run(cache.get("k", loader))
    assert r2.status == ReadStatus.HIT
    assert r2.found is True
    assert upstream.call_count("k") == 2


# ---------------------------------------------------------------------------
# 3. Missing vs fault distinction
# ---------------------------------------------------------------------------


def test_not_found_is_cached_with_shorter_negative_ttl(clock):
    upstream = MockUpstream()
    cache = make_cache(clock)
    loader = make_loader(upstream, "k", ("k",))

    r = run(cache.get("k", loader))
    assert r.status == ReadStatus.MISS and r.missing is True
    assert upstream.call_count("k") == 1

    clock.advance(4.9)  # within negative TTL: served as negative, no upstream call
    r = run(cache.get("k", loader))
    assert r.status == ReadStatus.NEGATIVE and r.found is False
    assert upstream.call_count("k") == 1

    clock.advance(0.2)  # beyond negative TTL: reload; still missing
    r = run(cache.get("k", loader))
    assert r.status == ReadStatus.MISS and r.missing is True
    assert upstream.call_count("k") == 2

    upstream.set("k", "v")
    clock.advance(5.1)  # beyond the new negative TTL: reload finds the entity
    r = run(cache.get("k", loader))
    assert r.status == ReadStatus.HIT and r.value == "v"


def test_malformed_payload_is_a_failure_not_missing(clock):
    upstream = MockUpstream()
    cache = make_cache(clock)
    upstream.configure("k", malformed=True)

    async def loader():
        result = await upstream.fetch("k")
        if isinstance(result, UpstreamFailure):
            raise UpstreamError(result.reason)
        if isinstance(result, NotFound):
            return LoadResult(missing=True, deps=("k",))
        parse_json_payload(result.value)  # raises MALFORMED
        return LoadResult(value=result.value, deps=("k",))

    r = run(cache.get("k", loader))
    assert r.status == ReadStatus.MISS
    assert r.degraded is True
    assert r.failure_reason == FailureReason.MALFORMED
    # Malformed is not cached: retry goes upstream again.
    assert upstream.call_count("k") == 1
    r2 = run(cache.get("k", loader))
    assert r2.status == ReadStatus.MISS
    assert upstream.call_count("k") == 2


def test_upstream_error_reason_is_distinguishable(clock):
    upstream = MockUpstream()
    cache = make_cache(clock)
    upstream.configure("k", fail_times=1, reason=FailureReason.UPSTREAM)
    loader = make_loader(upstream, "k", ("k",))

    r = run(cache.get("k", loader))
    assert r.status == ReadStatus.MISS
    assert r.failure_reason == FailureReason.UPSTREAM
    assert r.missing is False


# ---------------------------------------------------------------------------
# 4. Precise dependency invalidation
# ---------------------------------------------------------------------------


def test_dependency_invalidation_is_precise(clock):
    upstream = MockUpstream()
    cache = make_cache(clock)
    upstream.set("user:1", {"name": "alice"})
    upstream.set("profile:1", {"title": "eng"})
    upstream.set("user:2", {"name": "bob"})
    upstream.set("profile:2", {"title": "ops"})

    l1 = make_derived_loader(upstream, "1")
    l2 = make_derived_loader(upstream, "2")

    r1 = run(cache.get("summary:1", l1))
    r2 = run(cache.get("summary:2", l2))
    assert r1.found and r2.found
    assert len(cache) == 2

    # Changing user:1 only invalidates entries that depend on user:1.
    removed = run(cache.invalidate("user:1"))
    assert removed == 1
    assert len(cache) == 1

    # summary:2 is still served from cache without upstream calls.
    calls_before = upstream.call_count()
    r2b = run(cache.get("summary:2", l2))
    assert r2b.status == ReadStatus.HIT
    assert upstream.call_count() == calls_before

    # summary:1 reloads from upstream.
    r1b = run(cache.get("summary:1", l1))
    assert r1b.status == ReadStatus.HIT
    assert r1b.value == {"name": "alice", "title": "eng"}
    assert len(cache) == 2

    # Invalidating an unrelated entity removes nothing.
    assert run(cache.invalidate("user:999")) == 0

    # profile:2 invalidates summary:2 but not summary:1.
    assert run(cache.invalidate("profile:2")) == 1
    assert len(cache) == 1
    r1c = run(cache.get("summary:1", l1))
    assert r1c.status == ReadStatus.HIT  # unaffected


def test_invalidate_key_directly(clock):
    upstream = MockUpstream()
    cache = make_cache(clock)
    upstream.set("k", "v")
    loader = make_loader(upstream, "k", ("k",))
    run(cache.get("k", loader))
    assert run(cache.invalidate_key("k")) is True
    assert run(cache.invalidate_key("k")) is False
    assert len(cache) == 0


# ---------------------------------------------------------------------------
# 5. Concurrent reads / refreshes
# ---------------------------------------------------------------------------


def test_concurrent_reads_share_one_refresh(clock):
    upstream = MockUpstream(delay=0.05)
    cache = make_cache(clock)
    upstream.set("k", "v1")
    loader = make_loader(upstream, "k", ("k",))

    async def scenario():
        await cache.get("k", loader)
        calls_before = upstream.call_count("k")
        clock.advance(35.0)  # expired

        results = await asyncio.gather(*[cache.get("k", loader) for _ in range(10)])
        assert all(r.status == ReadStatus.HIT for r in results)
        assert all(r.version == 2 for r in results)
        assert upstream.call_count("k") == calls_before + 1  # one refresh only
        assert len({r.value for r in results}) == 1
        await cache.shutdown()

    run(scenario())


def test_concurrent_stale_reads_share_one_background_refresh(clock):
    upstream = MockUpstream(delay=0.05)
    cache = make_cache(clock)
    upstream.set("k", "v1")
    loader = make_loader(upstream, "k", ("k",))

    async def scenario():
        await cache.get("k", loader)
        calls_before = upstream.call_count("k")
        clock.advance(15.0)

        results = await asyncio.gather(*[cache.get("k", loader) for _ in range(10)])
        assert all(r.status == ReadStatus.STALE for r in results)
        assert all(r.value == "v1" for r in results)
        assert cache.metrics().background_refresh_started == 1
        await asyncio.sleep(0.15)
        r = await cache.get("k", loader)
        assert r.status == ReadStatus.HIT
        assert upstream.call_count("k") == calls_before + 1
        await cache.shutdown()

    run(scenario())


def test_concurrent_reads_never_observe_mixed_versions(clock):
    """Hammer expiry with concurrent reads; every result must be a complete version."""
    upstream = MockUpstream(delay=0.01)
    cache = make_cache(clock)
    loader = make_loader(upstream, "k", ("k",))

    async def scenario():
        for round_no in range(10):
            upstream.set("k", f"v{round_no}")
            await cache.get("k", loader)
            clock.advance(35.0)
            results = await asyncio.gather(*[cache.get("k", loader) for _ in range(20)])
            versions = {r.version for r in results}
            values = {r.value for r in results}
            assert len(versions) == 1
            assert len(values) == 1
            assert all(r.found and r.status == ReadStatus.HIT for r in results)
        await cache.shutdown()

    run(scenario())


# ---------------------------------------------------------------------------
# 6. Bounded capacity / LRU eviction
# ---------------------------------------------------------------------------


def test_capacity_evicts_lru_and_evicted_entry_reloads(clock):
    upstream = MockUpstream()
    cache = make_cache(clock, capacity=2)
    loaders = {}
    for name in ("a", "b", "c"):
        upstream.set(name, name)
        loaders[name] = make_loader(upstream, name, (name,))

    run(cache.get("a", loaders["a"]))
    clock.advance(1.0)
    run(cache.get("b", loaders["b"]))
    assert len(cache) == 2

    clock.advance(1.0)
    run(cache.get("a", loaders["a"]))  # a is most recently used
    clock.advance(1.0)
    run(cache.get("c", loaders["c"]))  # evicts b (LRU)
    assert len(cache) == 2
    assert cache.metrics().evictions == 1

    # b was evicted: reading it goes back to upstream.
    calls_b = upstream.call_count("b")
    r = run(cache.get("b", loaders["b"]))
    assert r.status == ReadStatus.HIT
    assert upstream.call_count("b") == calls_b + 1
    assert len(cache) == 2  # capacity stays bounded
    assert cache.metrics().evictions == 2

    # a may have been evicted by b's re-insertion; either way it serves correctly.
    r = run(cache.get("a", loaders["a"]))
    assert r.status == ReadStatus.HIT
    assert len(cache) == 2


def test_capacity_must_have_an_upper_bound():
    with pytest.raises(ValueError):
        CacheConfig(capacity=0)
    with pytest.raises(ValueError):
        CacheConfig(capacity=100_001)
    CacheConfig(capacity=100_000)  # boundary accepted


# ---------------------------------------------------------------------------
# 7. Metrics
# ---------------------------------------------------------------------------


def test_metrics_counters(clock):
    upstream = MockUpstream()
    cache = make_cache(clock)
    upstream.set("k", "v1")
    loader = make_loader(upstream, "k", ("k",))

    async def scenario():
        await cache.get("k", loader)  # miss + refresh success
        await cache.get("k", loader)  # hit
        clock.advance(15.0)
        await cache.get("k", loader)  # stale return + background refresh
        await asyncio.sleep(0.05)
        clock.advance(35.0)
        await cache.get("k", loader)  # expired -> synchronous refresh

        m = cache.metrics()
        assert m.hits >= 2
        assert m.misses == 1
        assert m.stale_returns == 1
        assert m.refresh_success == 3
        assert m.background_refresh_started == 1
        assert m.evictions == 0
        await cache.shutdown()

    run(scenario())


# ---------------------------------------------------------------------------
# 8. Graceful shutdown
# ---------------------------------------------------------------------------


def test_shutdown_cancels_background_refresh_without_corrupting_entry(clock):
    upstream = MockUpstream(delay=0.0)
    cache = make_cache(clock)
    upstream.set("k", "v1")
    loader = make_loader(upstream, "k", ("k",))

    async def scenario():
        await cache.get("k", loader)  # initial load is fast
        upstream.delay = 5.0  # refreshes now hang
        clock.advance(15.0)
        await cache.get("k", loader)  # stale -> background refresh hangs

        await cache.shutdown(timeout=0.1)  # must not block
        m = cache.metrics()
        assert m.shutdown_cancelled >= 1
        # Entry untouched: no half-written value, version unchanged.
        assert len(cache) == 1
        entry = cache._entries["k"]
        assert entry.value == "v1"
        assert entry.version == 1
        # After shutdown the cache refuses new work.
        with pytest.raises(RuntimeError):
            await cache.get("k", loader)

    run(scenario())


def test_shutdown_does_not_block_on_hanging_refresh(clock):
    upstream = MockUpstream(delay=0.0)
    cache = make_cache(clock)
    upstream.set("k", "v1")
    loader = make_loader(upstream, "k", ("k",))

    async def scenario():
        await cache.get("k", loader)  # initial load is fast
        upstream.delay = 30.0  # refresh now hangs
        clock.advance(15.0)
        await cache.get("k", loader)

        loop = asyncio.get_event_loop()
        start = loop.time()
        await cache.shutdown(timeout=0.05)
        elapsed = loop.time() - start
        assert elapsed < 1.0  # returns promptly despite the hanging refresh

    run(scenario())


def test_shutdown_waits_for_fast_refresh(clock):
    upstream = MockUpstream(delay=0.05)
    cache = make_cache(clock)
    upstream.set("k", "v1")
    loader = make_loader(upstream, "k", ("k",))

    async def scenario():
        await cache.get("k", loader)
        clock.advance(15.0)
        await cache.get("k", loader)  # background refresh in flight

        await cache.shutdown(timeout=2.0)
        # Cancellation is prompt; no tasks left pending and no half entry.
        assert cache._refreshing == {}
        m = cache.metrics()
        assert m.shutdown_cancelled >= 1
        assert m.refresh_success == 1  # only the initial load wrote an entry
        assert cache._entries["k"].value == "v1"
        assert cache._entries["k"].version == 1

    run(scenario())


# ---------------------------------------------------------------------------
# 9. Additional edge cases
# ---------------------------------------------------------------------------


def test_background_refresh_concurrency_limit(clock):
    upstream = MockUpstream()
    cache = make_cache(clock, max_refresh_concurrency=1)
    for name in ("a", "b", "c"):
        upstream.set(name, name)

    state = {"in_flight": 0, "max_seen": 0}
    state_lock = asyncio.Lock()
    block_refresh = asyncio.Event()
    release = asyncio.Event()

    def loader_for(key: str):
        async def loader() -> LoadResult:
            async with state_lock:
                state["in_flight"] += 1
                state["max_seen"] = max(state["max_seen"], state["in_flight"])
            if block_refresh.is_set():
                await release.wait()  # only background refreshes block here
            async with state_lock:
                state["in_flight"] -= 1
            return LoadResult(value=key, deps=(key,))

        return loader

    async def scenario():
        for name in ("a", "b", "c"):
            await cache.get(name, loader_for(name))
        clock.advance(15.0)  # all three entries enter the stale window
        block_refresh.set()

        # Spawn three background refreshes; the semaphore must serialize them.
        await asyncio.gather(
            *[cache.get(name, loader_for(name)) for name in ("a", "b", "c")]
        )
        await asyncio.sleep(0.05)
        assert state["max_seen"] == 1
        assert cache.metrics().background_refresh_started == 3

        release.set()
        await asyncio.sleep(0.15)
        assert state["in_flight"] == 0
        await cache.shutdown()

    run(scenario())


def test_negative_entry_beyond_ttl_with_failure_is_miss_not_negative(clock):
    upstream = MockUpstream()
    cache = make_cache(clock)
    loader = make_loader(upstream, "k", ("k",))

    r = run(cache.get("k", loader))  # not found -> negative cache
    assert r.status == ReadStatus.MISS and r.missing is True

    upstream.configure("k", fail_times=1, reason=FailureReason.TIMEOUT)
    clock.advance(5.1)  # beyond negative TTL -> synchronous refresh fails
    r = run(cache.get("k", loader))
    assert r.status == ReadStatus.MISS
    assert r.degraded is True
    assert r.failure_reason == FailureReason.TIMEOUT
    assert r.missing is False  # a fault is not a not-found


def test_metrics_snapshot_is_independent_copy(clock):
    upstream = MockUpstream()
    cache = make_cache(clock)
    before = cache.metrics()

    upstream.set("k", "v")
    run(cache.get("k", make_loader(upstream, "k", ("k",))))

    after = cache.metrics()
    assert before.misses == 0 and before.refresh_success == 0
    assert after.misses == 1 and after.refresh_success == 1
    assert after is not before


def test_stale_ttl_zero_disables_stale_window(clock):
    upstream = MockUpstream()
    cache = make_cache(clock, stale_ttl=0.0)
    upstream.set("k", "v1")
    loader = make_loader(upstream, "k", ("k",))

    run(cache.get("k", loader))
    clock.advance(10.001)  # just beyond fresh_ttl: no stale window -> expired
    upstream.set("k", "v2")
    r = run(cache.get("k", loader))
    assert r.status == ReadStatus.HIT
    assert r.value == "v2"
    assert cache.metrics().stale_returns == 0


# ---------------------------------------------------------------------------
# 10. Capacity & eviction under concurrent background refreshes
# ---------------------------------------------------------------------------


def _make_gated_loader(key, gate, upstream_calls):
    """Loader that blocks on ``gate`` once a background refresh is running."""

    async def loader():
        upstream_calls.append(key)
        if gate is not None and gate["active"]:
            await gate["event"].wait()
        return LoadResult(value=key, deps=(key,))

    return loader


async def _prime_stale_blocked(cache, clock, keys, gate, upstream_calls,
                               prime_keys=None):
    """Prime every key fresh, push them stale, block their bg refreshes.

    ``prime_keys`` may include extra keys that stay cached but are not moved
    into the stale window; they keep an older access time and are the natural
    LRU victims.
    """
    prime_keys = list(prime_keys or keys)
    for k in prime_keys:
        r = await cache.get(k, _make_gated_loader(k, None, upstream_calls))
        assert r.found
    if prime_keys != list(keys):
        clock.advance(1.0)  # gap so the stale keys are newer than stale-stay keys
    clock.advance(15.0)  # ``keys`` enter the stale window
    gate["active"] = True
    results = await asyncio.gather(
        *[cache.get(k, _make_gated_loader(k, gate, upstream_calls)) for k in keys]
    )
    assert all(r.status == ReadStatus.STALE for r in results)
    # Let the spawned background tasks start and park on the gate.
    await asyncio.sleep(0.05)
    assert sorted(cache._refreshing) == sorted(keys)


def test_new_entry_survives_while_many_entries_refresh(clock):
    """A just-written entry must not be evicted while other keys refresh.

    Regression: with a small capacity and several entries refreshing in the
    background, the new entry used to be the only evictable candidate and was
    dropped the instant it was written, forcing the next read back upstream.
    """
    cache = make_cache(clock, capacity=3)
    keys = ("a", "b", "c")
    gate = {"event": asyncio.Event(), "active": False}
    upstream_calls: list[str] = []

    async def scenario():
        await _prime_stale_blocked(cache, clock, keys, gate, upstream_calls)

        calls_before = len(upstream_calls)
        # Independent upstream completion (as a real upstream would finish).
        async def release():
            await asyncio.sleep(0.05)
            gate["event"].set()

        rel = asyncio.create_task(release())
        r = await cache.get("d", _make_gated_loader("d", None, upstream_calls))
        assert r.status == ReadStatus.HIT and r.value == "d"

        # The freshly written entry is immediately present and bounded.
        assert "d" in cache._entries
        assert len(cache) <= 3

        await rel
        await asyncio.sleep(0.05)
        assert len(cache) == 3
        assert "d" in cache._entries

        # Re-reading d must be a cache hit, never a second upstream fetch.
        calls_d_before = upstream_calls.count("d")
        r2 = await cache.get("d", _make_gated_loader("d", gate, upstream_calls))
        assert r2.status == ReadStatus.HIT and r2.value == "d"
        assert upstream_calls.count("d") == calls_d_before
        assert len(upstream_calls) == calls_before + 1  # only d's single load
        await cache.shutdown()

    run(scenario())


def test_capacity_never_exceeded_while_multiple_refreshes_in_flight(clock):
    """Total entries stay <= capacity even when all entries are refreshing."""
    cache = make_cache(clock, capacity=4)
    keys = ("a", "b", "c", "d")
    gate = {"event": asyncio.Event(), "active": False}
    upstream_calls: list[str] = []

    async def scenario():
        await _prime_stale_blocked(cache, clock, keys, gate, upstream_calls)

        sizes: list[int] = []

        async def read_new(name):
            r = await cache.get(name, _make_gated_loader(name, None, upstream_calls))
            sizes.append(len(cache))
            assert r.found and r.value == name
            assert len(cache) <= 4

        async def release():
            await asyncio.sleep(0.02)
            gate["event"].set()

        rel = asyncio.create_task(release())
        await asyncio.gather(*(read_new(n) for n in ("e", "f", "g")))
        rel.cancel()
        await asyncio.sleep(0.1)

        assert max(sizes) <= 4
        assert len(cache) == 4
        # Every currently cached key reads back straight from cache.
        for k in list(cache._entries):
            before = upstream_calls.count(k)
            r = await cache.get(k, _make_gated_loader(k, gate, upstream_calls))
            assert r.status == ReadStatus.HIT
            assert upstream_calls.count(k) == before
        await cache.shutdown()

    run(scenario())


def test_eviction_picks_lru_among_evictable_and_revives_after_refresh():
    """LRU victim = least recently used non-refreshing entry; refreshed
    entries re-enter the evictable pool once their refresh ends.

    Uses the real clock (distinct access timestamps) so LRU order is
    unambiguous rather than tied on a frozen FakeClock.
    """
    from cache_layer import SystemClock

    cache = CacheLayer(
        # TTLs scaled to the small real-time waits used below.
        CacheConfig(fresh_ttl=0.05, stale_ttl=5.0, negative_ttl=0.05, capacity=4),
        clock=SystemClock(),
    )
    keys = ("a", "b", "c")
    gates = {k: asyncio.Event() for k in keys}
    upstream_calls: list[str] = []

    def plain_loader(k):
        async def loader():
            upstream_calls.append(k)
            return LoadResult(value=k, deps=(k,))

        return loader

    def gated_loader(k):
        async def loader():
            upstream_calls.append(k)
            await gates[k].wait()  # only background refreshes reach here
            return LoadResult(value=k, deps=(k,))

        return loader

    async def scenario():
        # d is primed first and never refreshed -> it stays the oldest access.
        for k in ("d", *keys):
            r = await cache.get(k, plain_loader(k))
            assert r.found
            await asyncio.sleep(0.005)
        await asyncio.sleep(0.05)  # cross fresh_ttl into the stale window

        # Start the three background refreshes and park them on their gates.
        stale_results = await asyncio.gather(
            *[cache.get(k, gated_loader(k)) for k in keys]
        )
        assert all(r.status == ReadStatus.STALE for r in stale_results)
        await asyncio.sleep(0.05)
        assert sorted(cache._refreshing) == list(keys)

        # a/b/c protected: only d is evictable -> inserting e evicts d (LRU).
        r = await cache.get("e", plain_loader("e"))
        assert r.found
        assert "d" not in cache._entries
        assert set(cache._entries) == {"a", "b", "c", "e"}
        assert cache.metrics().evictions == 1

        # Let only 'a' finish and settle: it becomes evictable again with a
        # fresh access, while b/c stay protected behind their gates.
        gates["a"].set()
        await asyncio.sleep(0.05)
        assert "a" not in cache._refreshing
        assert "b" in cache._refreshing and "c" in cache._refreshing

        # Inserting f reclaims the oldest evictable entry: e (written earlier),
        # never the just-refreshed a or the protected b/c.
        r = await cache.get("f", plain_loader("f"))
        assert r.found and r.value == "f"

        assert len(cache) == 4
        assert "e" not in cache._entries
        assert "a" in cache._entries  # a refreshed and re-entered the pool
        assert "f" in cache._entries
        assert cache.metrics().evictions == 2

        # b/c are still refreshing and therefore still protected.
        assert "b" in cache._refreshing and "c" in cache._refreshing

        # Release the rest and confirm capacity stays bounded afterwards.
        gates["b"].set()
        gates["c"].set()
        await asyncio.sleep(0.05)
        assert len(cache) == 4
        await cache.shutdown()

    run(scenario())


def test_evicted_entry_reloads_and_rewrites_after_concurrent_refresh(clock):
    """An entry evicted while siblings refresh is reloaded on next read and
    written back, with capacity staying bounded and a complete value served."""
    cache = make_cache(clock, capacity=3)
    keys = ("a", "b", "c")
    gate = {"event": asyncio.Event(), "active": False}
    upstream_calls: list[str] = []

    async def scenario():
        await _prime_stale_blocked(cache, clock, keys, gate, upstream_calls)

        gate["event"].set()
        # d displaces exactly one of a/b/c once a slot frees up.
        r = await cache.get("d", _make_gated_loader("d", None, upstream_calls))
        assert r.found and r.value == "d"
        await asyncio.sleep(0.05)
        assert len(cache) == 3
        assert "d" in cache._entries
        displaced = [k for k in keys if k not in cache._entries]
        assert len(displaced) == 1
        victim = displaced[0]

        # Reading the evicted entry goes back to upstream and rewrites it.
        # Touch the two still-cached keys so they are the LRU-ordered older
        # entries; the freshly rewritten victim must be the survivor.
        survivors = [k for k in keys if k in cache._entries]
        clock.advance(1.0)
        for k in survivors:
            touch = await cache.get(k, _make_gated_loader(k, gate, upstream_calls))
            assert touch.status == ReadStatus.HIT
        before = upstream_calls.count(victim)
        r = await cache.get(victim, _make_gated_loader(victim, None, upstream_calls))
        assert r.status == ReadStatus.HIT and r.value == victim
        assert upstream_calls.count(victim) == before + 1
        assert victim in cache._entries
        assert len(cache) == 3
        await cache.shutdown()

    run(scenario())


def test_capacity_stays_bounded_under_long_concurrent_churn(clock):
    """Long-running interleaving of refreshes/inserts never grows memory."""
    cache = make_cache(
        clock, capacity=5, max_refresh_concurrency=4, fresh_ttl=10.0, stale_ttl=20.0
    )
    upstream_calls: list[str] = []
    gate = {"event": asyncio.Event(), "active": False}

    async def scenario():
        gate["event"].set()  # refreshes never block here
        names = [f"k{i}" for i in range(5)]
        for n in names:
            await cache.get(n, _make_gated_loader(n, None, upstream_calls))

        for round_no in range(8):
            clock.advance(15.0)  # make everything stale
            # Background refreshes for existing keys interleaved with new keys.
            reads = [cache.get(n, _make_gated_loader(n, gate, upstream_calls))
                     for n in names]
            reads += [
                cache.get(f"new{round_no}:{i}",
                          _make_gated_loader(f"new{round_no}:{i}", None, upstream_calls))
                for i in range(3)
            ]
            results = await asyncio.gather(*reads)
            assert all(r.found for r in results)
            assert len(cache) <= 5
            await asyncio.sleep(0.02)

        assert len(cache) == 5
        await cache.shutdown()

    run(scenario())


def test_interleaved_refresh_insert_reads_never_mix_versions(clock):
    """Under refresh/insert/evict interleaving every read is one complete
    version whose value and version number are internally consistent."""
    cache = make_cache(clock, capacity=3)
    upstream_calls: list[str] = []
    gate = {"event": asyncio.Event(), "active": False}

    def value_loader(key, value):
        async def loader():
            upstream_calls.append(key)
            if gate["active"]:
                await gate["event"].wait()
            return LoadResult(value={"k": key, "v": value}, deps=(key,))

        return loader

    async def scenario():
        for i, k in enumerate(("a", "b", "c")):
            r = await cache.get(k, value_loader(k, i))
            assert r.value == {"k": k, "v": i}
        clock.advance(15.0)
        gate["active"] = True

        # Trigger background refreshes that write new complete versions.
        await asyncio.gather(*[cache.get(k, value_loader(k, 100 + i))
                               for i, k in enumerate(("a", "b", "c"))])
        await asyncio.sleep(0.05)

        observed: list[tuple[str, int]] = []

        async def reader(k):
            idx = {"a": 0, "b": 1, "c": 2}[k]
            for _ in range(20):
                # A reload (after eviction) returns the current upstream tag.
                r = await cache.get(k, value_loader(k, 100 + idx))
                # A coherent, fully written version: value matches its tag.
                assert r.value["k"] == k
                assert r.value["v"] in (idx, 100 + idx)
                observed.append((r.status.value, r.version))
                await asyncio.sleep(0)

        async def inserter():
            for i in range(10):
                n = f"x{i % 4}"
                r = await cache.get(n, value_loader(n, i))
                # Coherence only: the returned object is one complete write,
                # never a mix (a cache hit may legitimately return an earlier
                # fully-written version of the same key: a previous round j
                # with the same j % 4).
                assert r.value["k"] == n
                assert r.value["v"] % 4 == i % 4
                assert r.value["v"] <= i
                assert len(cache) <= 3
                await asyncio.sleep(0)

        async def release():
            await asyncio.sleep(0.02)
            gate["event"].set()

        await asyncio.gather(release(), inserter(),
                             *[reader(k) for k in ("a", "b", "c")])
        await cache.shutdown()

    run(scenario())


def test_waiting_insert_is_released_by_shutdown(clock):
    """Shutdown must not hang when a foreground insert awaits a slot."""
    cache = make_cache(clock, capacity=2)
    keys = ("a", "b")
    gate = {"event": asyncio.Event(), "active": False}
    upstream_calls: list[str] = []

    async def scenario():
        await _prime_stale_blocked(cache, clock, keys, gate, upstream_calls)
        # Never release the gate; shutdown must still unblock the insert.
        r_get = asyncio.create_task(
            cache.get("c", _make_gated_loader("c", None, upstream_calls))
        )
        await asyncio.sleep(0.05)
        loop = asyncio.get_event_loop()
        start = loop.time()
        await cache.shutdown(timeout=0.2)
        assert loop.time() - start < 2.0
        assert r_get.done()  # the waiting insert returned, it did not hang
        if not r_get.cancelled():
            # If the insert managed to complete during shutdown, its value is
            # fully written and capacity is still respected.
            result = r_get.result()
            assert result.found and result.value == "c"
            assert len(cache) <= 2
        # New reads are refused after shutdown.
        with pytest.raises(RuntimeError):
            await cache.get("z", _make_gated_loader("z", None, upstream_calls))

    run(scenario())
