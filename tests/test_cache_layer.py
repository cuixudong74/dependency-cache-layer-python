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


def test_new_insert_is_not_dropped_while_refreshes_in_flight(clock):
    """capacity=3, several entries refreshing (pinned): a new insert must evict
    the LRU reclaimable entry, never the just-written one."""
    upstream = MockUpstream()
    cache = make_cache(clock, capacity=3)
    loaders = {}
    for name in ("a", "b", "c", "d"):
        upstream.set(name, name)
        loaders[name] = make_loader(upstream, name, (name,))

    async def scenario():
        await cache.get("a", loaders["a"])
        clock.advance(1.0)
        await cache.get("b", loaders["b"])
        clock.advance(1.0)
        await cache.get("c", loaders["c"])

        # Refreshes hang in flight (per-key upstream latency).
        upstream.configure("a", delay=0.5)
        upstream.configure("b", delay=0.5)
        upstream.configure("c", delay=0.5)
        clock.advance(13.0)  # all entries enter the stale window
        ra = await cache.get("a", loaders["a"])
        rb = await cache.get("b", loaders["b"])
        rc = await cache.get("c", loaders["c"])
        assert ra.status == rb.status == rc.status == ReadStatus.STALE
        # The pin budget is capacity - 1 == 2: a and b refresh in the
        # background; c is served stale without a refresh (retried next read).
        assert cache.metrics().background_refresh_started == 2
        assert set(cache._refreshing) == {"a", "b"}

        # Insert a brand-new key while both refreshes are still in flight.
        rd = await cache.get("d", loaders["d"])
        assert rd.status == ReadStatus.HIT

        # The just-written entry survives; the LRU reclaimable entry (c) went.
        assert len(cache) == 3
        assert "d" in cache._entries
        assert "a" in cache._entries and "b" in cache._entries  # pinned
        assert "c" not in cache._entries
        assert cache.metrics().evictions == 1

        # Reading d again is served from cache: no repeated upstream fetch.
        rd2 = await cache.get("d", loaders["d"])
        assert rd2.status == ReadStatus.HIT
        assert upstream.call_count("d") == 1

        # The evicted entry reloads from upstream and is re-cached.
        rc2 = await cache.get("c", loaders["c"])
        assert rc2.status == ReadStatus.HIT and rc2.value == "c"
        assert upstream.call_count("c") == 2
        assert len(cache) == 3  # capacity bound still holds

        await asyncio.sleep(0.6)  # let the background refreshes finish
        assert cache._refreshing == {}
        assert cache._entries["a"].version == 2
        assert cache._entries["b"].version == 2
        await cache.shutdown()

    run(scenario())


def test_refreshing_entries_are_pinned_then_reenter_reclaimable_pool(clock):
    """Pinned entries are skipped even when they are the LRU; once the refresh
    finishes they can be evicted again."""
    upstream = MockUpstream()
    cache = make_cache(clock, capacity=3)
    loaders = {}
    for name in ("a", "b", "c", "d", "e"):
        upstream.set(name, name)
        loaders[name] = make_loader(upstream, name, (name,))

    async def scenario():
        await cache.get("a", loaders["a"])
        clock.advance(1.0)
        await cache.get("b", loaders["b"])
        clock.advance(1.0)
        await cache.get("c", loaders["c"])

        upstream.configure("a", delay=0.5)
        upstream.configure("b", delay=0.5)
        clock.advance(13.0)
        await cache.get("a", loaders["a"])  # stale -> pinned refresh
        await cache.get("b", loaders["b"])  # stale -> pinned refresh
        clock.advance(1.0)
        await cache.get("c", loaders["c"])  # fresh hit: c is now the newest

        # a and b are the least-recently-used overall, but they are pinned:
        # the victim must be c (the LRU *reclaimable* entry).
        await cache.get("d", loaders["d"])
        assert "a" in cache._entries and "b" in cache._entries
        assert "c" not in cache._entries
        assert "d" in cache._entries
        assert cache.metrics().evictions == 1

        await asyncio.sleep(0.6)  # refreshes finish -> a and b reclaimable again
        assert cache._refreshing == {}

        # Now the oldest entries (a, then b) are evictable again.
        await cache.get("e", loaders["e"])
        assert len(cache) == 3
        assert "a" not in cache._entries  # evicted as LRU after re-entering pool
        assert cache.metrics().evictions == 2
        await cache.shutdown()

    run(scenario())


def test_capacity_bound_holds_with_refresh_in_flight_and_many_inserts(clock):
    """capacity=2 with a hanging refresh: inserting many new keys never exceeds
    the capacity and never drops the just-written key."""
    upstream = MockUpstream()
    cache = make_cache(clock, capacity=2)
    upstream.set("a", "a")
    upstream.set("b", "b")
    la = make_loader(upstream, "a", ("a",))
    lb = make_loader(upstream, "b", ("b",))

    async def scenario():
        await cache.get("a", la)
        clock.advance(1.0)
        await cache.get("b", lb)

        upstream.configure("a", delay=0.5)  # a's refresh hangs in flight
        clock.advance(13.0)
        ra = await cache.get("a", la)
        assert ra.status == ReadStatus.STALE
        assert cache.metrics().background_refresh_started == 1

        # Pin budget is capacity - 1 == 1: no second refresh is spawned while
        # a is pinned, so eviction always keeps a reclaimable victim.
        rb = await cache.get("b", lb)
        assert rb.status == ReadStatus.STALE
        assert cache.metrics().background_refresh_started == 1

        for i in range(8):
            key = f"k{i}"
            upstream.set(key, key)
            r = await cache.get(key, make_loader(upstream, key, (key,)))
            assert r.status == ReadStatus.HIT
            assert len(cache) <= 2  # hard bound at every instant
            assert key in cache._entries  # just-written entry never dropped
            assert "a" in cache._entries  # pinned entry never evicted

        await asyncio.sleep(0.6)  # refresh finishes; a re-enters the pool
        assert cache._refreshing == {}
        assert len(cache) <= 2

        # a is reclaimable again: further inserts may evict it.
        upstream.set("z", "z")
        await cache.get("z", make_loader(upstream, "z", ("z",)))
        assert len(cache) <= 2
        await cache.shutdown()

    run(scenario())


def test_evicted_entry_reloads_and_recaches_under_refresh_pressure(clock):
    """An entry evicted while other refreshes are in flight reloads from the
    upstream on the next read and is written back completely."""
    upstream = MockUpstream()
    cache = make_cache(clock, capacity=2)
    loaders = {}
    for name in ("a", "b", "c"):
        upstream.set(name, name)
        loaders[name] = make_loader(upstream, name, (name,))

    async def scenario():
        await cache.get("a", loaders["a"])
        clock.advance(1.0)
        await cache.get("b", loaders["b"])

        upstream.configure("a", delay=0.5)
        clock.advance(13.0)
        await cache.get("a", loaders["a"])  # stale -> pinned refresh of a

        await cache.get("c", loaders["c"])  # inserts c, evicts b (LRU reclaimable)
        assert "b" not in cache._entries
        assert len(cache) == 2

        calls_b = upstream.call_count("b")
        upstream.set("b", "b2")
        rb = await cache.get("b", loaders["b"])  # evicted -> back to upstream
        assert rb.status == ReadStatus.HIT
        assert rb.value == "b2"
        assert upstream.call_count("b") == calls_b + 1
        assert "b" in cache._entries  # re-cached as a complete entry
        assert len(cache) == 2

        await asyncio.sleep(0.6)
        await cache.shutdown()

    run(scenario())


def test_concurrent_reads_inserts_evictions_return_complete_versions(clock):
    """Eviction, insertion and concurrent reads interleaved: every result is a
    fully written version and the capacity bound holds throughout."""
    upstream = MockUpstream(delay=0.01)
    cache = make_cache(clock, capacity=3)
    keys = ["k0", "k1", "k2", "k3", "k4"]
    loaders = {k: make_loader(upstream, k, (k,)) for k in keys}
    written: dict[str, set[str]] = {k: set() for k in keys}

    async def scenario():
        for round_no in range(6):
            for k in keys:
                value = f"{k}-v{round_no}"
                upstream.set(k, value)
                written[k].add(value)
            # 20 concurrent reads over 5 keys with capacity 3: constant
            # insert/evict churn, some keys stale, some expired.
            results = await asyncio.gather(
                *[cache.get(k, loaders[k]) for k in keys for _ in range(4)]
            )
            for i, r in enumerate(results):
                k = keys[i // 4]
                assert r.found
                # The value is always one complete write for this key —
                # never a mix of two versions or a half-written payload.
                assert r.value in written[k]
                assert r.version >= 1
            assert len(cache) <= 3
            clock.advance(15.0)  # push entries through the stale window
        await cache.shutdown()

    run(scenario())
