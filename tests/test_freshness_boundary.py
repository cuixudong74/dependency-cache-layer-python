"""新鲜度阈值的确定性边界行为。"""

from __future__ import annotations

import time

import pytest

from cachelayer.models import CacheStatus, Loaded


def test_boundary_age_equals_ttl_is_fresh(cache, clock, make_loader):
    loader = make_loader({"v": 1})
    r = cache.get("k", loader)
    assert r.status is CacheStatus.FRESH and r.from_upstream
    clock.advance(10.0)  # age == ttl，边界点归新鲜
    r = cache.get("k", loader)
    assert r.status is CacheStatus.FRESH
    assert loader.calls["calls"] == 1


def test_just_after_ttl_is_stale_and_triggers_background_refresh(cache, clock, make_loader):
    loader = make_loader({"v": 1})
    cache.get("k", loader)
    clock.advance(10.0000001)
    r = cache.get("k", loader)
    assert r.status is CacheStatus.STALE
    assert r.value == {"v": 1}  # 先返回上一次完整结果
    # 后台刷新被发起（真实 worker 线程）
    for _ in range(200):
        if cache.metrics().background_refreshes == 1:
            break
        time.sleep(0.005)
    assert cache.metrics().background_refreshes == 1


def test_boundary_age_equals_ttl_plus_window_is_still_stale(cache, clock, make_loader):
    cache.get("k", make_loader({"v": 1}))
    clock.advance(20.0)  # age == ttl + stale_window：仍可容忍陈旧
    assert cache.get("k", make_loader({"v": 1})).status is CacheStatus.STALE


def test_beyond_window_is_expired_and_refreshes_synchronously(cache, clock):
    cache.get("k", lambda: Loaded({"v": 1}))
    clock.advance(20.0000001)
    # 过期：同步回源（loader 立即返回），不返回陈旧数据
    gen_before = cache._store["k"].generation
    r = cache.get("k", lambda: Loaded({"v": 2}))
    assert r.status is CacheStatus.FRESH
    assert r.value == {"v": 2}
    assert cache._store["k"].generation == gen_before + 1
    snap = cache.metrics()
    assert snap.synchronous_refreshes >= 1


def test_negative_cache_boundary(cache, clock):
    cache.get("missing", lambda: Loaded(value=None, missing=True))
    clock.advance(2.0)  # age == missing_ttl：负缓存仍有效
    r = cache.get("missing", lambda: Loaded(value=None, missing=True))
    assert r.status is CacheStatus.NEGATIVE_FRESH
    clock.advance(0.0000001)  # 越过：允许重新回源
    r = cache.get("missing", lambda: Loaded(value={"now": "exists"}))
    assert r.status is CacheStatus.FRESH and r.value == {"now": "exists"}


def test_zero_stale_window_means_strict_expiry(clock):
    from cachelayer.cache import CacheLayer
    from cachelayer.models import CacheConfig

    layer = CacheLayer(CacheConfig(ttl=5, stale_window=0, missing_ttl=1), clock=clock)
    try:
        layer.get("k", lambda: Loaded({"v": 1}))
        clock.advance(5.0000001)
        r = layer.get("k", lambda: Loaded({"v": 2}))
        assert r.status is CacheStatus.FRESH and r.value == {"v": 2}
    finally:
        layer.shutdown()
