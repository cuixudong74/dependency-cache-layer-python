"""有界容量、LRU 淘汰、上界校验、被淘汰条目再次读取正常回源。"""

from __future__ import annotations

import pytest

from cachelayer.cache import CacheLayer
from cachelayer.clock import FakeClock
from cachelayer.models import CacheConfig, Loaded


def _layer(capacity, **kw):
    return CacheLayer(CacheConfig(capacity=capacity, **kw), clock=FakeClock(0))


def test_capacity_is_hard_bound_with_lru():
    cache = _layer(3)
    for k in "abc":
        cache.get(k, lambda k=k: Loaded(k, [f"e:{k}"]))
    cache.get("a", lambda: Loaded("a"))  # touch a -> LRU: b,c,a
    cache.get("d", lambda: Loaded("d"))  # 淘汰 b
    assert cache.contains("a") and cache.contains("c") and cache.contains("d")
    assert not cache.contains("b")
    assert cache.metrics().evictions == 1
    cache.shutdown()


def test_refresh_of_existing_key_does_not_grow_capacity():
    cache = _layer(2)
    cache.get("a", lambda: Loaded(1))
    cache.get("b", lambda: Loaded(1))
    cache.get("a", lambda: Loaded(2))
    cache.get("a", lambda: Loaded(3))
    assert cache.size() == 2 and cache.metrics().evictions == 0
    cache.shutdown()


def test_evicted_entry_reads_through_to_upstream():
    cache = _layer(1)
    cache.get("a", lambda: Loaded("A"))
    cache.get("b", lambda: Loaded("B"))
    assert not cache.contains("a")
    assert cache.get("a", lambda: Loaded("A2")).value == "A2"
    assert not cache.contains("b")  # 容量 1：b 又被 a 淘汰
    cache.shutdown()


def test_eviction_cleans_dependency_index():
    cache = _layer(1)
    cache.get("a", lambda: Loaded(1, ["x"]))
    assert cache._deps.keys_depending_on("x") == {"a"}
    cache.get("b", lambda: Loaded(2, ["y"]))
    assert cache._deps.keys_depending_on("x") == set()
    assert cache._deps.keys_depending_on("y") == {"b"}
    assert cache.invalidate_dependency("x") == 0  # 已淘汰：no-op
    cache.shutdown()


def test_expired_entries_evicted_before_fresh_even_if_recent_lru():
    # 容量满时：存在过期项优先回收过期项，即使它在 LRU 最新位置
    from cachelayer.models import CacheEntry
    cache = _layer(3, ttl=10, stale_window=0)
    # 直接构造不同写入时间：old 写于 0，keep1/keep2 写于 15（均在 ttl 内）
    cur = cache._clock.now()
    cache._store_lock.acquire()
    try:
        cache._store_unlocked("old", CacheEntry("old", 1, 0.0, 10, 0, ("e:old",), False, 1))
        cache._store_unlocked("keep1", CacheEntry("keep1", 2, 15.0, 10, 0, ("e:k1",), False, 1))
        cache._store_unlocked("keep2", CacheEntry("keep2", 3, 15.0, 10, 0, ("e:k2",), False, 1))
        # old 恰好位于 LRU 最右端（最新）
        cache._store.move_to_end("old")
    finally:
        cache._store_lock.release()
    cache._clock.advance(20)  # now=20：old(age20) 过期；keep*(age5) 新鲜
    incoming = CacheEntry("incoming", 4, 20.0, 10, 0, (), False, 1)
    with cache._store_lock:
        cache._store_unlocked("incoming", incoming)
    assert cache.contains("incoming") and not cache.contains("old")
    assert cache.contains("keep1") and cache.contains("keep2")
    cache.shutdown()


def test_capacity_upper_bound_is_enforced():
    with pytest.raises(ValueError):
        CacheConfig(capacity=10_001)
    CacheConfig(capacity=10_000, max_capacity=10_000)


def test_long_run_memory_is_bounded():
    cache = _layer(8)
    for i in range(500):
        cache.get(f"k{i}", lambda i=i: Loaded(i))
    assert cache.size() <= 8
    assert cache.metrics().evictions == 492
    cache.shutdown()
