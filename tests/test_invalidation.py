"""按依赖精准失效：只清真正依赖的条目，无关条目不受影响。"""

from __future__ import annotations

from cachelayer.models import CacheStatus, Loaded


def _load(value, *deps):
    return lambda: Loaded(value=value, dependencies=list(deps))


def test_only_dependent_entries_are_removed(clock):
    from cachelayer.cache import CacheLayer
    from cachelayer.models import CacheConfig
    cache = CacheLayer(CacheConfig(capacity=16), clock=clock)
    cache.get("feed:1", _load({"id": 1}, "user:1", "user:2"))
    cache.get("feed:2", _load({"id": 2}, "user:2"))
    cache.get("user:1", _load({"id": 1}, "user:1"))
    cache.get("user:3", _load({"id": 3}, "user:3"))
    cache.get("solo", _load({"id": 9}))  # 无依赖

    removed = cache.invalidate_dependency("user:1")
    assert removed == 2  # feed:1 与 user:1
    assert not cache.contains("feed:1")
    assert not cache.contains("user:1")
    assert cache.contains("feed:2")
    assert cache.contains("user:3")
    assert cache.contains("solo")


def test_invalidation_of_unrelated_entity_is_noop(cache):
    cache.get("feed:1", _load(1, "user:1"))
    assert cache.invalidate_dependency("user:999") == 0
    assert cache.contains("feed:1")
    assert cache.metrics().invalidations == 0


def test_dependency_index_updated_on_refresh(cache, clock):
    cache.get("k", lambda: Loaded(value=1, dependencies=["a"]))
    # 刷新后依赖集合变化：不再依赖 a，改为依赖 b
    clock.advance(21)
    cache.get("k", lambda: Loaded(value=2, dependencies=["b"]))
    assert cache.invalidate_dependency("a") == 0
    assert cache.contains("k")
    assert cache.invalidate_dependency("b") == 1
    assert not cache.contains("k")


def test_invalidation_then_read_reloads_from_upstream(cache, upstream):
    from services import FeedService, UserService

    upstream.put_user("u1", "A", 1)
    upstream.set_feed("f1", ["u1"])
    users = UserService(cache, upstream)
    feeds = FeedService(cache, upstream)
    users.get_user("u1")
    feeds.get_feed("f1")
    assert upstream.call_count("feed:f1") == 1
    assert cache.invalidate_dependency("user:u1") == 2
    feeds.get_feed("f1")
    assert upstream.call_count("feed:f1") == 2  # 被清后正常回源


def test_negative_entries_are_also_precisely_invalidated(cache):
    cache.get("ghost", lambda: Loaded(value=None, missing=True, dependencies=["user:ghost"]))
    assert cache.invalidate_dependency("user:ghost") == 1
    assert not cache.contains("ghost")
