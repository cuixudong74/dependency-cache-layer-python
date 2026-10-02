"""缺失（可短 TTL 负缓存）与故障（不缓存为缺失，原因可区分）。"""

from __future__ import annotations

import pytest

from cachelayer.errors import (
    UpstreamFormatError,
    UpstreamNotFoundError,
    UpstreamUnavailableError,
)
from cachelayer.models import CacheStatus, Loaded
from services import UserService


def test_not_found_is_cached_with_short_ttl(cache, upstream, clock):
    users = UserService(cache, upstream)
    r = users.get_user("ghost")
    assert r.status is CacheStatus.MISS and r.value is None
    assert upstream.call_count("user:ghost") == 1
    clock.advance(1.9)
    assert users.get_user("ghost").status is CacheStatus.NEGATIVE_FRESH
    assert upstream.call_count("user:ghost") == 1  # 未再回源
    clock.advance(0.2)  # 越过 missing_ttl
    upstream.put_user("ghost", "NowReal", 5)
    r = users.get_user("ghost")
    assert r.status is CacheStatus.FRESH and r.value["name"] == "NowReal"


def test_unavailable_is_not_cached_as_missing(cache, upstream):
    users = UserService(cache, upstream)
    upstream.inject_fault("user:x", "unavailable")
    with pytest.raises(UpstreamUnavailableError):
        users.get_user("x")
    assert cache.size() == 0 and not cache.contains("user:x")
    assert cache.metrics().misses == 0  # 故障不是未命中


def test_timeout_is_not_cached_as_missing(cache, upstream):
    users = UserService(cache, upstream)
    upstream.inject_fault("user:x", "timeout")
    with pytest.raises(UpstreamUnavailableError) as ei:
        users.get_user("x")
    assert ei.value.reason == "upstream timeout"
    assert cache.size() == 0


def test_format_error_is_not_cached_as_missing(cache, upstream):
    users = UserService(cache, upstream)
    upstream.inject_fault("user:x", "malformed")
    with pytest.raises(UpstreamFormatError):
        users.get_user("x")
    assert cache.size() == 0


def test_failure_after_miss_ttl_does_not_overwrite_negative_with_failure(cache, clock):
    # 先确认缺失（负缓存），TTL 内上游故障：仍返回负缓存、不打上游
    cache.get("k", lambda: Loaded(value=None, missing=True, dependencies=["e:k"]))
    def boom():
        raise UpstreamUnavailableError("k", "500")
    clock.advance(1)
    r = cache.get("k", boom)
    assert r.status is CacheStatus.NEGATIVE_FRESH


def test_error_reasons_are_distinguishable_types():
    assert UpstreamNotFoundError("k").key == "k"
    with pytest.raises(UpstreamUnavailableError) as a:
        raise UpstreamUnavailableError("k", "timeout")
    assert a.value.reason == "timeout"
    with pytest.raises(UpstreamFormatError) as b:
        raise UpstreamFormatError("k", "bad shape")
    assert b.value.detail == "bad shape"


def test_negative_entry_never_serves_stale(cache, clock):
    # 负缓存没有陈旧窗口：越过 missing_ttl 后必须重新回源，绝不 STALE 回落
    cache.get("k", lambda: Loaded(value=None, missing=True))
    clock.advance(100)
    r = cache.get("k", lambda: Loaded(value={"born": True}))
    assert r.status is CacheStatus.FRESH and r.value == {"born": True}
