"""陈旧回落、后台刷新失败保留旧值、过期同步刷新失败给降级标记。"""

from __future__ import annotations

import threading
import time

import pytest

from cachelayer.errors import UpstreamUnavailableError
from cachelayer.models import CacheStatus, Loaded


def _wait_until(predicate, timeout: float = 2.0) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return True
        time.sleep(0.005)
    return False


def test_stale_returns_old_complete_value_and_refreshes(cache, clock, make_loader):
    cache.get("k", make_loader({"v": "old"}, ["e:1"]))
    clock.advance(10.5)
    r = cache.get("k", make_loader({"v": "new"}, ["e:1"]))
    assert r.status is CacheStatus.STALE and r.value == {"v": "old"}
    # 新版本被完整发布后，读到的必是完整新版本
    assert _wait_until(lambda: cache._store["k"].value == {"v": "new"})
    r = cache.get("k", make_loader({"v": "new"}))
    assert r.status is CacheStatus.FRESH and r.value == {"v": "new"}


def test_background_refresh_is_deduplicated(cache, clock, make_loader):
    cache.get("k", make_loader({"v": 1}))
    clock.advance(11)
    gate = threading.Event()
    started = threading.Event()

    def flaky():
        started.set()
        gate.wait(2)
        return Loaded({"v": 2})

    # 多个陈旧读取并发，只应发起一次后台刷新
    threads = [threading.Thread(target=lambda: cache.get("k", flaky)) for _ in range(5)]
    for t in threads:
        t.start()
    assert _wait_until(started.is_set)
    time.sleep(0.05)
    assert cache.metrics().background_refreshes == 1
    gate.set()
    for t in threads:
        t.join(2)
    assert _wait_until(lambda: cache._store["k"].value == {"v": 2})


def test_background_failure_keeps_previous_complete_value(cache, clock):
    cache.get("k", lambda: Loaded({"v": "good"}, ["e:1"]))
    clock.advance(11)  # 陈旧窗口

    attempts = {"n": 0}
    def failing_loader():
        attempts["n"] += 1
        raise UpstreamUnavailableError("k", "boom")

    cache.get("k", failing_loader)  # 陈旧返回 + 后台刷新失败
    assert _wait_until(lambda: cache.metrics().refresh_failures >= 1)
    entry = cache._store["k"]
    assert entry.value == {"v": "good"}  # 完整旧值原样保留，不替换成失败
    assert entry.generation == 1
    assert entry.degrades and entry.degrades[0].startswith("refresh failed")
    assert entry.written_at  # 数据/计时不变，不伪装成新数据


def test_expired_refresh_failure_returns_degraded_marker(cache, clock):
    cache.get("k", lambda: Loaded({"v": "old"}, ["e:1"]))
    clock.advance(21)  # 严格越过陈旧窗口

    def failing():
        raise UpstreamUnavailableError("k", "timeout")

    r = cache.get("k", failing)
    assert r.status is CacheStatus.DEGRADED
    assert r.value == {"v": "old"}  # 上一次完整结果
    assert r.degrades and "refresh failed" in r.degrades[0]
    m = cache.metrics()
    assert m.refresh_failures >= 1 and m.degrades >= 1


def test_expired_with_no_previous_value_failure_is_raised(cache, clock):
    # 该场景：无旧值时 loader 抛错，必须抛出，不得返回假数据 / 假缺失
    with pytest.raises(UpstreamUnavailableError):
        cache.get("brandnew", lambda: (_ for _ in ()).throw(UpstreamUnavailableError("brandnew", "500")))
    assert cache.size() == 0
    assert not cache.contains("brandnew")


def test_after_recovery_expired_read_gets_new_value(cache, clock):
    good = {"v": 1}
    loaders = [lambda: Loaded(good, ["e:1"]),
               lambda: (_ for _ in ()).throw(UpstreamUnavailableError("k", "boom")),
               lambda: Loaded({"v": 2}, ["e:1"])]
    cache.get("k", loaders[0])
    clock.advance(21)
    r = cache.get("k", loaders[1])
    assert r.status is CacheStatus.DEGRADED
    r = cache.get("k", loaders[2])  # 恢复：同步刷新成功
    assert r.status is CacheStatus.FRESH and r.value == {"v": 2}
