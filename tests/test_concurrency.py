"""并发读取 / 刷新一致性：

- 并发读取到的永远是某一次完整写入的版本，不见新旧混合；
- 并发缺失合并为单次回源；
- 过期并发读取共享同一次同步刷新；
- 高并发混合读写下所有观测值都取自有限且完整的版本集合。
"""

from __future__ import annotations

import threading
import time

from cachelayer.errors import UpstreamUnavailableError
from cachelayer.models import CacheStatus, Loaded


def test_concurrent_readers_always_see_complete_versions(cache, clock):
    """后台多次刷新 + 多读者并发：读到的 value 必须整体属于某一版。"""
    versions = {i: {"ver": i, "payload": list(range(i))} for i in range(1, 21)}
    idx = {"i": 0}
    gate = threading.Event()
    lock = threading.Lock()

    def loader() -> Loaded:
        with lock:
            idx["i"] += 1
            i = idx["i"]
        return Loaded(value=versions[i], dependencies=["e:1"])

    # 预热 v1
    cache.get("k", loader)

    stop = threading.Event()
    seen_bad = []

    def reader():
        while not stop.is_set():
            r = cache.get("k", loader)
            v = r.value
            expected = versions[v["ver"]]
            if v != expected:  # 任何撕裂 / 半写都会出现在这里
                seen_bad.append(v)

    readers = [threading.Thread(target=reader) for _ in range(8)]
    for t in readers:
        t.start()

    def refresher():
        # 不断制造过期并刷新
        base = 100.0
        for i in range(19):
            base += 21  # 每步越过 ttl+stale_window
            with lock:
                pass
            cache._clock.advance(21)
            # 触发同步刷新（过期分支）
            r = cache.get("k", loader)
            assert r.value == versions[r.value["ver"]]

    rt = threading.Thread(target=refresher)
    rt.start(); rt.join(5); assert not rt.is_alive()
    stop.set()
    for t in readers:
        t.join(2)
    assert not seen_bad, seen_bad[:3]


def test_concurrent_miss_coalesces_to_single_upstream_call(cache):
    started = threading.Event()
    gate = threading.Event()
    calls = {"n": 0}
    lock = threading.Lock()

    def loader() -> Loaded:
        with lock:
            calls["n"] += 1
        started.set()
        gate.wait(2)
        return Loaded(value={"v": 1}, dependencies=["e:1"])

    errors = []
    results = []

    def worker():
        try:
            results.append(cache.get("k", loader))
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    assert started.wait(2)
    time.sleep(0.05)
    assert calls["n"] == 1
    gate.set()
    for t in threads:
        t.join(2)
    assert not errors
    assert all(r.value == {"v": 1} for r in results)
    assert calls["n"] == 1
    assert cache.metrics().misses == 1


def test_concurrent_expired_readers_share_one_refresh(cache, clock):
    cache.get("k", lambda: Loaded({"v": "old"}, ["e:1"]))
    clock.advance(21)
    started = threading.Event(); gate = threading.Event()
    calls = {"n": 0}
    lock = threading.Lock()

    def loader():
        with lock:
            calls["n"] += 1
        started.set()
        gate.wait(2)
        return Loaded({"v": "new"}, ["e:1"])

    results = []
    def worker():
        results.append(cache.get("k", loader))
    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    assert started.wait(2)
    time.sleep(0.05)
    gate.set()
    for t in threads:
        t.join(3)
    assert len(results) == 6
    assert all(r.status is CacheStatus.FRESH and r.value == {"v": "new"} for r in results)
    assert calls["n"] == 1


def test_concurrent_stale_readers_single_background_refresh(cache, clock):
    cache.get("k", lambda: Loaded({"v": 1}, ["e:1"]))
    clock.advance(11)
    gate = threading.Event(); started = threading.Event()
    lock = threading.Lock(); calls = {"n": 0}

    def loader():
        with lock:
            calls["n"] += 1
        started.set(); gate.wait(2)
        return Loaded({"v": 2}, ["e:1"])

    threads = [threading.Thread(target=lambda: cache.get("k", loader)) for _ in range(6)]
    for t in threads:
        t.start()
    assert started.wait(2)
    time.sleep(0.05)
    assert cache.metrics().background_refreshes == 1
    gate.set()
    for t in threads:
        t.join(2)
    assert calls["n"] == 1


def test_no_half_written_entry_visible_even_while_refresh_inflight(cache, clock):
    cache.get("k", lambda: Loaded({"v": 1}, ["e:1"]))
    clock.advance(11)
    hold = threading.Event(); started = threading.Event()

    def slow():
        started.set(); hold.wait(2)
        return Loaded({"v": 2}, ["e:1"])

    cache.get("k", slow)  # 陈旧返回，后台刷新开始但未提交
    assert started.wait(2)
    time.sleep(0.05)
    # 刷新进行中：读到的必须还是完整旧版本
    entry = cache._store["k"]
    assert entry.value == {"v": 1}
    hold.set()
    time.sleep(0.2)
    assert cache._store["k"].value == {"v": 2}
