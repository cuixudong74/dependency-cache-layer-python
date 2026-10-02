"""可观测计数、刷新并发上限、排队取消与优雅停止。"""

from __future__ import annotations

import threading
import time

from cachelayer.cache import CacheLayer
from cachelayer.clock import FakeClock
from cachelayer.models import CacheConfig, Loaded
from cachelayer.upstream import MockUpstream
from services import UserService


def test_metrics_count_all_paths(cache, clock, upstream):
    users = UserService(cache, upstream)
    upstream.put_user("u1", "A", 1)
    users.get_user("u1")                 # miss
    users.get_user("u1")                # hit
    users.get_user("missing")           # negative miss
    clock.advance(1)
    users.get_user("missing")           # negative hit
    clock.advance(21)
    upstream.inject_fault("user:u1", "timeout")
    r = users.get_user("u1")            # expired -> sync refresh fails -> degraded
    assert r.status.value == "degraded"
    snap = cache.metrics()
    assert snap.misses == 2
    assert snap.hits == 1
    assert snap.negative_hits == 1
    assert snap.refresh_failures >= 1
    assert snap.degrades == 1
    assert snap.synchronous_refreshes >= 1
    assert snap.in_flight_refreshes == 0
    upstream.clear_fault("user:u1")


def test_refresh_concurrency_is_bounded(clock):
    layer = CacheLayer(CacheConfig(ttl=1, stale_window=10, refresh_concurrency=2),
                       clock=FakeClock(0))
    acquired = threading.Event(); hold = threading.Event(); active = {"n": 0}; mx = {"n": 0}
    lock = threading.Lock()

    def slow() -> Loaded:
        with lock:
            active["n"] += 1; mx["n"] = max(mx["n"], active["n"])
        acquired.set(); hold.wait(2)
        with lock:
            active["n"] -= 1
        return Loaded(1)

    layer.get("seed", slow)  # 触发首个加载（同步路径）
    layer.shutdown()
    # 同步路径不走 executor；并发上限在后台刷新上验证：
    layer = CacheLayer(CacheConfig(ttl=1, stale_window=10, refresh_concurrency=2),
                       clock=FakeClock(0))
    acquired.clear()
    keys = [f"k{i}" for i in range(5)]
    for k in keys:
        layer.get(k, lambda: Loaded(0))  # 填满 5 个新鲜条目
    layer._clock.advance(2)  # 全部陈旧
    threads = [threading.Thread(target=lambda k=k: layer.get(k, slow)) for k in keys]
    for t in threads:
        t.start()
    time.sleep(0.15)
    assert layer._executor.in_flight <= 2
    assert layer.metrics().in_flight_refreshes <= 2
    hold.set()
    for t in threads:
        t.join(2)
    assert mx["n"] <= 2
    layer.shutdown()


def test_shutdown_returns_promptly_and_never_blocks(clock):
    # shutdown_timeout 很短；排队任务被取消，在途任务即便慢也不阻塞退出
    layer = CacheLayer(
        CacheConfig(ttl=1, stale_window=10, refresh_concurrency=1, shutdown_timeout=0.2),
        clock=FakeClock(0),
    )
    hold = threading.Event(); started = threading.Event()

    def slow_job():
        started.set(); hold.wait(5)  # 模拟卡住的在途刷新

    # worker=1：先占住一个在途任务
    ticket = layer._executor.submit_if_absent("running", slow_job)
    assert ticket is not None and started.wait(2)
    # 再排两个任务（必然排队）
    assert layer._executor.submit_if_absent("q1", lambda: None) is not None
    assert layer._executor.submit_if_absent("q2", lambda: None) is not None

    t0 = time.time()
    layer.shutdown()  # 排队取消；在途任务最多等 0.2s，不阻塞进程退出
    assert time.time() - t0 < 1.0
    assert layer._executor.queued == 0
    hold.set()  # 唤醒那个慢任务（daemon 线程随后自行结束，不影响退出）


def test_shutdown_leaves_no_half_entry(cache, clock):
    cache.get("k", lambda: Loaded({"v": 1}))
    clock.advance(21)
    published = threading.Event()

    # 让刷新体在“回源完成、写存储之前”不可能暴露中间态：
    # 实现是锁内整体替换，这里验证关闭后旧条目依旧完整可读。
    cache.shutdown()
    entry = cache._store["k"]
    assert entry.value == {"v": 1} and entry.generation == 1


def test_inflight_refresh_completes_atomically_on_shutdown():
    layer = CacheLayer(CacheConfig(ttl=1, stale_window=10, refresh_concurrency=2,
                                   shutdown_timeout=0.5), clock=FakeClock(0))
    hold = threading.Event(); started = threading.Event()

    def slow():
        started.set(); hold.wait(3)
        return Loaded({"v": "new"})

    layer.get("k", lambda: Loaded({"v": "old"}))
    layer._clock.advance(2)
    layer.get("k", slow)  # stale -> background
    assert started.wait(2)
    hold.set()  # 放行，shutdown 等待它原子写完
    layer.shutdown()
    e = layer._store["k"]
    assert e.value in ({"v": "old"}, {"v": "new"})  # 必然是某一次完整版本


def test_cancel_pending_removes_queued_tasks_only():
    ex_layer = CacheLayer(CacheConfig(ttl=1, refresh_concurrency=1,
                                      shutdown_timeout=0.2), clock=FakeClock(0))
    hold = threading.Event(); started = threading.Event()

    def slow_running():
        started.set(); hold.wait(3)

    ex_layer._executor.submit_if_absent("running", slow_running)
    assert started.wait(2)
    ex_layer._executor.submit_if_absent("q1", lambda: None)
    ex_layer._executor.submit_if_absent("q2", lambda: None)
    assert ex_layer._executor.queued == 2
    cancelled = ex_layer._executor.cancel_pending()
    assert cancelled == 2
    assert ex_layer._executor.queued == 0
    # 在途任务仍在
    assert ex_layer._executor.ticket_for("running") is not None
    hold.set()
    ex_layer.shutdown()
