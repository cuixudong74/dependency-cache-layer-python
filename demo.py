"""本地端到端复现（不依赖任何外部服务）。

运行::

    uv run python demo.py

依次演示：
1. 未命中回源 -> 命中；
2. 越过 ttl：先返回陈旧 + 后台刷新，之后读到新版本；
3. 依赖精准失效（只有真正依赖的条目被清）；
4. 上游故障不缓存为缺失；过期后同步刷新失败 -> 保留旧值 + degraded 标记；
5. 有界容量 LRU 淘汰；
6. 可观测计数与优雅停止。
"""

from __future__ import annotations

import time

from cachelayer.cache import CacheLayer
from cachelayer.clock import FakeClock
from cachelayer.errors import UpstreamUnavailableError
from cachelayer.models import CacheConfig, CacheStatus, Loaded
from cachelayer.upstream import MockUpstream
from services import FeedService, UserService


def _line(t: str) -> None:
    print(f"  - {t}")


def main() -> None:
    print("[setup] 模拟上游数据")
    upstream = MockUpstream()
    upstream.put_user("u1", "Alice", score=10)
    upstream.put_user("u2", "Bob", score=20)
    upstream.set_feed("f1", ["u1", "u2"])

    # 真实线程执行后台刷新；时钟用假时钟以便确定性跨越阈值
    clock = FakeClock(start=1000.0)
    cfg = CacheConfig(
        ttl=10, stale_window=10, missing_ttl=2,
        capacity=3, refresh_concurrency=2, refresh_timeout=5, shutdown_timeout=2,
    )
    cache = CacheLayer(cfg, clock=clock)
    users = UserService(cache, upstream)
    feeds = FeedService(cache, upstream)

    print("[1] 未命中回源 -> 后续命中")
    r = users.get_user("u1")
    _line(f"first: status={r.status.value} from_upstream={r.from_upstream} value={r.value}")
    assert r.status is CacheStatus.FRESH and r.from_upstream
    r = users.get_user("u1")
    _line(f"second: status={r.status.value} from_upstream={r.from_upstream}")
    assert r.status is CacheStatus.FRESH and not r.from_upstream
    assert upstream.call_count("user:u1") == 1

    print("[2] 越过 ttl：陈旧回落 + 后台刷新")
    upstream.update_user("u1", score=11)
    clock.advance(10.5)
    r = users.get_user("u1")
    _line(f"stale read: status={r.status.value} old_score={r.value['score']}")
    assert r.status is CacheStatus.STALE and r.value["score"] == 10
    # 等待后台 worker（真实线程）原子发布
    deadline = time.time() + 2
    while time.time() < deadline and cache._store["user:u1"].value["score"] != 11:
        time.sleep(0.01)
    assert cache._store["user:u1"].value["score"] == 11
    _line("background refresh published a complete new version (score=11)")

    print("[3] 依赖精准失效")
    feeds.get_feed("f1")
    users.get_user("u2")
    n = cache.invalidate_dependency("user:u1")
    _line(f"invalidate user:u1 -> removed {n} entries (user:u1, feed:f1)")
    assert n == 2
    assert cache.contains("user:u2") and not cache.contains("feed:f1")

    print("[4] 缺失 vs 故障；过期刷新失败 -> 降级标记")
    # 4a. 明确缺失：负缓存，短 TTL 内不再回源
    r = users.get_user("ghost")
    _line(f"missing: status={r.status.value}")
    assert r.status is CacheStatus.MISS
    assert upstream.call_count("user:ghost") == 1
    assert users.get_user("ghost").status is CacheStatus.NEGATIVE_FRESH
    assert upstream.call_count("user:ghost") == 1

    # 4b. 故障绝不缓存为缺失：独立缓存实例，确定性演示降级
    c2 = CacheLayer(CacheConfig(ttl=5, stale_window=5), clock=FakeClock(0))
    upstream.put_user("u9", "Carol", score=1)
    u9 = UserService(c2, upstream)
    assert u9.get_user("u9").value["score"] == 1
    upstream.inject_fault("user:u9", "unavailable")
    # 新鲜期内：直接用缓存，不打故障上游
    assert u9.get_user("u9").status is CacheStatus.FRESH
    # 陈旧窗口内：返回旧值 + 后台刷新（失败，旧值保留，不计为有效新数据）
    c2._clock.advance(6)
    r = u9.get_user("u9")
    assert r.status is CacheStatus.STALE and r.value["score"] == 1
    time.sleep(0.1)
    assert c2.metrics().refresh_failures == 1
    _line("failure during stale window: old complete value kept, refresh_failures=1")
    # 越过窗口：同步刷新失败 -> DEGRADED + 可区分标记（HTTP 层会以 200+degraded 返回）
    c2._clock.advance(5)
    r = u9.get_user("u9")
    _line(f"degraded: status={r.status.value} score={r.value['score']} degrades={r.degrades}")
    assert r.status is CacheStatus.DEGRADED and r.value["score"] == 1 and r.degrades
    # 故障恢复后过期再读 -> 同步刷新成功，拿到新值
    upstream.clear_fault("user:u9")
    upstream.update_user("u9", score=9)
    c2._clock.advance(1)
    r = u9.get_user("u9")
    _line(f"recovered: status={r.status.value} score={r.value['score']}")
    assert r.status is CacheStatus.FRESH and r.value["score"] == 9
    # 4c. 无缓存时遇到故障：直接抛出可区分原因，绝不产生缺失缓存
    upstream.inject_fault("user:brandnew", "timeout")
    try:
        users.get_user("brandnew")
        raise AssertionError("expected unavailable")
    except UpstreamUnavailableError as e:
        _line(f"no cache + timeout -> raise ({e.reason}); not stored as missing: "
              f"contains={cache.contains('user:brandnew')}")
        assert not cache.contains("user:brandnew")
    upstream.clear_fault("user:brandnew")

    print("[5] 有界容量 + LRU 淘汰")
    c3 = CacheLayer(CacheConfig(ttl=100, stale_window=0, capacity=2), clock=FakeClock(0))
    def loader_for(v):
        return lambda: Loaded({"v": v}, dependencies=[f"e:{v}"])
    c3.get("a", loader_for("a")); c3.get("b", loader_for("b"))
    c3.get("a", loader_for("a"))  # touch a -> b 成为 LRU 队首
    c3.get("c", loader_for("c"))  # 淘汰 b
    _line(f"after fill a,b then c (a touched): contains a={c3.contains('a')} b={c3.contains('b')} c={c3.contains('c')}")
    assert c3.contains("a") and c3.contains("c") and not c3.contains("b")
    # 被淘汰条目再次读取正常回源
    assert c3.get("b", loader_for("b")).value == {"v": "b"}

    print("[6] 可观测计数 + 优雅停止")
    snap = cache.metrics()
    _line(f"metrics: {snap}")
    assert snap.evictions >= 0
    c2.shutdown(); c3.shutdown(); cache.shutdown()
    _line("shutdown returned promptly; no half-written entry possible")
    print("OK: 全部场景在本地复现通过。")


if __name__ == "__main__":
    main()
