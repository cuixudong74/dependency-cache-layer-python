"""HTTP 端到端：读取全部经缓存层；状态与降级标记显式可见。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from api import create_app
from cachelayer.models import CacheConfig
from cachelayer.clock import FakeClock


@pytest.fixture
def client(upstream):
    clock = FakeClock(0)
    cfg = CacheConfig(ttl=10, stale_window=10, missing_ttl=2,
                      capacity=16, shutdown_timeout=1)
    app = create_app(cfg, upstream=upstream, clock=clock)
    with TestClient(app) as c:
        yield c, clock, app.state.cache


def test_read_path_fresh_then_cached(client, upstream):
    c, clock, cache = client
    upstream.put_user("u1", "Alice", 1)
    r = c.get("/users/u1")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "fresh" and body["from_upstream"] is True
    assert body["value"]["name"] == "Alice"
    r = c.get("/users/u1")
    assert r.json()["status"] == "fresh" and r.json()["from_upstream"] is False
    assert upstream.call_count("user:u1") == 1


def test_missing_returns_404_then_negative_cached(client, upstream):
    c, clock, cache = client
    r = c.get("/users/ghost")
    assert r.status_code == 404
    assert r.json()["detail"]["status"] == "miss"
    assert c.get("/users/ghost").json()["detail"]["status"] == "negative_fresh"


def test_upstream_failure_is_502_with_reason(client, upstream):
    c, clock, cache = client
    upstream.put_user("u9", "C", 1)
    assert c.get("/users/u9").status_code == 200
    upstream.inject_fault("user:u9", "timeout")
    clock.advance(21)
    r = c.get("/users/u9")
    # 过期且刷新失败：旧值保留 + degraded 标记（200，非 502）
    assert r.status_code == 200
    assert r.json()["status"] == "degraded" and r.json()["degrades"]
    # 全新 key 遇故障 -> 502，原因可区分
    upstream.inject_fault("user:new", "malformed")
    r = c.get("/users/new")
    assert r.status_code == 502 and r.json()["detail"]["error"] == "upstream_format"


def test_dependency_notify_invalidates_only_dependents(client, upstream):
    c, clock, cache = client
    upstream.put_user("u1", "A", 1); upstream.put_user("u2", "B", 2)
    upstream.set_feed("f1", ["u1", "u2"])
    c.get("/users/u1"); c.get("/users/u2"); c.get("/feeds/f1")
    r = c.post("/admin/notify/user/u1")
    assert r.json() == {"invalidated": 2}
    assert cache.contains("user:u2") and not cache.contains("feed:f1")


def test_metrics_and_health_endpoints(client, upstream):
    c, clock, cache = client
    upstream.put_user("u1", "A", 1)
    c.get("/users/u1"); c.get("/users/u1")
    m = c.get("/metrics").json()
    assert m["hits"] >= 1 and m["misses"] >= 1
    assert c.get("/health").json() == {"ok": True, "size": cache.size()}
