"""HTTP 适配层：所有读取经缓存层，状态与降级标记随响应显式返回。

错误语义：
- 上游明确不存在（负缓存）：HTTP 404 + ``status=negative_fresh/miss``；
- 上游故障 / 超时 / 格式错误 / 过期刷新失败且无旧值：HTTP 502，原因可区分；
- 刷新失败但有上一份完整结果：HTTP 200 + ``status=degraded`` + ``degrades`` 标记。
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from cachelayer.cache import CacheLayer
from cachelayer.clock import Clock
from cachelayer.errors import (
    UpstreamFormatError,
    UpstreamNotFoundError,
    UpstreamUnavailableError,
)
from cachelayer.models import CacheConfig, CacheStatus
from cachelayer.upstream import MockUpstream
from services import FeedService, UserService


def create_app(config: CacheConfig | None = None, *, cache: CacheLayer | None = None,
               upstream: MockUpstream | None = None, clock: Clock | None = None) -> FastAPI:
    upstream = upstream or MockUpstream()
    cache = cache or CacheLayer(config, clock=clock)
    users = UserService(cache, upstream)
    feeds = FeedService(cache, upstream)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.upstream = upstream
        app.state.cache = cache
        try:
            yield
        finally:  # 进程关闭：在途刷新原子收尾，不阻塞退出
            cache.shutdown()

    app = FastAPI(title="dependency-cache-layer", lifespan=lifespan)
    app.state.upstream = upstream
    app.state.cache = cache
    app.state.users = users
    app.state.feeds = feeds

    def _serve(result):
        body = result.to_response()
        if result.status is CacheStatus.MISS or result.status is CacheStatus.NEGATIVE_FRESH:
            raise HTTPException(status_code=404, detail=body)
        return body

    @app.get("/users/{user_id}")
    def get_user(user_id: str):
        try:
            return _serve(users.get_user(user_id))
        except UpstreamNotFoundError as e:
            raise HTTPException(status_code=404, detail={"error": "not_found", "key": e.key})
        except UpstreamUnavailableError as e:
            raise HTTPException(status_code=502, detail={"error": "upstream_unavailable",
                                                        "key": e.key, "reason": e.reason})
        except UpstreamFormatError as e:
            raise HTTPException(status_code=502, detail={"error": "upstream_format",
                                                        "key": e.key, "detail": e.detail})

    @app.get("/feeds/{feed_id}")
    def get_feed(feed_id: str):
        try:
            return _serve(feeds.get_feed(feed_id))
        except UpstreamNotFoundError as e:
            raise HTTPException(status_code=404, detail={"error": "not_found", "key": e.key})
        except UpstreamUnavailableError as e:
            raise HTTPException(status_code=502, detail={"error": "upstream_unavailable",
                                                        "key": e.key, "reason": e.reason})
        except UpstreamFormatError as e:
            raise HTTPException(status_code=502, detail={"error": "upstream_format",
                                                        "key": e.key, "detail": e.detail})

    # ---- 管理 / 演示接口 ----
    @app.post("/admin/users/{user_id}")
    def put_user(user_id: str, name: str, score: int):
        upstream.put_user(user_id, name, score)
        return {"ok": True}

    @app.post("/admin/feeds/{feed_id}")
    def put_feed(feed_id: str, user_ids: str):
        ids = [s for s in user_ids.split(",") if s]
        upstream.set_feed(feed_id, ids)
        return {"ok": True}

    @app.post("/admin/notify/{entity_kind}/{entity_id}")
    def notify_change(entity_kind: str, entity_id: str):
        """上游实体变化通知 -> 精准失效。"""
        n = cache.invalidate_dependency(f"{entity_kind}:{entity_id}")
        return {"invalidated": n}

    @app.post("/admin/fault/{entity_kind}/{entity_id}")
    def inject_fault(entity_kind: str, entity_id: str, kind: str):
        try:
            upstream.inject_fault(f"{entity_kind}:{entity_id}", kind)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        return {"ok": True}

    @app.delete("/admin/fault/{entity_kind}/{entity_id}")
    def clear_fault(entity_kind: str, entity_id: str):
        upstream.clear_fault(f"{entity_kind}:{entity_id}")
        return {"ok": True}

    @app.get("/metrics")
    def metrics():
        snap = cache.metrics()
        from dataclasses import asdict
        return asdict(snap)

    @app.get("/health")
    def health():
        return {"ok": True, "size": cache.size()}

    return app
