"""Demo FastAPI application whose read path is converged onto the cache layer.

The upstream is a local, injectable :class:`MockUpstream` (no external
services). Configuration is read from environment variables with sensible
defaults; see README.md for the freshness semantics.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from cache_layer import (
    CacheConfig,
    CacheLayer,
    FailureReason,
    LoadResult,
    MockUpstream,
    NotFound,
    ReadStatus,
    UpstreamError,
    UpstreamFailure,
)

FRESH_TTL = float(os.getenv("CACHE_FRESH_TTL", "10"))
STALE_TTL = float(os.getenv("CACHE_STALE_TTL", "60"))
NEGATIVE_TTL = float(os.getenv("CACHE_NEGATIVE_TTL", "5"))
CAPACITY = int(os.getenv("CACHE_CAPACITY", "1000"))
MAX_REFRESH_CONCURRENCY = int(os.getenv("CACHE_MAX_REFRESH_CONCURRENCY", "4"))


class UserPayload(BaseModel):
    name: str


class ProfilePayload(BaseModel):
    title: str


def _summary_loader(upstream: MockUpstream, user_id: str):
    """Derived loader: a summary depends on both user:{id} and profile:{id}."""

    async def loader() -> LoadResult:
        user = await upstream.fetch(f"user:{user_id}")
        if isinstance(user, UpstreamFailure):
            raise UpstreamError(user.reason, user.message)
        if isinstance(user, NotFound):
            return LoadResult(missing=True, deps=(f"user:{user_id}",))
        profile = await upstream.fetch(f"profile:{user_id}")
        if isinstance(profile, UpstreamFailure):
            raise UpstreamError(profile.reason, profile.message)
        if isinstance(profile, NotFound):
            return LoadResult(
                missing=True, deps=(f"user:{user_id}", f"profile:{user_id}")
            )
        try:
            value = {"name": user.value["name"], "title": profile.value["title"]}
        except (KeyError, TypeError):
            raise UpstreamError(FailureReason.MALFORMED, "malformed payload")
        return LoadResult(
            value=value, deps=(f"user:{user_id}", f"profile:{user_id}")
        )

    return loader


@asynccontextmanager
async def lifespan(app: FastAPI):
    upstream = MockUpstream()
    upstream.set("user:1", {"name": "alice"})
    upstream.set("profile:1", {"title": "engineer"})
    upstream.set("user:2", {"name": "bob"})
    upstream.set("profile:2", {"title": "operator"})

    config = CacheConfig(
        fresh_ttl=FRESH_TTL,
        stale_ttl=STALE_TTL,
        negative_ttl=NEGATIVE_TTL,
        capacity=CAPACITY,
        max_refresh_concurrency=MAX_REFRESH_CONCURRENCY,
    )
    cache = CacheLayer(config)
    app.state.upstream = upstream
    app.state.cache = cache
    try:
        yield
    finally:
        await cache.shutdown()


app = FastAPI(title="Dependency cache layer demo", lifespan=lifespan)


@app.get("/")
async def root() -> dict[str, Any]:
    return {
        "service": "dependency-cache-layer",
        "endpoints": [
            "GET /users/{user_id}/summary",
            "PUT /admin/users/{user_id}",
            "PUT /admin/profiles/{user_id}",
            "DELETE /admin/users/{user_id}",
            "GET /admin/metrics",
        ],
    }


@app.get("/users/{user_id}/summary")
async def get_user_summary(user_id: str) -> dict[str, Any]:
    cache: CacheLayer = app.state.cache
    upstream: MockUpstream = app.state.upstream
    result = await cache.get(f"summary:{user_id}", _summary_loader(upstream, user_id))

    if result.status == ReadStatus.MISS and result.failure_reason is not None:
        raise HTTPException(
            status_code=502,
            detail=f"upstream failure: {result.failure_reason.value}",
        )
    if not result.found:
        raise HTTPException(status_code=404, detail=f"user {user_id} not found")

    return {
        "user_id": user_id,
        "summary": result.value,
        "status": result.status.value,
        "freshness": result.freshness.value if result.freshness else None,
        "degraded": result.degraded,
        "version": result.version,
    }


@app.put("/admin/users/{user_id}")
async def put_user(user_id: str, payload: UserPayload) -> dict[str, Any]:
    upstream: MockUpstream = app.state.upstream
    cache: CacheLayer = app.state.cache
    upstream.set(f"user:{user_id}", {"name": payload.name})
    removed = await cache.invalidate(f"user:{user_id}")
    return {"user_id": user_id, "invalidated_entries": removed}


@app.put("/admin/profiles/{user_id}")
async def put_profile(user_id: str, payload: ProfilePayload) -> dict[str, Any]:
    upstream: MockUpstream = app.state.upstream
    cache: CacheLayer = app.state.cache
    upstream.set(f"profile:{user_id}", {"title": payload.title})
    removed = await cache.invalidate(f"profile:{user_id}")
    return {"user_id": user_id, "invalidated_entries": removed}


@app.delete("/admin/users/{user_id}")
async def delete_user(user_id: str) -> dict[str, Any]:
    upstream: MockUpstream = app.state.upstream
    cache: CacheLayer = app.state.cache
    upstream.delete(f"user:{user_id}")
    removed = await cache.invalidate(f"user:{user_id}")
    return {"user_id": user_id, "invalidated_entries": removed}


@app.get("/admin/metrics")
async def get_metrics() -> dict[str, Any]:
    cache: CacheLayer = app.state.cache
    return {"metrics": cache.metrics().__dict__, "cached_entries": len(cache)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
