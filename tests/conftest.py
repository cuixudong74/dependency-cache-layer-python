"""公共 fixtures：假时钟、模拟上游、可自定义配置的缓存层。"""

from __future__ import annotations

import threading
from typing import Sequence

import pytest

from cachelayer.cache import CacheLayer
from cachelayer.clock import FakeClock
from cachelayer.models import CacheConfig, Loaded
from cachelayer.upstream import MockUpstream


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(start=1000.0)


@pytest.fixture
def upstream() -> MockUpstream:
    return MockUpstream()


@pytest.fixture
def config() -> CacheConfig:
    return CacheConfig(
        ttl=10.0,
        stale_window=10.0,
        missing_ttl=2.0,
        capacity=4,
        refresh_concurrency=2,
        refresh_timeout=5.0,
        shutdown_timeout=0.5,
    )


@pytest.fixture
def cache(config: CacheConfig, clock: FakeClock) -> CacheLayer:
    layer = CacheLayer(config, clock=clock)
    yield layer
    layer.shutdown()


def build_loader(value, deps: Sequence[str] = (), *, missing: bool = False, ttl: float | None = None):
    """构造一个计数加载器。"""
    state = {"calls": 0}

    def loader() -> Loaded:
        state["calls"] += 1
        return Loaded(value=value, dependencies=tuple(deps), missing=missing, ttl=ttl)

    loader.calls = state  # type: ignore[attr-defined]
    return loader


@pytest.fixture
def make_loader():
    return build_loader
