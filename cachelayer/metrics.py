"""可观测计数：命中 / 未命中 / 负缓存命中 / 陈旧返回 / 后台与同步刷新 /
刷新失败 / 降级返回 / 精准失效 / 淘汰。"""

from __future__ import annotations

import threading
from dataclasses import dataclass


@dataclass(frozen=True)
class MetricsSnapshot:
    hits: int
    misses: int
    negative_hits: int
    stale_served: int
    background_refreshes: int
    synchronous_refreshes: int
    refresh_failures: int
    degrades: int
    invalidations: int
    evictions: int
    in_flight_refreshes: int


class Metrics:
    """线程安全的计数器集合。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0
        self._negative_hits = 0
        self._stale_served = 0
        self._background_refreshes = 0
        self._synchronous_refreshes = 0
        self._refresh_failures = 0
        self._degrades = 0
        self._invalidations = 0
        self._evictions = 0

    def record_hit(self) -> None:
        with self._lock:
            self._hits += 1

    def record_miss(self) -> None:
        with self._lock:
            self._misses += 1

    def record_negative_hit(self) -> None:
        with self._lock:
            self._negative_hits += 1

    def record_stale_served(self) -> None:
        with self._lock:
            self._stale_served += 1

    def record_background_refresh(self) -> None:
        with self._lock:
            self._background_refreshes += 1

    def record_synchronous_refresh(self) -> None:
        with self._lock:
            self._synchronous_refreshes += 1

    def record_refresh_failure(self) -> None:
        with self._lock:
            self._refresh_failures += 1

    def record_degrade(self) -> None:
        with self._lock:
            self._degrades += 1

    def record_invalidation(self, n: int = 1) -> None:
        with self._lock:
            self._invalidations += n

    def record_eviction(self) -> None:
        with self._lock:
            self._evictions += 1

    def snapshot(self, in_flight: int = 0) -> MetricsSnapshot:
        with self._lock:
            return MetricsSnapshot(
                hits=self._hits,
                misses=self._misses,
                negative_hits=self._negative_hits,
                stale_served=self._stale_served,
                background_refreshes=self._background_refreshes,
                synchronous_refreshes=self._synchronous_refreshes,
                refresh_failures=self._refresh_failures,
                degrades=self._degrades,
                invalidations=self._invalidations,
                evictions=self._evictions,
                in_flight_refreshes=in_flight,
            )
