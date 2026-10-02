"""Configuration for the cache access layer."""

from __future__ import annotations

from dataclasses import dataclass

# Absolute upper bound for capacity to prevent misconfiguration from causing
# unbounded memory growth.
HARD_CAPACITY_LIMIT = 100_000


@dataclass(frozen=True)
class CacheConfig:
    """Freshness thresholds and operational knobs.

    Threshold semantics (boundaries are deterministic)::

        age <= fresh_ttl                       -> FRESH
        fresh_ttl < age <= fresh_ttl+stale_ttl -> STALE (return stale + refresh)
        age > fresh_ttl+stale_ttl              -> EXPIRED (wait for refresh)

    Negative (not-found) entries are fresh for ``negative_ttl`` and then
    reloaded synchronously.
    """

    fresh_ttl: float = 5.0
    stale_ttl: float = 60.0
    negative_ttl: float = 2.0
    capacity: int = 1000
    max_refresh_concurrency: int = 4
    shutdown_timeout: float = 5.0

    def __post_init__(self) -> None:
        if self.fresh_ttl <= 0:
            raise ValueError("fresh_ttl must be > 0")
        if self.stale_ttl < 0:
            raise ValueError("stale_ttl must be >= 0")
        if self.negative_ttl <= 0:
            raise ValueError("negative_ttl must be > 0")
        if not (1 <= self.capacity <= HARD_CAPACITY_LIMIT):
            raise ValueError(
                f"capacity must be in [1, {HARD_CAPACITY_LIMIT}], got {self.capacity}"
            )
        if self.max_refresh_concurrency < 1:
            raise ValueError("max_refresh_concurrency must be >= 1")
        if self.shutdown_timeout <= 0:
            raise ValueError("shutdown_timeout must be > 0")
