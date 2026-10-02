"""Counters for observability."""

from __future__ import annotations

from dataclasses import dataclass, replace


@dataclass
class Metrics:
    """Mutable counters; use :meth:`snapshot` for a point-in-time copy."""

    hits: int = 0
    misses: int = 0
    negative_hits: int = 0
    stale_returns: int = 0
    degraded_returns: int = 0
    refresh_success: int = 0
    refresh_failures: int = 0
    background_refresh_started: int = 0
    evictions: int = 0
    invalidations: int = 0
    shutdown_cancelled: int = 0

    def snapshot(self) -> "Metrics":
        return replace(self)
