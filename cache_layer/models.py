"""Data models for the cache access layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Collection

# Sentinel stored in place of a value for negatively cached (not-found) entries.
MISSING = object()


class Freshness(str, Enum):
    """Freshness tier of a cached entry at read time."""

    FRESH = "fresh"
    STALE = "stale"
    EXPIRED = "expired"


class ReadStatus(str, Enum):
    """Outcome of a cache read."""

    HIT = "hit"                 # fresh value served from cache
    STALE = "stale"             # stale value served, background refresh triggered
    DEGRADED = "degraded"       # stale value served, but refresh failed
    MISS = "miss"               # no usable value (first load failed)
    NEGATIVE = "negative"       # cached upstream not-found


class FailureReason(str, Enum):
    """Distinguishable reasons for upstream/refresh failures."""

    TIMEOUT = "timeout"
    UPSTREAM = "upstream_error"
    MALFORMED = "malformed"
    CANCELLED = "cancelled"


@dataclass
class LoadResult:
    """Result returned by a loader after a successful upstream fetch.

    ``missing`` indicates the upstream explicitly reported not-found.
    ``deps`` lists the upstream entities this derived result depends on.
    """

    value: Any = None
    deps: Collection[str] = ()
    missing: bool = False


@dataclass
class CacheEntry:
    """A single, fully-written cache entry.

    Entries are replaced atomically (reference swap) only after a loader
    completes, so readers never observe a half-written entry.
    """

    key: str
    value: Any                       # real value or MISSING sentinel
    is_missing: bool
    created_at: float                # monotonic timestamp of last successful write
    last_access: float               # monotonic timestamp of last read (LRU)
    deps: frozenset[str]
    version: int
    refresh_failed: bool = False
    failure_reason: FailureReason | None = None


@dataclass
class ReadResult:
    """Outcome of :meth:`CacheLayer.get`."""

    status: ReadStatus
    value: Any = None
    found: bool = False
    freshness: Freshness | None = None
    degraded: bool = False
    missing: bool = False
    failure_reason: FailureReason | None = None
    version: int = 0
