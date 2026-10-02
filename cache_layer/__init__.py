"""Dependency-aware cache access layer for upstream data sources."""

from .cache import CacheLayer
from .clock import Clock, FakeClock, SystemClock
from .config import CacheConfig
from .exceptions import UpstreamError
from .metrics import Metrics
from .models import (
    CacheEntry,
    FailureReason,
    Freshness,
    LoadResult,
    ReadResult,
    ReadStatus,
)
from .upstream import Found, MockUpstream, NotFound, Upstream, UpstreamFailure, parse_json_payload

__all__ = [
    "CacheConfig",
    "CacheLayer",
    "CacheEntry",
    "Clock",
    "FailureReason",
    "FakeClock",
    "Found",
    "Freshness",
    "LoadResult",
    "Metrics",
    "MockUpstream",
    "NotFound",
    "parse_json_payload",
    "ReadResult",
    "ReadStatus",
    "SystemClock",
    "Upstream",
    "UpstreamError",
    "UpstreamFailure",
]
