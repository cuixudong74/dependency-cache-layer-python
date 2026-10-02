"""Clock abstraction so freshness thresholds can be controlled deterministically in tests."""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    """Monotonic clock used for freshness/TTL decisions."""

    def monotonic(self) -> float:
        """Return a monotonically increasing number of seconds."""
        ...


class SystemClock:
    """Real clock backed by ``time.monotonic``."""

    def monotonic(self) -> float:
        return time.monotonic()


class FakeClock:
    """Manually advanced clock for deterministic, instant tests.

    The clock does not advance on its own; tests call :meth:`advance` to move
    forward in time across freshness boundaries.
    """

    def __init__(self, start: float = 1000.0) -> None:
        self._now = start

    def monotonic(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("clock cannot be moved backwards")
        self._now += seconds
