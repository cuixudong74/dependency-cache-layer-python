"""可注入的时间源，便于在测试中确定性地推进时间。"""

from __future__ import annotations

import threading
import time
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """单调时钟协议（秒）。"""

    def now(self) -> float: ...


class SystemClock:
    """基于 :func:`time.monotonic` 的真实单调时钟，不受系统时间回拨影响。"""

    def now(self) -> float:
        return time.monotonic()


class FakeClock:
    """测试用假时钟：时间只会随 :meth:`advance` 显式向前走。"""

    def __init__(self, start: float = 0.0) -> None:
        self._now = float(start)
        self._lock = threading.Lock()

    def now(self) -> float:
        with self._lock:
            return self._now

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("time cannot move backwards")
        with self._lock:
            self._now += seconds
