"""新鲜度状态、缓存结果、条目与配置等核心数据模型。

阈值边界语义（确定性，:math:`age = now - written\\_at`）::

    普通条目:
        age <= ttl                         -> FRESH   （边界点算新鲜）
        ttl < age <= ttl + stale_window    -> STALE   （先返回旧值并后台刷新）
        age >  ttl + stale_window          -> EXPIRED （严格越界，同步等刷新）
    缺失条目（负缓存）:
        age <= missing_ttl                 -> NEGATIVE_FRESH
        age >  missing_ttl                 -> 视为需重新回源，不做陈旧回落
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, Sequence


class CacheStatus(enum.Enum):
    """返回给调用方的明确状态。"""

    FRESH = "fresh"
    STALE = "stale"
    EXPIRED = "expired"
    NEGATIVE_FRESH = "negative_fresh"
    DEGRADED = "degraded"
    MISS = "miss"


@dataclass(frozen=True)
class Loaded:
    """加载器回源成功的结果。

    :param value:        已解析好的业务对象
    :param dependencies: 本结果依赖的上游实体 id（任一变化即失效）
    :param ttl:          可选条目级 ttl 覆盖（秒）
    :param missing:       True 表示上游明确“不存在”，走较短的 missing_ttl
    """

    value: Any = None
    dependencies: Sequence[str] = field(default_factory=tuple)
    ttl: float | None = None
    missing: bool = False


@dataclass(frozen=True)
class CacheResult:
    """一次缓存读取的完整结果（某个已完整写入版本的快照）。"""

    key: str
    value: Any
    status: CacheStatus
    age: float
    ttl: float
    stale_window: float
    degrades: tuple[str, ...]
    from_upstream: bool

    @property
    def is_usable(self) -> bool:
        """调用方是否可以把 value 当作有效数据使用。"""
        return self.status not in (CacheStatus.MISS,)

    def to_response(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "value": self.value,
            "status": self.status.value,
            "age": round(self.age, 6),
            "ttl": self.ttl,
            "stale_window": self.stale_window,
            "degrades": list(self.degrades),
            "from_upstream": self.from_upstream,
        }


@dataclass(frozen=True)
class CacheConfig:
    """缓存层配置，时间单位均为秒。

    :param ttl:                 新鲜存活时间
    :param stale_window:        新鲜之后可容忍陈旧的窗口长度
    :param missing_ttl:         明确缺失的较短存活时间
    :param capacity:             容量上限（条目数）
    :param max_capacity:         容量可配置范围的硬上界
    :param refresh_concurrency:  后台刷新任务并发上限
    :param refresh_timeout:      过期同步读取等待单次刷新的上限
    :param shutdown_timeout:     关闭时等待在途刷新原子收尾的上限
    """

    ttl: float = 30.0
    stale_window: float = 30.0
    missing_ttl: float = 5.0
    capacity: int = 1024
    max_capacity: int = 10_000
    refresh_concurrency: int = 4
    refresh_timeout: float = 10.0
    shutdown_timeout: float = 5.0

    def __post_init__(self) -> None:
        if self.ttl <= 0:
            raise ValueError("ttl must be positive")
        if self.stale_window < 0:
            raise ValueError("stale_window must be >= 0")
        if self.missing_ttl <= 0:
            raise ValueError("missing_ttl must be positive")
        if self.capacity <= 0:
            raise ValueError("capacity must be positive")
        if self.max_capacity < 1:
            raise ValueError("max_capacity must be >= 1")
        if self.capacity > self.max_capacity:
            raise ValueError(f"capacity {self.capacity} exceeds max_capacity {self.max_capacity}")
        if self.refresh_concurrency < 1:
            raise ValueError("refresh_concurrency must be >= 1")
        if self.refresh_timeout <= 0:
            raise ValueError("refresh_timeout must be positive")
        if self.shutdown_timeout < 0:
            raise ValueError("shutdown_timeout must be >= 0")


@dataclass(frozen=True)
class CacheEntry:
    """一次完整写入的不可变快照（原子替换单位）。"""

    key: str
    value: Any
    written_at: float
    ttl: float
    stale_window: float
    dependencies: tuple[str, ...]
    missing: bool
    generation: int
    degrades: tuple[str, ...] = field(default_factory=tuple)

    def age(self, now: float) -> float:
        return now - self.written_at

    def is_fresh(self, now: float) -> bool:
        """age <= ttl，含边界点。"""
        return self.age(now) <= self.ttl

    def is_stale(self, now: float) -> bool:
        """ttl < age <= ttl + stale_window；负缓存不参与陈旧回落。"""
        if self.missing:
            return False
        age = self.age(now)
        return self.ttl < age <= self.ttl + self.stale_window

    def is_expired(self, now: float) -> bool:
        """严格越过可容忍窗口：普通条目 age > ttl+stale_window；
        负缓存 age > missing_ttl（其 ttl 即 missing_ttl）。"""
        age = self.age(now)
        if self.missing:
            return age > self.ttl
        return age > self.ttl + self.stale_window
