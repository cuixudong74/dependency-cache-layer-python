"""面向上游数据源的缓存访问层。

模块划分::

    clock             可注入时间源
    errors            上游故障 / 缺失 / 格式错误等可区分异常
    models            新鲜度状态、缓存结果、条目与配置
    metrics           命中 / 未命中 / 陈旧 / 刷新失败 / 淘汰计数
    deps              按依赖的精准失效索引
    refresh           有并发上限、可取消的后台刷新执行器
    cache             缓存层主体（新鲜度分级、陈旧回落、原子写入、有界淘汰）
    upstream          本地可注入的模拟上游数据源
"""

from cachelayer.clock import Clock, FakeClock, SystemClock
from cachelayer.errors import (
    CacheError,
    UpstreamFormatError,
    UpstreamNotFoundError,
    UpstreamUnavailableError,
)
from cachelayer.models import (
    CacheConfig,
    CacheEntry,
    CacheResult,
    CacheStatus,
    Loaded,
)
from cachelayer.cache import CacheLayer

__all__ = [
    "CacheError",
    "UpstreamFormatError",
    "UpstreamNotFoundError",
    "UpstreamUnavailableError",
    "CacheConfig",
    "CacheEntry",
    "CacheResult",
    "CacheStatus",
    "Loaded",
    "CacheLayer",
    "Clock",
    "FakeClock",
    "SystemClock",
]
