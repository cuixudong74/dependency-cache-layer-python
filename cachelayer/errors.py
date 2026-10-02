"""缓存层使用的可区分错误类型。

缺失（not found）与故障（unavailable / timeout / format）必须严格区分：
只有上游“明确不存在”才允许按较短存活时间做负缓存；
故障、超时、格式错误一律不得被当作不存在缓存，必须按可区分原因抛出。
"""

from __future__ import annotations



class CacheError(Exception):
    """缓存层错误基类。"""


class UpstreamNotFoundError(CacheError):
    """上游明确表示实体不存在（可负缓存）。"""

    def __init__(self, key: str) -> None:
        self.key = key
        super().__init__(f"upstream entity not found: {key}")


class UpstreamUnavailableError(CacheError):
    """上游故障 / 超时等临时性错误（不可缓存为缺失）。"""

    def __init__(self, key: str, reason: str = "upstream unavailable", *, cause: BaseException | None = None) -> None:
        self.key = key
        self.reason = reason
        self.__cause__ = cause
        super().__init__(f"{reason}: {key}")


class UpstreamFormatError(CacheError):
    """上游返回的数据格式错误（不可缓存为缺失）。"""

    def __init__(self, key: str, detail: str = "malformed payload", *, cause: BaseException | None = None) -> None:
        self.key = key
        self.detail = detail
        self.__cause__ = cause
        super().__init__(f"malformed payload for {key}: {detail}")
