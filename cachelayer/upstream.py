"""本地可注入的模拟上游数据源，替代任何外部服务。

- 内存 users 实体与 feed 组合（feed 依赖若干 user）；
- 按 key 注入 not_found / unavailable / timeout / malformed 故障；
- 门闩可让某次调用阻塞（并发 / 超时测试用）；
- 每个 key 的调用计数，便于验证回源次数。
"""

from __future__ import annotations

import threading
from typing import Any, Protocol, runtime_checkable

from cachelayer.errors import (
    UpstreamFormatError,
    UpstreamNotFoundError,
    UpstreamUnavailableError,
)

_FAULT_KINDS = frozenset({"not_found", "unavailable", "timeout", "malformed"})


@runtime_checkable
class DataSource(Protocol):
    """上游数据源协议。"""

    def get_user(self, user_id: str) -> dict[str, Any]: ...
    def get_feed(self, feed_id: str) -> dict[str, Any]: ...


class MockUpstream:
    """线程安全的内存模拟上游。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._users: dict[str, dict[str, Any]] = {}
        self._feeds: dict[str, list[str]] = {}
        self._faults: dict[str, str] = {}
        self._gates: dict[str, threading.Event] = {}
        self._calls: dict[str, int] = {}

    # ------------------------------------------------------------ 数据编排
    def put_user(self, user_id: str, name: str, score: int) -> None:
        with self._lock:
            self._users[user_id] = {"id": user_id, "name": name, "score": score}

    def update_user(self, user_id: str, **fields: Any) -> None:
        """模拟上游实体变化（分数 / 名称更新）。"""
        with self._lock:
            if user_id not in self._users:
                raise UpstreamNotFoundError(f"user:{user_id}")
            self._users[user_id].update(fields)
            self._users[user_id]["id"] = user_id

    def set_feed(self, feed_id: str, user_ids: list[str]) -> None:
        with self._lock:
            self._feeds[feed_id] = list(user_ids)

    def snapshot_users(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {k: dict(v) for k, v in self._users.items()}

    # ------------------------------------------------------------ 读取
    def get_user(self, user_id: str) -> dict[str, Any]:
        key = f"user:{user_id}"
        self._before_call(key)
        kind = self._faults.get(key)
        if kind is not None:
            self._raise_fault(key, kind)
        with self._lock:
            user = self._users.get(user_id)
            if user is None:
                raise UpstreamNotFoundError(key)
            return dict(user)

    def get_feed(self, feed_id: str) -> dict[str, Any]:
        key = f"feed:{feed_id}"
        self._before_call(key)
        kind = self._faults.get(key)
        if kind is not None:
            self._raise_fault(key, kind)
        with self._lock:
            member_ids = self._feeds.get(feed_id)
            if member_ids is None:
                raise UpstreamNotFoundError(key)
            users: list[dict[str, Any]] = []
            for uid in member_ids:
                u = self._users.get(uid)
                if u is None:  # 组合依赖缺失，无法物化
                    raise UpstreamUnavailableError(
                        key, f"feed composition broken: missing member {uid}"
                    )
                users.append(dict(u))
            return {"id": feed_id, "users": users}

    # ------------------------------------------------------------ 故障注入
    def inject_fault(self, key: str, kind: str) -> None:
        """kind ∈ not_found | unavailable | timeout | malformed。"""
        if kind not in _FAULT_KINDS:
            raise ValueError(f"unknown fault kind: {kind}")
        with self._lock:
            self._faults[key] = kind

    def clear_fault(self, key: str) -> None:
        with self._lock:
            self._faults.pop(key, None)

    def set_gate(self, key: str | None, gate: threading.Event | None) -> None:
        """设置门闩后，指定 key 的下一次（及持续）调用会阻塞到门闩 set。
        传 key=None / gate=None 清除全部门闩。"""
        with self._lock:
            if gate is None:
                self._gates.clear()
            else:
                assert key is not None
                self._gates[key] = gate

    # ------------------------------------------------------------ 观测
    def call_count(self, key: str) -> int:
        with self._lock:
            return self._calls.get(key, 0)

    def total_calls(self) -> int:
        with self._lock:
            return sum(self._calls.values())

    # ------------------------------------------------------------ 内部
    def _before_call(self, key: str) -> None:
        gate = self._gates.get(key)
        if gate is not None:
            gate.wait()
        with self._lock:
            self._calls[key] = self._calls.get(key, 0) + 1

    @staticmethod
    def _raise_fault(key: str, kind: str) -> None:
        if kind == "not_found":
            raise UpstreamNotFoundError(key)
        if kind == "unavailable":
            raise UpstreamUnavailableError(key, "upstream unavailable")
        if kind == "timeout":
            raise UpstreamUnavailableError(key, "upstream timeout")
        if kind == "malformed":
            raise UpstreamFormatError(key, "unexpected payload shape")
        raise UpstreamUnavailableError(key, f"unknown fault: {kind}")
