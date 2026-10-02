"""按依赖的精准失效索引。

每个缓存条目声明自己依赖的上游实体 id；反向索引 entity -> 依赖它的 key 集合。
实体变化时只命中真正依赖它的 key，其他条目不受影响。
"""

from __future__ import annotations

import threading
from collections import defaultdict
from typing import Iterable


class DependencyIndex:
    """双向索引：key -> (deps)，dependency -> {keys}。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._deps_of: dict[str, set[str]] = {}
        self._keys_for: dict[str, set[str]] = defaultdict(set)

    def set_dependencies(self, key: str, dependencies: Iterable[str]) -> None:
        """登记 / 覆盖某 key 的依赖集合（空集合表示无依赖，不参与失效）。"""
        new_deps = {d for d in dependencies if d}
        with self._lock:
            old = self._deps_of.get(key, set())
            for dep in old - new_deps:
                self._keys_for[dep].discard(key)
                if not self._keys_for[dep]:
                    del self._keys_for[dep]
            for dep in new_deps - old:
                self._keys_for[dep].add(key)
            if new_deps:
                self._deps_of[key] = new_deps
            else:
                self._deps_of.pop(key, None)

    def remove(self, key: str) -> None:
        """条目被淘汰 / 失效时同步清理反向索引。"""
        with self._lock:
            for dep in self._deps_of.pop(key, ()):
                self._keys_for[dep].discard(key)
                if not self._keys_for[dep]:
                    del self._keys_for[dep]

    def keys_depending_on(self, dependency: str) -> set[str]:
        """返回依赖该实体的 key 集合副本（调用方可安全迭代 / 删除）。"""
        with self._lock:
            return set(self._keys_for.get(dependency, ()))

    def dependencies_of(self, key: str) -> set[str]:
        with self._lock:
            return set(self._deps_of.get(key, ()))

    def clear(self) -> None:
        with self._lock:
            self._deps_of.clear()
            self._keys_for.clear()
