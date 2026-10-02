"""缓存层主体：新鲜度分级 / 陈旧回落 / 精准失效 / 原子版本 / 有界淘汰。

读取分支（:math:`age = now - written\\_at`，边界确定）::

    无缓存          -> loader 回源（single-flight 合并并发缺失）
    普通条目:
      age <= ttl                       -> FRESH
      ttl < age <= ttl+stale_window    -> STALE：返回旧值 + 触发后台刷新
      age >  ttl+stale_window          -> 同步等待刷新；失败则带降级标记回落
    缺失条目:
      age <= missing_ttl               -> NEGATIVE_FRESH（MISS 语义的短 TTL 缓存）
      age >  missing_ttl               -> 重新回源

并发一致性：条目是不可变快照，读侧持锁拿引用后释放锁；
写侧在锁内整体替换字典值 + LRU 序 + 依赖索引，任何时刻读到的都是某次完整版本。
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Callable

from cachelayer.clock import Clock, SystemClock
from cachelayer.deps import DependencyIndex
from cachelayer.errors import CacheError
from cachelayer.metrics import Metrics, MetricsSnapshot
from cachelayer.models import CacheConfig, CacheEntry, CacheResult, CacheStatus, Loaded
from cachelayer.refresh import BoundedRefreshExecutor

Loader = Callable[[], Loaded]


class _LoadHolder:
    """同步回源 single-flight 的共享结果槽。"""

    __slots__ = ("event", "entry", "error")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.entry: CacheEntry | None = None
        self.error: BaseException | None = None


class CacheLayer:
    def __init__(
        self,
        config: CacheConfig | None = None,
        *,
        clock: Clock | None = None,
    ) -> None:
        self._config = config or CacheConfig()
        self._clock = clock or SystemClock()
        self._store: OrderedDict[str, CacheEntry] = OrderedDict()
        self._deps = DependencyIndex()
        self._metrics = Metrics()
        self._executor = BoundedRefreshExecutor(
            self._config.refresh_concurrency,
            shutdown_timeout=self._config.shutdown_timeout,
        )
        self._store_lock = threading.RLock()
        # 回源 single-flight：无缓存时并发缺失合并为一次上游调用
        self._loading: dict[str, "_LoadHolder"] = {}

    # ================================================================== 读取
    def get(self, key: str, loader: Loader) -> CacheResult:
        now = self._clock.now()
        with self._store_lock:
            entry = self._store.get(key)

        if entry is None:
            return self._load_synchronously(key, loader)

        now = self._clock.now()
        if entry.missing:
            if entry.is_fresh(now):
                self._metrics.record_negative_hit()
                return self._result(entry, CacheStatus.NEGATIVE_FRESH, now)
            # 负缓存过期：重新回源（失败不吞错）
            return self._load_synchronously(key, loader)

        if entry.is_fresh(now):
            self._touch(key)
            self._metrics.record_hit()
            return self._result(entry, CacheStatus.FRESH, now)

        if entry.is_stale(now):
            # 陈旧窗口内：先完整返回旧值，再确保有一个后台刷新在跑
            self._touch(key)
            self._metrics.record_hit()
            self._metrics.record_stale_served()
            self._ensure_background_refresh(key, loader)
            return self._result(entry, CacheStatus.STALE, now)

        # 严格过期：必须等待刷新完成
        return self._refresh_expired(key, entry, loader)

    # ====================================================== 无缓存 / 同步回源
    def _load_synchronously(self, key: str, loader: Loader) -> CacheResult:
        """合并并发缺失：同一时刻同一 key 只回源一次，等待者共享结果 / 错误。"""
        with self._store_lock:
            holder = self._loading.get(key)
            if holder is None:
                holder = _LoadHolder()
                self._loading[key] = holder
                leader = True
            else:
                leader = False

        if not leader:
            if not holder.event.wait(timeout=self._config.refresh_timeout):
                raise CacheError(f"timed out waiting in-flight load: {key}")
            if holder.error is not None:
                raise holder.error
            assert holder.entry is not None
            now = self._clock.now()
            # 合并到别人的回源：对本调用方而言是命中缓存（负缓存计 negative_hits）
            if holder.entry.missing:
                self._metrics.record_negative_hit()
            else:
                self._metrics.record_hit()
            return self._result(
                holder.entry,
                CacheStatus.NEGATIVE_FRESH if holder.entry.missing else CacheStatus.FRESH,
                now,
                from_upstream=False,
            )

        # leader：回源并发布，随后放行所有等待者；仅成功回源才计 miss
        try:
            entry = self._fetch_and_publish(key, loader)
            holder.entry = entry
            self._metrics.record_miss()
        except BaseException as exc:  # noqa: BLE001 - 共享给全部等待者
            holder.error = exc
        finally:
            with self._store_lock:
                self._loading.pop(key, None)
                holder.event.set()
        if holder.error is not None:
            raise holder.error
        now = self._clock.now()
        return self._result(
            entry,
            CacheStatus.MISS if entry.missing else CacheStatus.FRESH,
            now,
            from_upstream=True,
        )

    # ============================================================== 过期刷新
    def _refresh_expired(self, key: str, entry: CacheEntry, loader: Loader) -> CacheResult:
        """过期分支：同步等待刷新；失败时回落旧值并打降级标记。"""
        self._metrics.record_synchronous_refresh()
        ticket = self._executor.ticket_for(key)
        if ticket is None:
            ticket = self._executor.submit_if_absent(key, lambda: self._do_refresh(key, loader))
        if ticket is None:  # 竞态：刚刚被别人登记
            ticket = self._executor.ticket_for(key)
        finished = ticket.wait(timeout=self._config.refresh_timeout) if ticket is not None else False
        if not finished:
            # 刷新仍未完成（慢上游）。过期不允许把旧值当有效数据：抛可区分错误。
            raise CacheError(f"refresh timed out: {key}")

        now = self._clock.now()
        with self._store_lock:
            new_entry = self._store.get(key)
        if new_entry is not None and not new_entry.degrades and new_entry is not entry:
            # 刷新成功并发布了全新版本
            self._metrics.record_hit()
            return self._result(new_entry, CacheStatus.FRESH, now)

        # 刷新失败：保留上一次完整结果 + 可区分降级标记，不当成有效新数据。
        # 失败标记可能已由后台任务原子发布到保留版本上，优先采用它。
        degraded_entry = new_entry if new_entry is not None and new_entry.degrades else entry
        self._metrics.record_degrade()
        return self._result(degraded_entry, CacheStatus.DEGRADED, now)

    def _ensure_background_refresh(self, key: str, loader: Loader) -> None:
        existing = self._executor.ticket_for(key)
        if existing is not None:
            return  # 已在途，去重不重复计数
        ticket = self._executor.submit_if_absent(key, lambda: self._do_refresh(key, loader))
        if ticket is not None:
            self._metrics.record_background_refresh()

    def refresh_in_background(self, key: str, loader: Loader) -> bool:
        """显式请求后台刷新；已在途则去重。返回是否由本次发起。"""
        if self._executor.ticket_for(key) is not None:
            return False
        ticket = self._executor.submit_if_absent(key, lambda: self._do_refresh(key, loader))
        if ticket is None:
            return False
        self._metrics.record_background_refresh()
        return True

    def _do_refresh(self, key: str, loader: Loader) -> None:
        """后台 worker / 同步刷新共用的刷新体。

        成功 -> 原子发布新版本；失败 -> 旧条目原样保留（由读者据此降级）。
        任何异常都在此吞掉并计数，绝不让 worker 线程带着半写状态退出。
        """
        try:
            self._fetch_and_publish(key, loader)
        except CacheError as exc:
            self._metrics.record_refresh_failure()
            self._publish_degraded(key, reason=str(exc))
        except Exception as exc:  # noqa: BLE001 - loader 的任何意外都属刷新失败
            self._metrics.record_refresh_failure()
            self._publish_degraded(key, reason=f"{type(exc).__name__}: {exc}")

    def _publish_degraded(self, key: str, *, reason: str) -> None:
        """刷新失败：把上一份完整结果以“带降级标记”的新快照原子重发。

        数据、written_at、ttl、generation 全部不变，只追加可区分标记，
        因此既不伪装成新数据，也不会把旧值变新鲜。
        """
        note = f"refresh failed: {reason}"
        with self._store_lock:
            old = self._store.get(key)
            if old is None or note in old.degrades:
                return
            marked = CacheEntry(
                key=old.key,
                value=old.value,
                written_at=old.written_at,
                ttl=old.ttl,
                stale_window=old.stale_window,
                dependencies=old.dependencies,
                missing=old.missing,
                generation=old.generation,
                degrades=(*old.degrades, note),
            )
            self._store[key] = marked  # 同 key 覆盖：不改变容量与 LRU 淘汰序
            self._deps.set_dependencies(key, marked.dependencies)

    # ============================================================== 回源发布
    def _fetch_and_publish(self, key: str, loader: Loader) -> CacheEntry:
        """调用 loader（锁外，避免慢上游阻塞读），随后在锁内原子发布。"""
        loaded = loader()
        if not isinstance(loaded, Loaded):
            raise TypeError(f"loader for {key} must return Loaded, got {type(loaded)!r}")
        now = self._clock.now()
        with self._store_lock:
            prev = self._store.get(key)
            generation = (prev.generation + 1) if prev is not None else 1
            # 条目级 ttl 覆盖；缺失条目使用短 ttl，且不参与陈旧回落
            ttl = loaded.ttl if loaded.ttl is not None else (
                self._config.missing_ttl if loaded.missing else self._config.ttl
            )
            if ttl <= 0:
                raise ValueError(f"effective ttl must be positive: {key}")
            entry = CacheEntry(
                key=key,
                value=loaded.value,
                written_at=now,
                ttl=ttl,
                stale_window=0.0 if loaded.missing else self._config.stale_window,
                dependencies=tuple(dict.fromkeys(loaded.dependencies)),
                missing=loaded.missing,
                generation=generation,
            )
            self._store_unlocked(key, entry)
            return entry

    # ============================================================== 存储淘汰
    def _store_unlocked(self, key: str, entry: CacheEntry) -> None:
        """调用方持锁。写入 + LRU 更新 + 容量回收（先淘汰过期，再 LRU）。"""
        if key in self._store:
            self._store.pop(key)
        else:
            while len(self._store) >= self._config.capacity:
                evicted_key = self._select_eviction_unlocked()
                if evicted_key is None:  # 容量为 0 的配置在构造期已拒绝
                    raise CacheError(f"capacity exhausted, cannot place {key}")
                self._evict_unlocked(evicted_key)
        self._store[key] = entry
        self._deps.set_dependencies(key, entry.dependencies)

    def _select_eviction_unlocked(self) -> str | None:
        """优先淘汰已过期 / 负缓存条目；否则按 LRU 选最久未访问的 key。
        （写入前若 key 已存在会先被移除，故这里不会选到正在写入的 key。）"""
        now = self._clock.now()
        lru_key: str | None = None
        for k, e in self._store.items():
            if e.is_expired(now):
                return k
            if lru_key is None:
                lru_key = k
        return lru_key

    def _evict_unlocked(self, key: str) -> None:
        self._store.pop(key, None)
        self._deps.remove(key)
        self._metrics.record_eviction()

    def _touch(self, key: str) -> None:
        with self._store_lock:
            if key in self._store:
                self._store.move_to_end(key)

    # ============================================================== 精准失效
    def invalidate_dependency(self, dependency: str) -> int:
        """实体变化时调用：只清除真正依赖它的条目。返回清除条数。"""
        keys = self._deps.keys_depending_on(dependency)
        removed = 0
        with self._store_lock:
            for key in keys:
                if key in self._store:
                    del self._store[key]
                    self._deps.remove(key)
                    removed += 1
        if removed:
            self._metrics.record_invalidation(removed)
        return removed

    def invalidate_key(self, key: str) -> bool:
        with self._store_lock:
            existed = self._store.pop(key, None) is not None
            if existed:
                self._deps.remove(key)
        if existed:
            self._metrics.record_invalidation(1)
        return existed

    # ============================================================== 观测/停止
    def metrics(self) -> MetricsSnapshot:
        return self._metrics.snapshot(in_flight=self._executor.in_flight)

    def size(self) -> int:
        with self._store_lock:
            return len(self._store)

    def contains(self, key: str) -> bool:
        with self._store_lock:
            return key in self._store

    def shutdown(self) -> None:
        """优雅停止：在途刷新原子收尾；超时不阻塞退出，缓存内无半写条目。"""
        self._executor.shutdown(wait=True)

    # ============================================================== 结果组装
    def _result(
        self,
        entry: CacheEntry,
        status: CacheStatus,
        now: float,
        *,
        from_upstream: bool = False,
    ) -> CacheResult:
        return CacheResult(
            key=entry.key,
            value=entry.value,
            status=status,
            age=entry.age(now),
            ttl=entry.ttl,
            stale_window=entry.stale_window,
            degrades=entry.degrades,
            from_upstream=from_upstream,
        )
