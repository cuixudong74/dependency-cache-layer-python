"""有并发上限、按 key 去重、可优雅停止的后台刷新执行器。

- single-flight：提交在锁内原子登记，同一时刻同一 key 至多一个在途 / 排队刷新；
- 有界并发：超过 ``max_workers`` 的任务排队等待，:meth:`cancel_pending` 可取消排队任务；
- 已过期的同步读者通过 :class:`RefreshTicket` 等待对应刷新结束；
- :meth:`shutdown` 拒绝新提交、取消排队任务、在途任务由其自身原子收尾；
  最多等待 ``shutdown_timeout``，超时即返回——worker 为 daemon 线程，不阻塞进程退出；
  缓存中永远只保留某次完整写入的版本，不会出现半个条目。
"""

from __future__ import annotations

import queue
import threading
from typing import Callable, Protocol


class RefreshTicket(Protocol):
    """一次刷新的句柄。"""

    def wait(self, timeout: float | None = None) -> bool:
        """阻塞到刷新结束。True=已完成，False=超时。"""
        ...

    @property
    def cancelled(self) -> bool: ...

    @property
    def key(self) -> str: ...


class _Ticket:
    __slots__ = ("_event", "cancelled", "key", "failed")

    def __init__(self, key: str) -> None:
        self.key = key
        self._event = threading.Event()
        self.cancelled = False
        self.failed = False

    def complete(self) -> None:
        self._event.set()

    def cancel(self) -> None:
        self.cancelled = True
        self._event.set()

    def wait(self, timeout: float | None = None) -> bool:
        return self._event.wait(timeout)


class _Task:
    __slots__ = ("key", "job", "ticket")

    def __init__(self, key: str, job: Callable[[], None], ticket: _Ticket) -> None:
        self.key = key
        self.job = job
        self.ticket = ticket


class BoundedRefreshExecutor:
    def __init__(self, max_workers: int, *, shutdown_timeout: float = 5.0) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be >= 1")
        self._max_workers = max_workers
        self._shutdown_timeout = shutdown_timeout
        self._sem = threading.Semaphore(max_workers)
        self._lock = threading.Lock()
        self._inflight: dict[str, _Ticket] = {}
        self._queued_keys: set[str] = set()
        self._q: queue.Queue[_Task | None] = queue.Queue()
        self._stopping = False
        self._fully_stopped = threading.Event()
        self._active_tasks = 0
        self._alive_workers = max_workers
        self._workers = [
            threading.Thread(target=self._worker_loop, name=f"cache-refresh-{i}", daemon=True)
            for i in range(max_workers)
        ]
        for t in self._workers:
            t.start()

    # ------------------------------------------------------------------ 提交
    def submit_if_absent(self, key: str, job: Callable[[], None]) -> RefreshTicket | None:
        """原子登记并提交。返回 ticket 表示本次发起；None 表示已有在途 / 排队任务或正在停止。"""
        with self._lock:
            if self._stopping or key in self._inflight:
                return None
            ticket = _Ticket(key)
            self._inflight[key] = ticket
            self._queued_keys.add(key)
        self._q.put(_Task(key, job, ticket))
        return ticket

    def ticket_for(self, key: str) -> RefreshTicket | None:
        with self._lock:
            return self._inflight.get(key)

    def cancel_pending(self) -> int:
        """取消所有排队中（尚未开始执行）的任务，返回取消条数。在途任务不受影响。"""
        drained: list[_Task | None] = []
        while True:
            try:
                drained.append(self._q.get_nowait())
            except queue.Empty:
                break
        cancelled = 0
        for task in drained:
            if task is None:
                self._q.put(None)  # 停止哨兵不能丢
                continue
            # worker 取出任务会先把 key 移出 _queued_keys；移不出说明已在执行
            if not self._try_cancel_queued(task):
                self._q.put(task)
            else:
                cancelled += 1
        return cancelled

    def _try_cancel_queued(self, task: _Task) -> bool:
        with self._lock:
            if task.key not in self._queued_keys:
                return False  # 已被 worker 取走
            self._queued_keys.discard(task.key)
            if self._inflight.get(task.key) is task.ticket:
                del self._inflight[task.key]
        task.ticket.cancel()
        return True

    # ------------------------------------------------------------------ 观测
    @property
    def in_flight(self) -> int:
        with self._lock:
            return self._active_tasks

    @property
    def queued(self) -> int:
        with self._lock:
            return len(self._queued_keys)

    # ------------------------------------------------------------------ 停止
    def shutdown(self, wait: bool = True) -> None:
        with self._lock:
            if self._stopping:
                return
            self._stopping = True

        # 取消全部排队任务
        while True:
            try:
                task = self._q.get_nowait()
            except queue.Empty:
                break
            if task is None:
                self._q.put(None)
                break
            with self._lock:
                self._queued_keys.discard(task.key)
                if self._inflight.get(task.key) is task.ticket:
                    del self._inflight[task.key]
            task.ticket.cancel()

        for _ in self._workers:
            self._q.put(None)
        if wait:
            self._fully_stopped.wait(timeout=self._shutdown_timeout)

    # ------------------------------------------------------------------ 内部
    def _worker_loop(self) -> None:
        while True:
            task = self._q.get()
            if task is None:
                with self._lock:
                    self._alive_workers -= 1
                    fully = self._alive_workers == 0
                if fully:
                    self._fully_stopped.set()
                return
            with self._lock:
                self._queued_keys.discard(task.key)
                self._active_tasks += 1
            self._sem.acquire()
            try:
                if not task.ticket.cancelled:
                    try:
                        task.job()
                    except BaseException:  # noqa: BLE001 - job 自身负责记录；线程不能死
                        task.ticket.failed = True
                    finally:
                        task.ticket.complete()
            finally:
                self._sem.release()
                with self._lock:
                    self._active_tasks -= 1
                    if self._inflight.get(task.key) is task.ticket:
                        del self._inflight[task.key]
