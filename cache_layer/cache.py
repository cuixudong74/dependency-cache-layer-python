"""Dependency-aware cache access layer.

Implements freshness tiers (fresh / stale / expired), stale-while-revalidate
with background refresh, precise dependency invalidation, atomic versioned
writes, negative caching for not-found, bounded LRU capacity, metrics and
graceful shutdown.
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Collection

from .clock import Clock, SystemClock
from .config import CacheConfig
from .exceptions import UpstreamError
from .metrics import Metrics
from .models import (
    CacheEntry,
    FailureReason,
    Freshness,
    LoadResult,
    MISSING,
    ReadResult,
    ReadStatus,
)

Loader = Callable[[], Awaitable[LoadResult]]


def _to_failure_reason(reason: FailureReason | str) -> FailureReason:
    if isinstance(reason, FailureReason):
        return reason
    try:
        return FailureReason(reason)
    except ValueError:
        return FailureReason.UPSTREAM


class CacheLayer:
    """An async, dependency-aware cache sitting in front of an upstream.

    Concurrency model: per-key asyncio locks serialize writers/readers of the
    same key; entries are replaced by swapping a fully constructed
    :class:`CacheEntry` reference, so a reader never observes a half-written
    version. Background refreshes are bounded by a semaphore and tracked so
    :meth:`shutdown` can cancel them.

    Capacity is enforced under a single structural lock shared with a
    condition variable. Entries with a refresh in flight are protected from
    eviction until that refresh completes and makes them evictable again; an
    over-capacity insert that finds every entry protected briefly waits on the
    condition instead of discarding the entry it just wrote, so the cached
    count never exceeds ``capacity`` in any interleaving.
    """

    def __init__(self, config: CacheConfig, clock: Clock | None = None) -> None:
        self._config = config
        self._clock = clock or SystemClock()
        self._metrics = Metrics()
        self._entries: dict[str, CacheEntry] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._refreshing: dict[str, asyncio.Task] = {}
        self._semaphore = asyncio.Semaphore(config.max_refresh_concurrency)
        self._master = asyncio.Lock()
        # Bound to ``self._master``: signalled whenever a background refresh
        # finishes and its key becomes evictable again, so over-capacity
        # inserts can wait for a reclaimable slot instead of evicting a
        # just-inserted entry.
        self._capacity_cond = asyncio.Condition(self._master)
        self._shutting_down = False

    # -- public API --------------------------------------------------------

    async def get(self, key: str, loader: Loader) -> ReadResult:
        """Read ``key``, serving from cache or refreshing via ``loader``.

        The loader is only called when a refresh is required; it must return a
        :class:`LoadResult` or raise :class:`UpstreamError` on failure.
        """
        if self._shutting_down:
            raise RuntimeError("cache layer is shut down")

        lock = await self._lock_for(key)
        async with lock:
            entry = self._entries.get(key)
            now = self._clock.monotonic()

            if entry is not None:
                age = now - entry.created_at
                ttl = (
                    self._config.negative_ttl
                    if entry.is_missing
                    else self._config.fresh_ttl
                )

                # Fresh (negative entries are fresh within negative_ttl).
                if age <= ttl:
                    entry.last_access = now
                    if entry.is_missing:
                        self._metrics.negative_hits += 1
                        return ReadResult(
                            status=ReadStatus.NEGATIVE,
                            found=False,
                            missing=True,
                            freshness=Freshness.FRESH,
                            version=entry.version,
                        )
                    self._metrics.hits += 1
                    return ReadResult(
                        status=ReadStatus.HIT,
                        value=entry.value,
                        found=True,
                        freshness=Freshness.FRESH,
                        version=entry.version,
                    )

                # Stale window: return the previous complete result and kick
                # off a background refresh (deduped per key).
                if not entry.is_missing and age <= (
                    self._config.fresh_ttl + self._config.stale_ttl
                ):
                    entry.last_access = now
                    await self._spawn_background_refresh(key, loader)
                    if entry.refresh_failed:
                        self._metrics.degraded_returns += 1
                        return ReadResult(
                            status=ReadStatus.DEGRADED,
                            value=entry.value,
                            found=True,
                            degraded=True,
                            freshness=Freshness.STALE,
                            failure_reason=entry.failure_reason,
                            version=entry.version,
                        )
                    self._metrics.stale_returns += 1
                    return ReadResult(
                        status=ReadStatus.STALE,
                        value=entry.value,
                        found=True,
                        freshness=Freshness.STALE,
                        version=entry.version,
                    )

            # Miss, expired, or expired-negative: must wait for refresh.
            return await self._refresh_locked(key, loader)

    async def invalidate(self, entity: str) -> int:
        """Invalidate every cached entry that depends on ``entity``.

        Entries not depending on the entity are untouched. Returns the number
        of invalidated entries.
        """
        async with self._master:
            targets = [k for k, e in self._entries.items() if entity in e.deps]
            for k in targets:
                self._entries.pop(k, None)
                if k not in self._refreshing:
                    self._locks.pop(k, None)
            self._metrics.invalidations += len(targets)
            if targets:
                self._capacity_cond.notify_all()
            return len(targets)

    async def invalidate_key(self, key: str) -> bool:
        """Invalidate a single cache key directly."""
        async with self._master:
            existed = self._entries.pop(key, None) is not None
            if key not in self._refreshing:
                self._locks.pop(key, None)
            if existed:
                self._metrics.invalidations += 1
                self._capacity_cond.notify_all()
            return existed

    async def shutdown(self, timeout: float | None = None) -> None:
        """Cancel in-flight background refreshes and wait briefly.

        Cancelled refreshes never write entries (the swap happens only after a
        loader completes), so no half-written entry is left behind. Pending
        tasks after ``timeout`` are abandoned without blocking exit.
        """
        if self._shutting_down:
            return

        async with self._master:
            self._shutting_down = True
            tasks = [t for t in self._refreshing.values() if t is not None]
            for task in tasks:
                task.cancel()
            # Release any foreground insert waiting for a reclaimable slot so
            # shutdown cannot be blocked by the capacity condition.
            self._capacity_cond.notify_all()

        if tasks:
            await asyncio.wait(tasks, timeout=timeout or self._config.shutdown_timeout)
        pending = [t for t in tasks if not t.done()]
        if pending:
            self._metrics.shutdown_cancelled += len(pending)
        self._refreshing.clear()

    def metrics(self) -> Metrics:
        return self._metrics.snapshot()

    def __len__(self) -> int:
        return len(self._entries)

    # -- internals ----------------------------------------------------------

    async def _lock_for(self, key: str) -> asyncio.Lock:
        async with self._master:
            lock = self._locks.get(key)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[key] = lock
            return lock

    async def _spawn_background_refresh(self, key: str, loader: Loader) -> None:
        async with self._master:
            if key in self._refreshing:
                return  # a refresh for this key is already in flight
            self._metrics.background_refresh_started += 1
            task = asyncio.create_task(self._background_refresh(key, loader))
            self._refreshing[key] = task
            task.add_done_callback(self._on_refresh_done)

    async def _background_refresh(self, key: str, loader: Loader) -> None:
        try:
            async with self._semaphore:
                lock = self._locks.get(key)
                if lock is None:
                    return
                async with lock:
                    await self._refresh_locked(key, loader)
        except asyncio.CancelledError:
            # Shutdown: entry is untouched (swap only happens on success).
            raise
        except Exception:
            # Unexpected failure: do not touch the existing entry.
            self._metrics.refresh_failures += 1
        finally:
            # The refresh has ended one way or another: the entry (if any) is
            # a complete, coherent version again and may be chosen as an LRU
            # victim. Wake an over-capacity insert waiting for a slot.
            async with self._master:
                self._refreshing.pop(key, None)
                self._capacity_cond.notify_all()

    def _on_refresh_done(self, task: asyncio.Task) -> None:
        if task.cancelled() and self._shutting_down:
            self._metrics.shutdown_cancelled += 1

    async def _refresh_locked(self, key: str, loader: Loader) -> ReadResult:
        """Run a loader and atomically swap the entry. Caller holds key lock."""
        try:
            loaded = await loader()
        except asyncio.CancelledError:
            raise
        except UpstreamError as exc:
            reason = _to_failure_reason(exc.reason)
            self._metrics.refresh_failures += 1
            entry = self._entries.get(key)
            if entry is not None and not entry.is_missing:
                # Keep the previous complete result, marked degraded.
                entry.refresh_failed = True
                entry.failure_reason = reason
                entry.last_access = self._clock.monotonic()
                self._metrics.degraded_returns += 1
                return ReadResult(
                    status=ReadStatus.DEGRADED,
                    value=entry.value,
                    found=True,
                    degraded=True,
                    freshness=Freshness.EXPIRED,
                    failure_reason=reason,
                    version=entry.version,
                )
            # No usable previous value: a failure is never cached as data.
            self._metrics.misses += 1
            return ReadResult(
                status=ReadStatus.MISS,
                found=False,
                degraded=True,
                failure_reason=reason,
            )
        else:
            now = self._clock.monotonic()
            new_entry = CacheEntry(
                key=key,
                value=MISSING if loaded.missing else loaded.value,
                is_missing=loaded.missing,
                created_at=now,
                last_access=now,
                deps=frozenset(loaded.deps),
                version=0,  # assigned below under the master lock
            )
            # Install the fully constructed entry and enforce capacity as one
            # atomic step. If the cache is full of entries whose refresh is in
            # flight (and therefore protected), wait for one of them to become
            # evictable again rather than discarding this just-written entry.
            async with self._capacity_cond:
                old = self._entries.get(key)
                new_entry.version = (old.version + 1) if old is not None else 1
                had_entry = old is not None
                if not had_entry:
                    while (
                        not self._shutting_down
                        and len(self._entries) >= self._config.capacity
                        and not self._has_evictable_locked(key)
                    ):
                        await self._capacity_cond.wait()
                self._entries[key] = new_entry
                self._evict_if_needed_locked()
            self._metrics.refresh_success += 1

            if loaded.missing:
                self._metrics.misses += 1
                return ReadResult(
                    status=ReadStatus.MISS,
                    found=False,
                    missing=True,
                    freshness=Freshness.FRESH,
                    version=new_entry.version,
                )
            if had_entry:
                self._metrics.hits += 1
            else:
                self._metrics.misses += 1
            return ReadResult(
                status=ReadStatus.HIT,
                value=new_entry.value,
                found=True,
                freshness=Freshness.FRESH,
                version=new_entry.version,
            )

    def _has_evictable_locked(self, protected_key: str | None = None) -> bool:
        """Whether any over-capacity slot can be reclaimed right now.

        ``protected_key`` is the key being inserted: it is not yet in
        ``_entries`` (or is being replaced in place), so it never counts as a
        victim of its own write. Caller holds ``self._master``.
        """
        return any(
            k != protected_key and k not in self._refreshing
            for k in self._entries
        )

    def _evict_if_needed_locked(self) -> None:
        """Evict least-recently-used entries until within capacity.

        Only entries without a refresh in flight are eligible: they are the
        only "reclaimable" entries. The caller has already reserved enough
        reclaimable slots (waiting on :attr:`_capacity_cond` when the cache
        was momentarily full of refreshing entries), so this always shrinks
        the cache back to ``capacity``. Caller holds ``self._master``.
        """
        while len(self._entries) > self._config.capacity:
            candidates = [
                (e.last_access, k)
                for k, e in self._entries.items()
                if k not in self._refreshing
            ]
            if not candidates:  # pragma: no cover - slots are reserved above
                break
            candidates.sort(key=lambda item: (item[0], item[1]))
            _, victim = candidates[0]
            self._entries.pop(victim, None)
            if victim not in self._refreshing:
                self._locks.pop(victim, None)
            self._metrics.evictions += 1
