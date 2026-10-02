"""Upstream data source: result types, protocol and an injectable mock.

The mock replaces a real upstream (HTTP/RPC) so the whole chain is locally
reproducible. It supports configurable latency, injected failures, malformed
payloads and explicit not-found responses.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any, Collection, Protocol

from .models import FailureReason


@dataclass
class Found:
    """A successful upstream fetch."""

    value: Any
    deps: Collection[str] = field(default_factory=tuple)


@dataclass
class NotFound:
    """The upstream explicitly reports the entity does not exist."""

    deps: Collection[str] = field(default_factory=tuple)


@dataclass
class UpstreamFailure:
    """A distinguishable upstream/transport failure."""

    reason: FailureReason
    message: str = ""


class Upstream(Protocol):
    async def fetch(self, key: str) -> Found | NotFound | UpstreamFailure:
        """Fetch one upstream entity."""
        ...


@dataclass
class _Behavior:
    fail_times: int = 0
    reason: FailureReason = FailureReason.UPSTREAM
    delay: float | None = None
    malformed: bool = False


class MockUpstream:
    """In-memory upstream with injectable latency/faults.

    ``delay`` applies to every fetch; per-key overrides are configured via
    :meth:`configure`. All fetches are recorded in :attr:`calls` for tests.
    """

    def __init__(self, delay: float = 0.0) -> None:
        self.delay = delay
        self._store: dict[str, Any] = {}
        self._behavior: dict[str, _Behavior] = {}
        self.calls: list[tuple[str, float]] = []

    # -- mutation / injection API -----------------------------------------

    def set(self, key: str, value: Any) -> None:
        self._store[key] = value

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def configure(
        self,
        key: str,
        *,
        fail_times: int = 0,
        reason: FailureReason = FailureReason.UPSTREAM,
        delay: float | None = None,
        malformed: bool = False,
    ) -> None:
        self._behavior[key] = _Behavior(
            fail_times=fail_times, reason=reason, delay=delay, malformed=malformed
        )

    def call_count(self, key: str | None = None) -> int:
        if key is None:
            return len(self.calls)
        return sum(1 for k, _ in self.calls if k == key)

    # -- Upstream protocol -------------------------------------------------

    async def fetch(self, key: str) -> Found | NotFound | UpstreamFailure:
        now = time.monotonic()
        self.calls.append((key, now))
        behavior = self._behavior.get(key)
        delay = behavior.delay if behavior and behavior.delay is not None else self.delay
        if delay > 0:
            await asyncio.sleep(delay)

        if behavior is not None:
            if behavior.fail_times > 0:
                behavior.fail_times -= 1
                return UpstreamFailure(reason=behavior.reason, message=f"injected {behavior.reason.value} for {key}")
            if behavior.malformed:
                # Return a payload that cannot be parsed as the expected shape.
                return Found(value='{"broken": ', deps=(key,))

        if key in self._store:
            return Found(value=self._store[key], deps=(key,))
        return NotFound(deps=(key,))


def parse_json_payload(raw: Any) -> Any:
    """Parse a JSON payload, raising :class:`UpstreamError` on malformed data."""
    from .exceptions import UpstreamError

    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError) as exc:
        raise UpstreamError(FailureReason.MALFORMED, f"malformed payload: {exc}") from exc
