"""Exceptions raised by loaders when an upstream fetch fails."""

from __future__ import annotations


class UpstreamError(Exception):
    """Raised when an upstream fetch fails for a distinguishable reason.

    ``reason`` is one of the :class:`~cache_layer.models.FailureReason` values.
    Failures are never cached as valid data.
    """

    def __init__(self, reason: str, message: str = "") -> None:
        super().__init__(message or reason)
        self.reason = reason
        self.message = message or reason
