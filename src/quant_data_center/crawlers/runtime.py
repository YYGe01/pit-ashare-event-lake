"""Runtime helpers shared by crawler sources."""

from __future__ import annotations

import time


class CrawlSourceTimeoutError(TimeoutError):
    """Raised when a crawler source exceeds its configured source deadline."""


def make_deadline(timeout_seconds: float | int | None) -> float | None:
    if timeout_seconds is None:
        return None
    timeout = float(timeout_seconds)
    if timeout <= 0:
        return None
    return time.monotonic() + timeout


def raise_if_deadline_exceeded(deadline: float | None, *, source_id: str) -> None:
    if deadline is not None and time.monotonic() >= deadline:
        raise CrawlSourceTimeoutError(f"source timeout exceeded for {source_id}")


def request_timeout(
    *,
    deadline: float | None,
    default_seconds: float | int,
    source_id: str,
) -> float:
    timeout = float(default_seconds)
    if timeout <= 0:
        raise ValueError("request timeout seconds must be positive")
    if deadline is None:
        return timeout
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise CrawlSourceTimeoutError(f"source timeout exceeded for {source_id}")
    return max(0.001, min(timeout, remaining))


def sleep_with_deadline(
    seconds: float,
    *,
    deadline: float | None,
    source_id: str,
) -> None:
    if seconds <= 0:
        return
    if deadline is None:
        time.sleep(seconds)
        return
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise CrawlSourceTimeoutError(f"source timeout exceeded for {source_id}")
    time.sleep(min(seconds, remaining))
