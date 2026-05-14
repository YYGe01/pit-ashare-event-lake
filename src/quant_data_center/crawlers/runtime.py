"""Runtime helpers shared by crawler sources."""

from __future__ import annotations

from contextlib import contextmanager
import os
import threading
import time
from typing import Any, Callable


class CrawlSourceTimeoutError(TimeoutError):
    """Raised when a crawler source exceeds its configured source deadline."""


PROXY_ENV_VARS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
)
_PROXY_ENV_LOCK = threading.RLock()


@contextmanager
def environment_proxy_scope(*, use_environment_proxy: bool):
    """Temporarily disable requests-compatible environment proxies."""

    if use_environment_proxy:
        yield
        return
    with _PROXY_ENV_LOCK:
        saved = {name: os.environ.get(name) for name in PROXY_ENV_VARS}
        for name in PROXY_ENV_VARS:
            os.environ.pop(name, None)
        try:
            yield
        finally:
            for name, value in saved.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


def call_with_proxy_policy(
    function: Callable[..., Any],
    *args: Any,
    use_environment_proxy: bool,
    **kwargs: Any,
) -> Any:
    """Call an HTTP/provider function under the configured proxy policy."""

    with environment_proxy_scope(use_environment_proxy=use_environment_proxy):
        return function(*args, **kwargs)


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
