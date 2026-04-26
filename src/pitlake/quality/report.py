"""Daily quality report helpers."""

from __future__ import annotations

from collections import Counter
from typing import Any


def summarize_quality_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    by_status = Counter(result["status"] for result in results)
    by_severity = Counter(result["severity"] for result in results)
    return {
        "check_count": len(results),
        "by_status": dict(by_status),
        "by_severity": dict(by_severity),
    }

