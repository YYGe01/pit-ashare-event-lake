"""Date-window scan helpers for rolling document list sources."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Callable


DEFAULT_ROLLING_SCAN_HARD_MAX_PAGES = 200
DEFAULT_ROLLING_SCAN_LOOKAHEAD_PAGES = 2


@dataclass
class PageScanResult:
    """Per-page date-window classification summary."""

    target_rows: list[dict[str, Any]]
    metrics: dict[str, Any]


class RollingDateWindowScanner:
    """Track a descending rolling-list scan until one target date is covered."""

    def __init__(
        self,
        *,
        target_date: str,
        max_pages: int | None = None,
        hard_max_pages: int = DEFAULT_ROLLING_SCAN_HARD_MAX_PAGES,
        lookahead_pages_after_older: int = DEFAULT_ROLLING_SCAN_LOOKAHEAD_PAGES,
    ) -> None:
        self.target_date = target_date
        self.target_start = datetime.combine(date.fromisoformat(target_date), time.min)
        self.target_end = self.target_start + timedelta(days=1)
        self.page_limit = max_pages if max_pages is not None else hard_max_pages
        self.page_limit_kind = "max_pages" if max_pages is not None else "hard_max_pages"
        self.lookahead_pages_after_older = max(1, lookahead_pages_after_older)
        self.page_count_scanned = 0
        self.newer_skipped_count = 0
        self.target_provider_record_count = 0
        self.older_seen_count = 0
        self.unknown_date_count = 0
        self._saw_target_date = False
        self._older_pages_after_target = 0
        self.date_scan_complete = False
        self.stop_reason: str | None = None

    def scan_page(
        self,
        *,
        page_num: int,
        rows: list[dict[str, Any]],
        publish_time_getter: Callable[[dict[str, Any]], Any],
    ) -> PageScanResult:
        self.page_count_scanned += 1
        target_rows: list[dict[str, Any]] = []
        newer_count = 0
        target_count = 0
        older_count = 0
        unknown_count = 0
        dated_values: list[datetime] = []

        for row in rows:
            publish_time = parse_publish_datetime(publish_time_getter(row))
            if publish_time is None:
                unknown_count += 1
                continue
            dated_values.append(publish_time)
            if publish_time >= self.target_end:
                newer_count += 1
            elif publish_time >= self.target_start:
                target_count += 1
                target_rows.append(row)
            else:
                older_count += 1

        self.newer_skipped_count += newer_count
        self.target_provider_record_count += target_count
        self.older_seen_count += older_count
        self.unknown_date_count += unknown_count
        if target_count:
            self._saw_target_date = True
            self._older_pages_after_target = 0

        page_max_time = max(dated_values) if dated_values else None
        page_min_time = min(dated_values) if dated_values else None
        page_is_older_than_target = page_max_time is not None and page_max_time < self.target_start
        if not rows:
            self.date_scan_complete = True
            self.stop_reason = "empty_page"
        elif page_is_older_than_target:
            if self._saw_target_date:
                self._older_pages_after_target += 1
                if self._older_pages_after_target >= self.lookahead_pages_after_older:
                    self.date_scan_complete = True
                    self.stop_reason = "older_page_lookahead"
            else:
                self.date_scan_complete = True
                self.stop_reason = "older_than_target_without_target"

        return PageScanResult(
            target_rows=target_rows,
            metrics={
                "page_num": page_num,
                "item_count": len(rows),
                "newer_skipped_count": newer_count,
                "target_record_count": target_count,
                "older_seen_count": older_count,
                "unknown_date_count": unknown_count,
                "page_min_publish_time": _format_datetime(page_min_time),
                "page_max_publish_time": _format_datetime(page_max_time),
                "page_is_older_than_target": page_is_older_than_target,
            },
        )

    def should_continue(self, *, next_page_num: int) -> bool:
        if self.date_scan_complete:
            return False
        if next_page_num > self.page_limit:
            self.stop_reason = self.page_limit_kind
            self.date_scan_complete = False
            return False
        return True

    def manifest_fields(self) -> dict[str, Any]:
        return {
            "date_scan_strategy": "rolling_desc_publish_time_window",
            "target_date": self.target_date,
            "target_start": self.target_start.isoformat(sep=" "),
            "target_end_exclusive": self.target_end.isoformat(sep=" "),
            "date_scan_complete": self.date_scan_complete,
            "date_scan_stop_reason": self.stop_reason,
            "date_scan_page_limit": self.page_limit,
            "date_scan_page_limit_kind": self.page_limit_kind,
            "date_scan_lookahead_pages_after_older": self.lookahead_pages_after_older,
            "date_scan_pages_scanned": self.page_count_scanned,
            "newer_skipped_count": self.newer_skipped_count,
            "target_provider_record_count": self.target_provider_record_count,
            "older_seen_count": self.older_seen_count,
            "unknown_date_count": self.unknown_date_count,
        }


def exact_date_query_scan_fields(
    *,
    target_date: str,
    page_count_scanned: int,
    source_reported_page_count: int | None,
    max_pages: int | None,
    provider_record_count: int,
) -> dict[str, Any]:
    """Build manifest fields for sources that accept an exact date query."""

    truncated_by_max_pages = (
        max_pages is not None
        and source_reported_page_count is not None
        and max_pages < source_reported_page_count
    )
    return {
        "date_scan_strategy": "source_exact_date_query",
        "target_date": target_date,
        "date_scan_complete": not truncated_by_max_pages,
        "date_scan_stop_reason": "max_pages"
        if truncated_by_max_pages
        else "source_page_count_exhausted",
        "date_scan_pages_scanned": page_count_scanned,
        "source_reported_page_count": source_reported_page_count,
        "date_scan_page_limit": max_pages,
        "date_scan_page_limit_kind": "max_pages" if max_pages is not None else None,
        "target_provider_record_count": provider_record_count,
        "provider_record_count": provider_record_count,
    }


def parse_publish_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.replace(microsecond=0)
    if isinstance(value, date):
        return datetime.combine(value, time.min)
    text = str(value).strip().replace("T", " ")
    if not text:
        return None
    if text.isdigit() and len(text) in {10, 13}:
        timestamp = float(text)
        if timestamp > 10_000_000_000:
            timestamp = timestamp / 1000
        return datetime.fromtimestamp(timestamp).replace(microsecond=0)
    for fmt, width in (
        ("%Y-%m-%d %H:%M:%S", 19),
        ("%Y-%m-%d %H:%M", 16),
        ("%Y-%m-%d", 10),
    ):
        try:
            return datetime.strptime(text[:width], fmt).replace(microsecond=0)
        except ValueError:
            continue
    return None


def _format_datetime(value: datetime | None) -> str | None:
    return value.isoformat(sep=" ") if value else None
