"""Date-window scan helpers for rolling document list sources."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Callable


DEFAULT_ROLLING_SCAN_HARD_MAX_PAGES = 200
DEFAULT_ROLLING_SCAN_LOOKAHEAD_PAGES = 2
DEFAULT_ROLLING_SCAN_BOUNDARY_LOOKBACK_PAGES = 2


@dataclass
class PageScanResult:
    """Per-page date-window classification summary."""

    target_rows: list[dict[str, Any]]
    metrics: dict[str, Any]


@dataclass
class RollingFetchedPage:
    """One fetched rolling-list page plus its date-window classification."""

    page_num: int
    rows: list[dict[str, Any]]
    page: dict[str, Any]
    scan_result: PageScanResult


@dataclass
class RollingDateWindowScan:
    """Optimized rolling-list scan result."""

    pages: list[dict[str, Any]]
    provider_rows: list[dict[str, Any]]
    target_rows: list[dict[str, Any]]
    manifest_fields: dict[str, Any]


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
        page_scan = classify_rolling_page(
            target_date=self.target_date,
            rows=rows,
            publish_time_getter=publish_time_getter,
        )
        newer_count = int(page_scan.metrics["newer_skipped_count"])
        target_count = int(page_scan.metrics["target_record_count"])
        older_count = int(page_scan.metrics["older_seen_count"])
        unknown_count = int(page_scan.metrics["unknown_date_count"])

        self.newer_skipped_count += newer_count
        self.target_provider_record_count += target_count
        self.older_seen_count += older_count
        self.unknown_date_count += unknown_count
        if target_count:
            self._saw_target_date = True
            self._older_pages_after_target = 0

        page_is_older_than_target = bool(page_scan.metrics["page_is_older_than_target"])
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

        page_scan.metrics["page_num"] = page_num
        return page_scan

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


def scan_rolling_date_window(
    *,
    target_date: str,
    fetch_page: Callable[[int], tuple[list[dict[str, Any]], dict[str, Any]]],
    publish_time_getter: Callable[[dict[str, Any]], Any],
    record_key_getter: Callable[[dict[str, Any]], Any] | None = None,
    seen_record_keys: set[str] | None = None,
    max_pages: int | None = None,
    hard_max_pages: int = DEFAULT_ROLLING_SCAN_HARD_MAX_PAGES,
    lookahead_pages_after_older: int = DEFAULT_ROLLING_SCAN_LOOKAHEAD_PAGES,
    boundary_lookback_pages: int = DEFAULT_ROLLING_SCAN_BOUNDARY_LOOKBACK_PAGES,
    before_fetch: Callable[[], None] | None = None,
) -> RollingDateWindowScan:
    """Fetch a descending rolling list with exponential probing and local scan.

    The source must support random page access. Probe pages locate the first page
    that is not wholly newer than the target date, then a sequential scan covers
    the target date and a small older-page lookahead.
    """

    page_limit = max_pages if max_pages is not None else hard_max_pages
    page_limit_kind = "max_pages" if max_pages is not None else "hard_max_pages"
    lookahead = max(1, lookahead_pages_after_older)
    boundary_lookback = max(0, boundary_lookback_pages)
    target_start = datetime.combine(date.fromisoformat(target_date), time.min)
    target_end = target_start + timedelta(days=1)
    fetched_pages: dict[int, RollingFetchedPage] = {}
    fetch_order: list[int] = []

    def get_page(page_num: int) -> RollingFetchedPage | None:
        if page_num < 1 or page_num > page_limit:
            return None
        if page_num in fetched_pages:
            return fetched_pages[page_num]
        if fetch_order and before_fetch is not None:
            before_fetch()
        rows, page = fetch_page(page_num)
        page_scan = classify_rolling_page(
            target_date=target_date,
            rows=rows,
            publish_time_getter=publish_time_getter,
        )
        page_payload = dict(page)
        page_payload.update(page_scan.metrics)
        page_payload.setdefault("page_num", page_num)
        page_payload.setdefault("items", rows)
        fetched = RollingFetchedPage(
            page_num=page_num,
            rows=rows,
            page=page_payload,
            scan_result=page_scan,
        )
        fetched_pages[page_num] = fetched
        fetch_order.append(page_num)
        return fetched

    first = get_page(1)
    final_start_page = 1
    stop_reason: str | None = None
    complete = False
    target_rows: list[dict[str, Any]] = []
    sequential_page_nums: list[int] = []
    seen_keys = {str(value).strip() for value in seen_record_keys or set() if str(value).strip()}
    cursor_stop_page: int | None = None
    cursor_stop_key: str | None = None
    cursor_seen_record_count = 0

    if first is None:
        stop_reason = page_limit_kind
    elif not first.rows:
        complete = True
        stop_reason = "empty_page"
    elif _page_wholly_newer(first):
        lower_newer = 1
        upper_not_newer: int | None = None
        probe_page = 2
        while probe_page <= page_limit:
            fetched = get_page(probe_page)
            if fetched is None:
                break
            if not _page_wholly_newer(fetched):
                upper_not_newer = probe_page
                break
            lower_newer = probe_page
            probe_page *= 2

        if upper_not_newer is None:
            stop_reason = page_limit_kind
        else:
            while upper_not_newer - lower_newer > 1:
                midpoint = (lower_newer + upper_not_newer) // 2
                fetched = get_page(midpoint)
                if fetched is not None and _page_wholly_newer(fetched):
                    lower_newer = midpoint
                else:
                    upper_not_newer = midpoint
            final_start_page = max(1, upper_not_newer - boundary_lookback)

    if stop_reason is None:
        saw_target = False
        older_pages_after_target = 0
        page_num = final_start_page
        while page_num <= page_limit:
            fetched = get_page(page_num)
            if fetched is None:
                stop_reason = page_limit_kind
                break
            sequential_page_nums.append(page_num)
            metrics = fetched.scan_result.metrics
            first_seen_index, first_seen_key, seen_count = _first_seen_record_index(
                fetched.rows,
                record_key_getter=record_key_getter,
                seen_record_keys=seen_keys,
            )
            if first_seen_index is not None:
                cursor_stop_page = page_num
                cursor_stop_key = first_seen_key
                cursor_seen_record_count += seen_count
                effective_rows = fetched.rows[:first_seen_index]
                effective_scan = classify_rolling_page(
                    target_date=target_date,
                    rows=effective_rows,
                    publish_time_getter=publish_time_getter,
                )
                target_rows.extend(effective_scan.target_rows)
                complete = True
                stop_reason = "cursor_seen"
                break
            page_target_count = int(metrics["target_record_count"])
            if not fetched.rows:
                complete = True
                stop_reason = "empty_page"
                break
            if page_target_count:
                saw_target = True
                older_pages_after_target = 0
                target_rows.extend(fetched.scan_result.target_rows)
            elif metrics["page_is_older_than_target"]:
                if saw_target:
                    older_pages_after_target += 1
                    if older_pages_after_target >= lookahead:
                        complete = True
                        stop_reason = "older_page_lookahead"
                        break
                else:
                    complete = True
                    stop_reason = "older_than_target_without_target"
                    break
            page_num += 1
        if stop_reason is None:
            stop_reason = page_limit_kind

    aggregate_metrics = _aggregate_page_metrics(fetched_pages.values())
    pages = [fetched_pages[page_num].page for page_num in sorted(fetched_pages)]
    provider_rows = [
        row
        for page_num in sorted(fetched_pages)
        for row in fetched_pages[page_num].rows
    ]
    manifest = {
        "date_scan_strategy": "rolling_desc_publish_time_window_fast_seek",
        "target_date": target_date,
        "target_start": target_start.isoformat(sep=" "),
        "target_end_exclusive": target_end.isoformat(sep=" "),
        "date_scan_complete": complete,
        "date_scan_stop_reason": stop_reason,
        "date_scan_page_limit": page_limit,
        "date_scan_page_limit_kind": page_limit_kind,
        "date_scan_lookahead_pages_after_older": lookahead,
        "date_scan_boundary_lookback_pages": boundary_lookback,
        "date_scan_pages_scanned": len(fetched_pages),
        "date_scan_fetch_strategy": "exponential_probe_binary_seek_then_sequential_window",
        "date_scan_probe_pages": [page for page in fetch_order if page not in sequential_page_nums],
        "date_scan_sequential_start_page": final_start_page,
        "date_scan_sequential_pages": sequential_page_nums,
        "date_scan_first_target_page": _first_target_page(fetched_pages),
        "date_scan_last_target_page": _last_target_page(fetched_pages),
        "incremental_cursor_enabled": bool(seen_keys),
        "incremental_cursor_seen_record_count": cursor_seen_record_count,
        "incremental_cursor_stop_page": cursor_stop_page,
        "incremental_cursor_stop_key": cursor_stop_key,
        **aggregate_metrics,
    }
    if cursor_stop_page is not None:
        manifest["target_provider_record_count"] = len(target_rows)
    return RollingDateWindowScan(
        pages=pages,
        provider_rows=provider_rows,
        target_rows=target_rows,
        manifest_fields=manifest,
    )


def classify_rolling_page(
    *,
    target_date: str,
    rows: list[dict[str, Any]],
    publish_time_getter: Callable[[dict[str, Any]], Any],
) -> PageScanResult:
    target_start = datetime.combine(date.fromisoformat(target_date), time.min)
    target_end = target_start + timedelta(days=1)
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
        if publish_time >= target_end:
            newer_count += 1
        elif publish_time >= target_start:
            target_count += 1
            target_rows.append(row)
        else:
            older_count += 1
    page_max_time = max(dated_values) if dated_values else None
    page_min_time = min(dated_values) if dated_values else None
    page_is_older_than_target = page_max_time is not None and page_max_time < target_start
    return PageScanResult(
        target_rows=target_rows,
        metrics={
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


def _page_wholly_newer(page: RollingFetchedPage) -> bool:
    metrics = page.scan_result.metrics
    dated_count = (
        int(metrics["newer_skipped_count"])
        + int(metrics["target_record_count"])
        + int(metrics["older_seen_count"])
    )
    return (
        bool(page.rows)
        and dated_count > 0
        and int(metrics["target_record_count"]) == 0
        and int(metrics["older_seen_count"]) == 0
        and int(metrics["newer_skipped_count"]) == dated_count
    )


def _aggregate_page_metrics(pages: Any) -> dict[str, int]:
    metrics = {
        "newer_skipped_count": 0,
        "target_provider_record_count": 0,
        "older_seen_count": 0,
        "unknown_date_count": 0,
    }
    for page in pages:
        page_metrics = page.scan_result.metrics
        metrics["newer_skipped_count"] += int(page_metrics["newer_skipped_count"])
        metrics["target_provider_record_count"] += int(page_metrics["target_record_count"])
        metrics["older_seen_count"] += int(page_metrics["older_seen_count"])
        metrics["unknown_date_count"] += int(page_metrics["unknown_date_count"])
    return metrics


def _first_seen_record_index(
    rows: list[dict[str, Any]],
    *,
    record_key_getter: Callable[[dict[str, Any]], Any] | None,
    seen_record_keys: set[str],
) -> tuple[int | None, str | None, int]:
    if record_key_getter is None or not seen_record_keys:
        return None, None, 0
    first_index: int | None = None
    first_key: str | None = None
    seen_count = 0
    for index, row in enumerate(rows):
        key = _clean_record_key(record_key_getter(row))
        if key and key in seen_record_keys:
            seen_count += 1
            if first_index is None:
                first_index = index
                first_key = key
    return first_index, first_key, seen_count


def _clean_record_key(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _first_target_page(pages: dict[int, RollingFetchedPage]) -> int | None:
    target_pages = [
        page_num
        for page_num, page in pages.items()
        if int(page.scan_result.metrics["target_record_count"]) > 0
    ]
    return min(target_pages) if target_pages else None


def _last_target_page(pages: dict[int, RollingFetchedPage]) -> int | None:
    target_pages = [
        page_num
        for page_num, page in pages.items()
        if int(page.scan_result.metrics["target_record_count"]) > 0
    ]
    return max(target_pages) if target_pages else None


def _format_datetime(value: datetime | None) -> str | None:
    return value.isoformat(sep=" ") if value else None
