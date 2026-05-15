"""CNINFO investor-interaction metadata crawler."""

from __future__ import annotations

import json
import math
import os
import re
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from quant_data_center.crawlers.metrics import build_document_source_metrics
from quant_data_center.crawlers.runtime import (
    call_with_proxy_policy,
    make_deadline,
    raise_if_deadline_exceeded,
    request_timeout,
    sleep_with_deadline,
)
from quant_data_center.factor_engine.text_events import (
    RISK_EVENTS,
    RuleBasedTextEventClassifier,
    event_matches_any,
)
from quant_data_center.factor_engine.text_factors import NEW_BUSINESS_KEYWORDS
from quant_data_center.settings import QdcSettings
from quant_data_center.storage.database import QdcDatabase
from quant_data_center.storage.objects import QdcObjectStore
from quant_data_center.storage.silver import SilverStore
from quant_data_center.utils.instruments import instrument_to_symbol, normalize_instrument


CNINFO_IR_KEYBOARD_URL = "https://irm.cninfo.com.cn/newircs/index/queryKeyboardInfo"
CNINFO_IR_QUESTION_URL = "https://irm.cninfo.com.cn/newircs/company/question"
CNINFO_IR_DETAIL_URL = "https://irm.cninfo.com.cn/ircs/question/questionDetail"
CNINFO_IR_REFERER = "https://irm.cninfo.com.cn/"
PARSER_VERSION = "cninfo_investor_interaction_v1"
DEFAULT_UNFILTERED_INSTRUMENT_LIMIT = 50
DEFAULT_INSTRUMENT_MAX_PAGES = 2


class CninfoInvestorInteractionCrawler:
    """Fetch CNINFO investor-interaction questions for one publish date."""

    def __init__(self, settings: QdcSettings) -> None:
        self.settings = settings
        self.database = QdcDatabase(settings)
        self.objects = QdcObjectStore(settings)
        self.silver = SilverStore(settings)

    def crawl_date(
        self,
        *,
        source_id: str,
        crawl_date: str,
        page_size: int = 30,
        max_pages: int | None = None,
        min_delay_seconds: float = 3.0,
        request_timeout_seconds: float = 30.0,
        source_timeout_seconds: float | None = None,
        instrument_filter: list[str] | None = None,
        instrument_parallelism: int | None = None,
        instrument_limit: int | None = None,
    ) -> dict[str, Any]:
        requests = __import__("requests")
        deadline = make_deadline(source_timeout_seconds)
        observed_at = _timestamp()
        instruments, instrument_mode = self._resolve_instruments(
            instrument_filter,
            instrument_limit=instrument_limit,
        )
        worker_count = min(_instrument_parallelism(instrument_parallelism), max(1, len(instruments)))
        effective_max_pages = _effective_max_pages(max_pages)
        org_cache = _load_org_cache(self.settings)
        rate_limiter = _RequestRateLimiter(
            _request_interval_seconds(
                min_delay_seconds=min_delay_seconds,
                worker_count=worker_count,
            )
        )

        pages: list[dict[str, Any]] = []
        provider_rows: list[dict[str, Any]] = []
        org_failures: list[dict[str, str]] = []
        question_failures: list[dict[str, str | int]] = []
        org_cache_updates: dict[str, dict[str, str]] = {}
        org_cache_hit_count = 0
        request_count = 0
        target_date_instrument_count = 0
        instrument_status_counts: Counter[str] = Counter()
        instrument_results = _crawl_instruments(
            requests=requests,
            settings=self.settings,
            instruments=instruments,
            org_cache=org_cache,
            rate_limiter=rate_limiter,
            deadline=deadline,
            source_id=source_id,
            crawl_date=crawl_date,
            page_size=page_size,
            max_pages=effective_max_pages,
            request_timeout_seconds=request_timeout_seconds,
            worker_count=worker_count,
        )
        for result in instrument_results:
            provider_rows.extend(result["provider_rows"])
            pages.extend(result["pages"])
            org_failures.extend(result["org_failures"])
            question_failures.extend(result["question_failures"])
            org_cache_updates.update(result["org_cache_updates"])
            org_cache_hit_count += int(result["org_cache_hit_count"])
            request_count += int(result["request_count"])
            status = str(result["instrument_status"])
            instrument_status_counts[status] += 1
            if status == "has_target_date":
                target_date_instrument_count += 1
        if org_cache_updates:
            org_cache.update(org_cache_updates)
            _write_org_cache(self.settings, org_cache)

        raw_object_id = self.objects.put_json(
            dataset="investor_interaction",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"cninfo_investor_interaction_{crawl_date}",
            payload={
                "function": "cninfo_investor_interaction",
                "url": CNINFO_IR_QUESTION_URL,
                "params": {
                    "crawl_date": crawl_date,
                    "page_size": page_size,
                    "max_pages": effective_max_pages,
                    "requested_max_pages": max_pages,
                    "instrument_filter": instrument_filter or [],
                    "instrument_mode": instrument_mode,
                    "instrument_count": len(instruments),
                    "instrument_parallelism": worker_count,
                    "instrument_limit": instrument_limit,
                },
                "org_cache": {
                    "path": str(_org_cache_path(self.settings)),
                    "hit_count": org_cache_hit_count,
                    "update_count": len(org_cache_updates),
                },
                "request_count": request_count,
                "instrument_status_counts": dict(sorted(instrument_status_counts.items())),
                "target_date_instrument_count": target_date_instrument_count,
                "org_failures": org_failures,
                "question_failures": question_failures,
                "pages": pages,
            },
        )
        bronze_object_id = self.objects.put_bronze_parquet(
            dataset="investor_interaction",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"cninfo_investor_interaction_{crawl_date}",
            records=_bronze_records(provider_rows),
        )
        records = _normalize_investor_interactions(
            source_id=source_id,
            rows=provider_rows,
            crawl_date=crawl_date,
            observed_at=observed_at,
            raw_object_id=raw_object_id,
        )
        source_metrics = build_document_source_metrics(
            provider_record_count=len(provider_rows),
            provider_record_keys=(_provider_key(row) for row in provider_rows),
            parsed_record_keys=(_provider_key(row) for row in provider_rows if _is_parsable_row(row)),
            mapped_source_record_ids=(record.get("source_record_id") for record in records),
        )
        document_bundle = self.objects.put_document_bundle(
            dataset="investor_interaction",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"cninfo_investor_interaction_{crawl_date}",
            manifest={
                "function": "cninfo_investor_interaction",
                "url": CNINFO_IR_QUESTION_URL,
                "accepted_date_rule": "answer/update time is converted to Asia/Shanghai and must equal crawl_date; unanswered rows fall back to question time",
                "copyright_policy": "metadata_and_inline_preview; public question/reply text is retained for factor explainability",
                "raw_object_id": raw_object_id,
                "provider_record_count": len(provider_rows),
                "instrument_filter": instrument_filter or [],
                "instrument_mode": instrument_mode,
                "instrument_count": len(instruments),
                "instrument_parallelism": worker_count,
                "instrument_limit": instrument_limit,
                "requested_max_pages": max_pages,
                "effective_max_pages": effective_max_pages,
                "org_cache_hit_count": org_cache_hit_count,
                "org_cache_update_count": len(org_cache_updates),
                "request_count": request_count,
                "instrument_status_counts": dict(sorted(instrument_status_counts.items())),
                "target_date_instrument_count": target_date_instrument_count,
                "org_failure_count": len(org_failures),
                "question_failure_count": len(question_failures),
                **source_metrics,
            },
            records=records,
        )
        row_count = self.silver.upsert_investor_interactions(records)
        return {
            "document_count": row_count,
            "raw_object_count": (
                2
                + int(document_bundle["records_object_id"] is not None)
                + int(bronze_object_id is not None)
            ),
            "raw_object_id": raw_object_id,
            "bronze_object_id": bronze_object_id,
            **document_bundle,
            "provider_record_count": len(provider_rows),
            "mapped_record_count": row_count,
            "instrument_count": len(instruments),
            "instrument_parallelism": worker_count,
            "instrument_limit": instrument_limit,
            "requested_max_pages": max_pages,
            "effective_max_pages": effective_max_pages,
            "org_cache_hit_count": org_cache_hit_count,
            "org_cache_update_count": len(org_cache_updates),
            "request_count": request_count,
            "instrument_status_counts": dict(sorted(instrument_status_counts.items())),
            "target_date_instrument_count": target_date_instrument_count,
            "org_failure_count": len(org_failures),
            "question_failure_count": len(question_failures),
            **source_metrics,
            "observed_at": observed_at,
        }

    def _resolve_instruments(
        self,
        instrument_filter: list[str] | None,
        *,
        instrument_limit: int | None,
    ) -> tuple[list[str], str]:
        if instrument_filter:
            return sorted({normalize_instrument(value) for value in instrument_filter}), "explicit"
        instruments = self.database.stock_basic_instruments(active_only=True)
        max_instruments = _unfiltered_instrument_limit(instrument_limit)
        if max_instruments is None:
            return instruments, "stock_basic_active_all"
        return instruments[:max_instruments], f"stock_basic_active_first_{max_instruments}"


def _crawl_instruments(
    *,
    requests: Any,
    settings: QdcSettings,
    instruments: list[str],
    org_cache: dict[str, dict[str, str]],
    rate_limiter: "_RequestRateLimiter",
    deadline: float | None,
    source_id: str,
    crawl_date: str,
    page_size: int,
    max_pages: int | None,
    request_timeout_seconds: float,
    worker_count: int,
) -> list[dict[str, Any]]:
    post_client = _make_post_client(requests)
    try:
        if worker_count <= 1:
            return [
                _crawl_one_instrument(
                    post=post_client.post,
                    settings=settings,
                    instrument=instrument,
                    org_cache=org_cache,
                    rate_limiter=rate_limiter,
                    deadline=deadline,
                    source_id=source_id,
                    crawl_date=crawl_date,
                    page_size=page_size,
                    max_pages=max_pages,
                    request_timeout_seconds=request_timeout_seconds,
                )
                for instrument in instruments
            ]
        indexed_results: list[tuple[int, dict[str, Any]]] = []
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(
                    _crawl_one_instrument,
                    post=post_client.post,
                    settings=settings,
                    instrument=instrument,
                    org_cache=org_cache,
                    rate_limiter=rate_limiter,
                    deadline=deadline,
                    source_id=source_id,
                    crawl_date=crawl_date,
                    page_size=page_size,
                    max_pages=max_pages,
                    request_timeout_seconds=request_timeout_seconds,
                ): index
                for index, instrument in enumerate(instruments)
            }
            for future in as_completed(futures):
                indexed_results.append((futures[future], future.result()))
        indexed_results.sort(key=lambda item: item[0])
        return [result for _index, result in indexed_results]
    finally:
        post_client.close()


def _crawl_one_instrument(
    *,
    post: Any,
    settings: QdcSettings,
    instrument: str,
    org_cache: dict[str, dict[str, str]],
    rate_limiter: "_RequestRateLimiter",
    deadline: float | None,
    source_id: str,
    crawl_date: str,
    page_size: int,
    max_pages: int | None,
    request_timeout_seconds: float,
) -> dict[str, Any]:
    provider_rows: list[dict[str, Any]] = []
    pages: list[dict[str, Any]] = []
    org_failures: list[dict[str, str]] = []
    question_failures: list[dict[str, str | int]] = []
    org_cache_updates: dict[str, dict[str, str]] = {}
    org_cache_hit_count = 0
    request_count = 0
    provider_date_counts: Counter[str] = Counter()
    latest_provider_publish_date: str | None = None
    oldest_provider_publish_date: str | None = None
    target_row_count = 0
    question_elapsed_ms: float | None = None
    symbol = instrument_to_symbol(instrument)
    cached_item = org_cache.get(symbol) or {}
    try:
        org_id = _cached_org_id(org_cache, symbol)
        if org_id:
            org_cache_hit_count = 1
        else:
            rate_limiter.wait(deadline=deadline, source_id=source_id)
            request_count += 1
            org_id = _fetch_org_id(
                post=post,
                settings=settings,
                symbol=symbol,
                deadline=deadline,
                source_id=source_id,
                request_timeout_seconds=request_timeout_seconds,
            )
    except Exception as exc:
        error_text = str(exc)[:300]
        org_failures.append({"instrument": instrument, "error": error_text})
        org_cache_updates[symbol] = _cache_entry(
            previous=cached_item,
            symbol=symbol,
            instrument=instrument,
            org_status="missing_org",
            question_status="not_requested",
            crawl_date=crawl_date,
            error=error_text,
        )
        return {
            "provider_rows": provider_rows,
            "pages": pages,
            "org_failures": org_failures,
            "question_failures": question_failures,
            "org_cache_updates": org_cache_updates,
            "org_cache_hit_count": org_cache_hit_count,
            "request_count": request_count,
            "instrument_status": "missing_org",
        }
    page_num = 1
    total_pages: int | None = None
    question_started = time.monotonic()
    while total_pages is None or page_num <= total_pages:
        raise_if_deadline_exceeded(deadline, source_id=source_id)
        params = _query_params(
            symbol=symbol,
            org_id=org_id,
            crawl_date=crawl_date,
            page_size=page_size,
            page_num=page_num,
        )
        try:
            rate_limiter.wait(deadline=deadline, source_id=source_id)
            request_count += 1
            response = call_with_proxy_policy(
                post,
                CNINFO_IR_QUESTION_URL,
                headers=_headers(),
                params=params,
                timeout=request_timeout(
                    deadline=deadline,
                    default_seconds=request_timeout_seconds,
                    source_id=source_id,
                ),
                use_environment_proxy=settings.use_environment_proxy,
            )
            response.raise_for_status()
            body = response.json()
        except Exception as exc:
            error_text = str(exc)[:300]
            question_failures.append(
                {
                    "instrument": instrument,
                    "stock_code": symbol,
                    "page_num": page_num,
                    "error": error_text,
                }
            )
            break
        rows = _extract_rows(body)
        scan = _date_scan_summary(rows=rows, crawl_date=crawl_date, page_size=page_size)
        provider_date_counts.update(scan["date_counts"])
        target_row_count += int(scan["target_row_count"])
        latest_provider_publish_date = _max_date(
            latest_provider_publish_date,
            scan["latest_provider_publish_date"],
        )
        oldest_provider_publish_date = _min_date(
            oldest_provider_publish_date,
            scan["oldest_provider_publish_date"],
        )
        for row in rows:
            row["__instrument"] = instrument
            row["__org_id"] = org_id
        provider_rows.extend(rows)
        total_pages = _total_pages(body, page_size=page_size)
        if max_pages is not None:
            total_pages = min(total_pages, max(1, int(max_pages)))
        pages.append(
            {
                "instrument": instrument,
                "stock_code": symbol,
                "org_id": org_id,
                "page_num": page_num,
                "request": params,
                "status_code": response.status_code,
                "provider_record_count": len(rows),
                "total_pages": total_pages,
                "hits": body.get("total"),
                "target_row_count": scan["target_row_count"],
                "latest_provider_publish_date": scan["latest_provider_publish_date"],
                "oldest_provider_publish_date": scan["oldest_provider_publish_date"],
                "date_stop_reason": scan["stop_reason"],
            }
        )
        if scan["stop_reason"]:
            break
        page_num += 1
    question_elapsed_ms = round((time.monotonic() - question_started) * 1000, 3)
    instrument_status = _instrument_status(
        provider_rows=provider_rows,
        target_row_count=target_row_count,
        question_failures=question_failures,
    )
    org_cache_updates[symbol] = _cache_entry(
        previous=cached_item,
        symbol=symbol,
        instrument=instrument,
        org_id=org_id,
        org_status="ok",
        question_status=instrument_status,
        crawl_date=crawl_date,
        target_row_count=target_row_count,
        provider_record_count=len(provider_rows),
        page_count=len(pages),
        latest_provider_publish_date=latest_provider_publish_date,
        oldest_provider_publish_date=oldest_provider_publish_date,
        provider_date_counts=dict(sorted(provider_date_counts.items())),
        latency_ms=question_elapsed_ms,
        error=question_failures[-1]["error"] if question_failures else None,
    )
    return {
        "provider_rows": provider_rows,
        "pages": pages,
        "org_failures": org_failures,
        "question_failures": question_failures,
        "org_cache_updates": org_cache_updates,
        "org_cache_hit_count": org_cache_hit_count,
        "request_count": request_count,
        "instrument_status": instrument_status,
    }


def _fetch_org_id(
    *,
    post: Any,
    settings: QdcSettings,
    symbol: str,
    deadline: float | None,
    source_id: str,
    request_timeout_seconds: float,
) -> str:
    response = call_with_proxy_policy(
        post,
        CNINFO_IR_KEYBOARD_URL,
        headers=_headers(),
        params={"_t": "1691144074"},
        data={"keyWord": symbol},
        timeout=request_timeout(
            deadline=deadline,
            default_seconds=request_timeout_seconds,
            source_id=source_id,
        ),
        use_environment_proxy=settings.use_environment_proxy,
    )
    response.raise_for_status()
    data = response.json()
    for item in data.get("data") or []:
        if str(item.get("stockCode") or "").zfill(6) == symbol:
            org_id = str(item.get("secid") or "").strip()
            if org_id:
                return org_id
    raise ValueError(f"missing CNINFO IR orgId for {symbol}")


class _RequestRateLimiter:
    def __init__(self, interval_seconds: float) -> None:
        self.interval_seconds = max(0.0, float(interval_seconds or 0.0))
        self._lock = threading.Lock()
        self._next_request_at = 0.0

    def wait(self, *, deadline: float | None, source_id: str) -> None:
        if self.interval_seconds <= 0:
            return
        with self._lock:
            now = time.monotonic()
            wait_seconds = max(0.0, self._next_request_at - now)
            self._next_request_at = max(self._next_request_at, now) + self.interval_seconds
        if wait_seconds > 0:
            sleep_with_deadline(wait_seconds, deadline=deadline, source_id=source_id)


class _ModulePostClient:
    def __init__(self, post: Any) -> None:
        self.post = post

    def close(self) -> None:
        return None


class _ThreadLocalPostClient:
    def __init__(self, requests: Any) -> None:
        self.requests = requests
        self._local = threading.local()
        self._lock = threading.Lock()
        self._sessions: list[Any] = []

    def post(self, *args: Any, **kwargs: Any) -> Any:
        session = getattr(self._local, "session", None)
        if session is None:
            session = self.requests.Session()
            self._local.session = session
            with self._lock:
                self._sessions.append(session)
        return session.post(*args, **kwargs)

    def close(self) -> None:
        with self._lock:
            sessions = list(self._sessions)
            self._sessions.clear()
        for session in sessions:
            close = getattr(session, "close", None)
            if close:
                close()


def _make_post_client(requests: Any) -> Any:
    if os.environ.get("QDC_INVESTOR_INTERACTION_DISABLE_SESSION") == "1":
        return _ModulePostClient(requests.post)
    session_factory = getattr(requests, "Session", None)
    if session_factory is None:
        return _ModulePostClient(requests.post)
    return _ThreadLocalPostClient(requests)


def _request_interval_seconds(*, min_delay_seconds: float, worker_count: int) -> float:
    raw = os.environ.get("QDC_INVESTOR_INTERACTION_REQUEST_INTERVAL_SECONDS")
    if raw not in (None, ""):
        try:
            return max(0.0, float(raw))
        except ValueError:
            return max(0.0, float(min_delay_seconds or 0.0))
    return max(0.0, float(min_delay_seconds or 0.0) / max(1, int(worker_count or 1)))


def _instrument_parallelism(value: int | None) -> int:
    if value is not None:
        return max(1, int(value))
    raw = os.environ.get("QDC_INVESTOR_INTERACTION_INSTRUMENT_PARALLELISM") or os.environ.get(
        "QDC_INVESTOR_INTERACTION_WORKERS"
    )
    if not raw:
        return 1
    try:
        return max(1, int(raw))
    except ValueError:
        return 1


def _org_cache_path(settings: QdcSettings) -> Any:
    return settings.data_root / "cache" / "cninfo_investor_interaction_orgs.json"


def _load_org_cache(settings: QdcSettings) -> dict[str, dict[str, str]]:
    path = _org_cache_path(settings)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    items = data.get("items") if isinstance(data, dict) else data
    if not isinstance(items, dict):
        return {}
    cache: dict[str, dict[str, str]] = {}
    for symbol, item in items.items():
        normalized_symbol = str(symbol).zfill(6)
        if isinstance(item, str):
            cache[normalized_symbol] = {"symbol": normalized_symbol, "org_id": item}
        elif isinstance(item, dict):
            org_id = str(item.get("org_id") or "").strip()
            status_keys = {"org_status", "question_status", "last_checked_date"}
            if org_id or status_keys.intersection(item):
                cache[normalized_symbol] = {
                    str(key): str(value)
                    for key, value in item.items()
                    if value is not None
                }
    return cache


def _write_org_cache(settings: QdcSettings, cache: dict[str, dict[str, str]]) -> None:
    path = _org_cache_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "updated_at": _timestamp(),
        "items": dict(sorted(cache.items())),
    }
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def _cached_org_id(org_cache: dict[str, dict[str, str]], symbol: str) -> str | None:
    item = org_cache.get(str(symbol).zfill(6))
    if not item:
        return None
    org_id = str(item.get("org_id") or "").strip()
    return org_id or None


def _cache_entry(
    *,
    previous: dict[str, str],
    symbol: str,
    instrument: str,
    org_status: str,
    question_status: str,
    crawl_date: str,
    org_id: str | None = None,
    target_row_count: int = 0,
    provider_record_count: int = 0,
    page_count: int = 0,
    latest_provider_publish_date: str | None = None,
    oldest_provider_publish_date: str | None = None,
    provider_date_counts: dict[str, int] | None = None,
    latency_ms: float | None = None,
    error: str | None = None,
) -> dict[str, str]:
    entry = dict(previous)
    entry.update(
        {
            "symbol": symbol,
            "instrument": instrument,
            "org_status": org_status,
            "question_status": question_status,
            "last_checked_date": crawl_date,
            "updated_at": _timestamp(),
            "target_row_count": str(target_row_count),
            "provider_record_count": str(provider_record_count),
            "page_count": str(page_count),
        }
    )
    if org_id:
        entry["org_id"] = org_id
    if latest_provider_publish_date:
        entry["latest_provider_publish_date"] = latest_provider_publish_date
        entry["last_question_seen_date"] = latest_provider_publish_date
    if oldest_provider_publish_date:
        entry["oldest_provider_publish_date"] = oldest_provider_publish_date
    if target_row_count > 0:
        entry["last_target_date"] = crawl_date
        entry["consecutive_no_data_days"] = "0"
    elif question_status in {"no_target_date", "no_rows"}:
        entry["consecutive_no_data_days"] = str(_next_no_data_days(previous))
    if provider_date_counts is not None:
        entry["provider_date_counts_json"] = json.dumps(
            provider_date_counts,
            ensure_ascii=False,
            sort_keys=True,
        )
    if latency_ms is not None:
        entry["last_latency_ms"] = str(latency_ms)
    if error:
        entry["last_error"] = error
    else:
        entry.pop("last_error", None)
    return {str(key): str(value) for key, value in entry.items() if value is not None}


def _next_no_data_days(previous: dict[str, str]) -> int:
    try:
        return max(0, int(previous.get("consecutive_no_data_days") or 0)) + 1
    except ValueError:
        return 1


def _query_params(
    *,
    symbol: str,
    org_id: str,
    crawl_date: str,
    page_size: int,
    page_num: int,
) -> dict[str, str]:
    return {
        "_t": "1691142650",
        "stockcode": symbol,
        "orgId": org_id,
        "pageSize": str(max(1, int(page_size))),
        "pageNum": str(page_num),
        "keyWord": "",
        "startDay": crawl_date,
        "endDay": crawl_date,
    }


def _headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 QDC-Crawler/0.1 (+local research)",
        "Referer": CNINFO_IR_REFERER,
        "Accept": "application/json,text/plain,*/*",
    }


def _extract_rows(body: Any) -> list[dict[str, Any]]:
    if not isinstance(body, dict):
        return []
    rows = body.get("rows") or []
    return [row for row in rows if isinstance(row, dict)]


def _total_pages(body: dict[str, Any], *, page_size: int) -> int:
    total_page = body.get("totalPage")
    if total_page not in (None, ""):
        return max(1, int(total_page or 1))
    total = body.get("total")
    if total:
        return max(1, math.ceil(int(total) / max(1, int(page_size))))
    return 1


def _date_scan_summary(
    *,
    rows: list[dict[str, Any]],
    crawl_date: str,
    page_size: int,
) -> dict[str, Any]:
    row_dates = [_row_publish_date(row) for row in rows]
    dated = [value for value in row_dates if value]
    date_counts = Counter(dated)
    target_row_count = date_counts.get(crawl_date, 0)
    older_row_count = sum(count for value, count in date_counts.items() if value < crawl_date)
    newer_row_count = sum(count for value, count in date_counts.items() if value > crawl_date)
    latest_date = max(dated) if dated else None
    oldest_date = min(dated) if dated else None
    stop_reason: str | None = None
    if not rows:
        stop_reason = "empty_page"
    elif older_row_count > 0:
        stop_reason = "older_than_target_date"
    elif target_row_count > 0 and len(rows) < max(1, int(page_size)):
        stop_reason = "target_date_exhausted"
    elif target_row_count > 0 and oldest_date and oldest_date > crawl_date:
        stop_reason = "target_date_exhausted"
    return {
        "date_counts": date_counts,
        "target_row_count": target_row_count,
        "older_row_count": older_row_count,
        "newer_row_count": newer_row_count,
        "latest_provider_publish_date": latest_date,
        "oldest_provider_publish_date": oldest_date,
        "stop_reason": stop_reason,
    }


def _instrument_status(
    *,
    provider_rows: list[dict[str, Any]],
    target_row_count: int,
    question_failures: list[dict[str, str | int]],
) -> str:
    if question_failures:
        return "question_failed"
    if target_row_count > 0:
        return "has_target_date"
    if provider_rows:
        return "no_target_date"
    return "no_rows"


def _max_date(left: str | None, right: str | None) -> str | None:
    if not left:
        return right
    if not right:
        return left
    return max(left, right)


def _min_date(left: str | None, right: str | None) -> str | None:
    if not left:
        return right
    if not right:
        return left
    return min(left, right)


def _row_publish_date(row: dict[str, Any]) -> str | None:
    answer_text = _clean_text(row.get("attachedContent"))
    answer_time = _millis_time(row.get("attachedPubDate")) or (
        _millis_time(row.get("updateDate")) if answer_text else None
    )
    publish_time = answer_time or _millis_time(row.get("updateDate")) or _millis_time(row.get("pubDate"))
    return publish_time[:10] if publish_time else None


def _bronze_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{str(key): _bronze_value(value) for key, value in row.items()} for row in rows]


def _bronze_value(value: Any) -> Any:
    if value is None:
        return value
    if isinstance(value, (bool, int, float, str)):
        return str(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _normalize_investor_interactions(
    *,
    source_id: str,
    rows: list[dict[str, Any]],
    crawl_date: str,
    observed_at: str,
    raw_object_id: str,
) -> list[dict[str, Any]]:
    classifier = RuleBasedTextEventClassifier()
    records: dict[str, dict[str, Any]] = {}
    for row in rows:
        question_text = _clean_text(row.get("mainContent"))
        question_time = _millis_time(row.get("pubDate"))
        stock_code = _clean_text(row.get("stockCode"))
        answer_text = _clean_text(row.get("attachedContent"))
        answer_time = _millis_time(row.get("attachedPubDate")) or (
            _millis_time(row.get("updateDate")) if answer_text else None
        )
        publish_time = answer_time or _millis_time(row.get("updateDate")) or question_time
        publish_date = publish_time[:10] if publish_time else None
        if not question_text or publish_date != crawl_date or not stock_code:
            continue
        try:
            instrument = normalize_instrument(stock_code)
        except ValueError:
            continue
        source_record_id = _provider_key(row)
        interaction_id = f"cninfo_ir_{_slug(source_record_id)}_{instrument}"
        classification = classifier.classify(
            title=question_text,
            body=answer_text,
            document_type="investor_interaction",
        )
        records[interaction_id] = {
            "investor_interaction_id": interaction_id,
            "publish_date": publish_date,
            "publish_time": publish_time,
            "instrument": instrument,
            "title": question_text,
            "url": f"{CNINFO_IR_DETAIL_URL}?questionId={source_record_id}",
            "source_id": source_id,
            "source_record_id": source_record_id,
            "source_sec_code": stock_code,
            "source_sec_name": _clean_text(row.get("companyShortName")),
            "question_text": question_text,
            "question_time": question_time,
            "answer_text": answer_text,
            "answer_time": answer_time,
            "reply_status": "replied" if answer_text else "pending",
            "reply_delay_hours": _reply_delay_hours(question_time, answer_time),
            "questioner": _clean_text(row.get("authorName")),
            "industry": _first_value(row.get("trade")),
            "channel": _channel_label(row.get("pubClient")),
            "topic_tags": _topic_tags(question_text, answer_text, classification),
            "sentiment_score": classification.sentiment_score,
            "observed_at": observed_at,
            "collect_time": observed_at,
            "raw_object_id": raw_object_id,
            "parser_version": PARSER_VERSION,
        }
    return list(records.values())


def _provider_key(row: dict[str, Any]) -> str:
    return _clean_text(row.get("indexId")) or _fallback_record_id(row)


def _is_parsable_row(row: dict[str, Any]) -> bool:
    return bool(
        _clean_text(row.get("mainContent"))
        and (_millis_time(row.get("attachedPubDate")) or _millis_time(row.get("updateDate")) or _millis_time(row.get("pubDate")))
        and _clean_text(row.get("stockCode"))
        and _provider_key(row)
    )


def _fallback_record_id(row: dict[str, Any]) -> str:
    values = [
        _clean_text(row.get("stockCode")),
        _clean_text(row.get("pubDate")),
        _clean_text(row.get("mainContent")),
    ]
    return "|".join(value for value in values if value)


def _millis_time(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        millis = int(float(value))
    except (TypeError, ValueError):
        return None
    parsed = datetime.fromtimestamp(millis / 1000, tz=timezone.utc).astimezone(
        ZoneInfo("Asia/Shanghai")
    )
    return parsed.replace(tzinfo=None, microsecond=0).isoformat(" ")


def _reply_delay_hours(question_time: str | None, answer_time: str | None) -> float | None:
    if not question_time or not answer_time:
        return None
    try:
        question = datetime.fromisoformat(question_time)
        answer = datetime.fromisoformat(answer_time)
    except ValueError:
        return None
    delay = (answer - question).total_seconds() / 3600
    return round(delay, 4) if delay >= 0 else None


def _topic_tags(question_text: str, answer_text: str | None, classification: Any) -> str:
    tags = list(classification.event_types)
    if event_matches_any(classification, RISK_EVENTS) and "risk" not in tags:
        tags.append("risk")
    if _contains_new_business_topic(question_text, answer_text):
        tags.append("new_business")
    return ",".join(dict.fromkeys(tags))


def _contains_new_business_topic(question_text: str, answer_text: str | None) -> bool:
    text = f"{question_text or ''} {answer_text or ''}".upper()
    return any(keyword.upper() in text for keyword in NEW_BUSINESS_KEYWORDS)


def _channel_label(value: Any) -> str | None:
    text = _clean_text(value)
    return {
        "2": "APP",
        "4": "website",
        "5": "wechat",
        "6": "website",
    }.get(text or "", text)


def _first_value(value: Any) -> str | None:
    if isinstance(value, (list, tuple)) and value:
        return _clean_text(value[0])
    return _clean_text(value)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _slug(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9_.=-]+", "_", str(value or "").strip())
    return text[:96] or "unknown"


def _timestamp() -> str:
    return datetime.now().replace(microsecond=0).isoformat(" ")


def _effective_max_pages(value: int | None) -> int | None:
    if value is not None:
        return max(1, int(value))
    raw = os.environ.get("QDC_INVESTOR_INTERACTION_DEFAULT_MAX_PAGES")
    if raw and raw.strip().lower() in {"0", "all", "none", "unlimited"}:
        return None
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            return DEFAULT_INSTRUMENT_MAX_PAGES
    return DEFAULT_INSTRUMENT_MAX_PAGES


def _unfiltered_instrument_limit(value: int | None = None) -> int | None:
    if value is not None:
        if int(value) <= 0:
            return None
        return max(1, int(value))
    raw = os.environ.get("QDC_INVESTOR_INTERACTION_MAX_INSTRUMENTS")
    if not raw:
        return DEFAULT_UNFILTERED_INSTRUMENT_LIMIT
    if raw.strip().lower() in {"0", "all", "none", "unlimited"}:
        return None
    try:
        return max(1, int(raw))
    except ValueError:
        return DEFAULT_UNFILTERED_INSTRUMENT_LIMIT
