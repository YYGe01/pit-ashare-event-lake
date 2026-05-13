"""Vendor-level public finance news crawlers.

These adapters are intentionally shallow: they keep list/API metadata and
short inline text previews, then map records to active A-share instruments by
known stock code/name hints.
"""

from __future__ import annotations

import hashlib
import html
import json
import re
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urljoin

from quant_data_center.crawlers.runtime import (
    make_deadline,
    raise_if_deadline_exceeded,
    request_timeout,
    sleep_with_deadline,
)
from quant_data_center.settings import QdcSettings
from quant_data_center.storage.database import QdcDatabase
from quant_data_center.storage.objects import QdcObjectStore
from quant_data_center.storage.silver import SilverStore
from quant_data_center.utils.instruments import normalize_instrument


VENDOR_NEWS_SOURCE_IDS = (
    "sina",
    "wallstreetcn",
    "10jqka",
    "eastmoney",
    "yuncaijing",
    "fenghuang",
    "jinrongjie",
    "cls",
    "yicai",
)
AKSHARE_SOURCE_FUNCTIONS = {
    "sina": ("stock_info_global_sina", {}),
    "10jqka": ("stock_info_global_ths", {}),
    "eastmoney": ("stock_info_global_em", {}),
    "cls": ("stock_info_global_cls", {"symbol": "全部"}),
}
SOURCE_REFERERS = {
    "sina": "https://finance.sina.com.cn/7x24/",
    "wallstreetcn": "https://wallstreetcn.com/live/global",
    "10jqka": "https://news.10jqka.com.cn/realtimenews.html",
    "eastmoney": "https://kuaixun.eastmoney.com/7_24.html",
    "yuncaijing": "https://www.yuncaijing.com/insider/main.html",
    "fenghuang": "https://finance.ifeng.com/",
    "jinrongjie": "https://stock.jrj.com.cn/",
    "cls": "https://www.cls.cn/telegraph",
    "yicai": "https://www.yicai.com/brief/",
}
PARSER_VERSION = "vendor_news_v1"
MAX_BODY_PREVIEW_CHARS = 1200
MAX_RAW_ROW_CHARS = 8000


class VendorNewsCrawler:
    """Fetch public quick-news feeds from configured vendor source IDs."""

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
    ) -> dict[str, Any]:
        if source_id not in VENDOR_NEWS_SOURCE_IDS:
            raise ValueError(f"unsupported vendor news source_id: {source_id}")
        requests = __import__("requests")
        deadline = make_deadline(source_timeout_seconds)
        observed_at = _timestamp()
        fetch_result = _fetch_provider_rows(
            requests_module=requests,
            source_id=source_id,
            crawl_date=crawl_date,
            page_size=page_size,
            max_pages=max_pages,
            min_delay_seconds=min_delay_seconds,
            request_timeout_seconds=request_timeout_seconds,
            deadline=deadline,
        )
        provider_rows = fetch_result["rows"]
        stem = f"{source_id}_vendor_news_{crawl_date}"
        raw_object_id = self.objects.put_json(
            dataset="news",
            source_id=source_id,
            partition_value=crawl_date,
            stem=stem,
            payload={
                "function": "vendor_news",
                "source_id": source_id,
                "params": {
                    "crawl_date": crawl_date,
                    "page_size": page_size,
                    "max_pages": max_pages,
                    "instrument_filter": instrument_filter or [],
                },
                "pages": fetch_result["pages"],
            },
        )
        bronze_object_id = self.objects.put_bronze_parquet(
            dataset="news",
            source_id=source_id,
            partition_value=crawl_date,
            stem=stem,
            records=provider_rows,
        )
        records = _normalize_news(
            source_id=source_id,
            rows=provider_rows,
            instrument_hints=self._instrument_hints(instrument_filter=instrument_filter),
            observed_at=observed_at,
            raw_object_id=raw_object_id,
        )
        body_count = sum(1 for record in records if record.get("body_text"))
        document_bundle = self.objects.put_document_bundle(
            dataset="news",
            source_id=source_id,
            partition_value=crawl_date,
            stem=stem,
            manifest={
                "function": "vendor_news",
                "source_id": source_id,
                "base_url": SOURCE_REFERERS.get(source_id),
                "accepted_date_rule": (
                    "publish_time must be parsed from the vendor list/API payload; "
                    "publish_date is derived from publish_time"
                ),
                "copyright_policy": (
                    "copyright-aware preview: inline list/API text is truncated "
                    f"to {MAX_BODY_PREVIEW_CHARS} characters; full article HTML is not persisted"
                ),
                "instrument_filter": instrument_filter or [],
                "raw_object_id": raw_object_id,
                "provider_record_count": len(provider_rows),
                "body_inline_preview_count": body_count,
            },
            records=records,
        )
        row_count = self.silver.upsert_news(records)
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
            "body_inline_preview_count": body_count,
            "observed_at": observed_at,
        }

    def _instrument_hints(
        self,
        *,
        instrument_filter: list[str] | None = None,
    ) -> list[dict[str, str]]:
        normalized_filter = (
            {normalize_instrument(value) for value in instrument_filter}
            if instrument_filter
            else None
        )
        with self.database.connect() as conn:
            rows = conn.execute(
                """
                select instrument, symbol, name
                from qdc_silver.stock_basic
                where coalesce(is_active, true)
                order by instrument
                """
            ).fetchall()
        if normalized_filter is not None:
            rows = [row for row in rows if str(row[0]) in normalized_filter]
        return [
            {
                "instrument": str(instrument),
                "symbol": str(symbol),
                "name": str(name or ""),
            }
            for instrument, symbol, name in rows
        ]


def _fetch_provider_rows(
    *,
    requests_module: Any,
    source_id: str,
    crawl_date: str,
    page_size: int,
    max_pages: int | None,
    min_delay_seconds: float,
    request_timeout_seconds: float,
    deadline: float | None,
) -> dict[str, Any]:
    if source_id in AKSHARE_SOURCE_FUNCTIONS:
        return _fetch_akshare_rows(
            source_id=source_id,
            crawl_date=crawl_date,
            page_size=page_size,
            max_pages=max_pages,
            deadline=deadline,
        )
    if source_id == "wallstreetcn":
        return _fetch_wallstreetcn_rows(
            requests_module=requests_module,
            source_id=source_id,
            crawl_date=crawl_date,
            page_size=page_size,
            request_timeout_seconds=request_timeout_seconds,
            deadline=deadline,
        )
    if source_id == "yicai":
        return _fetch_yicai_rows(
            requests_module=requests_module,
            source_id=source_id,
            crawl_date=crawl_date,
            page_size=page_size,
            max_pages=max_pages,
            min_delay_seconds=min_delay_seconds,
            request_timeout_seconds=request_timeout_seconds,
            deadline=deadline,
        )
    if source_id == "jinrongjie":
        return _fetch_jinrongjie_rows(
            requests_module=requests_module,
            source_id=source_id,
            crawl_date=crawl_date,
            page_size=page_size,
            request_timeout_seconds=request_timeout_seconds,
            deadline=deadline,
        )
    if source_id == "fenghuang":
        return _fetch_fenghuang_rows(
            requests_module=requests_module,
            source_id=source_id,
            crawl_date=crawl_date,
            page_size=page_size,
            request_timeout_seconds=request_timeout_seconds,
            deadline=deadline,
        )
    if source_id == "yuncaijing":
        return _fetch_yuncaijing_rows(
            requests_module=requests_module,
            source_id=source_id,
            crawl_date=crawl_date,
            page_size=page_size,
            request_timeout_seconds=request_timeout_seconds,
            deadline=deadline,
        )
    raise ValueError(f"unsupported vendor news source_id: {source_id}")


def _fetch_akshare_rows(
    *,
    source_id: str,
    crawl_date: str,
    page_size: int,
    max_pages: int | None,
    deadline: float | None,
) -> dict[str, Any]:
    raise_if_deadline_exceeded(deadline, source_id=source_id)
    function_name, kwargs = AKSHARE_SOURCE_FUNCTIONS[source_id]
    akshare = __import__("akshare")
    frame = getattr(akshare, function_name)(**kwargs)
    raw_rows = frame.to_dict("records") if hasattr(frame, "to_dict") else []
    limit = max(1, page_size) * max(1, max_pages or 1)
    rows = [
        row
        for row in (
            _standardize_akshare_row(
                source_id=source_id,
                crawl_date=crawl_date,
                row=item,
            )
            for item in raw_rows[:limit]
        )
        if row
    ]
    return {
        "rows": rows,
        "pages": [
            {
                "page_num": 1,
                "function": function_name,
                "kwargs": kwargs,
                "status_code": None,
                "news_count": len(rows),
                "items": rows,
            }
        ],
    }


def _standardize_akshare_row(
    *,
    source_id: str,
    crawl_date: str,
    row: dict[str, Any],
) -> dict[str, Any] | None:
    if source_id == "sina":
        return _standardize_row(
            source_id=source_id,
            crawl_date=crawl_date,
            raw=row,
            title=None,
            body_text=_field(row, names=("内容",), positions=(1,)),
            publish_time=_field(row, names=("时间",), positions=(0,)),
            url=None,
            source_record_id=None,
        )
    if source_id in {"10jqka", "eastmoney"}:
        return _standardize_row(
            source_id=source_id,
            crawl_date=crawl_date,
            raw=row,
            title=_field(row, names=("标题",), positions=(0,)),
            body_text=_field(row, names=("内容", "摘要"), positions=(1,)),
            publish_time=_field(row, names=("发布时间",), positions=(2,)),
            url=_field(row, names=("链接",), positions=(3,)),
            source_record_id=None,
        )
    if source_id == "cls":
        return _standardize_row(
            source_id=source_id,
            crawl_date=crawl_date,
            raw=row,
            title=_field(row, names=("标题",), positions=(0,)),
            body_text=_field(row, names=("内容",), positions=(1,)),
            publish_time=_field(row, names=("发布时间",), positions=(3,)),
            publish_date=_field(row, names=("发布日期",), positions=(2,)),
            url=None,
            source_record_id=None,
        )
    return None


def _fetch_wallstreetcn_rows(
    *,
    requests_module: Any,
    source_id: str,
    crawl_date: str,
    page_size: int,
    request_timeout_seconds: float,
    deadline: float | None,
) -> dict[str, Any]:
    raise_if_deadline_exceeded(deadline, source_id=source_id)
    url = "https://api-one-wscn.awtmt.com/apiv1/content/lives"
    params = {
        "channel": "global-channel",
        "client": "pc",
        "limit": str(page_size),
        "first_page": "true",
        "accept": "live,vip-live",
    }
    response = requests_module.get(
        url,
        headers=_headers(source_id),
        params=params,
        timeout=request_timeout(
            deadline=deadline,
            default_seconds=request_timeout_seconds,
            source_id=source_id,
        ),
    )
    response.raise_for_status()
    body = response.json()
    items = body.get("data", {}).get("items", []) if isinstance(body, dict) else []
    rows = [
        row
        for row in (
            _standardize_row(
                source_id=source_id,
                crawl_date=crawl_date,
                raw=item,
                title=item.get("title") or item.get("highlight_title"),
                body_text=item.get("content_text") or _html_to_text(item.get("content")),
                publish_time=item.get("display_time") or item.get("created_at"),
                url=_wallstreetcn_url(item),
                source_record_id=item.get("id"),
            )
            for item in items
            if isinstance(item, dict)
        )
        if row
    ]
    return {
        "rows": rows,
        "pages": [
            {
                "page_num": 1,
                "url": response.url,
                "request": params,
                "status_code": response.status_code,
                "news_count": len(rows),
                "items": rows,
            }
        ],
    }


def _fetch_yicai_rows(
    *,
    requests_module: Any,
    source_id: str,
    crawl_date: str,
    page_size: int,
    max_pages: int | None,
    min_delay_seconds: float,
    request_timeout_seconds: float,
    deadline: float | None,
) -> dict[str, Any]:
    pages = []
    rows: list[dict[str, Any]] = []
    page_count = max(1, max_pages or 1)
    url = "https://www.yicai.com/api/ajax/getbrieflist"
    for page_num in range(1, page_count + 1):
        raise_if_deadline_exceeded(deadline, source_id=source_id)
        params = {
            "page": str(page_num),
            "pagesize": str(page_size),
            "type": "0",
            "id": "0",
        }
        response = requests_module.get(
            url,
            headers=_headers(source_id),
            params=params,
            timeout=request_timeout(
                deadline=deadline,
                default_seconds=request_timeout_seconds,
                source_id=source_id,
            ),
        )
        response.raise_for_status()
        items = response.json()
        page_rows = [
            row
            for row in (
                _standardize_row(
                    source_id=source_id,
                    crawl_date=crawl_date,
                    raw=item,
                    title=item.get("LiveTitle") or item.get("NewsTitle"),
                    body_text=item.get("LiveContent"),
                    publish_time=item.get("CreateDate"),
                    url=item.get("ShareUrl"),
                    source_record_id=item.get("LiveID") or item.get("id"),
                    related_text=item.get("Stocks"),
                )
                for item in items
                if isinstance(item, dict)
            )
            if row
        ]
        rows.extend(page_rows)
        pages.append(
            {
                "page_num": page_num,
                "url": response.url,
                "request": params,
                "status_code": response.status_code,
                "news_count": len(page_rows),
                "items": page_rows,
            }
        )
        if page_num < page_count and min_delay_seconds > 0:
            sleep_with_deadline(min_delay_seconds, deadline=deadline, source_id=source_id)
    return {"rows": _dedupe_rows(rows), "pages": pages}


def _fetch_jinrongjie_rows(
    *,
    requests_module: Any,
    source_id: str,
    crawl_date: str,
    page_size: int,
    request_timeout_seconds: float,
    deadline: float | None,
) -> dict[str, Any]:
    raise_if_deadline_exceeded(deadline, source_id=source_id)
    url = f"https://stockjs.jrj.com.cn/share/news/yaowen/yw{crawl_date}.js"
    response = requests_module.get(
        url,
        headers=_headers(source_id),
        timeout=request_timeout(
            deadline=deadline,
            default_seconds=request_timeout_seconds,
            source_id=source_id,
        ),
    )
    response.raise_for_status()
    body = _json_from_maybe_javascript(response.text)
    items = body.get("newsinfo", []) if isinstance(body, dict) else []
    rows = [
        row
        for row in (
            _standardize_row(
                source_id=source_id,
                crawl_date=crawl_date,
                raw=item,
                title=item.get("title"),
                body_text=item.get("detail"),
                publish_time=item.get("makedate"),
                url=item.get("infourl") or item.get("mInfoUrl") or item.get("appInfoUrl"),
                source_record_id=item.get("iiid"),
                related_text=item.get("infosource"),
            )
            for item in items[:page_size]
            if isinstance(item, dict)
        )
        if row
    ]
    return {
        "rows": rows,
        "pages": [
            {
                "page_num": 1,
                "url": response.url,
                "status_code": response.status_code,
                "news_count": len(rows),
                "items": rows,
            }
        ],
    }


def _fetch_fenghuang_rows(
    *,
    requests_module: Any,
    source_id: str,
    crawl_date: str,
    page_size: int,
    request_timeout_seconds: float,
    deadline: float | None,
) -> dict[str, Any]:
    raise_if_deadline_exceeded(deadline, source_id=source_id)
    url = "https://shankapi.ifeng.com/api/finance/studio/24h/latest/getClsData"
    response = requests_module.get(
        url,
        headers=_headers(source_id),
        timeout=request_timeout(
            deadline=deadline,
            default_seconds=request_timeout_seconds,
            source_id=source_id,
        ),
    )
    response.raise_for_status()
    body = _json_from_maybe_javascript(response.text)
    items = body.get("data", []) if isinstance(body, dict) else []
    rows = [
        row
        for row in (
            _standardize_row(
                source_id=source_id,
                crawl_date=crawl_date,
                raw=item,
                title=None,
                body_text=item.get("brief"),
                publish_time=item.get("ctime"),
                url=None,
                source_record_id=item.get("id") or item.get("ucmsid"),
            )
            for item in items[:page_size]
            if isinstance(item, dict)
        )
        if row
    ]
    return {
        "rows": rows,
        "pages": [
            {
                "page_num": 1,
                "url": response.url,
                "status_code": response.status_code,
                "news_count": len(rows),
                "items": rows,
            }
        ],
    }


def _fetch_yuncaijing_rows(
    *,
    requests_module: Any,
    source_id: str,
    crawl_date: str,
    page_size: int,
    request_timeout_seconds: float,
    deadline: float | None,
) -> dict[str, Any]:
    raise_if_deadline_exceeded(deadline, source_id=source_id)
    url = f"https://www.yuncaijing.com/insider/list_{crawl_date}.html"
    response = requests_module.get(
        url,
        headers=_headers(source_id),
        timeout=request_timeout(
            deadline=deadline,
            default_seconds=request_timeout_seconds,
            source_id=source_id,
        ),
    )
    response.raise_for_status()
    if not response.encoding:
        response.encoding = response.apparent_encoding
    rows = _extract_yuncaijing_rows(
        text=response.text,
        source_id=source_id,
        crawl_date=crawl_date,
        limit=page_size,
    )
    return {
        "rows": rows,
        "pages": [
            {
                "page_num": 1,
                "url": response.url,
                "status_code": response.status_code,
                "news_count": len(rows),
                "items": rows,
            }
        ],
    }


def _extract_yuncaijing_rows(
    *,
    text: str,
    source_id: str,
    crawl_date: str,
    limit: int,
) -> list[dict[str, Any]]:
    rows = []
    pattern = re.compile(
        r"<li[^>]*data-id=\"(?P<id>\d+)\"[^>]*>.*?"
        r"<time>\s*(?P<time>.*?)\s*</time>.*?"
        r"<a[^>]*class=\"title\"[^>]*href=\"(?P<url>[^\"]+)\"[^>]*"
        r"(?:title=\"(?P<title_attr>[^\"]*)\")?[^>]*>(?P<title_body>.*?)</a>"
        r"(?P<tail>.*?)(?=</li>)",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(text):
        tail = match.group("tail")
        desc_match = re.search(
            r"<span[^>]*class=\"des\"[^>]*>(?P<desc>.*?)</span>",
            tail,
            flags=re.IGNORECASE | re.DOTALL,
        )
        title = _clean_text(
            html.unescape(match.group("title_attr") or _strip_tags(match.group("title_body")))
        )
        body_text = _clean_body_text(_strip_tags(desc_match.group("desc"))) if desc_match else None
        row = _standardize_row(
            source_id=source_id,
            crawl_date=crawl_date,
            raw={
                "id": match.group("id"),
                "time": _strip_tags(match.group("time")),
                "title": title,
                "body_text": body_text,
                "url": match.group("url"),
            },
            title=title,
            body_text=body_text,
            publish_time=_strip_tags(match.group("time")),
            url=urljoin(SOURCE_REFERERS[source_id], html.unescape(match.group("url"))),
            source_record_id=match.group("id"),
        )
        if row:
            rows.append(row)
        if len(rows) >= limit:
            break
    return rows


def _standardize_row(
    *,
    source_id: str,
    crawl_date: str,
    raw: dict[str, Any],
    title: Any,
    body_text: Any,
    publish_time: Any,
    url: Any,
    source_record_id: Any,
    publish_date: Any = None,
    related_text: Any = None,
) -> dict[str, Any] | None:
    body = _clean_body_text(body_text)
    clean_title = _clean_text(title) or _title_from_body(body)
    if not clean_title:
        return None
    normalized_publish_time = _parse_publish_time(
        publish_time=publish_time,
        publish_date=publish_date,
        crawl_date=crawl_date,
    )
    if not normalized_publish_time:
        return None
    clean_url = _clean_text(url)
    record_id = _clean_text(source_record_id) or _source_record_hash(
        source_id=source_id,
        title=clean_title,
        publish_time=normalized_publish_time,
        url=clean_url,
    )
    row = {
        "source_id": source_id,
        "source_record_id": record_id,
        "publish_date": normalized_publish_time[:10],
        "publish_time": normalized_publish_time,
        "title": clean_title,
        "url": clean_url,
        "body_text": body[:MAX_BODY_PREVIEW_CHARS] if body else None,
        "body_size_bytes": len(body.encode("utf-8")) if body else None,
        "related_text": _clean_text(related_text),
        "raw_row_json": _raw_row_json(raw),
    }
    if row["body_text"]:
        row["body_download_status"] = "success"
    return row


def _normalize_news(
    *,
    source_id: str,
    rows: list[dict[str, Any]],
    instrument_hints: list[dict[str, str]],
    observed_at: str,
    raw_object_id: str,
) -> list[dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for row in rows:
        title = _clean_text(row.get("title"))
        publish_time = _clean_text(row.get("publish_time"))
        publish_date = _clean_text(row.get("publish_date")) or (publish_time[:10] if publish_time else None)
        if not title or not publish_time or not publish_date:
            continue
        url = _clean_text(row.get("url"))
        body_text = _clean_body_text(row.get("body_text"))
        related_text = _clean_text(row.get("related_text"))
        source_record_id = _clean_text(row.get("source_record_id")) or _source_record_hash(
            source_id=source_id,
            title=title,
            publish_time=publish_time,
            url=url,
        )
        matches = _match_instruments(
            title=title,
            body_text=body_text,
            related_text=related_text,
            url=url,
            hints=instrument_hints,
        )
        for instrument in matches:
            news_id = f"{_slug(source_id)}_{_slug(source_record_id)}_{instrument}"
            record = {
                "news_id": news_id,
                "publish_date": publish_date,
                "publish_time": publish_time,
                "instrument": instrument,
                "title": title,
                "url": url,
                "source_id": source_id,
                "source_record_id": source_record_id,
                "observed_at": observed_at,
                "collect_time": observed_at,
                "raw_object_id": raw_object_id,
                "parser_version": PARSER_VERSION,
            }
            if body_text:
                record.update(
                    {
                        "body_text": body_text[:MAX_BODY_PREVIEW_CHARS],
                        "body_download_status": row.get("body_download_status") or "success",
                        "body_error_message": None,
                        "body_size_bytes": row.get("body_size_bytes"),
                    }
                )
            records[news_id] = record
    return list(records.values())


def _match_instruments(
    *,
    title: str,
    body_text: str | None,
    related_text: str | None,
    url: str | None,
    hints: list[dict[str, str]],
) -> list[str]:
    haystack = f"{title} {body_text or ''} {related_text or ''} {url or ''}"
    matched = []
    for hint in hints:
        symbol = hint["symbol"]
        name = hint["name"]
        if symbol and re.search(rf"(?<!\d){re.escape(symbol)}(?!\d)", haystack):
            matched.append(hint["instrument"])
            continue
        if name and name in haystack:
            matched.append(hint["instrument"])
    return sorted(set(matched))


def _parse_publish_time(
    *,
    publish_time: Any,
    publish_date: Any,
    crawl_date: str,
) -> str | None:
    date_part = _parse_publish_date(publish_date, crawl_date=crawl_date)
    if publish_time is None or publish_time == "":
        return f"{date_part} 00:00:00" if date_part else None
    if isinstance(publish_time, (int, float)):
        timestamp = float(publish_time)
        if timestamp > 10_000_000_000:
            timestamp = timestamp / 1000
        return datetime.fromtimestamp(timestamp).replace(microsecond=0).isoformat(sep=" ")
    text = _clean_text(publish_time)
    if not text:
        return f"{date_part} 00:00:00" if date_part else None
    if re.fullmatch(r"\d{10,13}", text):
        return _parse_publish_time(publish_time=int(text), publish_date=None, crawl_date=crawl_date)
    text = text.replace("T", " ").replace("/", "-")
    match = re.search(r"(?P<date>\d{4}-\d{1,2}-\d{1,2})\s+(?P<time>\d{1,2}:\d{2}(?::\d{2})?)", text)
    if match:
        return _format_datetime(match.group("date"), match.group("time"))
    match = re.fullmatch(r"(?P<date>\d{4}-\d{1,2}-\d{1,2})", text)
    if match:
        return _format_datetime(match.group("date"), "00:00:00")
    match = re.search(r"(?P<date>\d{1,2}-\d{1,2})\s+(?P<time>\d{1,2}:\d{2}(?::\d{2})?)", text)
    if match:
        year = crawl_date[:4]
        return _format_datetime(f"{year}-{match.group('date')}", match.group("time"))
    match = re.search(r"(?P<time>\d{1,2}:\d{2}(?::\d{2})?)", text)
    if match:
        base_date = date_part or crawl_date
        if "昨天" in text:
            base_date = (
                datetime.strptime(crawl_date, "%Y-%m-%d").date() - timedelta(days=1)
            ).isoformat()
        return _format_datetime(base_date, match.group("time"))
    return text[:19] if len(text) >= 16 else None


def _parse_publish_date(value: Any, *, crawl_date: str) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp = timestamp / 1000
        return datetime.fromtimestamp(timestamp).date().isoformat()
    text = _clean_text(value)
    if not text:
        return None
    if re.fullmatch(r"\d{10,13}", text):
        return _parse_publish_date(int(text), crawl_date=crawl_date)
    text = text.replace("/", "-")
    match = re.search(r"(?P<date>\d{4}-\d{1,2}-\d{1,2})", text)
    if match:
        return _format_date(match.group("date"))
    match = re.search(r"(?P<date>\d{1,2}-\d{1,2})", text)
    if match:
        return _format_date(f"{crawl_date[:4]}-{match.group('date')}")
    return None


def _format_datetime(date_text: str, time_text: str) -> str:
    clean_time = time_text if len(time_text.split(":")) == 3 else f"{time_text}:00"
    return f"{_format_date(date_text)} {clean_time.zfill(8)}"


def _format_date(value: str) -> str:
    parts = [int(part) for part in value.split("-")]
    if len(parts) != 3:
        raise ValueError(f"invalid date: {value}")
    return f"{parts[0]:04d}-{parts[1]:02d}-{parts[2]:02d}"


def _field(
    row: dict[str, Any],
    *,
    names: tuple[str, ...],
    positions: tuple[int, ...] = (),
) -> Any:
    for name in names:
        if name in row and row[name] not in (None, ""):
            return row[name]
    keys = list(row)
    for position in positions:
        if 0 <= position < len(keys):
            value = row.get(keys[position])
            if value not in (None, ""):
                return value
    return None


def _headers(source_id: str) -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 QDC-Crawler/0.1 (+local research)",
        "Referer": SOURCE_REFERERS.get(source_id, "https://finance.sina.com.cn/"),
        "Accept": "application/json,text/javascript,text/html,*/*;q=0.8",
    }


def _wallstreetcn_url(item: dict[str, Any]) -> str | None:
    uri = _clean_text(item.get("uri"))
    if uri:
        return uri
    live_id = _clean_text(item.get("id"))
    return f"https://wallstreetcn.com/livenews/{live_id}" if live_id else None


def _json_from_maybe_javascript(text: str) -> Any:
    clean = text.strip()
    if clean.startswith("var "):
        clean = re.sub(r"^var\s+\w+\s*=", "", clean).strip()
    match = re.match(r"^[\w$]+\((?P<body>.*)\)\s*;?\s*$", clean, flags=re.DOTALL)
    if match:
        clean = match.group("body").strip()
    if clean.endswith(";"):
        clean = clean[:-1]
    return json.loads(clean)


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("source_record_id") or row.get("url") or row.get("title"))
        deduped[key] = row
    return list(deduped.values())


def _title_from_body(value: str | None) -> str | None:
    text = _clean_body_text(value)
    if not text:
        return None
    match = re.match(r"【(?P<title>[^】]{2,80})】", text)
    if match:
        return f"【{match.group('title')}】"
    first_line = text.splitlines()[0]
    first_line = re.split(r"[。！？!?]\s*", first_line)[0]
    return first_line[:80] or None


def _html_to_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    text = re.sub(r"(?is)<(script|style).*?</\1>", " ", text)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\s*>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    return _clean_body_text(html.unescape(text))


def _strip_tags(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"<[^>]+>", " ", html.unescape(str(value)))


def _clean_body_text(value: Any) -> str | None:
    if value is None:
        return None
    lines = []
    for line in str(value).replace("\r", "\n").split("\n"):
        cleaned = re.sub(r"[ \t\u3000]+", " ", html.unescape(line)).strip()
        if cleaned:
            lines.append(cleaned)
    return "\n".join(lines) or None


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", html.unescape(str(value))).strip()
    return text or None


def _source_record_hash(
    *,
    source_id: str,
    title: str,
    publish_time: str,
    url: str | None,
) -> str:
    value = f"{source_id}|{publish_time}|{title}|{url or ''}"
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:16]


def _raw_row_json(row: dict[str, Any]) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)[:MAX_RAW_ROW_CHARS]


def _slug(value: str | None) -> str:
    text = value or "unknown"
    slug = re.sub(r"[^0-9A-Za-z]+", "_", text).strip("_")
    return slug[:80] or "unknown"


def _timestamp() -> str:
    return datetime.now().replace(microsecond=0).isoformat(sep=" ")
