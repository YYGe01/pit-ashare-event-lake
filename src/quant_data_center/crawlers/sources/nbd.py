"""NBD company-news crawler used as an optional metadata-only source."""

from __future__ import annotations

import html
import re
from datetime import datetime
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


NBD_ROOT = "https://www.nbd.com.cn/"
NBD_COLUMN_URLS = (
    "https://www.nbd.com.cn/columns/1285/",
)
PARSER_VERSION = "nbd_company_news_v1"


class NbdCompanyNewsCrawler:
    """Fetch NBD public list-page metadata and map titles to known stocks."""

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
        requests = __import__("requests")
        deadline = make_deadline(source_timeout_seconds)
        pages = []
        provider_rows: list[dict[str, Any]] = []
        observed_at = _timestamp()
        page_count = max_pages or 1
        for column_url in NBD_COLUMN_URLS:
            for page_num in range(1, page_count + 1):
                raise_if_deadline_exceeded(deadline, source_id=source_id)
                url = _column_page_url(column_url=column_url, page_num=page_num)
                response = requests.get(
                    url,
                    headers=_headers(),
                    timeout=request_timeout(
                        deadline=deadline,
                        default_seconds=request_timeout_seconds,
                        source_id=source_id,
                    ),
                )
                response.raise_for_status()
                rows = [
                    row
                    for row in _extract_rows(text=response.text, limit=max(200, page_size * 5))
                    if _row_publish_date(row) == crawl_date
                ][:page_size]
                provider_rows.extend(rows)
                pages.append(
                    {
                        "page_num": page_num,
                        "url": url,
                        "status_code": response.status_code,
                        "news_count": len(rows),
                        "items": rows,
                    }
                )
                if page_num < page_count and min_delay_seconds > 0:
                    sleep_with_deadline(
                        min_delay_seconds,
                        deadline=deadline,
                        source_id=source_id,
                    )
            if min_delay_seconds > 0:
                sleep_with_deadline(
                    min_delay_seconds,
                    deadline=deadline,
                    source_id=source_id,
                )

        raw_object_id = self.objects.put_json(
            dataset="news",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"nbd_company_news_{crawl_date}",
            payload={
                "function": "nbd_company_news_columns",
                "params": {
                    "crawl_date": crawl_date,
                    "page_size": page_size,
                    "max_pages": max_pages,
                    "column_urls": list(NBD_COLUMN_URLS),
                    "instrument_filter": instrument_filter or [],
                },
                "pages": pages,
            },
        )
        bronze_object_id = self.objects.put_bronze_parquet(
            dataset="news",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"nbd_company_news_{crawl_date}",
            records=provider_rows,
        )
        instrument_hints = self._instrument_hints(instrument_filter=instrument_filter)
        records = _normalize_news(
            source_id=source_id,
            rows=provider_rows,
            instrument_hints=instrument_hints,
            observed_at=observed_at,
            raw_object_id=raw_object_id,
        )
        document_bundle = self.objects.put_document_bundle(
            dataset="news",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"nbd_company_news_{crawl_date}",
            manifest={
                "function": "nbd_company_news_columns",
                "accepted_date_rule": "publish_time parsed from NBD list pages is required; publish_date is derived from publish_time, not crawl_date",
                "copyright_policy": "metadata_only",
                "instrument_filter": instrument_filter or [],
                "raw_object_id": raw_object_id,
                "provider_record_count": len(provider_rows),
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


def _column_page_url(*, column_url: str, page_num: int) -> str:
    if page_num <= 1:
        return column_url
    return urljoin(column_url, f"page/{page_num}/")


def _row_publish_date(row: dict[str, Any]) -> str | None:
    publish_time = _clean_text(row.get("publish_time"))
    if publish_time:
        return publish_time[:10]
    return _clean_text(row.get("publish_date"))


def _headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 QDC-Crawler/0.1 (+local research)",
        "Referer": NBD_ROOT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }


def _extract_rows(*, text: str, limit: int) -> list[dict[str, str]]:
    rows = _extract_full_timestamp_rows(text=text)
    rows.extend(_extract_column_rows(text=text))
    deduped: dict[str, dict[str, str]] = {}
    for row in rows:
        source_record_id = _article_id(row.get("url")) or row.get("url") or row.get("title")
        if source_record_id:
            deduped[source_record_id] = row
        if len(deduped) >= limit:
            break
    return list(deduped.values())


def _extract_full_timestamp_rows(*, text: str) -> list[dict[str, str]]:
    rows = []
    pattern = re.compile(
        r"<li[^>]*>.*?<a\s+href=\"(?P<url>[^\"]*?/articles/(?P<date>\d{4}-\d{2}-\d{2})/\d+\.html)\"[^>]*(?:title=\"(?P<title_attr>[^\"]*)\")?[^>]*>.*?</a>\s*<span>\s*(?P<publish_time>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s*</span>",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(text):
        title = _clean_text(html.unescape(match.group("title_attr") or ""))
        if not title:
            title = _title_from_anchor(match.group(0))
        publish_time = _clean_text(match.group("publish_time"))
        url = _absolute_url(match.group("url"))
        if title and publish_time and url:
            rows.append(
                {
                    "publish_date": publish_time[:10],
                    "publish_time": publish_time,
                    "url": url,
                    "title": title,
                }
            )
    return rows


def _extract_column_rows(*, text: str) -> list[dict[str, str]]:
    rows = []
    block_pattern = re.compile(
        r"<p\s+class=\"u-channeltime\">\s*(?P<date>\d{4}-\d{2}-\d{2})\s*</p>(?P<body>.*?)(?=<p\s+class=\"u-channeltime\">|</div>\s*<div\s+class=\"m-list\"|</body>|\Z)",
        flags=re.IGNORECASE | re.DOTALL,
    )
    item_pattern = re.compile(
        r"<li[^>]*class=\"u-news-title\"[^>]*>.*?<a\s+href=\"(?P<url>[^\"]*?/articles/(?P<url_date>\d{4}-\d{2}-\d{2})/\d+\.html)\"[^>]*(?:title=\"(?P<title_attr>[^\"]*)\")?[^>]*>(?P<title_body>.*?)</a>\s*<span>\s*(?P<time>\d{2}:\d{2}:\d{2})\s*</span>",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for block in block_pattern.finditer(text):
        fallback_date = _clean_text(block.group("date"))
        for match in item_pattern.finditer(block.group("body")):
            title = _clean_text(html.unescape(match.group("title_attr") or ""))
            if not title:
                title = _clean_text(_strip_tags(html.unescape(match.group("title_body"))))
            publish_date = _clean_text(match.group("url_date")) or fallback_date
            publish_time = f"{publish_date} {_clean_text(match.group('time'))}"
            url = _absolute_url(match.group("url"))
            if title and publish_date and publish_time and url:
                rows.append(
                    {
                        "publish_date": publish_date,
                        "publish_time": publish_time,
                        "url": url,
                        "title": title,
                    }
                )
    return rows


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
        publish_date = publish_time[:10] if publish_time else _clean_text(row.get("publish_date"))
        if not title or not publish_time or not publish_date:
            continue
        url = _clean_text(row.get("url"))
        source_record_id = _article_id(url) or url or title
        for instrument in _match_instruments(title=title, url=url, hints=instrument_hints):
            news_id = f"nbd_{_slug(source_record_id)}_{instrument}"
            records[news_id] = {
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
    return list(records.values())


def _match_instruments(
    *, title: str, url: str | None, hints: list[dict[str, str]]
) -> list[str]:
    haystack = f"{title} {url or ''}"
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


def _title_from_anchor(value: str) -> str | None:
    span_match = re.search(r"<span>\s*(?P<title>.*?)\s*</span>", value, flags=re.DOTALL)
    if span_match:
        return _clean_text(_strip_tags(html.unescape(span_match.group("title"))))
    anchor_match = re.search(r"<a[^>]*>(?P<title>.*?)</a>", value, flags=re.DOTALL)
    if anchor_match:
        return _clean_text(_strip_tags(html.unescape(anchor_match.group("title"))))
    return None


def _absolute_url(value: str | None) -> str | None:
    if not value:
        return None
    return urljoin(NBD_ROOT, html.unescape(value))


def _article_id(url: str | None) -> str | None:
    if not url:
        return None
    match = re.search(r"/articles/\d{4}-\d{2}-\d{2}/(\d+)\.html", url)
    return match.group(1) if match else None


def _strip_tags(value: str) -> str:
    return re.sub(r"<[^>]+>", "", value)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def _slug(value: str | None) -> str:
    text = value or "unknown"
    slug = re.sub(r"[^0-9A-Za-z]+", "_", text).strip("_")
    return slug[:80] or "unknown"


def _timestamp() -> str:
    return datetime.now().replace(microsecond=0).isoformat(sep=" ")
