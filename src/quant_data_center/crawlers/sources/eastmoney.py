"""Eastmoney rolling-news crawler used as a public metadata-only source."""

from __future__ import annotations

import html
import re
import time
from datetime import datetime
from typing import Any

from quant_data_center.settings import QdcSettings
from quant_data_center.storage.database import QdcDatabase
from quant_data_center.storage.objects import QdcObjectStore
from quant_data_center.storage.silver import SilverStore


EASTMONEY_ROLL_NEWS_URL_TEMPLATE = "https://roll.eastmoney.com/default_{page_num}.html"
EASTMONEY_REFERER = "https://roll.eastmoney.com/"
PARSER_VERSION = "eastmoney_roll_news_v1"


class EastmoneyRollNewsCrawler:
    """Fetch Eastmoney rolling-news metadata and map titles to known stocks."""

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
    ) -> dict[str, Any]:
        requests = __import__("requests")
        pages = []
        provider_rows: list[dict[str, Any]] = []
        observed_at = _timestamp()
        page_count = max_pages or 1
        for page_num in range(1, page_count + 1):
            url = EASTMONEY_ROLL_NEWS_URL_TEMPLATE.format(page_num=page_num)
            response = requests.get(url, headers=_headers(), timeout=30)
            response.raise_for_status()
            rows = _extract_rows(
                text=response.text,
                limit=page_size,
            )
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
                time.sleep(min_delay_seconds)

        raw_object_id = self.objects.put_json(
            dataset="news",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"eastmoney_roll_news_{crawl_date}",
            payload={
                "function": "eastmoney_roll_news_html",
                "params": {
                    "crawl_date": crawl_date,
                    "page_size": page_size,
                    "max_pages": max_pages,
                },
                "pages": pages,
            },
        )
        document_bundle = self.objects.put_document_bundle(
            dataset="news",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"eastmoney_roll_news_{crawl_date}",
            manifest={
                "function": "eastmoney_roll_news_html",
                "accepted_date_rule": "publish_time in page HTML is required; publish_date is derived from publish_time, not crawl_date",
                "copyright_policy": "metadata_only",
                "raw_object_id": raw_object_id,
            },
            records=provider_rows,
        )
        bronze_object_id = self.objects.put_bronze_parquet(
            dataset="news",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"eastmoney_roll_news_{crawl_date}",
            records=provider_rows,
        )
        records = _normalize_news(
            source_id=source_id,
            rows=provider_rows,
            instrument_hints=self._instrument_hints(),
            observed_at=observed_at,
            raw_object_id=raw_object_id,
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

    def _instrument_hints(self) -> list[dict[str, str]]:
        with self.database.connect() as conn:
            rows = conn.execute(
                """
                select instrument, symbol, name
                from qdc_silver.stock_basic
                where coalesce(is_active, true)
                order by instrument
                """
            ).fetchall()
        return [
            {
                "instrument": str(instrument),
                "symbol": str(symbol),
                "name": str(name or ""),
            }
            for instrument, symbol, name in rows
        ]


def _headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 QDC-Crawler/0.1 (+local research)",
        "Referer": EASTMONEY_REFERER,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }


def _extract_rows(*, text: str, limit: int) -> list[dict[str, str]]:
    rows = []
    pattern = re.compile(
        r"<li>\s*<span>\s*(?P<publish_time>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})\s*</span>"
        r"\s*\[<a[^>]*>(?P<category>.*?)</a>\]"
        r"\s*<a\s+href=\"(?P<url>[^\"]+)\"\s+title=\"(?P<title>[^\"]+)\"",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(text):
        publish_time = _clean_text(match.group("publish_time"))
        if not publish_time:
            continue
        rows.append(
            {
                "publish_time": f"{publish_time}:00"
                if len(publish_time) == 16
                else publish_time,
                "publish_date": publish_time[:10],
                "category": _clean_text(_strip_tags(match.group("category"))) or "",
                "url": html.unescape(match.group("url")),
                "title": _clean_text(html.unescape(match.group("title"))) or "",
            }
        )
        if len(rows) >= limit:
            break
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
            news_id = f"eastmoney_{_slug(source_record_id)}_{instrument}"
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


def _article_id(url: str | None) -> str | None:
    if not url:
        return None
    match = re.search(r"/a/(\d+)\.html", url)
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
