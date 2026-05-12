"""Sina finance rolling-news crawler used as a public metadata-only补位源."""

from __future__ import annotations

import re
import time
from datetime import datetime
from typing import Any

from quant_data_center.settings import QdcSettings
from quant_data_center.storage.database import QdcDatabase
from quant_data_center.storage.objects import QdcObjectStore
from quant_data_center.storage.silver import SilverStore


SINA_ROLL_NEWS_URL = "https://feed.mix.sina.com.cn/api/roll/get"
PARSER_VERSION = "sina_finance_news_v1"


class SinaFinanceNewsCrawler:
    """Fetch metadata from Sina finance roll-news pages and map titles to known stocks."""

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
            params = _query_params(page_num=page_num, page_size=page_size)
            response = requests.get(
                SINA_ROLL_NEWS_URL,
                headers=_headers(),
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            body = response.json()
            rows = _extract_rows(body)
            provider_rows.extend(rows)
            pages.append(
                {
                    "page_num": page_num,
                    "request": params,
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
            stem=f"sina_finance_news_{crawl_date}",
            payload={
                "function": "sina_finance_roll_news",
                "url": SINA_ROLL_NEWS_URL,
                "params": {
                    "crawl_date": crawl_date,
                    "page_size": page_size,
                    "max_pages": max_pages,
                },
                "pages": pages,
            },
        )
        bronze_object_id = self.objects.put_bronze_parquet(
            dataset="news",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"sina_finance_news_{crawl_date}",
            records=provider_rows,
        )
        records = _normalize_news(
            source_id=source_id,
            crawl_date=crawl_date,
            rows=provider_rows,
            instrument_hints=self._instrument_hints(),
        )
        row_count = self.silver.upsert_news(records)
        return {
            "document_count": row_count,
            "raw_object_count": 1 + int(bronze_object_id is not None),
            "raw_object_id": raw_object_id,
            "bronze_object_id": bronze_object_id,
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


def _query_params(*, page_num: int, page_size: int) -> dict[str, str]:
    return {
        "pageid": "153",
        "lid": "1686",
        "num": str(page_size),
        "page": str(page_num),
    }


def _headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 QDC-Crawler/0.1 (+local research)",
        "Referer": "https://finance.sina.com.cn/stock/",
        "Accept": "application/json,text/plain,*/*",
    }


def _extract_rows(body: dict[str, Any]) -> list[dict[str, Any]]:
    data = body.get("result") if isinstance(body.get("result"), dict) else body
    raw_rows = data.get("data") if isinstance(data, dict) else []
    return [row for row in raw_rows if isinstance(row, dict)]


def _normalize_news(
    *,
    source_id: str,
    crawl_date: str,
    rows: list[dict[str, Any]],
    instrument_hints: list[dict[str, str]],
) -> list[dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for row in rows:
        title = _clean_text(row.get("title") or row.get("stitle") or row.get("name"))
        if not title:
            continue
        publish_time = _publish_time(row)
        publish_date = publish_time[:10] if publish_time else crawl_date
        if publish_date != crawl_date:
            continue
        url = _clean_text(row.get("url") or row.get("wapurl"))
        source_record_id = _clean_text(row.get("id") or row.get("docid") or url or title)
        for instrument in _match_instruments(title=title, url=url, hints=instrument_hints):
            news_id = f"sina_{_slug(source_record_id)}_{instrument}"
            records[news_id] = {
                "news_id": news_id,
                "publish_date": publish_date,
                "instrument": instrument,
                "title": title,
                "url": url,
                "source_id": source_id,
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


def _publish_time(row: dict[str, Any]) -> str | None:
    value = row.get("ctime") or row.get("time") or row.get("datetime") or row.get("date")
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp = timestamp / 1000
        return datetime.fromtimestamp(timestamp).replace(microsecond=0).isoformat(sep=" ")
    text = str(value).strip()
    if re.fullmatch(r"\d{10,13}", text):
        return _publish_time({"ctime": int(text)})
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}$", text):
        return f"{text} 00:00:00"
    return text.replace("T", " ")[:19]


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
