"""Eastmoney rolling-news crawler used as a public metadata-only source."""

from __future__ import annotations

import html
import re
from datetime import datetime
from html.parser import HTMLParser
from typing import Any

from quant_data_center.crawlers.date_scan import scan_rolling_date_window
from quant_data_center.crawlers.runtime import (
    call_with_proxy_policy,
    make_deadline,
    raise_if_deadline_exceeded,
    request_timeout,
    sleep_with_deadline,
)
from quant_data_center.crawlers.metrics import build_document_source_metrics
from quant_data_center.settings import QdcSettings
from quant_data_center.storage.database import QdcDatabase
from quant_data_center.storage.objects import QdcObjectStore
from quant_data_center.storage.silver import SilverStore
from quant_data_center.utils.instruments import normalize_instrument


EASTMONEY_ROLL_NEWS_URL_TEMPLATE = "https://roll.eastmoney.com/default_{page_num}.html"
EASTMONEY_REFERER = "https://roll.eastmoney.com/"
PARSER_VERSION = "eastmoney_roll_news_v1"
MAX_BODY_PREVIEW_CHARS = 1200


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
        request_timeout_seconds: float = 30.0,
        source_timeout_seconds: float | None = None,
        instrument_filter: list[str] | None = None,
    ) -> dict[str, Any]:
        requests = __import__("requests")
        deadline = make_deadline(source_timeout_seconds)
        observed_at = _timestamp()

        def fetch_page(page_num: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
            raise_if_deadline_exceeded(deadline, source_id=source_id)
            url = EASTMONEY_ROLL_NEWS_URL_TEMPLATE.format(page_num=page_num)
            response = call_with_proxy_policy(
                requests.get,
                url,
                headers=_headers(),
                timeout=request_timeout(
                    deadline=deadline,
                    default_seconds=request_timeout_seconds,
                    source_id=source_id,
                ),
                use_environment_proxy=self.settings.use_environment_proxy,
            )
            response.raise_for_status()
            rows = _extract_rows(
                text=response.text,
                limit=page_size,
            )
            return rows, {
                "page_num": page_num,
                "url": url,
                "status_code": response.status_code,
                "news_count": len(rows),
            }

        scan = scan_rolling_date_window(
            target_date=crawl_date,
            max_pages=max_pages,
            fetch_page=fetch_page,
            publish_time_getter=lambda row: row.get("publish_time")
            or row.get("publish_date"),
            before_fetch=lambda: sleep_with_deadline(
                min_delay_seconds,
                deadline=deadline,
                source_id=source_id,
            )
            if min_delay_seconds > 0
            else None,
        )
        pages = scan.pages
        provider_rows = scan.provider_rows
        target_provider_rows = scan.target_rows

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
                    "instrument_filter": instrument_filter or [],
                },
                "pages": pages,
            },
        )
        bronze_object_id = self.objects.put_bronze_parquet(
            dataset="news",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"eastmoney_roll_news_{crawl_date}",
            records=provider_rows,
        )
        instrument_hints = self._instrument_hints(instrument_filter=instrument_filter)
        records = _normalize_news(
            source_id=source_id,
            rows=target_provider_rows,
            instrument_hints=instrument_hints,
            observed_at=observed_at,
            raw_object_id=raw_object_id,
        )
        source_metrics = build_document_source_metrics(
            provider_record_count=len(target_provider_rows),
            provider_record_keys=(_provider_key(row) for row in target_provider_rows),
            parsed_record_keys=(
                _provider_key(row) for row in target_provider_rows if _is_parsable_row(row)
            ),
            mapped_source_record_ids=(record.get("source_record_id") for record in records),
        )
        scan_fields = scan.manifest_fields
        mapping_failure_reason = (
            "empty_instrument_dictionary"
            if not instrument_hints and source_metrics["parsed_unique_record_count"]
            else None
        )
        body_stats = _attach_article_bodies(
            requests_module=requests,
            records=records,
            request_timeout_seconds=request_timeout_seconds,
            deadline=deadline,
            source_id=source_id,
            use_environment_proxy=self.settings.use_environment_proxy,
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
                "instrument_filter": instrument_filter or [],
                "raw_object_id": raw_object_id,
                "scanned_provider_record_count": len(provider_rows),
                "provider_record_count": len(target_provider_rows),
                "instrument_dictionary_count": len(instrument_hints),
                "mapping_failure_reason": mapping_failure_reason,
                "body_policy": (
                    "copyright-aware preview: extracted article body text is truncated "
                    f"to {MAX_BODY_PREVIEW_CHARS} characters; full article HTML is not persisted"
                ),
                **scan_fields,
                **source_metrics,
                **body_stats,
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
            "scanned_provider_record_count": len(provider_rows),
            "provider_record_count": len(target_provider_rows),
            "mapped_record_count": row_count,
            "instrument_dictionary_count": len(instrument_hints),
            "mapping_failure_reason": mapping_failure_reason,
            **scan_fields,
            **source_metrics,
            **body_stats,
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


def _is_parsable_row(row: dict[str, Any]) -> bool:
    return bool(
        _clean_text(row.get("title"))
        and _clean_text(row.get("publish_time"))
        and (_clean_text(row.get("publish_date")) or _clean_text(row.get("publish_time"))[:10])
    )


def _provider_key(row: dict[str, Any]) -> str:
    return (
        _article_id(_clean_text(row.get("url")))
        or _clean_text(row.get("url"))
        or f"{_clean_text(row.get('title'))}|{_clean_text(row.get('publish_time'))}"
    )


def _attach_article_bodies(
    *,
    requests_module: Any,
    records: list[dict[str, Any]],
    request_timeout_seconds: float,
    deadline: float | None,
    source_id: str,
    use_environment_proxy: bool,
) -> dict[str, int]:
    stats = {"body_downloaded_count": 0, "body_failed_count": 0, "body_skipped_count": 0}
    by_url: dict[str, dict[str, Any]] = {}
    for record in records:
        url = _clean_text(record.get("url"))
        if not url:
            record["body_download_status"] = "missing_url"
            stats["body_skipped_count"] += 1
            continue
        if url not in by_url:
            by_url[url] = _fetch_article_body(
                requests_module=requests_module,
                url=url,
                request_timeout_seconds=request_timeout_seconds,
                deadline=deadline,
                source_id=source_id,
                use_environment_proxy=use_environment_proxy,
            )
        result = by_url[url]
        for key, value in result.items():
            record[key] = value
        if result["body_download_status"] == "success":
            stats["body_downloaded_count"] += 1
        else:
            stats["body_failed_count"] += 1
    return stats


def _fetch_article_body(
    *,
    requests_module: Any,
    url: str,
    request_timeout_seconds: float,
    deadline: float | None,
    source_id: str,
    use_environment_proxy: bool,
) -> dict[str, Any]:
    try:
        raise_if_deadline_exceeded(deadline, source_id=source_id)
        response = call_with_proxy_policy(
            requests_module.get,
            url,
            headers=_article_headers(url),
            timeout=request_timeout(
                deadline=deadline,
                default_seconds=request_timeout_seconds,
                source_id=source_id,
            ),
            use_environment_proxy=use_environment_proxy,
        )
        response.raise_for_status()
        text = str(response.text or "")
        body_text = _extract_article_body_text(text)
        if not body_text:
            raise ValueError("article body text not found")
        return {
            "body_text": body_text[:MAX_BODY_PREVIEW_CHARS],
            "body_download_status": "success",
            "body_error_message": None,
            "body_size_bytes": len(text.encode(response.encoding or "utf-8", errors="ignore")),
        }
    except Exception as exc:
        return {
            "body_text": None,
            "body_download_status": "failed",
            "body_error_message": str(exc)[:500],
            "body_size_bytes": None,
        }


def _article_headers(url: str) -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 QDC-Crawler/0.1 (+local research)",
        "Referer": EASTMONEY_REFERER if "eastmoney.com" in url else EASTMONEY_REFERER,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }


def _extract_article_body_text(text: str) -> str:
    parser = _ContentBodyParser()
    parser.feed(text)
    parser.close()
    body_text = parser.text()
    if body_text:
        return body_text
    match = re.search(r"<article[^>]*>(?P<body>.*?)</article>", text, flags=re.I | re.S)
    if match:
        return _html_to_text(match.group("body"))
    return ""


class _ContentBodyParser(HTMLParser):
    _VOID_TAGS = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._capturing = False
        self._depth = 0
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = {name.lower(): value or "" for name, value in attrs}
        if not self._capturing and attrs_dict.get("id") == "ContentBody":
            self._capturing = True
            self._depth = 1
            return
        if not self._capturing:
            return
        if tag.lower() in {"p", "br", "div", "center"}:
            self._parts.append("\n")
        if tag.lower() not in self._VOID_TAGS:
            self._depth += 1

    def handle_endtag(self, tag: str) -> None:
        if not self._capturing:
            return
        if tag.lower() in self._VOID_TAGS:
            return
        if tag.lower() in {"p", "div", "center"}:
            self._parts.append("\n")
        self._depth -= 1
        if self._depth <= 0:
            self._capturing = False

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self._capturing and tag.lower() in {"br", "hr"}:
            self._parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self._capturing:
            self._parts.append(data)

    def text(self) -> str:
        return _normalize_body_text("".join(self._parts))


def _html_to_text(value: str) -> str:
    text = re.sub(r"(?is)<(script|style).*?</\1>", " ", value)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\s*>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    return _normalize_body_text(html.unescape(text))


def _normalize_body_text(value: str) -> str:
    lines = []
    for line in value.replace("\r", "\n").split("\n"):
        cleaned = re.sub(r"[ \t\u3000]+", " ", html.unescape(line)).strip()
        if cleaned:
            lines.append(cleaned)
    return "\n".join(lines)


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
