"""SSE announcement daily crawler."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any
from urllib.parse import urljoin

from quant_data_center.crawlers.date_scan import exact_date_query_scan_fields
from quant_data_center.crawlers.runtime import (
    make_deadline,
    raise_if_deadline_exceeded,
    request_timeout,
    sleep_with_deadline,
)
from quant_data_center.settings import QdcSettings
from quant_data_center.storage.objects import QdcObjectStore
from quant_data_center.storage.silver import SilverStore
from quant_data_center.utils.instruments import normalize_instrument


SSE_ANNOUNCEMENT_URL = "https://query.sse.com.cn/security/stock/queryCompanyBulletin.do"
SSE_REFERER = "https://www.sse.com.cn/disclosure/listedinfo/announcement/"
SSE_ROOT = "https://www.sse.com.cn/"
PARSER_VERSION = "sse_announcement_v1"


class SseAnnouncementCrawler:
    """Fetch SSE announcement list pages for one disclosure date."""

    def __init__(self, settings: QdcSettings) -> None:
        self.settings = settings
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
        download_pdfs: bool = True,
        pdf_limit: int | None = None,
        instrument_filter: list[str] | None = None,
        request_timeout_seconds: float = 30.0,
        source_timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        requests = __import__("requests")
        deadline = make_deadline(source_timeout_seconds)
        pages = []
        announcements = []
        observed_at = _timestamp()
        page_count = 1
        page_num = 1
        while page_num <= page_count:
            raise_if_deadline_exceeded(deadline, source_id=source_id)
            params = _query_params(
                crawl_date=crawl_date,
                page_num=page_num,
                page_size=page_size,
            )
            response = requests.get(
                SSE_ANNOUNCEMENT_URL,
                headers=_headers(),
                params=params,
                timeout=request_timeout(
                    deadline=deadline,
                    default_seconds=request_timeout_seconds,
                    source_id=source_id,
                ),
            )
            response.raise_for_status()
            body = response.json()
            page_rows = _extract_rows(body)
            announcements.extend(page_rows)
            page_count = _page_count(body) or page_count
            pages.append(
                {
                    "page_num": page_num,
                    "request": params,
                    "status_code": response.status_code,
                    "page_count": page_count,
                    "announcement_count": len(page_rows),
                    "announcements": page_rows,
                }
            )
            page_num += 1
            if max_pages is not None and page_num > max_pages:
                break
            if page_num <= page_count and min_delay_seconds > 0:
                sleep_with_deadline(
                    min_delay_seconds,
                    deadline=deadline,
                    source_id=source_id,
                )

        raw_object_id = self.objects.put_json(
            dataset="announcement",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"sse_announcement_{crawl_date}",
            payload={
                "function": "sse_query_company_bulletin",
                "url": SSE_ANNOUNCEMENT_URL,
                "params": {
                    "crawl_date": crawl_date,
                    "page_size": page_size,
                    "max_pages": max_pages,
                },
                "pages": pages,
            },
        )
        scan_fields = exact_date_query_scan_fields(
            target_date=crawl_date,
            page_count_scanned=len(pages),
            source_reported_page_count=page_count,
            max_pages=max_pages,
            provider_record_count=len(announcements),
        )
        bronze_object_id = self.objects.put_bronze_parquet(
            dataset="announcement",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"sse_announcement_{crawl_date}",
            records=announcements,
        )
        records = _normalize_announcements(
            source_id=source_id,
            crawl_date=crawl_date,
            rows=announcements,
            raw_object_id=raw_object_id,
            observed_at=observed_at,
            parser_version=PARSER_VERSION,
            instrument_filter=_normalized_instrument_filter(instrument_filter),
        )
        pdf_stats = _attach_pdf_objects(
            requests_module=requests,
            objects=self.objects,
            source_id=source_id,
            crawl_date=crawl_date,
            records=records,
            enabled=download_pdfs,
            pdf_limit=pdf_limit,
            min_delay_seconds=min_delay_seconds,
            request_timeout_seconds=45.0,
            deadline=deadline,
        )
        document_bundle = self.objects.put_document_bundle(
            dataset="announcement",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"sse_announcement_{crawl_date}",
            manifest={
                "function": "sse_query_company_bulletin",
                "url": SSE_ANNOUNCEMENT_URL,
                "accepted_date_rule": "SSEDATE must equal crawl_date",
                "raw_object_id": raw_object_id,
                "instrument_filter": instrument_filter or [],
                **scan_fields,
            },
            records=records,
        )
        row_count = self.silver.upsert_announcements(records)
        bundle_object_count = 1 + int(document_bundle["records_object_id"] is not None)
        return {
            "document_count": row_count,
            "raw_object_count": (
                1
                + bundle_object_count
                + int(bronze_object_id is not None)
                + pdf_stats["downloaded"]
            ),
            "raw_object_id": raw_object_id,
            "bronze_object_id": bronze_object_id,
            **document_bundle,
            "provider_record_count": len(announcements),
            **scan_fields,
            "pdf_downloaded_count": pdf_stats["downloaded"],
            "pdf_failed_count": pdf_stats["failed"],
            "pdf_skipped_count": pdf_stats["skipped"],
        }


def _query_params(*, crawl_date: str, page_num: int, page_size: int) -> dict[str, str]:
    return {
        "isPagination": "true",
        "productId": "",
        "securityType": "0101,120100,020100,020200,120200",
        "reportType2": "",
        "reportType": "ALL",
        "beginDate": crawl_date,
        "endDate": crawl_date,
        "pageHelp.pageSize": str(page_size),
        "pageHelp.pageNo": str(page_num),
        "pageHelp.beginPage": str(page_num),
        "pageHelp.cacheSize": "1",
        "pageHelp.endPage": str(page_num),
    }


def _headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 QDC-Crawler/0.1 (+local research)",
        "Referer": SSE_REFERER,
        "Accept": "application/json,text/plain,*/*",
    }


def _extract_rows(body: dict[str, Any]) -> list[dict[str, Any]]:
    result = body.get("result")
    if isinstance(result, list):
        return [row for row in result if isinstance(row, dict)]
    page_help = body.get("pageHelp")
    data = page_help.get("data") if isinstance(page_help, dict) else []
    return [row for row in data if isinstance(row, dict)]


def _page_count(body: dict[str, Any]) -> int | None:
    page_help = body.get("pageHelp")
    if not isinstance(page_help, dict):
        return None
    try:
        return int(page_help.get("pageCount"))
    except (TypeError, ValueError):
        return None


def _normalize_announcements(
    *,
    source_id: str,
    crawl_date: str,
    rows: list[dict[str, Any]],
    raw_object_id: str,
    observed_at: str,
    parser_version: str,
    instrument_filter: set[str] | None = None,
) -> list[dict[str, Any]]:
    records = []
    for row in rows:
        raw_code = _clean_text(row.get("SECURITY_CODE") or row.get("security_Code"))
        raw_title = _clean_text(row.get("TITLE") or row.get("title"))
        publish_date = _clean_text(row.get("SSEDATE") or row.get("SSEDate"))
        if (
            not raw_code
            or not raw_code.startswith("6")
            or not raw_title
            or publish_date != crawl_date
        ):
            continue
        try:
            instrument = normalize_instrument(raw_code)
        except ValueError:
            continue
        if instrument_filter is not None and instrument not in instrument_filter:
            continue
        raw_url = _clean_text(row.get("URL") or row.get("url"))
        url = urljoin(SSE_ROOT, raw_url) if raw_url else None
        source_record_id = raw_url or f"{raw_code}_{publish_date}_{raw_title}"
        records.append(
            {
                "announcement_id": f"sse_{_slug(source_record_id)}_{instrument}",
                "source_record_id": source_record_id,
                "source_sec_code": raw_code,
                "source_sec_name": _clean_text(
                    row.get("SECURITY_NAME") or row.get("security_Name")
                ),
                "publish_date": publish_date,
                "publish_time": f"{publish_date} 00:00:00",
                "instrument": instrument,
                "title": raw_title,
                "url": url,
                "pdf_url": url,
                "adjunct_url": raw_url,
                "observed_at": observed_at,
                "collect_time": observed_at,
                "raw_object_id": raw_object_id,
                "parser_version": parser_version,
                "source_id": source_id,
            }
        )
    return list({str(record["announcement_id"]): record for record in records}.values())


def _normalized_instrument_filter(values: list[str] | None) -> set[str] | None:
    if not values:
        return None
    instruments = set()
    for value in values:
        try:
            instruments.add(normalize_instrument(str(value)))
        except ValueError:
            continue
    return instruments


def _attach_pdf_objects(
    *,
    requests_module: Any,
    objects: QdcObjectStore,
    source_id: str,
    crawl_date: str,
    records: list[dict[str, Any]],
    enabled: bool,
    pdf_limit: int | None,
    min_delay_seconds: float,
    request_timeout_seconds: float,
    deadline: float | None,
) -> dict[str, int]:
    stats = {"downloaded": 0, "failed": 0, "skipped": 0}
    attempted = 0
    for record in records:
        pdf_url = str(record.get("pdf_url") or "").strip()
        if not pdf_url:
            _apply_pdf_result(record, {"pdf_download_status": "missing_url"})
            stats["skipped"] += 1
            continue
        if not enabled:
            _apply_pdf_result(record, {"pdf_download_status": "skipped"})
            stats["skipped"] += 1
            continue
        if pdf_limit is not None and attempted >= pdf_limit:
            _apply_pdf_result(record, {"pdf_download_status": "skipped_by_limit"})
            stats["skipped"] += 1
            continue
        if attempted > 0 and min_delay_seconds > 0:
            sleep_with_deadline(
                min_delay_seconds,
                deadline=deadline,
                source_id=source_id,
            )
        attempted += 1
        result = _download_pdf(
            requests_module=requests_module,
            objects=objects,
            source_id=source_id,
            crawl_date=crawl_date,
            record=record,
            pdf_url=pdf_url,
            request_timeout_seconds=request_timeout_seconds,
            deadline=deadline,
        )
        _apply_pdf_result(record, result)
        if result["pdf_download_status"] == "success":
            stats["downloaded"] += 1
        else:
            stats["failed"] += 1
    return stats


def _download_pdf(
    *,
    requests_module: Any,
    objects: QdcObjectStore,
    source_id: str,
    crawl_date: str,
    record: dict[str, Any],
    pdf_url: str,
    request_timeout_seconds: float,
    deadline: float | None,
) -> dict[str, Any]:
    try:
        raise_if_deadline_exceeded(deadline, source_id=source_id)
        response = requests_module.get(
            pdf_url,
            headers={**_headers(), "Accept": "application/pdf,*/*"},
            timeout=request_timeout(
                deadline=deadline,
                default_seconds=request_timeout_seconds,
                source_id=source_id,
            ),
        )
        response.raise_for_status()
        content = bytes(response.content)
        if not content:
            raise ValueError("empty PDF response")
        result = objects.put_bytes(
            dataset="announcement",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"sse_pdf_{record['announcement_id']}",
            content=content,
            suffix=".pdf",
            layer="raw_file",
        )
        return {
            "pdf_download_status": "success",
            "pdf_sha256": result["content_hash"],
            "pdf_size_bytes": result["size_bytes"],
            "pdf_object_id": result["object_id"],
            "pdf_error_message": None,
        }
    except Exception as exc:
        return {
            "pdf_download_status": "failed",
            "pdf_sha256": None,
            "pdf_size_bytes": None,
            "pdf_object_id": None,
            "pdf_error_message": str(exc)[:500],
        }


def _apply_pdf_result(record: dict[str, Any], result: dict[str, Any]) -> None:
    for key in (
        "pdf_download_status",
        "pdf_sha256",
        "pdf_size_bytes",
        "pdf_object_id",
        "pdf_error_message",
    ):
        if key in result:
            record[key] = result[key]


def _clean_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def _slug(value: str | None) -> str:
    text = value or "unknown"
    slug = re.sub(r"[^0-9A-Za-z]+", "_", text).strip("_")
    return slug[:80] or "unknown"


def _timestamp() -> str:
    return datetime.now().replace(microsecond=0).isoformat(" ")
