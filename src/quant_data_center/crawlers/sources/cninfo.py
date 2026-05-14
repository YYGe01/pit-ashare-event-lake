"""CNINFO announcement daily crawler."""

from __future__ import annotations

import html
import re
from datetime import datetime
from typing import Any

from quant_data_center.crawlers.date_scan import exact_date_query_scan_fields
from quant_data_center.settings import QdcSettings
from quant_data_center.crawlers.runtime import (
    make_deadline,
    raise_if_deadline_exceeded,
    request_timeout,
    sleep_with_deadline,
)
from quant_data_center.storage.objects import QdcObjectStore
from quant_data_center.storage.silver import SilverStore
from quant_data_center.utils.instruments import normalize_instrument


CNINFO_QUERY_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
CNINFO_REFERER = (
    "https://www.cninfo.com.cn/new/commonUrl/pageOfSearch?"
    "lastPage=index&url=disclosure/list/search"
)
CNINFO_STATIC_ROOT = "https://static.cninfo.com.cn/"
PARSER_VERSION = "cninfo_announcement_v1"


class CninfoAnnouncementCrawler:
    """Fetch CNINFO announcement list pages for one disclosure date."""

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
        total_pages = 1
        page_num = 1
        while page_num <= total_pages:
            raise_if_deadline_exceeded(deadline, source_id=source_id)
            payload = _query_payload(crawl_date=crawl_date, page_num=page_num, page_size=page_size)
            response = requests.post(
                CNINFO_QUERY_URL,
                headers=_headers(),
                data=payload,
                timeout=request_timeout(
                    deadline=deadline,
                    default_seconds=request_timeout_seconds,
                    source_id=source_id,
                ),
            )
            response.raise_for_status()
            body = response.json()
            page_announcements = list(body.get("announcements") or [])
            announcements.extend(page_announcements)
            total_pages = int(body.get("totalpages") or total_pages or 1)
            pages.append(
                {
                    "page_num": page_num,
                    "request": payload,
                    "status_code": response.status_code,
                    "total_pages": total_pages,
                    "total_record_num": body.get("totalRecordNum"),
                    "announcement_count": len(page_announcements),
                    "announcements": page_announcements,
                }
            )
            page_num += 1
            if max_pages is not None and page_num > max_pages:
                break
            if page_num <= total_pages and min_delay_seconds > 0:
                sleep_with_deadline(
                    min_delay_seconds,
                    deadline=deadline,
                    source_id=source_id,
                )

        raw_object_id = self.objects.put_json(
            dataset="announcement",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"cninfo_announcement_{crawl_date}",
            payload={
                "function": "cninfo_his_announcement_query",
                "url": CNINFO_QUERY_URL,
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
            source_reported_page_count=total_pages,
            max_pages=max_pages,
            provider_record_count=len(announcements),
        )
        bronze_object_id = self.objects.put_bronze_parquet(
            dataset="announcement",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"cninfo_announcement_{crawl_date}",
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
            stem=f"cninfo_announcement_{crawl_date}",
            manifest={
                "function": "cninfo_his_announcement_query",
                "url": CNINFO_QUERY_URL,
                "accepted_date_rule": "query seDate is exactly crawl_date; publish_date comes from announcementTime when present",
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
                1 + bundle_object_count + int(bronze_object_id is not None) + pdf_stats["downloaded"]
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


def _query_payload(*, crawl_date: str, page_num: int, page_size: int) -> dict[str, str]:
    return {
        "pageNum": str(page_num),
        "pageSize": str(page_size),
        "column": "szse",
        "tabName": "fulltext",
        "plate": "",
        "stock": "",
        "searchkey": "",
        "secid": "",
        "category": "",
        "trade": "",
        "seDate": f"{crawl_date}~{crawl_date}",
        "sortName": "",
        "sortType": "",
        "isHLtitle": "true",
    }


def _headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 QDC-Crawler/0.1 (+local research)",
        "Referer": CNINFO_REFERER,
        "Origin": "https://www.cninfo.com.cn",
    }


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
        raw_code = row.get("secCode")
        raw_id = row.get("announcementId")
        raw_title = row.get("announcementTitle") or row.get("shortTitle")
        if raw_code is None or raw_id is None or raw_title is None:
            continue
        try:
            instrument = normalize_instrument(str(raw_code))
        except ValueError:
            continue
        if instrument_filter is not None and instrument not in instrument_filter:
            continue
        publish_time = _announcement_time(row.get("announcementTime"))
        publish_date = publish_time[:10] if publish_time else crawl_date
        title = _clean_title(str(raw_title))
        adjunct_url = str(row.get("adjunctUrl") or "").strip()
        url = f"{CNINFO_STATIC_ROOT}{adjunct_url}" if adjunct_url else None
        records.append(
            {
                "announcement_id": f"cninfo_{raw_id}_{instrument}",
                "source_record_id": str(raw_id),
                "source_sec_code": str(raw_code),
                "source_sec_name": _clean_text(row.get("secName")),
                "publish_date": publish_date,
                "publish_time": publish_time,
                "instrument": instrument,
                "title": title,
                "url": url,
                "pdf_url": url,
                "adjunct_url": adjunct_url or None,
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
    cache: dict[str, dict[str, Any]] = {}
    attempted = 0
    for record in records:
        pdf_url = str(record.get("pdf_url") or "").strip()
        if not pdf_url:
            _apply_pdf_result(record, {"pdf_download_status": "missing_url"})
            stats["skipped"] += 1
            continue
        if pdf_url in cache:
            _apply_pdf_result(record, cache[pdf_url])
            continue
        if not enabled:
            result = {"pdf_download_status": "skipped"}
            cache[pdf_url] = result
            _apply_pdf_result(record, result)
            stats["skipped"] += 1
            continue
        if pdf_limit is not None and attempted >= pdf_limit:
            result = {"pdf_download_status": "skipped_by_limit"}
            cache[pdf_url] = result
            _apply_pdf_result(record, result)
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
        cache[pdf_url] = result
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
        content_type = str(response.headers.get("Content-Type") or "").lower()
        if "html" in content_type and not content.startswith(b"%PDF"):
            raise ValueError(f"unexpected PDF content type: {content_type}")
        result = objects.put_bytes(
            dataset="announcement",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"cninfo_pdf_{record['announcement_id']}",
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


def _announcement_time(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(value) / 1000).replace(microsecond=0).isoformat(" ")
    except (TypeError, ValueError, OSError):
        return None


def _clean_title(value: str) -> str:
    text = html.unescape(value)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _clean_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return _clean_title(str(value))


def _timestamp() -> str:
    return datetime.now().replace(microsecond=0).isoformat(" ")
