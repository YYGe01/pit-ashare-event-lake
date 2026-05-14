"""Eastmoney research-report metadata crawler."""

from __future__ import annotations

import math
import json
import re
from datetime import datetime
from typing import Any
from urllib.parse import quote

from quant_data_center.crawlers.metrics import build_document_source_metrics
from quant_data_center.crawlers.runtime import (
    call_with_proxy_policy,
    make_deadline,
    raise_if_deadline_exceeded,
    request_timeout,
    sleep_with_deadline,
)
from quant_data_center.settings import QdcSettings
from quant_data_center.storage.objects import QdcObjectStore
from quant_data_center.storage.silver import SilverStore
from quant_data_center.utils.instruments import normalize_instrument


EASTMONEY_REPORT_LIST_URL = "https://reportapi.eastmoney.com/report/list"
EASTMONEY_REPORT_REFERER = "https://data.eastmoney.com/report/stock.jshtml"
PARSER_VERSION = "eastmoney_research_report_v1"


class EastmoneyResearchReportCrawler:
    """Fetch Eastmoney stock research-report metadata for one publish date."""

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
        request_timeout_seconds: float = 30.0,
        source_timeout_seconds: float | None = None,
        instrument_filter: list[str] | None = None,
    ) -> dict[str, Any]:
        requests = __import__("requests")
        deadline = make_deadline(source_timeout_seconds)
        observed_at = _timestamp()
        normalized_filter = (
            {normalize_instrument(value) for value in instrument_filter}
            if instrument_filter
            else None
        )

        pages: list[dict[str, Any]] = []
        provider_rows: list[dict[str, Any]] = []
        page_num = 1
        total_pages: int | None = None
        while total_pages is None or page_num <= total_pages:
            raise_if_deadline_exceeded(deadline, source_id=source_id)
            if page_num > 1 and min_delay_seconds > 0:
                sleep_with_deadline(min_delay_seconds, deadline=deadline, source_id=source_id)
            params = _query_params(crawl_date=crawl_date, page_size=page_size, page_num=page_num)
            response = call_with_proxy_policy(
                requests.get,
                EASTMONEY_REPORT_LIST_URL,
                headers=_headers(),
                params=params,
                timeout=request_timeout(
                    deadline=deadline,
                    default_seconds=request_timeout_seconds,
                    source_id=source_id,
                ),
                use_environment_proxy=self.settings.use_environment_proxy,
            )
            response.raise_for_status()
            body = response.json()
            rows = _extract_rows(body)
            provider_rows.extend(rows)
            total_pages = _total_pages(body, page_size=page_size)
            if max_pages is not None:
                total_pages = min(total_pages, max(1, int(max_pages)))
            pages.append(
                {
                    "page_num": page_num,
                    "request": params,
                    "status_code": response.status_code,
                    "provider_record_count": len(rows),
                    "total_pages": total_pages,
                    "hits": body.get("hits") or body.get("TotalCount") or body.get("total"),
                }
            )
            if not rows:
                break
            page_num += 1

        raw_object_id = self.objects.put_json(
            dataset="research_report",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"eastmoney_research_report_{crawl_date}",
            payload={
                "function": "eastmoney_report_list",
                "url": EASTMONEY_REPORT_LIST_URL,
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
            dataset="research_report",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"eastmoney_research_report_{crawl_date}",
            records=_bronze_records(provider_rows),
        )
        records = _normalize_research_reports(
            source_id=source_id,
            rows=provider_rows,
            crawl_date=crawl_date,
            instrument_filter=normalized_filter,
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
            dataset="research_report",
            source_id=source_id,
            partition_value=crawl_date,
            stem=f"eastmoney_research_report_{crawl_date}",
            manifest={
                "function": "eastmoney_report_list",
                "url": EASTMONEY_REPORT_LIST_URL,
                "accepted_date_rule": "beginTime/endTime are set to crawl_date; publish_date is derived from publishDate",
                "copyright_policy": "metadata_only; PDF URL is retained but PDF bytes are not downloaded by default",
                "raw_object_id": raw_object_id,
                "provider_record_count": len(provider_rows),
                "instrument_filter": instrument_filter or [],
                **source_metrics,
            },
            records=records,
        )
        row_count = self.silver.upsert_research_reports(records)
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
            **source_metrics,
            "observed_at": observed_at,
        }


def _query_params(*, crawl_date: str, page_size: int, page_num: int) -> dict[str, str]:
    return {
        "industryCode": "*",
        "pageSize": str(max(1, int(page_size))),
        "industry": "*",
        "rating": "*",
        "ratingChange": "*",
        "beginTime": crawl_date,
        "endTime": crawl_date,
        "pageNo": str(page_num),
        "fields": "",
        "qType": "0",
        "orgCode": "",
        "code": "",
        "rcode": "",
        "p": str(page_num),
        "pageNum": str(page_num),
        "pageNumber": str(page_num),
    }


def _headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 QDC-Crawler/0.1 (+local research)",
        "Referer": EASTMONEY_REPORT_REFERER,
        "Accept": "application/json,text/plain,*/*",
    }


def _extract_rows(body: Any) -> list[dict[str, Any]]:
    if not isinstance(body, dict):
        return []
    rows = body.get("data") or body.get("Data") or []
    return [row for row in rows if isinstance(row, dict)]


def _bronze_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {str(key): _bronze_value(value) for key, value in row.items()}
        for row in rows
    ]


def _bronze_value(value: Any) -> Any:
    if value is None:
        return value
    if isinstance(value, (bool, int, float, str)):
        return str(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _total_pages(body: dict[str, Any], *, page_size: int) -> int:
    for key in ("TotalPage", "totalPage", "pages"):
        value = body.get(key)
        if value:
            return max(1, int(value))
    hits = body.get("hits") or body.get("TotalCount") or body.get("total")
    if hits:
        return max(1, math.ceil(int(hits) / max(1, int(page_size))))
    return 1


def _normalize_research_reports(
    *,
    source_id: str,
    rows: list[dict[str, Any]],
    crawl_date: str,
    instrument_filter: set[str] | None,
    observed_at: str,
    raw_object_id: str,
) -> list[dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for row in rows:
        title = _clean_text(row.get("title"))
        publish_time = _publish_time(row.get("publishDate"))
        publish_date = publish_time[:10] if publish_time else None
        stock_code = _clean_text(row.get("stockCode"))
        if not title or publish_date != crawl_date or not stock_code:
            continue
        try:
            instrument = normalize_instrument(stock_code)
        except ValueError:
            continue
        if instrument_filter is not None and instrument not in instrument_filter:
            continue
        source_record_id = _clean_text(row.get("infoCode")) or _fallback_record_id(row)
        report_id = f"eastmoney_research_{_slug(source_record_id)}_{instrument}"
        records[report_id] = {
            "research_report_id": report_id,
            "publish_date": publish_date,
            "publish_time": publish_time,
            "instrument": instrument,
            "title": title,
            "url": _report_url(row),
            "source_id": source_id,
            "source_record_id": source_record_id,
            "source_sec_code": stock_code,
            "source_sec_name": _clean_text(row.get("stockName")),
            "institution": _clean_text(row.get("orgSName")) or _clean_text(row.get("orgName")),
            "analyst": _clean_analyst(row.get("researcher") or row.get("author")),
            "rating": _clean_text(row.get("emRatingName")) or _clean_text(row.get("sRatingName")),
            "rating_change": _clean_text(row.get("ratingChange")),
            "industry": _clean_text(row.get("indvInduName")) or _clean_text(row.get("industryName")),
            "report_type": _clean_text(row.get("reportType")),
            "observed_at": observed_at,
            "collect_time": observed_at,
            "pdf_url": _pdf_url(row),
            "pdf_download_status": "skipped_metadata_only",
            "raw_object_id": raw_object_id,
            "parser_version": PARSER_VERSION,
        }
    return list(records.values())


def _provider_key(row: dict[str, Any]) -> str:
    return _clean_text(row.get("infoCode")) or _fallback_record_id(row)


def _is_parsable_row(row: dict[str, Any]) -> bool:
    return bool(
        _clean_text(row.get("title"))
        and _publish_time(row.get("publishDate"))
        and _clean_text(row.get("stockCode"))
        and _provider_key(row)
    )


def _publish_time(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text[:26], fmt)
            return parsed.replace(microsecond=0).isoformat(" ")
        except ValueError:
            continue
    match = re.search(r"\d{4}-\d{2}-\d{2}", text)
    if match:
        return f"{match.group(0)} 00:00:00"
    return None


def _report_url(row: dict[str, Any]) -> str | None:
    encode_url = _clean_text(row.get("encodeUrl"))
    if encode_url:
        return f"https://data.eastmoney.com/report/zw_stock.jshtml?encodeUrl={quote(encode_url, safe='')}"
    pdf_url = _pdf_url(row)
    if pdf_url:
        return pdf_url
    stock_code = _clean_text(row.get("stockCode"))
    return f"https://data.eastmoney.com/report/stock.jshtml?code={stock_code}" if stock_code else None


def _pdf_url(row: dict[str, Any]) -> str | None:
    pdf_url = _clean_text(row.get("pdfUrl"))
    if pdf_url:
        return pdf_url
    info_code = _clean_text(row.get("infoCode"))
    return f"https://pdf.dfcfw.com/pdf/H3_{info_code}_1.pdf" if info_code else None


def _clean_analyst(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple, set)):
        parts = [_clean_analyst(item) for item in value]
        return "、".join(item for item in parts if item)
    text = _clean_text(value)
    if not text:
        return None
    if "." in text:
        prefix, suffix = text.split(".", 1)
        if prefix.isdigit() and suffix.strip():
            text = suffix.strip()
    return text


def _fallback_record_id(row: dict[str, Any]) -> str:
    values = [
        _clean_text(row.get("stockCode")),
        _clean_text(row.get("publishDate")),
        _clean_text(row.get("title")),
        _clean_text(row.get("orgSName")) or _clean_text(row.get("orgName")),
    ]
    return "|".join(value for value in values if value)


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
