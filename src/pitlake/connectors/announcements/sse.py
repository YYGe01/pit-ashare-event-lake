"""SSE connector for listed company announcement index rows."""

from __future__ import annotations

import json
import re
import time
from datetime import datetime
from typing import Any

import requests

from pitlake.connectors.base import BaseConnector, RequestPlan, RunStats
from pitlake.quality.checks import CheckResult, QualityRunner
from pitlake.utils import CN_TZ, sha256_json

SSE_QUERY_URL = "https://query.sse.com.cn/security/stock/queryCompanyBulletin.do"
SSE_REFERER = "https://www.sse.com.cn/disclosure/listedinfo/announcement/"
SSE_ROOT = "https://www.sse.com.cn"


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "item"):
        return _json_safe(value.item())
    if isinstance(value, float) and value != value:
        return None
    return value


def _date_to_iso(value: Any) -> str:
    text = str(_json_safe(value)).strip()
    compact = text.replace("-", "")[:8]
    return datetime.strptime(compact, "%Y%m%d").date().isoformat()


def _optional_datetime_to_iso(value: Any) -> str | None:
    text = str(_json_safe(value) or "").strip()
    if not text:
        return None
    for fmt, width in (("%Y-%m-%d %H:%M:%S", 19), ("%Y-%m-%d", 10)):
        try:
            parsed = datetime.strptime(text[:width], fmt).replace(tzinfo=CN_TZ)
            return parsed.isoformat(timespec="seconds")
        except ValueError:
            continue
    return None


def _plain_symbol(value: Any) -> str | None:
    text = str(_json_safe(value) or "").strip()
    if not text:
        return None
    return text.zfill(6)


def _sse_url(path: Any) -> str:
    text = str(_json_safe(path) or "").strip()
    if not text:
        return ""
    if text.startswith(("http://", "https://")):
        return text
    return SSE_ROOT + "/" + text.lstrip("/")


class SseAnnouncementConnector(BaseConnector):
    """Collect SSE announcement index rows without downloading PDF bodies."""

    connector_version = "0.1.0"

    def plan_requests(self) -> list[RequestPlan]:
        return []

    def collect(self, *, run_id: str, options: dict[str, Any] | None = None) -> RunStats:
        options = options or {}
        default_options = self.source_config.get("default_options", {})
        start_date = _date_to_iso(options.get("start_date") or default_options["start_date"])
        end_date = _date_to_iso(options.get("end_date") or default_options["end_date"])
        page_size = int(options.get("page_size") or default_options.get("page_size") or 25)
        max_pages = int(options.get("max_pages") or default_options.get("max_pages") or 1)
        report_type = str(options.get("report_type") or default_options.get("report_type") or "ALL")
        product_id = str(options.get("product_id") or default_options.get("product_id") or "")
        keyword = str(options.get("keyword") or default_options.get("keyword") or "")

        stats = RunStats()
        quality = QualityRunner()
        headers = self._headers()

        for page_num in range(1, max_pages + 1):
            params = self._request_params(
                start_date=start_date,
                end_date=end_date,
                page_num=page_num,
                page_size=page_size,
                report_type=report_type,
                product_id=product_id,
                keyword=keyword,
            )
            stats.request_count += 1
            try:
                response = requests.get(
                    SSE_QUERY_URL,
                    headers=headers,
                    params=params,
                    timeout=int(default_options.get("timeout_seconds") or 20),
                )
                response.raise_for_status()
                payload = self._parse_jsonp(response.text)
                records = list((payload.get("pageHelp") or {}).get("data") or [])
                raw = self.raw_store.put_json(
                    source_id=self.source_id,
                    provider_id=self.provider_id,
                    logical_dataset=self.logical_dataset,
                    payload={
                        "provider_id": self.provider_id,
                        "source_id": self.source_id,
                        "logical_dataset": self.logical_dataset,
                        "function": "queryCompanyBulletin.do",
                        "params": params,
                        "row_count": len(records),
                        "payload": payload,
                    },
                    run_id=run_id,
                    filename_prefix=f"sse_ann_{start_date.replace('-', '')}_{page_num}",
                    metadata={
                        "start_date": start_date,
                        "end_date": end_date,
                        "page_num": page_num,
                        "page_size": page_size,
                    },
                )
                self.metadata_store.insert_raw_object(
                    raw,
                    request_hash=sha256_json({"url": SSE_QUERY_URL, "params": params}),
                    request_url=SSE_QUERY_URL,
                    request_params=params,
                )
                raw_checks = quality.check_raw_write(raw)
                self.metadata_store.insert_quality_results(raw_checks)
                if quality.has_critical_failures(raw_checks):
                    stats.quarantine_count += len(records)
                    continue
                row_count = self._persist_records(
                    records=records,
                    raw=raw,
                    run_id=run_id,
                    quality=quality,
                )
                stats.success_count += 1
                stats.new_item_count += row_count["inserted"]
                stats.duplicate_count += row_count["duplicates"]
                stats.quarantine_count += row_count["quarantined"]
            except Exception as exc:
                self._record_source_error(
                    run_id=run_id,
                    observed_value=str(exc)[:1000],
                    sample_key=f"{start_date}:{page_num}",
                )
                stats.error_count += 1
        return stats

    def _headers(self) -> dict[str, str]:
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
            "Referer": SSE_REFERER,
            "Accept": "application/json, text/javascript, */*; q=0.01",
        }

    def _request_params(
        self,
        *,
        start_date: str,
        end_date: str,
        page_num: int,
        page_size: int,
        report_type: str,
        product_id: str,
        keyword: str,
    ) -> dict[str, str]:
        callback = f"jsonpCallback{int(time.time() * 1000)}"
        return {
            "jsonCallBack": callback,
            "isPagination": "true",
            "productId": product_id,
            "keyWord": keyword,
            "securityType": "0101,120100,020100,020200,120200",
            "reportType2": "",
            "reportType": report_type,
            "beginDate": start_date,
            "endDate": end_date,
            "pageHelp.pageSize": str(page_size),
            "pageHelp.pageNo": str(page_num),
            "pageHelp.beginPage": str(page_num),
            "pageHelp.cacheSize": "1",
            "pageHelp.endPage": str(page_num),
            "_": str(int(time.time() * 1000)),
        }

    def _parse_jsonp(self, text: str) -> dict[str, Any]:
        match = re.match(r"^[^(]+\((.*)\)\s*$", text, flags=re.S)
        if not match:
            raise ValueError("SSE response is not JSONP")
        payload = json.loads(match.group(1))
        if not isinstance(payload, dict):
            raise ValueError("SSE response payload is not an object")
        return payload

    def _persist_records(
        self,
        *,
        records: list[dict[str, Any]],
        raw: Any,
        run_id: str,
        quality: QualityRunner,
    ) -> dict[str, int]:
        inserted = 0
        duplicates = 0
        quarantined = 0
        for record in records:
            observed = self._normalize_record(record=record, raw=raw)
            checks = quality.check_required_fields(
                contract=self.contract,
                payload=observed,
                run_id=run_id,
                source_id=self.source_id,
            )
            self.metadata_store.insert_quality_results(checks)
            if quality.has_critical_failures(checks):
                quarantined += 1
                continue
            duplicate = self.metadata_store.raw_item_version_exists(
                logical_dataset=self.logical_dataset,
                provider_id=self.provider_id,
                source_item_key=observed["source_item_key"],
                content_hash=raw.content_hash,
            )
            if duplicate:
                duplicates += 1
                continue
            self.metadata_store.insert_raw_item_version(
                logical_dataset=self.logical_dataset,
                provider_id=self.provider_id,
                source_id=self.source_id,
                source_item_key=observed["source_item_key"],
                title=observed["title"],
                source_url=observed["source_url"],
                source_publish_time=observed["source_publish_time"],
                first_seen_at=raw.first_seen_at,
                stored_at=raw.stored_at,
                raw_object_id=raw.raw_object_id,
                content_hash=raw.content_hash,
                dedup_hash=sha256_json(
                    {
                        "provider_id": self.provider_id,
                        "source_url": observed["source_url"],
                        "title": observed["title"],
                    }
                ),
                quality_status="pass",
                observed_payload=observed,
            )
            inserted += 1
        return {"inserted": inserted, "duplicates": duplicates, "quarantined": quarantined}

    def _normalize_record(self, *, record: dict[str, Any], raw: Any) -> dict[str, Any]:
        instrument = _plain_symbol(record.get("SECURITY_CODE"))
        source_url = _sse_url(record.get("URL"))
        title = str(_json_safe(record.get("TITLE")) or "").strip()
        source_item_key = f"{self.provider_id}:{sha256_json({'source_url': source_url})[7:23]}"
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": source_item_key,
            "title": title,
            "source_url": source_url,
            "announcement_id": source_item_key.split(":", 1)[1],
            "instrument": instrument,
            "exchange": "SSE",
            "security_name": _json_safe(record.get("SECURITY_NAME")),
            "category": _json_safe(record.get("BULLETIN_TYPE") or record.get("BULLETIN_HEADING")),
            "source_publish_time": _optional_datetime_to_iso(
                record.get("ADDDATE") or record.get("SSEDATE")
            ),
            "pdf_url": source_url if source_url.lower().endswith(".pdf") else None,
            "metric_payload": {
                key: _json_safe(value)
                for key, value in record.items()
                if key
                not in {
                    "SECURITY_CODE",
                    "SECURITY_NAME",
                    "TITLE",
                    "URL",
                    "ADDDATE",
                    "SSEDATE",
                    "BULLETIN_TYPE",
                    "BULLETIN_HEADING",
                }
            },
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }

    def _record_source_error(self, *, run_id: str, observed_value: str, sample_key: str) -> None:
        self.metadata_store.insert_quality_results(
            [
                CheckResult(
                    check_name="sse_query_company_bulletin",
                    check_type="source_error",
                    severity="critical",
                    status="fail",
                    expected_value="successful SSE announcement index response",
                    observed_value=observed_value,
                    failed_count=1,
                    sample_failed_keys=[sample_key],
                    run_id=run_id,
                    logical_dataset=self.logical_dataset,
                    source_id=self.source_id,
                )
            ]
        )
