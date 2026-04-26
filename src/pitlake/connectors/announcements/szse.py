"""SZSE connector for listed company announcement index rows."""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any

import requests

from pitlake.connectors.base import BaseConnector, RequestPlan, RunStats
from pitlake.quality.checks import CheckResult, QualityRunner
from pitlake.utils import CN_TZ, sha256_json

SZSE_QUERY_URL = "https://www.szse.cn/api/disc/announcement/annList"
SZSE_REFERER = "https://www.szse.cn/disclosure/listed/notice/index.html"
SZSE_DOWNLOAD_ROOT = "https://disc.static.szse.cn/download"


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


def _first_list_value(value: Any) -> Any:
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _plain_symbol(value: Any) -> str | None:
    text = str(_json_safe(_first_list_value(value)) or "").strip()
    if not text:
        return None
    return text.zfill(6)


def _exchange_from_symbol(symbol: str | None) -> str | None:
    if symbol is None:
        return None
    if symbol.startswith("6"):
        return "SSE"
    if symbol.startswith(("0", "3")):
        return "SZSE"
    if symbol.startswith(("4", "8", "9")):
        return "BSE"
    return "UNKNOWN"


def _szse_url(path: Any) -> str:
    text = str(_json_safe(path) or "").strip()
    if not text:
        return ""
    if text.startswith(("http://", "https://")):
        return text
    return SZSE_DOWNLOAD_ROOT + "/" + text.lstrip("/")


class SzseAnnouncementConnector(BaseConnector):
    """Collect SZSE announcement index rows without downloading PDF bodies."""

    connector_version = "0.1.0"

    def plan_requests(self) -> list[RequestPlan]:
        return []

    def collect(self, *, run_id: str, options: dict[str, Any] | None = None) -> RunStats:
        options = options or {}
        default_options = self.source_config.get("default_options", {})
        start_date = _date_to_iso(options.get("start_date") or default_options["start_date"])
        end_date = _date_to_iso(options.get("end_date") or default_options["end_date"])
        page_size = int(options.get("page_size") or default_options.get("page_size") or 30)
        max_pages = int(options.get("max_pages") or default_options.get("max_pages") or 1)
        channel_code = str(
            options.get("channel_code") or default_options.get("channel_code") or "listedNotice_disc"
        )

        stats = RunStats()
        quality = QualityRunner()
        headers = self._headers()

        for page_num in range(1, max_pages + 1):
            request_json = {
                "seDate": [start_date, end_date],
                "channelCode": [channel_code],
                "pageSize": page_size,
                "pageNum": page_num,
            }
            request_params = {"random": str(time.time())}
            stats.request_count += 1
            try:
                response = requests.post(
                    SZSE_QUERY_URL,
                    params=request_params,
                    headers=headers,
                    json=request_json,
                    timeout=int(default_options.get("timeout_seconds") or 20),
                )
                response.raise_for_status()
                payload = response.json()
                records = list(payload.get("data") or [])
                raw = self.raw_store.put_json(
                    source_id=self.source_id,
                    provider_id=self.provider_id,
                    logical_dataset=self.logical_dataset,
                    payload={
                        "provider_id": self.provider_id,
                        "source_id": self.source_id,
                        "logical_dataset": self.logical_dataset,
                        "function": "announcement/annList",
                        "params": request_params,
                        "json": request_json,
                        "row_count": len(records),
                        "payload": payload,
                    },
                    run_id=run_id,
                    filename_prefix=f"szse_ann_{start_date.replace('-', '')}_{page_num}",
                    metadata={
                        "start_date": start_date,
                        "end_date": end_date,
                        "page_num": page_num,
                        "page_size": page_size,
                        "channel_code": channel_code,
                    },
                )
                self.metadata_store.insert_raw_object(
                    raw,
                    request_hash=sha256_json(
                        {"url": SZSE_QUERY_URL, "params": request_params, "json": request_json}
                    ),
                    request_url=SZSE_QUERY_URL,
                    request_params={"params": request_params, "json": request_json},
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
            "Referer": SZSE_REFERER,
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
        }

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
                        "announcement_id": observed["announcement_id"],
                        "source_url": observed["source_url"],
                    }
                ),
                quality_status="pass",
                observed_payload=observed,
            )
            inserted += 1
        return {"inserted": inserted, "duplicates": duplicates, "quarantined": quarantined}

    def _normalize_record(self, *, record: dict[str, Any], raw: Any) -> dict[str, Any]:
        instrument = _plain_symbol(record.get("secCode"))
        source_url = _szse_url(record.get("attachPath"))
        announcement_id = str(record.get("annId") or record.get("id") or "").strip()
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": (
                f"{self.provider_id}:{announcement_id}"
                if announcement_id
                else sha256_json({"source_url": source_url})
            ),
            "title": str(_json_safe(record.get("title")) or "").strip(),
            "source_url": source_url,
            "announcement_id": announcement_id or sha256_json({"source_url": source_url})[7:23],
            "instrument": instrument,
            "exchange": _exchange_from_symbol(instrument),
            "security_name": _json_safe(_first_list_value(record.get("secName"))),
            "category": _json_safe(record.get("bigCategoryId") or record.get("smallCategoryId")),
            "source_publish_time": _optional_datetime_to_iso(record.get("publishTime")),
            "pdf_url": source_url if str(record.get("attachFormat") or "").upper() == "PDF" else None,
            "metric_payload": {
                key: _json_safe(value)
                for key, value in record.items()
                if key
                not in {
                    "annId",
                    "id",
                    "title",
                    "attachPath",
                    "attachFormat",
                    "secCode",
                    "secName",
                    "publishTime",
                    "bigCategoryId",
                    "smallCategoryId",
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
                    check_name="szse_announcement_ann_list",
                    check_type="source_error",
                    severity="critical",
                    status="fail",
                    expected_value="successful SZSE announcement index response",
                    observed_value=observed_value,
                    failed_count=1,
                    sample_failed_keys=[sample_key],
                    run_id=run_id,
                    logical_dataset=self.logical_dataset,
                    source_id=self.source_id,
                )
            ]
        )
