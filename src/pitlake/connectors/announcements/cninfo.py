"""CNINFO connector for listed company announcement index rows."""

from __future__ import annotations

import html
import re
from datetime import datetime, timezone
from typing import Any

import requests

from pitlake.connectors.base import BaseConnector, RequestPlan, RunStats
from pitlake.quality.checks import CheckResult, QualityRunner
from pitlake.utils import CN_TZ, sha256_json

CNINFO_QUERY_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
CNINFO_REFERER = "https://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search"
CNINFO_STATIC_ROOT = "https://static.cninfo.com.cn/"


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


def _millis_to_iso(value: Any) -> str | None:
    value = _json_safe(value)
    if value in (None, ""):
        return None
    try:
        timestamp_ms = int(float(value))
    except (TypeError, ValueError):
        return None
    return (
        datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        .astimezone(CN_TZ)
        .isoformat(timespec="seconds")
    )


def _plain_symbol(value: Any) -> str | None:
    text = str(_json_safe(value) or "").strip()
    if not text:
        return None
    return text.zfill(6)


def _exchange_from_symbol(symbol: str | None, page_column: Any = None) -> str | None:
    if str(page_column or "").upper().startswith("SH"):
        return "SSE"
    if str(page_column or "").upper().startswith("SZ"):
        return "SZSE"
    if str(page_column or "").upper().startswith("BJ"):
        return "BSE"
    if symbol is None:
        return None
    if symbol.startswith("6"):
        return "SSE"
    if symbol.startswith(("0", "3")):
        return "SZSE"
    if symbol.startswith(("4", "8", "9")):
        return "BSE"
    return "UNKNOWN"


def _clean_title(value: Any) -> str:
    text = html.unescape(str(_json_safe(value) or "")).strip()
    text = re.sub(r"<[^>]+>", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _cninfo_url(path: Any) -> str:
    text = str(_json_safe(path) or "").strip()
    if not text:
        return ""
    if text.startswith(("http://", "https://")):
        return text
    return CNINFO_STATIC_ROOT + text.lstrip("/")


class CninfoAnnouncementConnector(BaseConnector):
    """Collect CNINFO announcement index rows without downloading PDF bodies."""

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
        column = str(options.get("column") or default_options.get("column") or "szse")
        category = str(options.get("category") or default_options.get("category") or "")
        stock = str(options.get("stock") or default_options.get("stock") or "")
        searchkey = str(options.get("searchkey") or default_options.get("searchkey") or "")

        stats = RunStats()
        quality = QualityRunner()
        headers = self._headers()

        for page_num in range(1, max_pages + 1):
            params = self._request_params(
                start_date=start_date,
                end_date=end_date,
                page_num=page_num,
                page_size=page_size,
                column=column,
                category=category,
                stock=stock,
                searchkey=searchkey,
            )
            stats.request_count += 1
            try:
                response = requests.post(
                    CNINFO_QUERY_URL,
                    headers=headers,
                    data=params,
                    timeout=int(default_options.get("timeout_seconds") or 20),
                )
                response.raise_for_status()
                payload = response.json()
                records = list(payload.get("announcements") or [])
                raw = self.raw_store.put_json(
                    source_id=self.source_id,
                    provider_id=self.provider_id,
                    logical_dataset=self.logical_dataset,
                    payload={
                        "provider_id": self.provider_id,
                        "source_id": self.source_id,
                        "logical_dataset": self.logical_dataset,
                        "function": "hisAnnouncement/query",
                        "params": params,
                        "row_count": len(records),
                        "payload": payload,
                    },
                    run_id=run_id,
                    filename_prefix=f"cninfo_ann_{start_date.replace('-', '')}_{page_num}",
                    metadata={
                        "start_date": start_date,
                        "end_date": end_date,
                        "page_num": page_num,
                        "page_size": page_size,
                        "column": column,
                    },
                )
                self.metadata_store.insert_raw_object(
                    raw,
                    request_hash=sha256_json({"url": CNINFO_QUERY_URL, "params": params}),
                    request_url=CNINFO_QUERY_URL,
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
            "Referer": CNINFO_REFERER,
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/javascript, */*; q=0.01",
        }

    def _request_params(
        self,
        *,
        start_date: str,
        end_date: str,
        page_num: int,
        page_size: int,
        column: str,
        category: str,
        stock: str,
        searchkey: str,
    ) -> dict[str, str]:
        return {
            "pageNum": str(page_num),
            "pageSize": str(page_size),
            "column": column,
            "tabName": "fulltext",
            "plate": "",
            "stock": stock,
            "searchkey": searchkey,
            "secid": "",
            "category": category,
            "trade": "",
            "seDate": f"{start_date}~{end_date}",
            "sortName": "",
            "sortType": "",
            "isHLtitle": "true",
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
        announcement_id = str(record.get("announcementId") or "").strip()
        title = _clean_title(record.get("announcementTitle") or record.get("shortTitle"))
        instrument = _plain_symbol(record.get("secCode"))
        source_url = _cninfo_url(record.get("adjunctUrl"))
        source_item_key = (
            f"{self.provider_id}:{announcement_id}"
            if announcement_id
            else sha256_json({"title": title, "source_url": source_url})
        )
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": source_item_key,
            "title": title,
            "source_url": source_url,
            "announcement_id": announcement_id or source_item_key.split(":", 1)[-1][:16],
            "instrument": instrument,
            "exchange": _exchange_from_symbol(instrument, record.get("pageColumn")),
            "security_name": _json_safe(record.get("secName") or record.get("tileSecName")),
            "category": _json_safe(record.get("announcementTypeName") or record.get("columnId")),
            "source_publish_time": _millis_to_iso(record.get("announcementTime")),
            "pdf_url": source_url if str(record.get("adjunctType") or "").upper() == "PDF" else None,
            "metric_payload": {
                key: _json_safe(value)
                for key, value in record.items()
                if key
                not in {
                    "announcementId",
                    "announcementTitle",
                    "shortTitle",
                    "secCode",
                    "secName",
                    "tileSecName",
                    "announcementTime",
                    "adjunctUrl",
                    "adjunctType",
                    "announcementTypeName",
                    "columnId",
                    "pageColumn",
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
                    check_name="cninfo_his_announcement_query",
                    check_type="source_error",
                    severity="critical",
                    status="fail",
                    expected_value="successful CNINFO announcement index response",
                    observed_value=observed_value,
                    failed_count=1,
                    sample_failed_keys=[sample_key],
                    run_id=run_id,
                    logical_dataset=self.logical_dataset,
                    source_id=self.source_id,
                )
            ]
        )
