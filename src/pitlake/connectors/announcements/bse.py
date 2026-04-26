"""BSE connector for listed company announcement index rows."""

from __future__ import annotations

import json
import re
import time
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any

import requests

from pitlake.connectors.base import BaseConnector, RequestPlan, RunStats
from pitlake.quality.checks import CheckResult, QualityRunner
from pitlake.utils import CN_TZ, sha256_json

BSE_QUERY_URL = "https://www.bse.cn/disclosureInfoController/companyAnnouncement.do"
BSE_REFERER = "https://www.bse.cn/disclosure/announcement.html"
BSE_ROOT = "https://www.bse.cn"
BSE_FIELDS = [
    "companyCd",
    "companyName",
    "disclosureTitle",
    "disclosurePostTitle",
    "destFilePath",
    "publishDate",
    "xxfcbj",
    "fileExt",
    "xxzrlx",
]


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


def _optional_date_to_iso_datetime(value: Any) -> str | None:
    text = str(_json_safe(value) or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.strptime(text[:10], "%Y-%m-%d").replace(tzinfo=CN_TZ)
    except ValueError:
        return None
    return parsed.isoformat(timespec="seconds")


def _plain_symbol(value: Any) -> str | None:
    text = str(_json_safe(value) or "").strip()
    if not text:
        return None
    return text.zfill(6)


def _bse_url(path: Any) -> str:
    text = str(_json_safe(path) or "").strip()
    if not text:
        return ""
    if text.startswith(("http://", "https://")):
        return text
    return BSE_ROOT + "/" + text.lstrip("/")


def _category_from_title(title: str) -> str | None:
    match = re.match(r"^\[([^\]]+)\]", title.strip())
    if not match:
        return None
    return match.group(1).strip() or None


def _announcement_id_from_path(path: str) -> str:
    name = PurePosixPath(path).name
    stem = name.rsplit(".", 1)[0]
    return stem or sha256_json({"source_url": path})[7:23]


class BseAnnouncementConnector(BaseConnector):
    """Collect BSE announcement index rows without downloading PDF bodies."""

    connector_version = "0.1.0"

    def plan_requests(self) -> list[RequestPlan]:
        return []

    def collect(self, *, run_id: str, options: dict[str, Any] | None = None) -> RunStats:
        options = options or {}
        default_options = self.source_config.get("default_options", {})
        start_date = _date_to_iso(options.get("start_date") or default_options["start_date"])
        end_date = _date_to_iso(options.get("end_date") or default_options["end_date"])
        max_pages = int(options.get("max_pages") or default_options.get("max_pages") or 1)
        disclosure_type = str(
            options.get("disclosure_type") or default_options.get("disclosure_type") or "5"
        )
        market_layer = str(options.get("market_layer") or default_options.get("market_layer") or "2")
        company_cd = str(options.get("company_cd") or default_options.get("company_cd") or "")
        keyword = str(options.get("keyword") or default_options.get("keyword") or "")
        hy_type = str(options.get("hy_type") or default_options.get("hy_type") or "")

        stats = RunStats()
        quality = QualityRunner()
        headers = self._headers()

        for page_num in range(max_pages):
            params = self._request_params(
                start_date=start_date,
                end_date=end_date,
                page_num=page_num,
                disclosure_type=disclosure_type,
                market_layer=market_layer,
                company_cd=company_cd,
                keyword=keyword,
                hy_type=hy_type,
            )
            stats.request_count += 1
            try:
                response = requests.get(
                    BSE_QUERY_URL,
                    headers=headers,
                    params=params,
                    timeout=int(default_options.get("timeout_seconds") or 20),
                )
                response.raise_for_status()
                payload = self._parse_jsonp(response.text)
                list_info = self._list_info(payload)
                records = list(list_info.get("content") or [])
                raw = self.raw_store.put_json(
                    source_id=self.source_id,
                    provider_id=self.provider_id,
                    logical_dataset=self.logical_dataset,
                    payload={
                        "provider_id": self.provider_id,
                        "source_id": self.source_id,
                        "logical_dataset": self.logical_dataset,
                        "function": "companyAnnouncement.do",
                        "params": params,
                        "row_count": len(records),
                        "payload": payload,
                    },
                    run_id=run_id,
                    filename_prefix=f"bse_ann_{start_date.replace('-', '')}_{page_num}",
                    metadata={
                        "start_date": start_date,
                        "end_date": end_date,
                        "page_num": page_num,
                        "disclosure_type": disclosure_type,
                        "market_layer": market_layer,
                    },
                )
                self.metadata_store.insert_raw_object(
                    raw,
                    request_hash=sha256_json({"url": BSE_QUERY_URL, "params": params}),
                    request_url=BSE_QUERY_URL,
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
            "Referer": BSE_REFERER,
            "Accept": "application/json, text/javascript, */*; q=0.01",
        }

    def _request_params(
        self,
        *,
        start_date: str,
        end_date: str,
        page_num: int,
        disclosure_type: str,
        market_layer: str,
        company_cd: str,
        keyword: str,
        hy_type: str,
    ) -> dict[str, Any]:
        return {
            "callback": f"jQuery{int(time.time() * 1000)}",
            "disclosureType[]": [disclosure_type],
            "disclosureSubtype[]": [""],
            "page": str(page_num),
            "companyCd": company_cd,
            "isNewThree": "1",
            "startTime": start_date,
            "endTime": end_date,
            "keyword": keyword,
            "xxfcbj[]": [market_layer],
            "hyType": hy_type,
            "needFields[]": BSE_FIELDS,
            "_": str(int(time.time() * 1000)),
        }

    def _parse_jsonp(self, text: str) -> list[Any]:
        match = re.match(r"^[^(]+\((.*)\)\s*$", text, flags=re.S)
        if not match:
            raise ValueError("BSE response is not JSONP")
        payload = json.loads(match.group(1))
        if not isinstance(payload, list):
            raise ValueError("BSE response payload is not a list")
        return payload

    def _list_info(self, payload: list[Any]) -> dict[str, Any]:
        if not payload or not isinstance(payload[0], dict):
            raise ValueError("BSE response is missing listInfo")
        list_info = payload[0].get("listInfo")
        if not isinstance(list_info, dict):
            raise ValueError("BSE response listInfo is not an object")
        return list_info

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
        instrument = _plain_symbol(record.get("companyCd"))
        source_url = _bse_url(record.get("destFilePath"))
        title = (
            str(_json_safe(record.get("disclosureTitle")) or "")
            + str(_json_safe(record.get("disclosurePostTitle")) or "")
        ).strip()
        announcement_id = _announcement_id_from_path(source_url)
        file_ext = str(record.get("fileExt") or "").lower()
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:{announcement_id}",
            "title": title,
            "source_url": source_url,
            "announcement_id": announcement_id,
            "instrument": instrument,
            "exchange": "BSE",
            "security_name": _json_safe(record.get("companyName")),
            "category": _category_from_title(title),
            "source_publish_time": _optional_date_to_iso_datetime(record.get("publishDate")),
            "pdf_url": source_url if file_ext == "pdf" or source_url.lower().endswith(".pdf") else None,
            "metric_payload": {
                key: _json_safe(value)
                for key, value in record.items()
                if key
                not in {
                    "companyCd",
                    "companyName",
                    "disclosureTitle",
                    "disclosurePostTitle",
                    "destFilePath",
                    "publishDate",
                    "fileExt",
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
                    check_name="bse_company_announcement_query",
                    check_type="source_error",
                    severity="critical",
                    status="fail",
                    expected_value="successful BSE announcement index response",
                    observed_value=observed_value,
                    failed_count=1,
                    sample_failed_keys=[sample_key],
                    run_id=run_id,
                    logical_dataset=self.logical_dataset,
                    source_id=self.source_id,
                )
            ]
        )
