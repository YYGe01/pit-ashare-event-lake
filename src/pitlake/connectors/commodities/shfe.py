"""SHFE official daily commodity futures connector."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import requests

from pitlake.connectors.base import BaseConnector, RequestPlan, RunStats
from pitlake.quality.checks import CheckResult, QualityRunner
from pitlake.utils import sha256_json

SHFE_DAILY_KX_URL = "https://www.shfe.com.cn/data/tradedata/future/dailydata/kx{date}.dat"
SHFE_REFERER = "https://www.shfe.com.cn/eng/reports/StatisticalData/DailyData/?query_params=kx"


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
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return datetime.strptime(text[:10], "%Y-%m-%d").date().isoformat()
    return datetime.strptime(text.replace("-", "")[:8], "%Y%m%d").date().isoformat()


def _date_to_yyyymmdd(value: Any) -> str:
    return _date_to_iso(value).replace("-", "")


def _as_float(value: Any) -> float | None:
    value = _json_safe(value)
    if value in (None, "", "-", "—"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    value = _json_safe(value)
    if value in (None, "", "-", "—"):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _contract_from_record(record: dict[str, Any]) -> str:
    product = str(record.get("PRODUCTGROUPID") or "").strip().lower()
    month = str(record.get("DELIVERYMONTH") or "").strip()
    return f"{product}{month}" if product and month else ""


class ShfeDailyConnector(BaseConnector):
    """Collect SHFE futures daily quotation rows from the official JSON endpoint."""

    connector_version = "0.1.0"

    def plan_requests(self) -> list[RequestPlan]:
        return []

    def collect(self, *, run_id: str, options: dict[str, Any] | None = None) -> RunStats:
        options = options or {}
        default_options = self.source_config.get("default_options", {})
        query_date = _date_to_yyyymmdd(options.get("end_date") or default_options["end_date"])
        limit_rows = options.get("limit_rows") or default_options.get("limit_rows")
        url = SHFE_DAILY_KX_URL.format(date=query_date)
        stats = RunStats(request_count=1)
        quality = QualityRunner()

        try:
            response = requests.get(url, headers=self._headers(), timeout=self._timeout(default_options))
            response.raise_for_status()
            payload = response.json()
            records = [
                record
                for record in payload.get("o_curinstrument") or []
                if record and _contract_from_record(record)
            ]
            if limit_rows:
                records = records[: int(limit_rows)]
            raw = self.raw_store.put_json(
                source_id=self.source_id,
                provider_id=self.provider_id,
                logical_dataset=self.logical_dataset,
                payload={
                    "provider_id": self.provider_id,
                    "source_id": self.source_id,
                    "logical_dataset": self.logical_dataset,
                    "function": "shfe_daily_kx",
                    "params": {"date": query_date, "url": url},
                    "row_count": len(records),
                    "payload": payload,
                },
                run_id=run_id,
                filename_prefix=f"shfe_kx_{query_date}",
                metadata={"query_date": query_date, "row_count": len(records)},
            )
            self.metadata_store.insert_raw_object(
                raw,
                request_hash=sha256_json({"url": url}),
                request_url=url,
                request_params={"date": query_date},
            )
            raw_checks = quality.check_raw_write(raw)
            self.metadata_store.insert_quality_results(raw_checks)
            if quality.has_critical_failures(raw_checks):
                stats.quarantine_count += len(records)
                return stats
            row_count = self._persist_records(
                records=records,
                trading_date=_date_to_iso(query_date),
                raw=raw,
                run_id=run_id,
                quality=quality,
            )
            stats.success_count = 1
            stats.new_item_count = row_count["inserted"]
            stats.duplicate_count = row_count["duplicates"]
            stats.quarantine_count += row_count["quarantined"]
        except Exception as exc:
            self._record_source_error(
                run_id=run_id,
                observed_value=str(exc)[:1000],
                sample_key=query_date,
            )
            stats.error_count = 1
        return stats

    def _headers(self) -> dict[str, str]:
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
            "Referer": SHFE_REFERER,
            "Accept": "application/json, text/plain, */*",
        }

    def _timeout(self, default_options: dict[str, Any]) -> int:
        return int(default_options.get("timeout_seconds") or 20)

    def _persist_records(
        self,
        *,
        records: list[dict[str, Any]],
        trading_date: str,
        raw: Any,
        run_id: str,
        quality: QualityRunner,
    ) -> dict[str, int]:
        inserted = 0
        duplicates = 0
        quarantined = 0
        for record in records:
            observed = self._normalize_record(record=record, trading_date=trading_date, raw=raw)
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
                title=f"SHFE {observed['contract']} commodity daily {observed['trading_date']}",
                source_url=SHFE_DAILY_KX_URL.format(date=trading_date.replace("-", "")),
                first_seen_at=raw.first_seen_at,
                stored_at=raw.stored_at,
                raw_object_id=raw.raw_object_id,
                content_hash=raw.content_hash,
                dedup_hash=sha256_json(
                    {
                        "provider_id": self.provider_id,
                        "exchange": observed["exchange"],
                        "contract": observed["contract"],
                        "trading_date": observed["trading_date"],
                    }
                ),
                quality_status="pass",
                observed_payload=observed,
            )
            inserted += 1
        return {"inserted": inserted, "duplicates": duplicates, "quarantined": quarantined}

    def _normalize_record(self, *, record: dict[str, Any], trading_date: str, raw: Any) -> dict[str, Any]:
        contract = _contract_from_record(record)
        product = str(record.get("PRODUCTGROUPID") or "").strip().lower()
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:SHFE:{contract}:{trading_date}",
            "exchange": "SHFE",
            "contract": contract,
            "trading_date": trading_date,
            "symbol": product or None,
            "open": _as_float(record.get("OPENPRICE")),
            "high": _as_float(record.get("HIGHESTPRICE")),
            "low": _as_float(record.get("LOWESTPRICE")),
            "close": _as_float(record.get("CLOSEPRICE")),
            "settlement": _as_float(record.get("SETTLEMENTPRICE")),
            "prev_settlement": _as_float(record.get("PRESETTLEMENTPRICE")),
            "volume": _as_int(record.get("VOLUME")),
            "open_interest": _as_int(record.get("OPENINTEREST")),
            "session": "daily",
            "metric_payload": {
                key: _json_safe(value)
                for key, value in record.items()
                if key
                not in {
                    "PRODUCTGROUPID",
                    "DELIVERYMONTH",
                    "OPENPRICE",
                    "HIGHESTPRICE",
                    "LOWESTPRICE",
                    "CLOSEPRICE",
                    "SETTLEMENTPRICE",
                    "PRESETTLEMENTPRICE",
                    "VOLUME",
                    "OPENINTEREST",
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
                    check_name="shfe_daily_kx_request",
                    check_type="source_error",
                    severity="critical",
                    status="fail",
                    expected_value="successful SHFE daily quotation response",
                    observed_value=observed_value,
                    failed_count=1,
                    sample_failed_keys=[sample_key],
                    run_id=run_id,
                    logical_dataset=self.logical_dataset,
                    source_id=self.source_id,
                )
            ]
        )
