"""AkShare connector for A-share trading calendar data."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pitlake.connectors.base import BaseConnector, RequestPlan, RunStats
from pitlake.quality.checks import CheckResult, QualityRunner
from pitlake.utils import sha256_json


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "item"):
        return _json_safe(value.item())
    if isinstance(value, float) and value != value:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _date_to_iso(value: Any) -> str:
    value = _json_safe(value)
    if value in (None, ""):
        raise ValueError("date value is required")
    if isinstance(value, str):
        text = value.strip()
    else:
        text = str(value).strip()

    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return datetime.strptime(text[:10], "%Y-%m-%d").date().isoformat()
    compact = text.replace("-", "")
    return datetime.strptime(compact[:8], "%Y%m%d").date().isoformat()


def _date_to_yyyymmdd(value: Any) -> str:
    return _date_to_iso(value).replace("-", "")


class AkshareTradingCalendarConnector(BaseConnector):
    """Collect A-share trading dates via akshare.tool_trade_date_hist_sina."""

    connector_version = "0.1.0"

    def plan_requests(self) -> list[RequestPlan]:
        return []

    def collect(self, *, run_id: str, options: dict[str, Any] | None = None) -> RunStats:
        options = options or {}
        akshare = __import__("akshare")
        stats = RunStats(request_count=1)
        quality = QualityRunner()
        default_options = self.source_config.get("default_options", {})
        calendar_id = str(options.get("calendar_id") or default_options.get("calendar_id", "cn_ashare"))
        start_date = options.get("start_date") or default_options.get("start_date")
        end_date = options.get("end_date") or default_options.get("end_date")
        start_iso = _date_to_iso(start_date) if start_date else None
        end_iso = _date_to_iso(end_date) if end_date else None

        try:
            df = akshare.tool_trade_date_hist_sina()
            records = self._filter_records(df=df, start_iso=start_iso, end_iso=end_iso)
            raw_payload = self._dataframe_payload(
                df=df,
                records=records,
                calendar_id=calendar_id,
                start_date=start_date,
                end_date=end_date,
            )
            raw = self.raw_store.put_json(
                source_id=self.source_id,
                provider_id=self.provider_id,
                logical_dataset=self.logical_dataset,
                payload=raw_payload,
                run_id=run_id,
                filename_prefix=f"{self.source_id}_{calendar_id}",
                metadata={
                    "calendar_id": calendar_id,
                    "start_date": _date_to_yyyymmdd(start_date) if start_date else None,
                    "end_date": _date_to_yyyymmdd(end_date) if end_date else None,
                    "akshare_function": "tool_trade_date_hist_sina",
                },
            )
            self.metadata_store.insert_raw_object(
                raw,
                request_hash=sha256_json(
                    {
                        "function": "tool_trade_date_hist_sina",
                        "calendar_id": calendar_id,
                        "start_date": _date_to_yyyymmdd(start_date) if start_date else None,
                        "end_date": _date_to_yyyymmdd(end_date) if end_date else None,
                    }
                ),
                request_url="akshare://tool_trade_date_hist_sina",
                request_params={
                    "calendar_id": calendar_id,
                    "start_date": _date_to_yyyymmdd(start_date) if start_date else None,
                    "end_date": _date_to_yyyymmdd(end_date) if end_date else None,
                },
            )
            raw_checks = quality.check_raw_write(raw)
            self.metadata_store.insert_quality_results(raw_checks)
            if quality.has_critical_failures(raw_checks):
                stats.quarantine_count += len(records)
                return stats

            row_count = self._persist_rows(
                records=records,
                calendar_id=calendar_id,
                raw=raw,
                run_id=run_id,
                quality=quality,
            )
            stats.success_count = 1
            stats.new_item_count = row_count["inserted"]
            stats.duplicate_count = row_count["duplicates"]
            stats.quarantine_count += row_count["quarantined"]
        except Exception as exc:
            self.metadata_store.insert_quality_results(
                [
                    CheckResult(
                        check_name="akshare_tool_trade_date_hist_sina_request",
                        check_type="source_error",
                        severity="critical",
                        status="fail",
                        expected_value="successful AkShare trading calendar response",
                        observed_value=str(exc)[:1000],
                        failed_count=1,
                        sample_failed_keys=[calendar_id],
                        run_id=run_id,
                        logical_dataset=self.logical_dataset,
                        source_id=self.source_id,
                    )
                ]
            )
            stats.error_count = 1
        return stats

    def _filter_records(
        self,
        *,
        df: Any,
        start_iso: str | None,
        end_iso: str | None,
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for record in df.to_dict(orient="records"):
            trading_date = _date_to_iso(record.get("trade_date"))
            if start_iso and trading_date < start_iso:
                continue
            if end_iso and trading_date > end_iso:
                continue
            normalized = {str(key): _json_safe(value) for key, value in record.items()}
            normalized["trade_date"] = trading_date
            records.append(normalized)
        return records

    def _dataframe_payload(
        self,
        *,
        df: Any,
        records: list[dict[str, Any]],
        calendar_id: str,
        start_date: Any,
        end_date: Any,
    ) -> dict[str, Any]:
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "logical_dataset": self.logical_dataset,
            "function": "tool_trade_date_hist_sina",
            "params": {
                "calendar_id": calendar_id,
                "start_date": _date_to_yyyymmdd(start_date) if start_date else None,
                "end_date": _date_to_yyyymmdd(end_date) if end_date else None,
            },
            "columns": [str(column) for column in df.columns],
            "source_row_count": len(df),
            "row_count": len(records),
            "records": records,
        }

    def _persist_rows(
        self,
        *,
        records: list[dict[str, Any]],
        calendar_id: str,
        raw: Any,
        run_id: str,
        quality: QualityRunner,
    ) -> dict[str, int]:
        inserted = 0
        duplicates = 0
        quarantined = 0
        for record in records:
            observed = self._normalize_record(record=record, calendar_id=calendar_id, raw=raw)
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
            source_item_key = observed["source_item_key"]
            duplicate = self.metadata_store.raw_item_version_exists(
                logical_dataset=self.logical_dataset,
                provider_id=self.provider_id,
                source_item_key=source_item_key,
                content_hash=raw.content_hash,
            )
            if duplicate:
                duplicates += 1
                continue
            self.metadata_store.insert_raw_item_version(
                logical_dataset=self.logical_dataset,
                provider_id=self.provider_id,
                source_id=self.source_id,
                source_item_key=source_item_key,
                first_seen_at=raw.first_seen_at,
                stored_at=raw.stored_at,
                raw_object_id=raw.raw_object_id,
                content_hash=raw.content_hash,
                title=f"{calendar_id} trading day {observed['trading_date']}",
                source_url="akshare://tool_trade_date_hist_sina",
                dedup_hash=sha256_json(
                    {
                        "provider_id": self.provider_id,
                        "calendar_id": calendar_id,
                        "trading_date": observed["trading_date"],
                    }
                ),
                quality_status="pass",
                observed_payload=observed,
            )
            inserted += 1
        return {"inserted": inserted, "duplicates": duplicates, "quarantined": quarantined}

    def _normalize_record(
        self,
        *,
        record: dict[str, Any],
        calendar_id: str,
        raw: Any,
    ) -> dict[str, Any]:
        trading_date = _date_to_iso(record.get("trade_date"))
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:{calendar_id}:{trading_date}",
            "calendar_id": calendar_id,
            "trading_date": trading_date,
            "is_trading_day": True,
            "session_type": "regular",
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }
