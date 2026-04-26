"""AkShare connector for A-share halt and resume status data."""

from __future__ import annotations

from datetime import date, datetime, timedelta
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
    text = str(value).strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return datetime.strptime(text[:10], "%Y-%m-%d").date().isoformat()
    compact = text.replace("-", "")
    return datetime.strptime(compact[:8], "%Y%m%d").date().isoformat()


def _date_to_yyyymmdd(value: Any) -> str:
    return _date_to_iso(value).replace("-", "")


def _optional_date_to_iso(value: Any) -> str | None:
    value = _json_safe(value)
    if value in (None, "", "NaT"):
        return None
    try:
        return _date_to_iso(value)
    except ValueError:
        return None


def _date_range(start_date: Any, end_date: Any) -> list[str]:
    start = datetime.strptime(_date_to_iso(start_date), "%Y-%m-%d").date()
    end = datetime.strptime(_date_to_iso(end_date), "%Y-%m-%d").date()
    if end < start:
        raise ValueError("end_date must be greater than or equal to start_date")
    days: list[str] = []
    current = start
    while current <= end:
        days.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return days


def _plain_symbol(symbol: Any) -> str:
    clean = str(_json_safe(symbol) or "").strip().lower()
    for prefix in ("sh", "sz", "bj"):
        if clean.startswith(prefix):
            return clean[len(prefix) :].zfill(6)
    return clean.zfill(6)


def _exchange_from_symbol(symbol: str) -> str:
    if symbol.startswith("6"):
        return "SSE"
    if symbol.startswith(("0", "3")):
        return "SZSE"
    if symbol.startswith(("4", "8", "9")):
        return "BSE"
    return "UNKNOWN"


def _exchange_from_market(value: Any, symbol: str) -> str:
    text = str(_json_safe(value) or "").strip().upper()
    if any(token in text for token in ("沪", "上海", "SSE", "SH")):
        return "SSE"
    if any(token in text for token in ("深", "深圳", "SZSE", "SZ")):
        return "SZSE"
    if any(token in text for token in ("北", "BSE", "BJ")):
        return "BSE"
    return _exchange_from_symbol(symbol)


def _get_value(record: dict[str, Any], aliases: tuple[str, ...], index: int) -> Any:
    for alias in aliases:
        if alias in record:
            return record[alias]
    values = list(record.values())
    if index < len(values):
        return values[index]
    return None


class AkshareTradeStatusConnector(BaseConnector):
    """Collect A-share halt records via akshare.stock_tfp_em."""

    connector_version = "0.1.0"

    def plan_requests(self) -> list[RequestPlan]:
        return []

    def collect(self, *, run_id: str, options: dict[str, Any] | None = None) -> RunStats:
        options = options or {}
        akshare = __import__("akshare")
        stats = RunStats()
        quality = QualityRunner()
        default_options = self.source_config.get("default_options", {})
        start_date = options.get("start_date") or default_options["start_date"]
        end_date = options.get("end_date") or default_options["end_date"]

        for query_date in _date_range(start_date, end_date):
            stats.request_count += 1
            try:
                df = akshare.stock_tfp_em(date=query_date)
                raw_payload = self._dataframe_payload(df=df, query_date=query_date)
                raw = self.raw_store.put_json(
                    source_id=self.source_id,
                    provider_id=self.provider_id,
                    logical_dataset=self.logical_dataset,
                    payload=raw_payload,
                    run_id=run_id,
                    filename_prefix=f"{self.source_id}_{query_date}",
                    metadata={
                        "query_date": query_date,
                        "akshare_function": "stock_tfp_em",
                    },
                )
                self.metadata_store.insert_raw_object(
                    raw,
                    request_hash=sha256_json(
                        {
                            "function": "stock_tfp_em",
                            "date": query_date,
                        }
                    ),
                    request_url="akshare://stock_tfp_em",
                    request_params={"date": query_date},
                )
                raw_checks = quality.check_raw_write(raw)
                self.metadata_store.insert_quality_results(raw_checks)
                if quality.has_critical_failures(raw_checks):
                    stats.quarantine_count += len(df)
                    continue

                row_count = self._persist_rows(
                    df=df,
                    query_date=query_date,
                    raw=raw,
                    run_id=run_id,
                    quality=quality,
                )
                stats.success_count += 1
                stats.new_item_count += row_count["inserted"]
                stats.duplicate_count += row_count["duplicates"]
                stats.quarantine_count += row_count["quarantined"]
            except Exception as exc:
                self.metadata_store.insert_quality_results(
                    [
                        CheckResult(
                            check_name="akshare_stock_tfp_em_request",
                            check_type="source_error",
                            severity="critical",
                            status="fail",
                            expected_value="successful AkShare halt/resume response",
                            observed_value=str(exc)[:1000],
                            failed_count=1,
                            sample_failed_keys=[query_date],
                            run_id=run_id,
                            logical_dataset=self.logical_dataset,
                            source_id=self.source_id,
                        )
                    ]
                )
                stats.error_count += 1
        return stats

    def _dataframe_payload(self, *, df: Any, query_date: str) -> dict[str, Any]:
        records = []
        for record in df.to_dict(orient="records"):
            records.append({str(key): _json_safe(value) for key, value in record.items()})
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "logical_dataset": self.logical_dataset,
            "function": "stock_tfp_em",
            "params": {"date": query_date},
            "columns": [str(column) for column in df.columns],
            "row_count": len(df),
            "records": records,
        }

    def _persist_rows(
        self,
        *,
        df: Any,
        query_date: str,
        raw: Any,
        run_id: str,
        quality: QualityRunner,
    ) -> dict[str, int]:
        inserted = 0
        duplicates = 0
        quarantined = 0
        for record in df.to_dict(orient="records"):
            if not record:
                quarantined += 1
                continue
            observed = self._normalize_record(record=record, query_date=query_date, raw=raw)
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
                title=f"{observed['instrument']} trade status {observed['trading_date']}",
                source_url="akshare://stock_tfp_em",
                dedup_hash=sha256_json(
                    {
                        "provider_id": self.provider_id,
                        "instrument": observed["instrument"],
                        "trading_date": observed["trading_date"],
                    }
                ),
                quality_status="pass",
                observed_payload=observed,
            )
            inserted += 1
        return {"inserted": inserted, "duplicates": duplicates, "quarantined": quarantined}

    def _normalize_record(self, *, record: dict[str, Any], query_date: str, raw: Any) -> dict[str, Any]:
        instrument = _plain_symbol(_get_value(record, ("代码", "SECURITY_CODE"), 1))
        market = _get_value(record, ("所属市场", "MARKET"), 7)
        halt_reason = _get_value(record, ("停牌原因", "SUSPEND_REASON"), 6)
        source_update_time = _get_value(record, ("预计复牌时间", "PREDICT_RESUME_DATE"), 8)
        trading_date = _date_to_iso(query_date)
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:{instrument}:{trading_date}",
            "instrument": instrument,
            "exchange": _exchange_from_market(market, instrument),
            "trading_date": trading_date,
            "trade_status": "halted",
            "halt_reason": _json_safe(halt_reason),
            "source_update_time": _optional_date_to_iso(source_update_time),
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }
