"""AkShare connector for P1 A-share financial indicators."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pitlake.connectors.base import BaseConnector, RequestPlan, RunStats
from pitlake.quality.checks import CheckResult, QualityRunner
from pitlake.utils import sha256_json


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "item"):
        return _json_safe(value.item())
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, float) and value != value:
        return None
    return value


def _date_to_iso(value: Any) -> str:
    value = _json_safe(value)
    if isinstance(value, (int, float)) and value > 10_000_000_000:
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc).date().isoformat()
    text = str(value).strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return datetime.strptime(text[:10], "%Y-%m-%d").date().isoformat()
    if len(text) >= 8 and text[:8].isdigit():
        return datetime.strptime(text[:8], "%Y%m%d").date().isoformat()
    raise ValueError(f"unsupported report date: {value}")


def _exchange_from_symbol(symbol: str) -> str:
    if symbol.startswith("6"):
        return "SSE"
    if symbol.startswith(("0", "3")):
        return "SZSE"
    if symbol.startswith(("4", "8", "9")):
        return "BSE"
    return "UNKNOWN"


def _plain_symbol(symbol: str) -> str:
    clean = str(symbol).lower().replace(".", "").replace("_", "")
    for prefix in ("sh", "sz", "bj"):
        if clean.startswith(prefix):
            return clean[len(prefix) :].zfill(6)
    return clean.zfill(6)


def _period_type(report_date: str) -> str:
    month_day = report_date[5:]
    return {
        "03-31": "Q1",
        "06-30": "H1",
        "09-30": "Q3",
        "12-31": "FY",
    }.get(month_day, "unknown")


class AkshareFinancialIndicatorConnector(BaseConnector):
    """Collect financial analysis indicators from akshare.stock_financial_analysis_indicator."""

    connector_version = "0.1.0"

    def plan_requests(self) -> list[RequestPlan]:
        return []

    def collect(self, *, run_id: str, options: dict[str, Any] | None = None) -> RunStats:
        options = options or {}
        akshare = __import__("akshare")
        stats = RunStats()
        quality = QualityRunner()
        default_options = self.source_config.get("default_options", {})
        symbols = self._resolve_symbols(options, default_options)
        start_year = self._resolve_start_year(options, default_options)

        for symbol in symbols:
            instrument = _plain_symbol(symbol)
            stats.request_count += 1
            try:
                df = akshare.stock_financial_analysis_indicator(
                    symbol=instrument,
                    start_year=start_year,
                )
                raw_payload = self._dataframe_payload(
                    df=df,
                    instrument=instrument,
                    start_year=start_year,
                )
                raw = self.raw_store.put_json(
                    source_id=self.source_id,
                    provider_id=self.provider_id,
                    logical_dataset=self.logical_dataset,
                    payload=raw_payload,
                    run_id=run_id,
                    filename_prefix=f"financial_indicator_{instrument}_{start_year}",
                    metadata={
                        "instrument": instrument,
                        "start_year": start_year,
                        "akshare_function": "stock_financial_analysis_indicator",
                    },
                )
                self.metadata_store.insert_raw_object(
                    raw,
                    request_hash=sha256_json(
                        {
                            "function": "stock_financial_analysis_indicator",
                            "symbol": instrument,
                            "start_year": start_year,
                        }
                    ),
                    request_url="akshare://stock_financial_analysis_indicator",
                    request_params={"symbol": instrument, "start_year": start_year},
                )
                raw_checks = quality.check_raw_write(raw)
                self.metadata_store.insert_quality_results(raw_checks)
                if quality.has_critical_failures(raw_checks):
                    stats.quarantine_count += 1
                    continue

                row_count = self._persist_rows(
                    df=df,
                    instrument=instrument,
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
                            check_name="akshare_financial_indicator_request",
                            check_type="source_error",
                            severity="critical",
                            status="fail",
                            expected_value="successful AkShare financial indicator response",
                            observed_value=str(exc)[:1000],
                            failed_count=1,
                            sample_failed_keys=[instrument],
                            run_id=run_id,
                            logical_dataset=self.logical_dataset,
                            source_id=self.source_id,
                        )
                    ]
                )
                stats.error_count += 1
        return stats

    def _resolve_symbols(
        self,
        options: dict[str, Any],
        default_options: dict[str, Any],
    ) -> list[str]:
        raw_symbols = options.get("symbols") or default_options.get("symbols") or []
        symbols = (
            [item.strip() for item in raw_symbols.split(",")]
            if isinstance(raw_symbols, str)
            else [str(item).strip() for item in raw_symbols]
        )
        symbols = [_plain_symbol(item) for item in symbols if item]
        limit = options.get("limit_symbols") or default_options.get("limit_symbols")
        symbols = symbols[: int(limit)] if limit else symbols
        if not symbols:
            raise ValueError("No symbols configured for AkShare financial indicator connector")
        return symbols

    def _resolve_start_year(self, options: dict[str, Any], default_options: dict[str, Any]) -> str:
        value = (
            options.get("start_year")
            or default_options.get("start_year")
            or options.get("start_date")
            or default_options.get("start_date")
        )
        if value is None:
            raise ValueError("No start_year configured for AkShare financial indicator connector")
        return str(value).replace("-", "")[:4]

    def _dataframe_payload(self, *, df: Any, instrument: str, start_year: str) -> dict[str, Any]:
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "logical_dataset": self.logical_dataset,
            "function": "stock_financial_analysis_indicator",
            "params": {"symbol": instrument, "start_year": start_year},
            "columns": [str(column) for column in df.columns],
            "row_count": len(df),
            "records": self._records(df),
        }

    def _records(self, df: Any) -> list[dict[str, Any]]:
        return [
            {str(key): _json_safe(value) for key, value in record.items()}
            for record in df.to_dict(orient="records")
        ]

    def _persist_rows(
        self,
        *,
        df: Any,
        instrument: str,
        raw: Any,
        run_id: str,
        quality: QualityRunner,
    ) -> dict[str, int]:
        inserted = 0
        duplicates = 0
        quarantined = 0
        for record in df.to_dict(orient="records"):
            try:
                observed = self._normalize_record(record=record, instrument=instrument, raw=raw)
            except ValueError:
                quarantined += 1
                continue
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
                title=f"{instrument} financial indicators {observed['report_date']}",
                source_url="akshare://stock_financial_analysis_indicator",
                first_seen_at=raw.first_seen_at,
                stored_at=raw.stored_at,
                raw_object_id=raw.raw_object_id,
                content_hash=raw.content_hash,
                dedup_hash=sha256_json(
                    {
                        "provider_id": self.provider_id,
                        "instrument": instrument,
                        "report_date": observed["report_date"],
                    }
                ),
                quality_status="pass",
                observed_payload=observed,
            )
            inserted += 1
        if inserted + duplicates + quarantined == 0:
            quarantined = 1
        return {"inserted": inserted, "duplicates": duplicates, "quarantined": quarantined}

    def _normalize_record(
        self,
        *,
        record: dict[str, Any],
        instrument: str,
        raw: Any,
    ) -> dict[str, Any]:
        report_date = _date_to_iso(self._date_value(record))
        metrics = {
            str(key): _json_safe(value)
            for key, value in record.items()
            if value not in (None, "") and key != self._date_key(record)
        }
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:{instrument}:{report_date}",
            "instrument": instrument,
            "exchange": _exchange_from_symbol(instrument),
            "report_date": report_date,
            "report_year": report_date[:4],
            "period_type": _period_type(report_date),
            "metric_payload": metrics,
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }

    def _date_key(self, record: dict[str, Any]) -> Any:
        for key in ("date", "report_date", "日期"):
            if key in record:
                return key
        return next(iter(record))

    def _date_value(self, record: dict[str, Any]) -> Any:
        return record[self._date_key(record)]
