"""AkShare connector for global market daily bars."""

from __future__ import annotations

from datetime import datetime
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


def _as_float(value: Any) -> float | None:
    value = _json_safe(value)
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _date_to_iso(value: Any) -> str:
    text = str(_json_safe(value)).strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return datetime.strptime(text[:10], "%Y-%m-%d").date().isoformat()
    return datetime.strptime(text.replace("-", "")[:8], "%Y%m%d").date().isoformat()


class AkshareGlobalMarketDailyConnector(BaseConnector):
    """Collect selected US/global market daily bars via akshare.stock_us_daily."""

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
        target_date = _date_to_iso(options.get("end_date") or default_options["end_date"])

        for symbol in symbols:
            stats.request_count += 1
            try:
                df = akshare.stock_us_daily(symbol=symbol, adjust="")
                raw_payload = self._dataframe_payload(df=df, symbol=symbol, target_date=target_date)
                raw = self.raw_store.put_json(
                    source_id=self.source_id,
                    provider_id=self.provider_id,
                    logical_dataset=self.logical_dataset,
                    payload=raw_payload,
                    run_id=run_id,
                    filename_prefix=f"global_{symbol}_{target_date.replace('-', '')}",
                    metadata={"symbol": symbol, "akshare_function": "stock_us_daily"},
                )
                self.metadata_store.insert_raw_object(
                    raw,
                    request_hash=sha256_json({"function": "stock_us_daily", "symbol": symbol}),
                    request_url="akshare://stock_us_daily",
                    request_params={"symbol": symbol, "adjust": ""},
                )
                raw_checks = quality.check_raw_write(raw)
                self.metadata_store.insert_quality_results(raw_checks)
                if quality.has_critical_failures(raw_checks):
                    stats.quarantine_count += 1
                    continue
                row_count = self._persist_rows(
                    df=df,
                    symbol=symbol,
                    target_date=target_date,
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
                            check_name="akshare_stock_us_daily_request",
                            check_type="source_error",
                            severity="critical",
                            status="fail",
                            expected_value="successful AkShare global market response",
                            observed_value=str(exc)[:1000],
                            failed_count=1,
                            sample_failed_keys=[symbol],
                            run_id=run_id,
                            logical_dataset=self.logical_dataset,
                            source_id=self.source_id,
                        )
                    ]
                )
                stats.error_count += 1
        return stats

    def _resolve_symbols(
        self, options: dict[str, Any], default_options: dict[str, Any]
    ) -> list[str]:
        raw_symbols = options.get("symbols") or default_options.get("symbols") or []
        if isinstance(raw_symbols, str):
            symbols = [item.strip() for item in raw_symbols.split(",") if item.strip()]
        else:
            symbols = [str(item).strip() for item in raw_symbols if str(item).strip()]
        limit = options.get("limit_symbols") or default_options.get("limit_symbols")
        return symbols[: int(limit)] if limit else symbols

    def _dataframe_payload(self, *, df: Any, symbol: str, target_date: str) -> dict[str, Any]:
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "logical_dataset": self.logical_dataset,
            "function": "stock_us_daily",
            "params": {"symbol": symbol, "target_date": target_date},
            "columns": [str(column) for column in df.columns],
            "row_count": len(df),
            "records": self._records(df),
        }

    def _records(self, df: Any) -> list[dict[str, Any]]:
        if "date" in df.columns:
            records = df.to_dict(orient="records")
        else:
            records = df.reset_index().rename(columns={"index": "date"}).to_dict(orient="records")
        return [
            {str(key): _json_safe(value) for key, value in record.items()} for record in records
        ]

    def _persist_rows(
        self,
        *,
        df: Any,
        symbol: str,
        target_date: str,
        raw: Any,
        run_id: str,
        quality: QualityRunner,
    ) -> dict[str, int]:
        inserted = 0
        duplicates = 0
        quarantined = 0
        for record in self._records(df):
            if _date_to_iso(record.get("date")) != target_date:
                continue
            observed = self._normalize_record(record=record, symbol=symbol, raw=raw)
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
                title=f"{symbol} global daily {observed['trading_date']}",
                source_url="akshare://stock_us_daily",
                first_seen_at=raw.first_seen_at,
                stored_at=raw.stored_at,
                raw_object_id=raw.raw_object_id,
                content_hash=raw.content_hash,
                dedup_hash=sha256_json(
                    {
                        "provider_id": self.provider_id,
                        "symbol": symbol,
                        "trading_date": observed["trading_date"],
                    }
                ),
                quality_status="pass",
                observed_payload=observed,
            )
            inserted += 1
        if inserted + duplicates + quarantined == 0:
            quarantined = 1
        return {"inserted": inserted, "duplicates": duplicates, "quarantined": quarantined}

    def _normalize_record(self, *, record: dict[str, Any], symbol: str, raw: Any) -> dict[str, Any]:
        trading_date = _date_to_iso(record.get("date"))
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:{symbol}:{trading_date}",
            "symbol": symbol,
            "trading_date": trading_date,
            "market": "US",
            "name": symbol,
            "open": _as_float(record.get("open")),
            "high": _as_float(record.get("high")),
            "low": _as_float(record.get("low")),
            "close": _as_float(record.get("close")),
            "currency": "USD",
            "timezone": "America/New_York",
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }
