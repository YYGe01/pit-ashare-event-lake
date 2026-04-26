"""AkShare connector for A-share daily OHLCV data."""

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


def _as_int(value: Any) -> int | None:
    value = _json_safe(value)
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _exchange_from_symbol(symbol: str) -> str:
    if symbol.startswith("6"):
        return "SSE"
    if symbol.startswith(("0", "3")):
        return "SZSE"
    if symbol.startswith(("4", "8", "9")):
        return "BSE"
    return "UNKNOWN"


def _akshare_daily_symbol(symbol: str) -> str:
    clean = symbol.lower().replace(".", "").replace("_", "")
    if clean.startswith(("sh", "sz", "bj")):
        return clean
    base = clean.zfill(6)
    if base.startswith("6"):
        return f"sh{base}"
    if base.startswith(("0", "3")):
        return f"sz{base}"
    if base.startswith(("4", "8", "9")):
        return f"bj{base}"
    return base


def _plain_symbol(symbol: str) -> str:
    clean = symbol.lower()
    for prefix in ("sh", "sz", "bj"):
        if clean.startswith(prefix):
            return clean[len(prefix) :].zfill(6)
    return clean.zfill(6)


def _date_to_yyyymmdd(value: str) -> str:
    compact = value.replace("-", "")
    datetime.strptime(compact, "%Y%m%d")
    return compact


class AkshareMarketDailyConnector(BaseConnector):
    """Collect selected A-share daily bars via akshare.stock_zh_a_daily."""

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
        start_date = _date_to_yyyymmdd(str(options.get("start_date") or default_options["start_date"]))
        end_date = _date_to_yyyymmdd(str(options.get("end_date") or default_options["end_date"]))
        adjust = str(options.get("adjust", default_options.get("adjust", "")))

        for symbol in symbols:
            stats.request_count += 1
            api_symbol = _akshare_daily_symbol(symbol)
            try:
                df = akshare.stock_zh_a_daily(
                    symbol=api_symbol,
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust,
                )
                raw_payload = self._dataframe_payload(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust,
                    df=df,
                )
                raw = self.raw_store.put_json(
                    source_id=self.source_id,
                    provider_id=self.provider_id,
                    logical_dataset=self.logical_dataset,
                    payload=raw_payload,
                    run_id=run_id,
                    filename_prefix=f"{self.source_id}_{symbol}_{start_date}_{end_date}",
                    metadata={
                        "symbol": symbol,
                        "api_symbol": api_symbol,
                        "start_date": start_date,
                        "end_date": end_date,
                        "adjust": adjust,
                        "akshare_function": "stock_zh_a_daily",
                    },
                )
                self.metadata_store.insert_raw_object(
                    raw,
                    request_hash=sha256_json(
                        {
                            "function": "stock_zh_a_daily",
                            "symbol": symbol,
                            "api_symbol": api_symbol,
                            "start_date": start_date,
                            "end_date": end_date,
                            "adjust": adjust,
                        }
                    ),
                    request_url="akshare://stock_zh_a_daily",
                    request_params={
                        "symbol": symbol,
                        "api_symbol": api_symbol,
                        "start_date": start_date,
                        "end_date": end_date,
                        "adjust": adjust,
                    },
                )
                raw_checks = quality.check_raw_write(raw)
                self.metadata_store.insert_quality_results(raw_checks)
                if quality.has_critical_failures(raw_checks):
                    stats.quarantine_count += 1
                    continue

                row_count = self._persist_rows(
                    df=df,
                    symbol=symbol,
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
                            check_name="akshare_stock_zh_a_daily_request",
                            check_type="source_error",
                            severity="critical",
                            status="fail",
                            expected_value="successful AkShare daily response",
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

    def _resolve_symbols(self, options: dict[str, Any], default_options: dict[str, Any]) -> list[str]:
        raw_symbols = options.get("symbols") or default_options.get("symbols") or []
        if isinstance(raw_symbols, str):
            symbols = [item.strip() for item in raw_symbols.split(",") if item.strip()]
        else:
            symbols = [str(item).strip() for item in raw_symbols if str(item).strip()]
        limit = options.get("limit_symbols") or default_options.get("limit_symbols")
        if limit:
            symbols = symbols[: int(limit)]
        if not symbols:
            raise ValueError("No symbols configured for AkShare market daily connector")
        return symbols

    def _dataframe_payload(
        self,
        *,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str,
        df: Any,
    ) -> dict[str, Any]:
        records = []
        for record in df.to_dict(orient="records"):
            records.append({str(key): _json_safe(value) for key, value in record.items()})
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "logical_dataset": self.logical_dataset,
            "function": "stock_zh_a_daily",
            "params": {
                "symbol": _akshare_daily_symbol(symbol),
                "start_date": start_date,
                "end_date": end_date,
                "adjust": adjust,
            },
            "columns": [str(column) for column in df.columns],
            "row_count": len(df),
            "records": records,
        }

    def _persist_rows(
        self,
        *,
        df: Any,
        symbol: str,
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
            observed = self._normalize_record(symbol=symbol, record=record, raw=raw)
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
                title=f"{symbol} daily bar {observed['trading_date']}",
                source_url="akshare://stock_zh_a_daily",
                dedup_hash=sha256_json(
                    {
                        "provider_id": self.provider_id,
                        "instrument": symbol,
                        "trading_date": observed["trading_date"],
                    }
                ),
                quality_status="pass",
                observed_payload=observed,
            )
            inserted += 1
        return {"inserted": inserted, "duplicates": duplicates, "quarantined": quarantined}

    def _normalize_record(self, *, symbol: str, record: dict[str, Any], raw: Any) -> dict[str, Any]:
        instrument = _plain_symbol(symbol)
        trading_date = str(_json_safe(record.get("date")))
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:{instrument}:{trading_date}",
            "instrument": instrument,
            "exchange": _exchange_from_symbol(instrument),
            "trading_date": trading_date,
            "open": _as_float(record.get("open")),
            "close": _as_float(record.get("close")),
            "high": _as_float(record.get("high")),
            "low": _as_float(record.get("low")),
            "volume": _as_int(record.get("volume")),
            "amount": _as_float(record.get("amount")),
            "turnover": _as_float(record.get("turnover")),
            "outstanding_share": _as_float(record.get("outstanding_share")),
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }
