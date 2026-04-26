"""AkShare connector for bootstrapped A-share daily price limits."""

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


def _as_float(value: Any) -> float | None:
    value = _json_safe(value)
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def _query_start_date(value: Any, lookback_days: int) -> str:
    target = datetime.strptime(_date_to_iso(value), "%Y-%m-%d").date()
    return (target - timedelta(days=lookback_days)).strftime("%Y%m%d")


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


def _limit_rate(symbol: str) -> tuple[float, str]:
    if symbol.startswith(("4", "8", "9")):
        return 0.30, "bse_normal_30pct_v0_inferred"
    if symbol.startswith(("300", "301", "688", "689")):
        return 0.20, "registration_board_normal_20pct_v0_inferred"
    return 0.10, "main_board_normal_10pct_v0_inferred"


class AksharePriceLimitConnector(BaseConnector):
    """Infer daily up/down limits from AkShare daily bars and board rules."""

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
        target_date = _date_to_yyyymmdd(options.get("end_date") or default_options["end_date"])
        lookback_days = int(options.get("lookback_days") or default_options.get("lookback_days", 14))
        query_start = _query_start_date(target_date, lookback_days)
        adjust = str(options.get("adjust", default_options.get("adjust", "")))

        for symbol in symbols:
            stats.request_count += 1
            api_symbol = _akshare_daily_symbol(symbol)
            try:
                df = akshare.stock_zh_a_daily(
                    symbol=api_symbol,
                    start_date=query_start,
                    end_date=target_date,
                    adjust=adjust,
                )
                raw_payload = self._dataframe_payload(
                    symbol=symbol,
                    query_start=query_start,
                    target_date=target_date,
                    adjust=adjust,
                    df=df,
                )
                raw = self.raw_store.put_json(
                    source_id=self.source_id,
                    provider_id=self.provider_id,
                    logical_dataset=self.logical_dataset,
                    payload=raw_payload,
                    run_id=run_id,
                    filename_prefix=f"{self.source_id}_{symbol}_{target_date}",
                    metadata={
                        "symbol": symbol,
                        "api_symbol": api_symbol,
                        "query_start": query_start,
                        "target_date": target_date,
                        "adjust": adjust,
                        "akshare_function": "stock_zh_a_daily",
                        "derivation": "prev_close_rule_based_limit",
                    },
                )
                self.metadata_store.insert_raw_object(
                    raw,
                    request_hash=sha256_json(
                        {
                            "function": "stock_zh_a_daily",
                            "symbol": symbol,
                            "api_symbol": api_symbol,
                            "query_start": query_start,
                            "target_date": target_date,
                            "adjust": adjust,
                        }
                    ),
                    request_url="akshare://stock_zh_a_daily",
                    request_params={
                        "symbol": symbol,
                        "api_symbol": api_symbol,
                        "start_date": query_start,
                        "end_date": target_date,
                        "adjust": adjust,
                    },
                )
                raw_checks = quality.check_raw_write(raw)
                self.metadata_store.insert_quality_results(raw_checks)
                if quality.has_critical_failures(raw_checks):
                    stats.quarantine_count += 1
                    continue

                inserted = self._persist_limit(
                    df=df,
                    symbol=symbol,
                    target_date=target_date,
                    raw=raw,
                    run_id=run_id,
                    quality=quality,
                )
                stats.success_count += 1
                stats.new_item_count += inserted["inserted"]
                stats.duplicate_count += inserted["duplicates"]
                stats.quarantine_count += inserted["quarantined"]
            except Exception as exc:
                self.metadata_store.insert_quality_results(
                    [
                        CheckResult(
                            check_name="akshare_price_limit_request",
                            check_type="source_error",
                            severity="critical",
                            status="fail",
                            expected_value="successful AkShare daily response for price limit inference",
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
            raise ValueError("No symbols configured for AkShare price limit connector")
        return symbols

    def _dataframe_payload(
        self,
        *,
        symbol: str,
        query_start: str,
        target_date: str,
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
            "derivation": "prev_close_rule_based_limit",
            "params": {
                "symbol": _akshare_daily_symbol(symbol),
                "start_date": query_start,
                "end_date": target_date,
                "adjust": adjust,
            },
            "columns": [str(column) for column in df.columns],
            "row_count": len(df),
            "records": records,
        }

    def _persist_limit(
        self,
        *,
        df: Any,
        symbol: str,
        target_date: str,
        raw: Any,
        run_id: str,
        quality: QualityRunner,
    ) -> dict[str, int]:
        observed = self._normalize_limit(df=df, symbol=symbol, target_date=target_date, raw=raw)
        checks = quality.check_required_fields(
            contract=self.contract,
            payload=observed,
            run_id=run_id,
            source_id=self.source_id,
        )
        self.metadata_store.insert_quality_results(checks)
        if quality.has_critical_failures(checks):
            return {"inserted": 0, "duplicates": 0, "quarantined": 1}

        source_item_key = observed["source_item_key"]
        duplicate = self.metadata_store.raw_item_version_exists(
            logical_dataset=self.logical_dataset,
            provider_id=self.provider_id,
            source_item_key=source_item_key,
            content_hash=raw.content_hash,
        )
        if duplicate:
            return {"inserted": 0, "duplicates": 1, "quarantined": 0}
        self.metadata_store.insert_raw_item_version(
            logical_dataset=self.logical_dataset,
            provider_id=self.provider_id,
            source_id=self.source_id,
            source_item_key=source_item_key,
            first_seen_at=raw.first_seen_at,
            stored_at=raw.stored_at,
            raw_object_id=raw.raw_object_id,
            content_hash=raw.content_hash,
            title=f"{observed['instrument']} price limit {observed['trading_date']}",
            source_url="akshare://stock_zh_a_daily",
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
        return {"inserted": 1, "duplicates": 0, "quarantined": 0}

    def _normalize_limit(self, *, df: Any, symbol: str, target_date: str, raw: Any) -> dict[str, Any]:
        instrument = _plain_symbol(symbol)
        rows = sorted(
            df.to_dict(orient="records"),
            key=lambda record: _date_to_iso(record.get("date")),
        )
        target_iso = _date_to_iso(target_date)
        prior_rows = [record for record in rows if _date_to_iso(record.get("date")) < target_iso]
        if not prior_rows:
            raise ValueError(f"No prior close found for {instrument} before {target_date}")
        prev_close = _as_float(prior_rows[-1].get("close"))
        if prev_close is None:
            raise ValueError(f"No numeric prior close found for {instrument} before {target_date}")
        rate, limit_rule = _limit_rate(instrument)
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:{instrument}:{target_iso}",
            "instrument": instrument,
            "exchange": _exchange_from_symbol(instrument),
            "trading_date": target_iso,
            "limit_up": round(prev_close * (1 + rate), 2),
            "limit_down": round(prev_close * (1 - rate), 2),
            "prev_close": prev_close,
            "limit_rule": limit_rule,
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }
