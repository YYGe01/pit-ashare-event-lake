"""BaoStock connector for A-share daily OHLCV shadow collection."""

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


def _plain_symbol(symbol: str) -> str:
    clean = str(symbol).lower().replace("_", ".")
    for prefix in ("sh.", "sz.", "bj."):
        if clean.startswith(prefix):
            return clean[len(prefix) :].zfill(6)
    for prefix in ("sh", "sz", "bj"):
        if clean.startswith(prefix):
            return clean[len(prefix) :].zfill(6)
    return clean.replace(".", "").zfill(6)


def _exchange_from_symbol(symbol: str) -> str:
    instrument = _plain_symbol(symbol)
    if instrument.startswith("6"):
        return "SSE"
    if instrument.startswith(("0", "3")):
        return "SZSE"
    if instrument.startswith(("4", "8", "9")):
        return "BSE"
    return "UNKNOWN"


def _baostock_symbol(symbol: str) -> str:
    instrument = _plain_symbol(symbol)
    if instrument.startswith("6"):
        return f"sh.{instrument}"
    if instrument.startswith(("0", "3")):
        return f"sz.{instrument}"
    if instrument.startswith(("4", "8", "9")):
        return f"bj.{instrument}"
    return instrument


def _date_to_iso(value: str) -> str:
    text = str(value).strip()
    compact = text.replace("-", "")
    return datetime.strptime(compact, "%Y%m%d").date().isoformat()


class BaoStockMarketDailyConnector(BaseConnector):
    """Collect selected A-share daily bars via BaoStock query_history_k_data_plus."""

    connector_version = "0.1.0"
    fields = (
        "date,code,open,high,low,close,preclose,volume,amount,"
        "adjustflag,turn,tradestatus,pctChg,isST"
    )

    def plan_requests(self) -> list[RequestPlan]:
        return []

    def collect(self, *, run_id: str, options: dict[str, Any] | None = None) -> RunStats:
        options = options or {}
        baostock = __import__("baostock")
        stats = RunStats()
        quality = QualityRunner()
        default_options = self.source_config.get("default_options", {})
        symbols = self._resolve_symbols(options, default_options)
        start_date = _date_to_iso(str(options.get("start_date") or default_options["start_date"]))
        end_date = _date_to_iso(str(options.get("end_date") or default_options["end_date"]))
        adjustflag = str(options.get("adjustflag") or default_options.get("adjustflag") or "3")

        login_result = baostock.login()
        if getattr(login_result, "error_code", "0") != "0":
            message = getattr(login_result, "error_msg", "BaoStock login failed")
            self._record_source_error(
                run_id=run_id,
                check_name="baostock_login",
                observed_value=str(message),
                sample_key="login",
            )
            return RunStats(error_count=1)

        try:
            for symbol in symbols:
                stats.request_count += 1
                api_symbol = _baostock_symbol(symbol)
                try:
                    result = baostock.query_history_k_data_plus(
                        api_symbol,
                        self.fields,
                        start_date=start_date,
                        end_date=end_date,
                        frequency="d",
                        adjustflag=adjustflag,
                    )
                    if getattr(result, "error_code", "0") != "0":
                        raise RuntimeError(getattr(result, "error_msg", "BaoStock query failed"))
                    records = self._result_records(result)
                    raw_payload = self._raw_payload(
                        symbol=symbol,
                        api_symbol=api_symbol,
                        start_date=start_date,
                        end_date=end_date,
                        adjustflag=adjustflag,
                        records=records,
                    )
                    raw = self.raw_store.put_json(
                        source_id=self.source_id,
                        provider_id=self.provider_id,
                        logical_dataset=self.logical_dataset,
                        payload=raw_payload,
                        run_id=run_id,
                        filename_prefix=(
                            f"baostock_daily_{symbol}_"
                            f"{start_date.replace('-', '')}_{end_date.replace('-', '')}"
                        ),
                        metadata={
                            "symbol": symbol,
                            "api_symbol": api_symbol,
                            "start_date": start_date,
                            "end_date": end_date,
                            "adjustflag": adjustflag,
                            "baostock_function": "query_history_k_data_plus",
                        },
                    )
                    request_params = {
                        "code": api_symbol,
                        "fields": self.fields,
                        "start_date": start_date,
                        "end_date": end_date,
                        "frequency": "d",
                        "adjustflag": adjustflag,
                    }
                    self.metadata_store.insert_raw_object(
                        raw,
                        request_hash=sha256_json(
                            {
                                "function": "query_history_k_data_plus",
                                **request_params,
                            }
                        ),
                        request_url="baostock://query_history_k_data_plus",
                        request_params=request_params,
                    )
                    raw_checks = quality.check_raw_write(raw)
                    self.metadata_store.insert_quality_results(raw_checks)
                    if quality.has_critical_failures(raw_checks):
                        stats.quarantine_count += 1
                        continue

                    row_count = self._persist_records(
                        records=records,
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
                    self._record_source_error(
                        run_id=run_id,
                        check_name="baostock_query_history_k_data_plus",
                        observed_value=str(exc)[:1000],
                        sample_key=symbol,
                    )
                    stats.error_count += 1
        finally:
            logout = getattr(baostock, "logout", None)
            if logout:
                logout()
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
            raise ValueError("No symbols configured for BaoStock market daily connector")
        return symbols

    def _result_records(self, result: Any) -> list[dict[str, Any]]:
        fields = [str(field) for field in result.fields]
        records: list[dict[str, Any]] = []
        while result.next():
            row = result.get_row_data()
            records.append(dict(zip(fields, [_json_safe(value) for value in row], strict=False)))
        return records

    def _raw_payload(
        self,
        *,
        symbol: str,
        api_symbol: str,
        start_date: str,
        end_date: str,
        adjustflag: str,
        records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "logical_dataset": self.logical_dataset,
            "function": "query_history_k_data_plus",
            "params": {
                "code": api_symbol,
                "fields": self.fields,
                "start_date": start_date,
                "end_date": end_date,
                "frequency": "d",
                "adjustflag": adjustflag,
            },
            "symbol": _plain_symbol(symbol),
            "columns": self.fields.split(","),
            "row_count": len(records),
            "records": records,
        }

    def _persist_records(
        self,
        *,
        records: list[dict[str, Any]],
        symbol: str,
        raw: Any,
        run_id: str,
        quality: QualityRunner,
    ) -> dict[str, int]:
        inserted = 0
        duplicates = 0
        quarantined = 0
        for record in records:
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
                title=f"{observed['instrument']} BaoStock daily bar {observed['trading_date']}",
                source_url="baostock://query_history_k_data_plus",
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

    def _normalize_record(self, *, symbol: str, record: dict[str, Any], raw: Any) -> dict[str, Any]:
        instrument = _plain_symbol(record.get("code") or symbol)
        trading_date = _date_to_iso(str(record.get("date")))
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:{instrument}:{trading_date}",
            "instrument": instrument,
            "exchange": _exchange_from_symbol(instrument),
            "trading_date": trading_date,
            "open": _as_float(record.get("open")),
            "high": _as_float(record.get("high")),
            "low": _as_float(record.get("low")),
            "close": _as_float(record.get("close")),
            "volume": _as_int(record.get("volume")),
            "amount": _as_float(record.get("amount")),
            "prev_close": _as_float(record.get("preclose")),
            "turnover": _as_float(record.get("turn")),
            "metric_payload": {
                key: _json_safe(value)
                for key, value in record.items()
                if key
                not in {
                    "date",
                    "code",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "amount",
                    "preclose",
                    "turn",
                }
            },
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }

    def _record_source_error(
        self,
        *,
        run_id: str,
        check_name: str,
        observed_value: str,
        sample_key: str,
    ) -> None:
        self.metadata_store.insert_quality_results(
            [
                CheckResult(
                    check_name=check_name,
                    check_type="source_error",
                    severity="critical",
                    status="fail",
                    expected_value="successful BaoStock response",
                    observed_value=observed_value,
                    failed_count=1,
                    sample_failed_keys=[sample_key],
                    run_id=run_id,
                    logical_dataset=self.logical_dataset,
                    source_id=self.source_id,
                )
            ]
        )
