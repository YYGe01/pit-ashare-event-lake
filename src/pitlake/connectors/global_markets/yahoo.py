"""Yahoo Finance connector for low-volume global daily shadow checks."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from pitlake.connectors.base import BaseConnector, RequestPlan, RunStats
from pitlake.quality.checks import CheckResult, QualityRunner
from pitlake.utils import sha256_json

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


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


def _date_to_epoch(value: Any) -> int:
    parsed = datetime.strptime(_date_to_iso(value), "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


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


def _symbol_list(raw_symbols: Any) -> list[str]:
    if isinstance(raw_symbols, str):
        return [item.strip() for item in raw_symbols.split(",") if item.strip()]
    return [str(item).strip() for item in raw_symbols or [] if str(item).strip()]


def _sequence_value(values: Any, index: int) -> Any:
    if not isinstance(values, list) or index >= len(values):
        return None
    return values[index]


class YahooFinanceGlobalDailyConnector(BaseConnector):
    """Collect selected global daily bars from Yahoo Finance chart metadata."""

    connector_version = "0.1.0"

    def plan_requests(self) -> list[RequestPlan]:
        default_options = self.source_config.get("default_options", {})
        end_date = default_options.get("end_date")
        if not end_date:
            return []
        period1 = _date_to_epoch(end_date)
        period2 = _date_to_epoch(_date_to_iso(end_date)) + 86_400
        requests = []
        for symbol in _symbol_list(default_options.get("symbols")):
            requests.append(
                RequestPlan(
                    url=YAHOO_CHART_URL.format(symbol=symbol),
                    params=self._params(period1=period1, period2=period2),
                    headers=self._headers(),
                    timeout_seconds=self._timeout(default_options),
                )
            )
        return requests

    def collect(self, *, run_id: str, options: dict[str, Any] | None = None) -> RunStats:
        options = options or {}
        default_options = self.source_config.get("default_options", {})
        symbols = self._resolve_symbols(options, default_options)
        target_date = _date_to_iso(options.get("end_date") or default_options["end_date"])
        period1 = _date_to_epoch(target_date)
        period2 = _date_to_epoch(
            (datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)).date().isoformat()
        )
        stats = RunStats()
        quality = QualityRunner()

        for symbol in symbols:
            stats.request_count += 1
            url = YAHOO_CHART_URL.format(symbol=symbol)
            params = self._params(period1=period1, period2=period2)
            try:
                response = requests.get(
                    url,
                    params=params,
                    headers=self._headers(),
                    timeout=self._timeout(default_options),
                )
                response.raise_for_status()
                payload = response.json()
                bars = self._bars_from_payload(payload=payload, symbol=symbol)
                raw = self.raw_store.put_json(
                    source_id=self.source_id,
                    provider_id=self.provider_id,
                    logical_dataset=self.logical_dataset,
                    payload={
                        "provider_id": self.provider_id,
                        "source_id": self.source_id,
                        "logical_dataset": self.logical_dataset,
                        "function": "yahoo_finance_chart",
                        "params": {"symbol": symbol, "target_date": target_date, **params},
                        "row_count": len(bars),
                        "payload": payload,
                    },
                    run_id=run_id,
                    filename_prefix=f"yahoo_{symbol}_{target_date.replace('-', '')}",
                    metadata={"symbol": symbol, "target_date": target_date, "row_count": len(bars)},
                )
                self.metadata_store.insert_raw_object(
                    raw,
                    request_hash=sha256_json({"url": url, "params": params}),
                    request_url=url,
                    request_params={"symbol": symbol, "target_date": target_date, **params},
                )
                raw_checks = quality.check_raw_write(raw)
                self.metadata_store.insert_quality_results(raw_checks)
                if quality.has_critical_failures(raw_checks):
                    stats.quarantine_count += len(bars) or 1
                    continue
                counts = self._persist_bars(
                    bars=bars,
                    target_date=target_date,
                    raw=raw,
                    run_id=run_id,
                    quality=quality,
                )
                stats.success_count += 1
                stats.new_item_count += counts["inserted"]
                stats.duplicate_count += counts["duplicates"]
                stats.quarantine_count += counts["quarantined"]
            except Exception as exc:
                self._record_source_error(
                    run_id=run_id,
                    observed_value=str(exc)[:1000],
                    sample_key=symbol,
                )
                stats.error_count += 1
        return stats

    def _resolve_symbols(
        self,
        options: dict[str, Any],
        default_options: dict[str, Any],
    ) -> list[str]:
        symbols = _symbol_list(options.get("symbols") or default_options.get("symbols"))
        limit = options.get("limit_symbols") or default_options.get("limit_symbols")
        return symbols[: int(limit)] if limit else symbols

    def _params(self, *, period1: int, period2: int) -> dict[str, Any]:
        return {
            "period1": period1,
            "period2": period2,
            "interval": "1d",
            "events": "history",
            "includeAdjustedClose": "true",
        }

    def _headers(self) -> dict[str, str]:
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
        }

    def _timeout(self, default_options: dict[str, Any]) -> int:
        return int(default_options.get("timeout_seconds") or 20)

    def _bars_from_payload(self, *, payload: dict[str, Any], symbol: str) -> list[dict[str, Any]]:
        chart = payload.get("chart") or {}
        if chart.get("error"):
            raise ValueError(chart["error"])
        results = chart.get("result") or []
        if not results:
            return []
        result = results[0]
        meta = result.get("meta") or {}
        timestamps = result.get("timestamp") or []
        quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
        adjclose = ((result.get("indicators") or {}).get("adjclose") or [{}])[0]
        bars = []
        for index, timestamp in enumerate(timestamps):
            trading_date = datetime.fromtimestamp(int(timestamp), tz=timezone.utc).date().isoformat()
            bars.append(
                {
                    "provider_id": self.provider_id,
                    "source_id": self.source_id,
                    "source_item_key": f"{self.provider_id}:{symbol}:{trading_date}",
                    "symbol": symbol,
                    "trading_date": trading_date,
                    "market": meta.get("exchangeName") or meta.get("fullExchangeName"),
                    "name": meta.get("shortName") or meta.get("longName") or symbol,
                    "open": _as_float(_sequence_value(quote.get("open"), index)),
                    "high": _as_float(_sequence_value(quote.get("high"), index)),
                    "low": _as_float(_sequence_value(quote.get("low"), index)),
                    "close": _as_float(_sequence_value(quote.get("close"), index)),
                    "currency": meta.get("currency"),
                    "timezone": meta.get("exchangeTimezoneName") or meta.get("timezone"),
                    "source_timestamp": datetime.fromtimestamp(
                        int(timestamp),
                        tz=timezone.utc,
                    ).isoformat(timespec="seconds"),
                    "metric_payload": {
                        "adjclose": _as_float(_sequence_value(adjclose.get("adjclose"), index)),
                        "volume": _as_int(_sequence_value(quote.get("volume"), index)),
                        "instrument_type": meta.get("instrumentType"),
                        "data_granularity": meta.get("dataGranularity"),
                        "range": meta.get("range"),
                    },
                }
            )
        return bars

    def _persist_bars(
        self,
        *,
        bars: list[dict[str, Any]],
        target_date: str,
        raw: Any,
        run_id: str,
        quality: QualityRunner,
    ) -> dict[str, int]:
        inserted = 0
        duplicates = 0
        quarantined = 0
        for bar in bars:
            if bar["trading_date"] != target_date:
                continue
            observed = {
                **bar,
                "first_seen_at": raw.first_seen_at,
                "raw_uri": raw.raw_uri,
                "content_hash": raw.content_hash,
            }
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
                title=f"{observed['symbol']} Yahoo daily {observed['trading_date']}",
                source_url=YAHOO_CHART_URL.format(symbol=observed["symbol"]),
                first_seen_at=raw.first_seen_at,
                stored_at=raw.stored_at,
                raw_object_id=raw.raw_object_id,
                content_hash=raw.content_hash,
                dedup_hash=sha256_json(
                    {
                        "provider_id": self.provider_id,
                        "symbol": observed["symbol"],
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

    def _record_source_error(self, *, run_id: str, observed_value: str, sample_key: str) -> None:
        self.metadata_store.insert_quality_results(
            [
                CheckResult(
                    check_name="yahoo_finance_chart_request",
                    check_type="source_error",
                    severity="critical",
                    status="fail",
                    expected_value="successful Yahoo Finance chart response",
                    observed_value=observed_value,
                    failed_count=1,
                    sample_failed_keys=[sample_key],
                    run_id=run_id,
                    logical_dataset=self.logical_dataset,
                    source_id=self.source_id,
                )
            ]
        )
