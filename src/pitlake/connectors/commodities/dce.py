"""DCE official daily commodity futures connector."""

from __future__ import annotations

from datetime import datetime
from io import StringIO
from typing import Any

import requests

from pitlake.connectors.base import BaseConnector, RequestPlan, RunStats
from pitlake.quality.checks import CheckResult, QualityRunner
from pitlake.utils import sha256_json

DCE_DAY_QUOTES_URL = "http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html"
DCE_REFERER = "http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html"


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
    if value in (None, "", "-", "--", "—"):
        return None
    text = str(value).replace(",", "").strip()
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    number = _as_float(value)
    return int(number) if number is not None else None


def _first_present(record: dict[str, Any], names: tuple[str, ...]) -> Any:
    for name in names:
        if name in record and record[name] not in (None, ""):
            return record[name]
    return None


def _records_from_table(text: str) -> list[dict[str, Any]]:
    pandas = __import__("pandas")
    frames = pandas.read_html(StringIO(text))
    records: list[dict[str, Any]] = []
    for frame in frames:
        frame.columns = [str(column).strip() for column in frame.columns]
        if not any("合约" in column for column in frame.columns):
            continue
        for record in frame.to_dict(orient="records"):
            normalized = {str(key).strip(): _json_safe(value) for key, value in record.items()}
            contract = str(_first_present(normalized, ("合约名称", "合约", "合约代码")) or "").strip()
            if not contract or contract in {"小计", "总计", "合计"}:
                continue
            records.append(normalized)
    return records


def _contract(record: dict[str, Any]) -> str:
    contract = str(_first_present(record, ("合约名称", "合约", "合约代码")) or "").strip().lower()
    return contract.replace(" ", "")


def _symbol_from_contract(contract: str) -> str:
    letters = "".join(ch for ch in contract if ch.isalpha())
    return letters.lower() or None


class DceDailyConnector(BaseConnector):
    """Collect DCE daily quotation table rows from the official public endpoint."""

    connector_version = "0.1.0"

    def plan_requests(self) -> list[RequestPlan]:
        default_options = self.source_config.get("default_options", {})
        if "end_date" not in default_options:
            return []
        query_date = _date_to_iso(default_options["end_date"])
        return [
            RequestPlan(
                url=DCE_DAY_QUOTES_URL,
                params=self._params(query_date=query_date, default_options=default_options),
                headers=self._headers(),
                timeout_seconds=self._timeout(default_options),
            )
        ]

    def collect(self, *, run_id: str, options: dict[str, Any] | None = None) -> RunStats:
        options = options or {}
        default_options = self.source_config.get("default_options", {})
        query_date = _date_to_iso(options.get("end_date") or default_options["end_date"])
        limit_rows = options.get("limit_rows") or default_options.get("limit_rows")
        params = self._params(query_date=query_date, default_options=default_options)
        stats = RunStats(request_count=1)
        quality = QualityRunner()

        try:
            response = requests.get(
                DCE_DAY_QUOTES_URL,
                params=params,
                headers=self._headers(),
                timeout=self._timeout(default_options),
            )
            response.raise_for_status()
            text = response.text
            records = _records_from_table(text)
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
                    "function": "dce_day_quotes",
                    "params": {"date": _date_to_yyyymmdd(query_date), **params},
                    "row_count": len(records),
                    "records": records,
                    "response_text": text,
                },
                run_id=run_id,
                filename_prefix=f"dce_day_quotes_{_date_to_yyyymmdd(query_date)}",
                metadata={"query_date": query_date, "row_count": len(records)},
            )
            self.metadata_store.insert_raw_object(
                raw,
                request_hash=sha256_json({"url": DCE_DAY_QUOTES_URL, "params": params}),
                request_url=DCE_DAY_QUOTES_URL,
                request_params={"date": _date_to_yyyymmdd(query_date), **params},
            )
            raw_checks = quality.check_raw_write(raw)
            self.metadata_store.insert_quality_results(raw_checks)
            if quality.has_critical_failures(raw_checks):
                stats.quarantine_count += len(records) or 1
                return stats
            counts = self._persist_records(
                records=records,
                trading_date=query_date,
                raw=raw,
                run_id=run_id,
                quality=quality,
            )
            stats.success_count = 1
            stats.new_item_count = counts["inserted"]
            stats.duplicate_count = counts["duplicates"]
            stats.quarantine_count += counts["quarantined"]
        except Exception as exc:
            self._record_source_error(
                run_id=run_id,
                observed_value=str(exc)[:1000],
                sample_key=query_date,
            )
            stats.error_count = 1
        return stats

    def _params(self, *, query_date: str, default_options: dict[str, Any]) -> dict[str, Any]:
        parsed = datetime.strptime(query_date, "%Y-%m-%d")
        month_value = parsed.month - 1
        if default_options.get("month_zero_based") is False:
            month_value = parsed.month
        return {
            "dayQuotes.variety": default_options.get("variety", "all"),
            "dayQuotes.trade_type": str(default_options.get("trade_type", "0")),
            "year": str(parsed.year),
            "month": str(month_value),
            "day": str(parsed.day),
        }

    def _headers(self) -> dict[str, str]:
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
            "Referer": DCE_REFERER,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
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
                title=f"DCE {observed['contract']} commodity daily {observed['trading_date']}",
                source_url=DCE_DAY_QUOTES_URL,
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
        if inserted + duplicates + quarantined == 0:
            quarantined = 1
        return {"inserted": inserted, "duplicates": duplicates, "quarantined": quarantined}

    def _normalize_record(self, *, record: dict[str, Any], trading_date: str, raw: Any) -> dict[str, Any]:
        contract = _contract(record)
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:DCE:{contract}:{trading_date}",
            "exchange": "DCE",
            "contract": contract,
            "trading_date": trading_date,
            "symbol": _symbol_from_contract(contract),
            "open": _as_float(_first_present(record, ("开盘价", "开盘"))),
            "high": _as_float(_first_present(record, ("最高价", "最高"))),
            "low": _as_float(_first_present(record, ("最低价", "最低"))),
            "close": _as_float(_first_present(record, ("收盘价", "收盘"))),
            "settlement": _as_float(_first_present(record, ("结算价", "今结算", "结算"))),
            "prev_settlement": _as_float(_first_present(record, ("前结算价", "昨结算"))),
            "volume": _as_int(_first_present(record, ("成交量", "成交量(手)"))),
            "open_interest": _as_int(_first_present(record, ("持仓量", "空盘量"))),
            "session": "daily",
            "metric_payload": {
                key: _json_safe(value)
                for key, value in record.items()
                if key
                not in {
                    "合约名称",
                    "合约",
                    "合约代码",
                    "开盘价",
                    "开盘",
                    "最高价",
                    "最高",
                    "最低价",
                    "最低",
                    "收盘价",
                    "收盘",
                    "结算价",
                    "今结算",
                    "结算",
                    "前结算价",
                    "昨结算",
                    "成交量",
                    "成交量(手)",
                    "持仓量",
                    "空盘量",
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
                    check_name="dce_day_quotes_request",
                    check_type="source_error",
                    severity="critical",
                    status="fail",
                    expected_value="successful DCE day quotes response",
                    observed_value=observed_value,
                    failed_count=1,
                    sample_failed_keys=[sample_key],
                    run_id=run_id,
                    logical_dataset=self.logical_dataset,
                    source_id=self.source_id,
                )
            ]
        )
