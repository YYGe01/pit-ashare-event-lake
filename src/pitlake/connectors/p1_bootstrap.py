"""P1 bootstrap connectors for public low-frequency data sources."""

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


def _date_to_iso(value: Any) -> str:
    value = _json_safe(value)
    if isinstance(value, (int, float)) and value > 10_000_000_000:
        return datetime.utcfromtimestamp(value / 1000).date().isoformat()
    text = str(value).strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return datetime.strptime(text[:10], "%Y-%m-%d").date().isoformat()
    compact = text.replace("-", "").replace("/", "")[:8]
    if compact.isdigit() and len(compact) == 8:
        return datetime.strptime(compact, "%Y%m%d").date().isoformat()
    if len(compact) == 6 and compact.isdigit():
        return datetime.strptime(compact + "01", "%Y%m%d").date().isoformat()
    raise ValueError(f"unsupported date: {value}")


def _plain_symbol(value: Any) -> str:
    clean = str(value).strip().lower().replace(".", "").replace("_", "")
    for prefix in ("sh", "sz", "bj"):
        if clean.startswith(prefix):
            return clean[len(prefix) :].zfill(6)
    digits = "".join(ch for ch in clean if ch.isdigit())
    return digits[-6:].zfill(6) if digits else clean


def _exchange_from_symbol(symbol: str) -> str:
    if symbol.startswith("6"):
        return "SSE"
    if symbol.startswith(("0", "3")):
        return "SZSE"
    if symbol.startswith(("4", "8", "9")):
        return "BSE"
    return "UNKNOWN"


def _records(df: Any) -> list[dict[str, Any]]:
    return [
        {str(key): _json_safe(value) for key, value in record.items()}
        for record in df.to_dict(orient="records")
    ]


def _first_present(record: dict[str, Any], names: tuple[str, ...]) -> Any:
    for name in names:
        if name in record and record[name] not in (None, ""):
            return record[name]
    return None


class _AkshareFrameConnector(BaseConnector):
    akshare_function = ""
    filename_prefix = "p1_bootstrap"

    def plan_requests(self) -> list[RequestPlan]:
        return []

    def collect(self, *, run_id: str, options: dict[str, Any] | None = None) -> RunStats:
        options = options or {}
        akshare = __import__("akshare")
        stats = RunStats()
        quality = QualityRunner()
        requests = self._request_params(options)
        function = getattr(akshare, self.akshare_function)

        for params in requests:
            stats.request_count += 1
            try:
                df = function(**params)
                raw = self.raw_store.put_json(
                    source_id=self.source_id,
                    provider_id=self.provider_id,
                    logical_dataset=self.logical_dataset,
                    payload={
                        "provider_id": self.provider_id,
                        "source_id": self.source_id,
                        "logical_dataset": self.logical_dataset,
                        "function": self.akshare_function,
                        "params": params,
                        "columns": [str(column) for column in df.columns],
                        "row_count": len(df),
                        "records": _records(df),
                    },
                    run_id=run_id,
                    filename_prefix=f"{self.filename_prefix}_{sha256_json(params)[7:15]}",
                    metadata={"akshare_function": self.akshare_function, "params": params},
                )
                self.metadata_store.insert_raw_object(
                    raw,
                    request_hash=sha256_json({"function": self.akshare_function, **params}),
                    request_url=f"akshare://{self.akshare_function}",
                    request_params=params,
                )
                raw_checks = quality.check_raw_write(raw)
                self.metadata_store.insert_quality_results(raw_checks)
                if quality.has_critical_failures(raw_checks):
                    stats.quarantine_count += 1
                    continue
                counts = self._persist_records(
                    records=_records(df),
                    params=params,
                    raw=raw,
                    run_id=run_id,
                    quality=quality,
                )
                stats.success_count += 1
                stats.new_item_count += counts["inserted"]
                stats.duplicate_count += counts["duplicates"]
                stats.quarantine_count += counts["quarantined"]
            except Exception as exc:
                self.metadata_store.insert_quality_results(
                    [
                        CheckResult(
                            check_name=f"{self.source_id}_request",
                            check_type="source_error",
                            severity="critical",
                            status="fail",
                            expected_value=f"successful {self.akshare_function} response",
                            observed_value=str(exc)[:1000],
                            failed_count=1,
                            sample_failed_keys=[str(params)],
                            run_id=run_id,
                            logical_dataset=self.logical_dataset,
                            source_id=self.source_id,
                        )
                    ]
                )
                stats.error_count += 1
        return stats

    def _request_params(self, options: dict[str, Any]) -> list[dict[str, Any]]:
        raise NotImplementedError

    def _normalize_record(
        self,
        *,
        record: dict[str, Any],
        params: dict[str, Any],
        raw: Any,
    ) -> dict[str, Any]:
        raise NotImplementedError

    def _persist_records(
        self,
        *,
        records: list[dict[str, Any]],
        params: dict[str, Any],
        raw: Any,
        run_id: str,
        quality: QualityRunner,
    ) -> dict[str, int]:
        inserted = 0
        duplicates = 0
        quarantined = 0
        for record in records:
            try:
                observed = self._normalize_record(record=record, params=params, raw=raw)
            except (KeyError, ValueError):
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
                title=observed["source_item_key"],
                source_url=f"akshare://{self.akshare_function}",
                first_seen_at=raw.first_seen_at,
                stored_at=raw.stored_at,
                raw_object_id=raw.raw_object_id,
                content_hash=raw.content_hash,
                dedup_hash=sha256_json(
                    {
                        "provider_id": self.provider_id,
                        "source_item_key": observed["source_item_key"],
                    }
                ),
                quality_status="pass",
                observed_payload=observed,
            )
            inserted += 1
        if inserted + duplicates + quarantined == 0:
            quarantined = 1
        return {"inserted": inserted, "duplicates": duplicates, "quarantined": quarantined}


class AkshareMacroChinaFinancialCreditConnector(_AkshareFrameConnector):
    connector_version = "0.1.0"
    akshare_function = "macro_china_new_financial_credit"
    filename_prefix = "macro_financial_credit"

    def _request_params(self, options: dict[str, Any]) -> list[dict[str, Any]]:
        return [{}]

    def _normalize_record(self, *, record: dict[str, Any], params: dict[str, Any], raw: Any) -> dict[str, Any]:
        date_value = _first_present(record, ("date", "日期", "统计时间", "月份", "month"))
        observation_date = _date_to_iso(date_value)
        metrics = {key: value for key, value in record.items() if value not in (None, "")}
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:china_financial_credit:{observation_date}",
            "indicator_group": "china_financial_credit",
            "region": "CN",
            "frequency": "monthly",
            "observation_date": observation_date,
            "metric_payload": metrics,
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }


class AkshareCapitalFlowConnector(_AkshareFrameConnector):
    connector_version = "0.1.0"
    akshare_function = "stock_individual_fund_flow"
    filename_prefix = "capital_flow"

    def _request_params(self, options: dict[str, Any]) -> list[dict[str, Any]]:
        default_options = self.source_config.get("default_options", {})
        raw_symbols = options.get("symbols") or default_options.get("symbols") or []
        symbols = raw_symbols.split(",") if isinstance(raw_symbols, str) else list(raw_symbols)
        limit = options.get("limit_symbols") or default_options.get("limit_symbols")
        symbols = [_plain_symbol(symbol) for symbol in symbols if str(symbol).strip()]
        symbols = symbols[: int(limit)] if limit else symbols
        return [{"stock": symbol, "market": "sh" if symbol.startswith("6") else "sz"} for symbol in symbols]

    def _normalize_record(self, *, record: dict[str, Any], params: dict[str, Any], raw: Any) -> dict[str, Any]:
        instrument = _plain_symbol(params["stock"])
        date_value = _first_present(record, ("date", "日期", "trading_date"))
        trading_date = _date_to_iso(date_value)
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:{instrument}:{trading_date}",
            "instrument": instrument,
            "exchange": _exchange_from_symbol(instrument),
            "trading_date": trading_date,
            "flow_scope": "stock_individual",
            "metric_payload": {key: value for key, value in record.items() if value not in (None, "")},
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }


class AkshareIndustryMembershipConnector(_AkshareFrameConnector):
    connector_version = "0.1.0"
    akshare_function = "stock_board_industry_cons_em"
    filename_prefix = "industry_membership"

    def _request_params(self, options: dict[str, Any]) -> list[dict[str, Any]]:
        default_options = self.source_config.get("default_options", {})
        raw_names = options.get("board_names") or default_options.get("board_names") or []
        names = raw_names.split(",") if isinstance(raw_names, str) else list(raw_names)
        limit = options.get("limit_boards") or default_options.get("limit_boards")
        names = [str(name).strip() for name in names if str(name).strip()]
        names = names[: int(limit)] if limit else names
        return [{"symbol": name} for name in names]

    def _normalize_record(self, *, record: dict[str, Any], params: dict[str, Any], raw: Any) -> dict[str, Any]:
        symbol_value = _first_present(record, ("代码", "股票代码", "stock_code", "symbol"))
        instrument = _plain_symbol(symbol_value)
        snapshot_date = raw.stored_at[:10]
        industry_name = str(params["symbol"])
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:{industry_name}:{instrument}:{snapshot_date}",
            "industry_name": industry_name,
            "instrument": instrument,
            "exchange": _exchange_from_symbol(instrument),
            "snapshot_date": snapshot_date,
            "metric_payload": {key: value for key, value in record.items() if value not in (None, "")},
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }


class AkshareConceptMembershipConnector(AkshareIndustryMembershipConnector):
    connector_version = "0.1.0"
    akshare_function = "stock_board_concept_cons_em"
    filename_prefix = "concept_membership"

    def _normalize_record(self, *, record: dict[str, Any], params: dict[str, Any], raw: Any) -> dict[str, Any]:
        observed = super()._normalize_record(record=record, params=params, raw=raw)
        concept_name = observed.pop("industry_name")
        observed["concept_name"] = concept_name
        observed["source_item_key"] = (
            f"{self.provider_id}:{concept_name}:{observed['instrument']}:{observed['snapshot_date']}"
        )
        return observed
