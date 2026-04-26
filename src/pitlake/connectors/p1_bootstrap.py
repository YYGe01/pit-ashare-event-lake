"""P1 bootstrap connectors for public low-frequency data sources."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from pitlake.connectors.base import BaseConnector, RequestPlan, ResponsePayload, RunStats
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


def _value_at(record: dict[str, Any], index: int) -> Any:
    values = list(record.values())
    if index >= len(values):
        return None
    return values[index]


def _first_option(options: dict[str, Any], default_options: dict[str, Any], name: str) -> Any:
    return options.get(name) if options.get(name) not in (None, "") else default_options.get(name)


def _insert_observed_record(
    *,
    connector: BaseConnector,
    observed: dict[str, Any],
    raw: Any,
    run_id: str,
    quality: QualityRunner,
    title: str | None = None,
    source_url: str | None = None,
    source_publish_time: str | None = None,
    source_update_time: str | None = None,
) -> str:
    checks = quality.check_required_fields(
        contract=connector.contract,
        payload=observed,
        run_id=run_id,
        source_id=connector.source_id,
    )
    connector.metadata_store.insert_quality_results(checks)
    if quality.has_critical_failures(checks):
        return "quarantined"
    duplicate = connector.metadata_store.raw_item_version_exists(
        logical_dataset=connector.logical_dataset,
        provider_id=connector.provider_id,
        source_item_key=observed["source_item_key"],
        content_hash=raw.content_hash,
    )
    if duplicate:
        return "duplicate"
    connector.metadata_store.insert_raw_item_version(
        logical_dataset=connector.logical_dataset,
        provider_id=connector.provider_id,
        source_id=connector.source_id,
        source_item_key=observed["source_item_key"],
        title=title or observed["source_item_key"],
        source_url=source_url,
        source_publish_time=source_publish_time,
        source_update_time=source_update_time,
        first_seen_at=raw.first_seen_at,
        stored_at=raw.stored_at,
        raw_object_id=raw.raw_object_id,
        content_hash=raw.content_hash,
        dedup_hash=sha256_json(
            {
                "provider_id": connector.provider_id,
                "source_item_key": observed["source_item_key"],
            }
        ),
        quality_status="pass",
        observed_payload=observed,
    )
    return "inserted"


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
            status = _insert_observed_record(
                connector=self,
                observed=observed,
                raw=raw,
                run_id=run_id,
                quality=quality,
                title=observed["source_item_key"],
                source_url=f"akshare://{self.akshare_function}",
            )
            if status == "duplicate":
                duplicates += 1
            elif status == "quarantined":
                quarantined += 1
            else:
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


class AkshareMarginTradingDetailConnector(_AkshareFrameConnector):
    connector_version = "0.1.0"
    filename_prefix = "margin_trading_detail"

    def collect(self, *, run_id: str, options: dict[str, Any] | None = None) -> RunStats:
        options = options or {}
        akshare = __import__("akshare")
        stats = RunStats()
        quality = QualityRunner()
        requests = self._request_params(options)
        for params in requests:
            stats.request_count += 1
            try:
                market = params["market"]
                function_name = (
                    "stock_margin_detail_sse" if market == "SSE" else "stock_margin_detail_szse"
                )
                df = getattr(akshare, function_name)(date=params["date"])
                raw = self.raw_store.put_json(
                    source_id=self.source_id,
                    provider_id=self.provider_id,
                    logical_dataset=self.logical_dataset,
                    payload={
                        "provider_id": self.provider_id,
                        "source_id": self.source_id,
                        "logical_dataset": self.logical_dataset,
                        "function": function_name,
                        "params": params,
                        "columns": [str(column) for column in df.columns],
                        "row_count": len(df),
                        "records": _records(df),
                    },
                    run_id=run_id,
                    filename_prefix=f"margin_trading_{market}_{params['date']}",
                    metadata={"akshare_function": function_name, "params": params},
                )
                self.metadata_store.insert_raw_object(
                    raw,
                    request_hash=sha256_json({"function": function_name, **params}),
                    request_url=f"akshare://{function_name}",
                    request_params=params,
                )
                raw_checks = quality.check_raw_write(raw)
                self.metadata_store.insert_quality_results(raw_checks)
                if quality.has_critical_failures(raw_checks):
                    stats.quarantine_count += 1
                    continue
                counts = self._persist_records(
                    records=_records(df),
                    params={"function": function_name, **params},
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
                            expected_value="successful AkShare margin trading detail response",
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
        default_options = self.source_config.get("default_options", {})
        date = str(_first_option(options, default_options, "end_date") or "20260424")
        date = date.replace("-", "")[:8]
        raw_markets = _first_option(options, default_options, "markets") or ["SSE", "SZSE"]
        markets = raw_markets.split(",") if isinstance(raw_markets, str) else list(raw_markets)
        return [{"date": date, "market": str(market).upper()} for market in markets]

    def _normalize_record(
        self,
        *,
        record: dict[str, Any],
        params: dict[str, Any],
        raw: Any,
    ) -> dict[str, Any]:
        date_value = _first_present(record, ("信用交易日期", "date", "trading_date")) or _value_at(record, 0)
        symbol_value = (
            _first_present(record, ("标的证券代码", "证券代码", "代码", "stock_code", "symbol"))
            or _value_at(record, 1)
        )
        instrument = _plain_symbol(symbol_value)
        trading_date = _date_to_iso(date_value)
        market = params["market"]
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:margin_trading:{market}:{instrument}:{trading_date}",
            "instrument": instrument,
            "exchange": market,
            "trading_date": trading_date,
            "flow_scope": f"margin_trading_{market.lower()}",
            "metric_payload": {key: value for key, value in record.items() if value not in (None, "")},
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }


class AkshareLhbDetailConnector(_AkshareFrameConnector):
    connector_version = "0.1.0"
    akshare_function = "stock_lhb_detail_em"
    filename_prefix = "lhb_detail"

    def _request_params(self, options: dict[str, Any]) -> list[dict[str, Any]]:
        default_options = self.source_config.get("default_options", {})
        start_date = str(_first_option(options, default_options, "start_date") or "20260424")
        end_date = str(_first_option(options, default_options, "end_date") or start_date)
        return [
            {
                "start_date": start_date.replace("-", "")[:8],
                "end_date": end_date.replace("-", "")[:8],
            }
        ]

    def _normalize_record(
        self,
        *,
        record: dict[str, Any],
        params: dict[str, Any],
        raw: Any,
    ) -> dict[str, Any]:
        symbol_value = (
            _first_present(record, ("代码", "证券代码", "stock_code", "symbol")) or _value_at(record, 1)
        )
        date_value = _first_present(record, ("上榜日", "date", "trading_date")) or _value_at(record, 3)
        instrument = _plain_symbol(symbol_value)
        trading_date = _date_to_iso(date_value)
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:lhb_detail:{instrument}:{trading_date}",
            "instrument": instrument,
            "exchange": _exchange_from_symbol(instrument),
            "trading_date": trading_date,
            "flow_scope": "lhb_detail",
            "metric_payload": {key: value for key, value in record.items() if value not in (None, "")},
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }


class AkshareHsgtNorthboundConnector(_AkshareFrameConnector):
    connector_version = "0.1.0"
    akshare_function = "stock_hsgt_hist_em"
    filename_prefix = "hsgt_northbound"

    def _request_params(self, options: dict[str, Any]) -> list[dict[str, Any]]:
        default_options = self.source_config.get("default_options", {})
        symbol = str(_first_option(options, default_options, "symbol") or "北向资金")
        return [{"symbol": symbol}]

    def _normalize_record(
        self,
        *,
        record: dict[str, Any],
        params: dict[str, Any],
        raw: Any,
    ) -> dict[str, Any]:
        date_value = _first_present(record, ("日期", "date", "trading_date")) or _value_at(record, 0)
        trading_date = _date_to_iso(date_value)
        instrument = "northbound_total"
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:hsgt_northbound:{trading_date}",
            "instrument": instrument,
            "exchange": "HKEX_CONNECT",
            "trading_date": trading_date,
            "flow_scope": "hsgt_northbound",
            "metric_payload": {key: value for key, value in record.items() if value not in (None, "")},
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }


class AkshareFundPortfolioHoldConnector(_AkshareFrameConnector):
    connector_version = "0.1.0"
    akshare_function = "stock_report_fund_hold"
    filename_prefix = "fund_portfolio_hold"

    def _request_params(self, options: dict[str, Any]) -> list[dict[str, Any]]:
        default_options = self.source_config.get("default_options", {})
        date = str(_first_option(options, default_options, "end_date") or "20260424")
        symbol = str(_first_option(options, default_options, "symbol") or "基金持仓")
        return [{"symbol": symbol, "date": date.replace("-", "")[:8]}]

    def _normalize_record(
        self,
        *,
        record: dict[str, Any],
        params: dict[str, Any],
        raw: Any,
    ) -> dict[str, Any]:
        symbol_value = (
            _first_present(record, ("股票代码", "证券代码", "代码", "stock_code", "symbol"))
            or _value_at(record, 0)
        )
        fund_value = (
            _first_present(record, ("基金代码", "fund_code", "基金简称", "基金名称"))
            or _value_at(record, 2)
            or "aggregate"
        )
        instrument = _plain_symbol(symbol_value)
        report_date = _date_to_iso(
            _first_present(record, ("报告期", "持仓日期", "date", "report_date")) or params["date"]
        )
        fund_code = str(fund_value).strip() or "aggregate"
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:fund_hold:{fund_code}:{instrument}:{report_date}",
            "fund_code": fund_code,
            "instrument": instrument,
            "exchange": _exchange_from_symbol(instrument),
            "report_date": report_date,
            "metric_payload": {key: value for key, value in record.items() if value not in (None, "")},
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }


class AkshareStockNewsConnector(_AkshareFrameConnector):
    connector_version = "0.1.0"
    akshare_function = "stock_news_em"
    filename_prefix = "stock_news"

    def _request_params(self, options: dict[str, Any]) -> list[dict[str, Any]]:
        default_options = self.source_config.get("default_options", {})
        raw_symbols = options.get("symbols") or default_options.get("symbols") or []
        symbols = raw_symbols.split(",") if isinstance(raw_symbols, str) else list(raw_symbols)
        limit = options.get("limit_symbols") or default_options.get("limit_symbols")
        symbols = [_plain_symbol(symbol) for symbol in symbols if str(symbol).strip()]
        symbols = symbols[: int(limit)] if limit else symbols
        return [{"symbol": symbol} for symbol in symbols]

    def _normalize_record(
        self,
        *,
        record: dict[str, Any],
        params: dict[str, Any],
        raw: Any,
    ) -> dict[str, Any]:
        instrument = _plain_symbol(params["symbol"])
        title = str(_first_present(record, ("新闻标题", "标题", "title", "新闻内容")) or _value_at(record, 1) or "")
        url = str(_first_present(record, ("新闻链接", "链接", "url", "source_url")) or "")
        publish_time = _first_present(record, ("发布时间", "时间", "datetime", "source_publish_time"))
        snapshot_date = _date_to_iso(publish_time or raw.stored_at[:10])
        news_key = sha256_json({"instrument": instrument, "title": title, "url": url})[7:23]
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:stock_news:{instrument}:{news_key}",
            "news_key": news_key,
            "news_scope": "stock_news",
            "instrument": instrument,
            "exchange": _exchange_from_symbol(instrument),
            "snapshot_date": snapshot_date,
            "title": title or news_key,
            "source_url": url or f"akshare://{self.akshare_function}",
            "source_timestamp": str(publish_time) if publish_time else None,
            "metric_payload": {key: value for key, value in record.items() if value not in (None, "")},
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }


class AkshareStockNewsMainCxConnector(_AkshareFrameConnector):
    connector_version = "0.1.0"
    akshare_function = "stock_news_main_cx"
    filename_prefix = "stock_news_main_cx"

    def _request_params(self, options: dict[str, Any]) -> list[dict[str, Any]]:
        return [{}]

    def _normalize_record(
        self,
        *,
        record: dict[str, Any],
        params: dict[str, Any],
        raw: Any,
    ) -> dict[str, Any]:
        title = str(_first_present(record, ("标题", "新闻标题", "title")) or _value_at(record, 1) or "")
        url = str(_first_present(record, ("链接", "新闻链接", "url", "source_url")) or "")
        publish_time = _first_present(record, ("发布时间", "时间", "datetime", "source_publish_time"))
        snapshot_date = _date_to_iso(publish_time or raw.stored_at[:10])
        news_key = sha256_json({"title": title, "url": url, "date": snapshot_date})[7:23]
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:stock_news_main_cx:{news_key}",
            "news_key": news_key,
            "news_scope": "main_financial_news",
            "snapshot_date": snapshot_date,
            "title": title or news_key,
            "source_url": url or f"akshare://{self.akshare_function}",
            "source_timestamp": str(publish_time) if publish_time else None,
            "metric_payload": {key: value for key, value in record.items() if value not in (None, "")},
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }


class AkshareBaiduEconomicNewsConnector(_AkshareFrameConnector):
    connector_version = "0.1.0"
    akshare_function = "news_economic_baidu"
    filename_prefix = "baidu_economic_news"

    def _request_params(self, options: dict[str, Any]) -> list[dict[str, Any]]:
        default_options = self.source_config.get("default_options", {})
        date = str(_first_option(options, default_options, "end_date") or "20260424")
        return [{"date": date.replace("-", "")[:8]}]

    def _normalize_record(
        self,
        *,
        record: dict[str, Any],
        params: dict[str, Any],
        raw: Any,
    ) -> dict[str, Any]:
        title = str(
            _first_present(record, ("标题", "新闻标题", "事件", "title")) or _value_at(record, 0) or ""
        )
        url = str(_first_present(record, ("链接", "新闻链接", "url", "source_url")) or "")
        event_date = _first_present(record, ("日期", "date"))
        event_time = _first_present(record, ("时间", "time"))
        publish_time = _first_present(record, ("发布时间", "datetime", "source_publish_time"))
        if not publish_time and event_date:
            publish_time = f"{event_date} {event_time}" if event_time else event_date
        snapshot_date = _date_to_iso(publish_time or params["date"])
        news_key = sha256_json({"title": title, "url": url, "date": snapshot_date})[7:23]
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:baidu_economic_news:{news_key}",
            "news_key": news_key,
            "news_scope": "economic_news",
            "snapshot_date": snapshot_date,
            "title": title or news_key,
            "source_url": url or f"akshare://{self.akshare_function}",
            "source_timestamp": str(publish_time) if publish_time else None,
            "metric_payload": {key: value for key, value in record.items() if value not in (None, "")},
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }


class AkshareStockHotRankConnector(_AkshareFrameConnector):
    connector_version = "0.1.0"
    akshare_function = "stock_hot_rank_em"
    filename_prefix = "stock_hot_rank"

    def _request_params(self, options: dict[str, Any]) -> list[dict[str, Any]]:
        return [{}]

    def _normalize_record(
        self,
        *,
        record: dict[str, Any],
        params: dict[str, Any],
        raw: Any,
    ) -> dict[str, Any]:
        symbol_value = (
            _first_present(record, ("代码", "股票代码", "证券代码", "stock_code", "symbol"))
            or _value_at(record, 1)
        )
        instrument = _plain_symbol(symbol_value)
        snapshot_date = raw.stored_at[:10]
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:hot_rank:{instrument}:{snapshot_date}",
            "sentiment_scope": "stock_hot_rank",
            "instrument": instrument,
            "exchange": _exchange_from_symbol(instrument),
            "snapshot_date": snapshot_date,
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


class GdeltDocArtListConnector(BaseConnector):
    """Collect low-frequency article metadata from the GDELT DOC 2.0 ArtList API."""

    connector_version = "0.1.0"
    endpoint = "https://api.gdeltproject.org/api/v2/doc/doc"

    def plan_requests(self) -> list[RequestPlan]:
        return []

    def collect(self, *, run_id: str, options: dict[str, Any] | None = None) -> RunStats:
        options = options or {}
        default_options = self.source_config.get("default_options", {})
        quality = QualityRunner()
        stats = RunStats()
        request = self._request_plan(options, default_options)
        stats.request_count += 1
        try:
            response = self.execute_request(request)
            response_payload = self._json_payload(response)
            raw = self.raw_store.put_json(
                source_id=self.source_id,
                provider_id=self.provider_id,
                logical_dataset=self.logical_dataset,
                payload={
                    "provider_id": self.provider_id,
                    "source_id": self.source_id,
                    "logical_dataset": self.logical_dataset,
                    "request": {
                        "url": request.url,
                        "params": request.params,
                        "final_url": response.final_url,
                        "status_code": response.status_code,
                        "headers": response.headers,
                    },
                    "payload": response_payload,
                },
                run_id=run_id,
                filename_prefix="gdelt_doc_artlist",
                metadata={"request_params": request.params, "final_url": response.final_url},
            )
            self.metadata_store.insert_raw_object(
                raw,
                request_hash=request.request_hash,
                request_url=request.url,
                request_params=request.params,
            )
            raw_checks = quality.check_raw_write(raw)
            self.metadata_store.insert_quality_results(raw_checks)
            if quality.has_critical_failures(raw_checks):
                stats.quarantine_count += 1
                return stats
            if response.status_code >= 400:
                raise ValueError(f"GDELT HTTP {response.status_code}")
            records = response_payload.get("articles") or []
            counts = self._persist_articles(
                records=records,
                query=str(request.params.get("query", "")),
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
                        expected_value="successful GDELT DOC ArtList response",
                        observed_value=str(exc)[:1000],
                        failed_count=1,
                        sample_failed_keys=[str(request.params)],
                        run_id=run_id,
                        logical_dataset=self.logical_dataset,
                        source_id=self.source_id,
                    )
                ]
            )
            stats.error_count += 1
        return stats

    def _request_plan(
        self,
        options: dict[str, Any],
        default_options: dict[str, Any],
    ) -> RequestPlan:
        query = _first_option(options, default_options, "query") or "china market"
        timespan = _first_option(options, default_options, "timespan") or "24h"
        maxrecords = int(_first_option(options, default_options, "maxrecords") or 20)
        return RequestPlan(
            url=self.endpoint,
            params={
                "query": str(query),
                "mode": "artlist",
                "format": "json",
                "timespan": str(timespan),
                "maxrecords": maxrecords,
                "sort": "datedesc",
            },
            timeout_seconds=int(_first_option(options, default_options, "timeout_seconds") or 30),
        )

    def _json_payload(self, response: ResponsePayload) -> dict[str, Any]:
        return json.loads(response.content.decode("utf-8-sig"))

    def _persist_articles(
        self,
        *,
        records: list[dict[str, Any]],
        query: str,
        raw: Any,
        run_id: str,
        quality: QualityRunner,
    ) -> dict[str, int]:
        inserted = 0
        duplicates = 0
        quarantined = 0
        for record in records:
            url = str(record.get("url") or "")
            title = str(record.get("title") or "")
            seen_date = _first_present(record, ("seendate", "seenDate", "date", "datetime"))
            snapshot_date = _date_to_iso(seen_date or raw.stored_at[:10])
            event_key = sha256_json({"url": url, "title": title})[7:23]
            observed = {
                "provider_id": self.provider_id,
                "source_id": self.source_id,
                "source_item_key": f"{self.provider_id}:{event_key}:{snapshot_date}",
                "event_key": event_key,
                "snapshot_date": snapshot_date,
                "query": query,
                "source_timestamp": str(seen_date) if seen_date else None,
                "metric_payload": {key: _json_safe(value) for key, value in record.items()},
                "first_seen_at": raw.first_seen_at,
                "raw_uri": raw.raw_uri,
                "content_hash": raw.content_hash,
            }
            status = _insert_observed_record(
                connector=self,
                observed=observed,
                raw=raw,
                run_id=run_id,
                quality=quality,
                title=title or observed["source_item_key"],
                source_url=url,
                source_publish_time=observed["source_timestamp"],
            )
            if status == "duplicate":
                duplicates += 1
            elif status == "quarantined":
                quarantined += 1
            else:
                inserted += 1
        if inserted + duplicates + quarantined == 0:
            quarantined = 1
        return {"inserted": inserted, "duplicates": duplicates, "quarantined": quarantined}


class OpenMeteoWeatherDailyConnector(BaseConnector):
    """Collect daily weather observations from the Open-Meteo archive API."""

    connector_version = "0.1.0"
    endpoint = "https://archive-api.open-meteo.com/v1/archive"
    default_daily_fields = (
        "temperature_2m_max",
        "temperature_2m_min",
        "temperature_2m_mean",
        "precipitation_sum",
        "wind_speed_10m_max",
    )

    def plan_requests(self) -> list[RequestPlan]:
        return []

    def collect(self, *, run_id: str, options: dict[str, Any] | None = None) -> RunStats:
        options = options or {}
        default_options = self.source_config.get("default_options", {})
        quality = QualityRunner()
        stats = RunStats()
        requests = self._request_plans(options, default_options)
        for request, location in requests:
            stats.request_count += 1
            try:
                response = self.execute_request(request)
                response_payload = json.loads(response.content.decode("utf-8-sig"))
                raw = self.raw_store.put_json(
                    source_id=self.source_id,
                    provider_id=self.provider_id,
                    logical_dataset=self.logical_dataset,
                    payload={
                        "provider_id": self.provider_id,
                        "source_id": self.source_id,
                        "logical_dataset": self.logical_dataset,
                        "location": location,
                        "request": {
                            "url": request.url,
                            "params": request.params,
                            "final_url": response.final_url,
                            "status_code": response.status_code,
                            "headers": response.headers,
                        },
                        "payload": response_payload,
                    },
                    run_id=run_id,
                    filename_prefix=f"open_meteo_{location['location_id']}",
                    metadata={"location": location, "request_params": request.params},
                )
                self.metadata_store.insert_raw_object(
                    raw,
                    request_hash=request.request_hash,
                    request_url=request.url,
                    request_params=request.params,
                )
                raw_checks = quality.check_raw_write(raw)
                self.metadata_store.insert_quality_results(raw_checks)
                if quality.has_critical_failures(raw_checks):
                    stats.quarantine_count += 1
                    continue
                if response.status_code >= 400:
                    raise ValueError(f"Open-Meteo HTTP {response.status_code}")
                counts = self._persist_daily_rows(
                    payload=response_payload,
                    location=location,
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
                            expected_value="successful Open-Meteo archive response",
                            observed_value=str(exc)[:1000],
                            failed_count=1,
                            sample_failed_keys=[str(location)],
                            run_id=run_id,
                            logical_dataset=self.logical_dataset,
                            source_id=self.source_id,
                        )
                    ]
                )
                stats.error_count += 1
        return stats

    def _request_plans(
        self,
        options: dict[str, Any],
        default_options: dict[str, Any],
    ) -> list[tuple[RequestPlan, dict[str, Any]]]:
        locations = _first_option(options, default_options, "locations") or []
        if isinstance(locations, dict):
            locations = [locations]
        start_date = _date_to_iso(_first_option(options, default_options, "start_date") or "2026-04-24")
        end_date = _date_to_iso(_first_option(options, default_options, "end_date") or start_date)
        daily_fields = _first_option(options, default_options, "daily_fields") or self.default_daily_fields
        if isinstance(daily_fields, str):
            daily_fields_text = daily_fields
        else:
            daily_fields_text = ",".join(str(field) for field in daily_fields)
        requests: list[tuple[RequestPlan, dict[str, Any]]] = []
        for location in locations:
            normalized = {
                "location_id": str(location["location_id"]),
                "latitude": float(location["latitude"]),
                "longitude": float(location["longitude"]),
            }
            requests.append(
                (
                    RequestPlan(
                        url=self.endpoint,
                        params={
                            "latitude": normalized["latitude"],
                            "longitude": normalized["longitude"],
                            "start_date": start_date,
                            "end_date": end_date,
                            "daily": daily_fields_text,
                            "timezone": str(default_options.get("timezone", "Asia/Shanghai")),
                        },
                        timeout_seconds=int(
                            _first_option(options, default_options, "timeout_seconds") or 30
                        ),
                    ),
                    normalized,
                )
            )
        if not requests:
            raise ValueError("No locations configured for Open-Meteo weather connector")
        return requests

    def _persist_daily_rows(
        self,
        *,
        payload: dict[str, Any],
        location: dict[str, Any],
        raw: Any,
        run_id: str,
        quality: QualityRunner,
    ) -> dict[str, int]:
        daily = payload.get("daily") or {}
        dates = daily.get("time") or []
        inserted = 0
        duplicates = 0
        quarantined = 0
        for index, date_value in enumerate(dates):
            observation_date = _date_to_iso(date_value)
            metrics = {
                key: _json_safe(values[index])
                for key, values in daily.items()
                if key != "time" and isinstance(values, list) and index < len(values)
            }
            observed = {
                "provider_id": self.provider_id,
                "source_id": self.source_id,
                "source_item_key": (
                    f"{self.provider_id}:{location['location_id']}:{observation_date}"
                ),
                "location_id": location["location_id"],
                "latitude": location["latitude"],
                "longitude": location["longitude"],
                "observation_date": observation_date,
                "metric_payload": metrics,
                "first_seen_at": raw.first_seen_at,
                "raw_uri": raw.raw_uri,
                "content_hash": raw.content_hash,
            }
            status = _insert_observed_record(
                connector=self,
                observed=observed,
                raw=raw,
                run_id=run_id,
                quality=quality,
                title=observed["source_item_key"],
                source_url="https://open-meteo.com/",
            )
            if status == "duplicate":
                duplicates += 1
            elif status == "quarantined":
                quarantined += 1
            else:
                inserted += 1
        if inserted + duplicates + quarantined == 0:
            quarantined = 1
        return {"inserted": inserted, "duplicates": duplicates, "quarantined": quarantined}
