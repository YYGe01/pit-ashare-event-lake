"""P2 bootstrap connectors for high-cost or high-complexity data categories.

Only low-cost public metadata/sample feeds are enabled here. Licensed full text,
Level-2, tick, and vendor event feeds stay as planned sources until entitlement
and storage budgets are explicit.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pitlake.connectors.p1_bootstrap import (
    _AkshareFrameConnector,
    _date_to_iso,
    _exchange_from_symbol,
    _first_present,
    _json_safe,
    _plain_symbol,
    _value_at,
)
from pitlake.quality.checks import QualityRunner
from pitlake.utils import CN_TZ, sha256_json


def _value_from_end(record: dict[str, Any], offset: int) -> Any:
    values = list(record.values())
    if offset >= len(values):
        return None
    return values[-1 - offset]


def _number(value: Any) -> float:
    value = _json_safe(value)
    if value in (None, ""):
        raise ValueError("missing numeric value")
    return float(value)


def _datetime_to_iso(value: Any) -> str:
    value = _json_safe(value)
    if isinstance(value, (int, float)) and value > 10_000_000_000:
        return (
            datetime.fromtimestamp(value / 1000, tz=timezone.utc)
            .astimezone(CN_TZ)
            .isoformat(timespec="seconds")
        )
    text = str(value).strip()
    if not text:
        raise ValueError("empty datetime")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if "T" in text:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=CN_TZ)
        return parsed.astimezone(CN_TZ).isoformat(timespec="seconds")
    if len(text) >= 19 and text[4] == "-" and text[7] == "-":
        parsed = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=CN_TZ)
        return parsed.isoformat(timespec="seconds")
    compact = text.replace("-", "").replace("/", "").replace(":", "").replace(" ", "")[:14]
    if len(compact) >= 14 and compact[:14].isdigit():
        parsed = datetime.strptime(compact[:14], "%Y%m%d%H%M%S").replace(tzinfo=CN_TZ)
        return parsed.isoformat(timespec="seconds")
    if len(compact) == 8 and compact.isdigit():
        parsed = datetime.strptime(compact, "%Y%m%d").replace(tzinfo=CN_TZ)
        return parsed.isoformat(timespec="seconds")
    raise ValueError(f"unsupported datetime: {value}")


def _akshare_minute_symbol(symbol: str) -> str:
    instrument = _plain_symbol(symbol)
    if str(symbol).lower().startswith(("sh", "sz", "bj")):
        prefix = str(symbol).lower()[:2]
    elif instrument.startswith("6"):
        prefix = "sh"
    elif instrument.startswith(("0", "3")):
        prefix = "sz"
    elif instrument.startswith(("4", "8", "9")):
        prefix = "bj"
    else:
        prefix = ""
    return f"{prefix}{instrument}" if prefix else instrument


def _configured_int(source_config: dict[str, Any], name: str, default: int) -> int:
    value = source_config.get("default_options", {}).get(name, default)
    return int(value or default)


class AkshareAshareMinuteBarConnector(_AkshareFrameConnector):
    """Collect selected A-share minute bars through AkShare for P2 bootstrap."""

    connector_version = "0.1.0"
    akshare_function = "stock_zh_a_minute"
    filename_prefix = "ashare_minute_bar"

    def _request_params(self, options: dict[str, Any]) -> list[dict[str, Any]]:
        default_options = self.source_config.get("default_options", {})
        raw_symbols = options.get("symbols") or default_options.get("symbols") or []
        symbols = raw_symbols.split(",") if isinstance(raw_symbols, str) else list(raw_symbols)
        limit = options.get("limit_symbols") or default_options.get("limit_symbols")
        symbols = [_akshare_minute_symbol(str(symbol)) for symbol in symbols if str(symbol).strip()]
        symbols = symbols[: int(limit)] if limit else symbols
        period = str(options.get("period") or default_options.get("period") or "1")
        adjust = str(options.get("adjust") or default_options.get("adjust") or "")
        return [{"symbol": symbol, "period": period, "adjust": adjust} for symbol in symbols]

    def _persist_records(
        self,
        *,
        records: list[dict[str, Any]],
        params: dict[str, Any],
        raw: Any,
        run_id: str,
        quality: QualityRunner,
    ) -> dict[str, int]:
        limit_rows = _configured_int(self.source_config, "limit_rows", 240)
        return super()._persist_records(
            records=records[-limit_rows:] if limit_rows else records,
            params=params,
            raw=raw,
            run_id=run_id,
            quality=quality,
        )

    def _normalize_record(
        self,
        *,
        record: dict[str, Any],
        params: dict[str, Any],
        raw: Any,
    ) -> dict[str, Any]:
        instrument = _plain_symbol(params["symbol"])
        snapshot_value = (
            _first_present(record, ("day", "时间", "datetime", "timestamp", "date"))
            or _value_at(record, 0)
        )
        snapshot_time = _datetime_to_iso(snapshot_value)
        trading_date = snapshot_time[:10]
        metric_payload = {key: value for key, value in record.items() if value not in (None, "")}
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": (
                f"{self.provider_id}:{instrument}:{params['period']}:{snapshot_time}"
            ),
            "instrument": instrument,
            "exchange": _exchange_from_symbol(instrument),
            "bar_period": f"{params['period']}m",
            "snapshot_time": snapshot_time,
            "trading_date": trading_date,
            "open": _number(_first_present(record, ("open", "开盘", "开盘价")) or _value_at(record, 1)),
            "high": _number(_first_present(record, ("high", "最高", "最高价")) or _value_at(record, 2)),
            "low": _number(_first_present(record, ("low", "最低", "最低价")) or _value_at(record, 3)),
            "close": _number(_first_present(record, ("close", "收盘", "收盘价")) or _value_at(record, 4)),
            "volume": _number(_first_present(record, ("volume", "成交量")) or _value_at(record, 5)),
            "amount": _number(_first_present(record, ("amount", "成交额")) or _value_at(record, 6)),
            "metric_payload": metric_payload,
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }


class AkshareStockResearchReportIndexConnector(_AkshareFrameConnector):
    """Collect research report metadata only; report PDF/body is not downloaded."""

    connector_version = "0.1.0"
    akshare_function = "stock_research_report_em"
    filename_prefix = "stock_research_report_index"

    def _request_params(self, options: dict[str, Any]) -> list[dict[str, Any]]:
        default_options = self.source_config.get("default_options", {})
        raw_symbols = options.get("symbols") or default_options.get("symbols") or []
        symbols = raw_symbols.split(",") if isinstance(raw_symbols, str) else list(raw_symbols)
        limit = options.get("limit_symbols") or default_options.get("limit_symbols")
        symbols = [_plain_symbol(symbol) for symbol in symbols if str(symbol).strip()]
        symbols = symbols[: int(limit)] if limit else symbols
        return [{"symbol": symbol} for symbol in symbols]

    def _persist_records(
        self,
        *,
        records: list[dict[str, Any]],
        params: dict[str, Any],
        raw: Any,
        run_id: str,
        quality: QualityRunner,
    ) -> dict[str, int]:
        limit_items = _configured_int(self.source_config, "limit_items", 50)
        return super()._persist_records(
            records=records[:limit_items] if limit_items else records,
            params=params,
            raw=raw,
            run_id=run_id,
            quality=quality,
        )

    def _normalize_record(
        self,
        *,
        record: dict[str, Any],
        params: dict[str, Any],
        raw: Any,
    ) -> dict[str, Any]:
        instrument = _plain_symbol(
            _first_present(record, ("股票代码", "证券代码", "代码", "symbol")) or params["symbol"]
        )
        title = str(
            _first_present(record, ("报告名称", "研报名称", "标题", "title"))
            or _value_at(record, 3)
            or ""
        ).strip()
        institution = str(
            _first_present(record, ("机构", "机构名称", "券商", "author")) or _value_at(record, 5) or "unknown"
        ).strip()
        rating = _first_present(record, ("东财评级", "评级", "rating")) or _value_at(record, 4)
        publish_value = (
            _first_present(record, ("日期", "发布时间", "报告日期", "publish_date"))
            or _value_from_end(record, 1)
            or raw.stored_at[:10]
        )
        publish_date = _date_to_iso(publish_value)
        source_url = str(
            _first_present(record, ("报告PDF链接", "PDF链接", "链接", "url", "source_url"))
            or _value_from_end(record, 0)
            or f"akshare://{self.akshare_function}"
        ).strip()
        report_key = sha256_json(
            {
                "instrument": instrument,
                "title": title,
                "source_url": source_url,
                "publish_date": publish_date,
            }
        )[7:23]
        metric_payload = {key: value for key, value in record.items() if value not in (None, "")}
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:research_report:{instrument}:{report_key}",
            "report_key": report_key,
            "instrument": instrument,
            "exchange": _exchange_from_symbol(instrument),
            "publish_date": publish_date,
            "title": title or report_key,
            "institution": institution or "unknown",
            "rating": _json_safe(rating),
            "source_url": source_url,
            "storage_permission": "metadata_only",
            "metric_payload": metric_payload,
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }


class AkshareStockCommentAggregateConnector(_AkshareFrameConnector):
    """Collect public stock comment aggregate metrics without storing post bodies."""

    connector_version = "0.1.0"
    akshare_function = "stock_comment_em"
    filename_prefix = "stock_comment_aggregate"

    def _request_params(self, options: dict[str, Any]) -> list[dict[str, Any]]:
        return [{}]

    def _persist_records(
        self,
        *,
        records: list[dict[str, Any]],
        params: dict[str, Any],
        raw: Any,
        run_id: str,
        quality: QualityRunner,
    ) -> dict[str, int]:
        limit_items = _configured_int(self.source_config, "limit_items", 200)
        return super()._persist_records(
            records=records[:limit_items] if limit_items else records,
            params=params,
            raw=raw,
            run_id=run_id,
            quality=quality,
        )

    def _normalize_record(
        self,
        *,
        record: dict[str, Any],
        params: dict[str, Any],
        raw: Any,
    ) -> dict[str, Any]:
        symbol_value = (
            _first_present(record, ("代码", "股票代码", "证券代码", "symbol"))
            or _value_at(record, 1)
        )
        instrument = _plain_symbol(symbol_value)
        snapshot_value = (
            _first_present(record, ("交易日", "日期", "date", "snapshot_date"))
            or _value_from_end(record, 0)
            or raw.stored_at[:10]
        )
        snapshot_date = _date_to_iso(snapshot_value)
        rank = _first_present(record, ("目前排名", "排名", "rank")) or _value_at(record, 11)
        metric_payload = {key: value for key, value in record.items() if value not in (None, "")}
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:stock_comment:{instrument}:{snapshot_date}",
            "sentiment_scope": "stock_comment_em_aggregate",
            "platform": "eastmoney_public_aggregate",
            "instrument": instrument,
            "exchange": _exchange_from_symbol(instrument),
            "snapshot_date": snapshot_date,
            "rank": _json_safe(rank),
            "metric_payload": metric_payload,
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }
