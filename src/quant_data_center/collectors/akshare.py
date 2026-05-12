"""AkShare collectors that write normalized qdc silver records."""

from __future__ import annotations

import json
import re
from hashlib import sha256
from datetime import date, datetime, timedelta
from typing import Any

from quant_data_center.settings import QdcSettings
from quant_data_center.storage.objects import QdcObjectStore
from quant_data_center.storage.silver import SilverStore
from quant_data_center.utils.instruments import (
    instrument_exchange,
    instrument_to_akshare_daily_symbol,
    instrument_to_symbol,
    normalize_instrument,
)


class AkshareSilverCollector:
    """Collect a small core A-share dataset set from AkShare into silver tables."""

    def __init__(self, settings: QdcSettings) -> None:
        self.settings = settings
        self.silver = SilverStore(settings)
        self.objects = QdcObjectStore(settings)

    def collect_stock_basic(self, *, source_id: str) -> int:
        akshare = __import__("akshare")
        df = akshare.stock_info_a_code_name()
        provider_records = _records(df)
        self._write_source_objects(
            dataset="stock_basic",
            source_id=source_id,
            partition_value="snapshot",
            stem="stock_info_a_code_name",
            raw_payload={
                "function": "stock_info_a_code_name",
                "records": provider_records,
            },
            bronze_records=provider_records,
        )
        records = []
        for row in provider_records:
            raw_code = _first_present(row, ("code", "证券代码", "股票代码", "代码"))
            if raw_code is None:
                continue
            instrument = normalize_instrument(str(raw_code))
            records.append(
                {
                    "instrument": instrument,
                    "symbol": instrument_to_symbol(instrument),
                    "exchange": instrument_exchange(instrument),
                    "name": _first_present(row, ("name", "证券简称", "股票简称", "名称")),
                    "is_active": True,
                    "source_id": source_id,
                }
            )
        return self.silver.upsert_stock_basic(records)

    def collect_universe_constituents(
        self,
        *,
        source_id: str,
        universe: str,
        index_symbol: str,
        snapshot_date: str | None = None,
    ) -> int:
        akshare = __import__("akshare")
        snapshot = _date_to_iso(snapshot_date or date.today().isoformat())
        cons_df = akshare.index_stock_cons_csindex(symbol=index_symbol)
        cons_records = _records(cons_df)
        weight_records = _safe_index_weight_records(akshare=akshare, index_symbol=index_symbol)
        weight_by_symbol = {
            _digits_only(_first_present(row, ("成分券代码", "样本代码", "证券代码", "股票代码", "代码", "code"))): _as_float(
                _first_present(row, ("权重", "权重(%)", "weight", "WEIGHT"))
            )
            for row in weight_records
            if _first_present(row, ("成分券代码", "样本代码", "证券代码", "股票代码", "代码", "code")) is not None
        }
        self._write_source_objects(
            dataset="universe_constituent",
            source_id=source_id,
            partition_value=snapshot,
            stem=f"{universe}_{index_symbol}_{snapshot}",
            raw_payload={
                "function": "index_stock_cons_csindex",
                "weight_function": "index_stock_cons_weight_csindex",
                "params": {
                    "universe": universe,
                    "index_symbol": index_symbol,
                    "snapshot_date": snapshot,
                },
                "constituents": cons_records,
                "weights": weight_records,
            },
            bronze_records=_combine_universe_bronze(
                constituents=cons_records,
                weights=weight_records,
            ),
        )
        records = []
        for row in cons_records:
            raw_code = _first_present(row, ("成分券代码", "样本代码", "证券代码", "股票代码", "代码", "code"))
            if raw_code is None:
                continue
            code = _digits_only(raw_code)
            instrument = normalize_instrument(code)
            records.append(
                {
                    "universe": universe,
                    "snapshot_date": snapshot,
                    "instrument": instrument,
                    "symbol": instrument_to_symbol(instrument),
                    "exchange": instrument_exchange(instrument),
                    "name": _first_present(row, ("成分券名称", "证券简称", "股票简称", "名称", "name")),
                    "weight": weight_by_symbol.get(code),
                    "source_id": source_id,
                }
            )
        return self.silver.upsert_universe_constituents(records)

    def collect_trade_calendar(
        self,
        *,
        source_id: str,
        start_date: str,
        end_date: str,
        calendar_id: str = "cn_ashare",
    ) -> int:
        akshare = __import__("akshare")
        df = akshare.tool_trade_date_hist_sina()
        provider_records = _records(df)
        start_iso = _date_to_iso(start_date)
        end_iso = _date_to_iso(end_date)
        self._write_source_objects(
            dataset="trade_calendar",
            source_id=source_id,
            partition_value=start_iso,
            stem=f"tool_trade_date_hist_sina_{start_iso}_{end_iso}",
            raw_payload={
                "function": "tool_trade_date_hist_sina",
                "params": {
                    "calendar_id": calendar_id,
                    "start_date": start_iso,
                    "end_date": end_iso,
                },
                "records": provider_records,
            },
            bronze_records=provider_records,
        )
        dates = sorted(
            {
                _date_to_iso(_first_present(row, ("trade_date", "日期", "calendarDate")))
                for row in provider_records
                if _first_present(row, ("trade_date", "日期", "calendarDate")) is not None
            }
        )
        records = []
        for index, trade_date in enumerate(dates):
            if trade_date < start_iso or trade_date > end_iso:
                continue
            records.append(
                {
                    "calendar_id": calendar_id,
                    "trade_date": trade_date,
                    "is_open": True,
                    "pre_trade_date": dates[index - 1] if index > 0 else None,
                    "next_trade_date": dates[index + 1] if index + 1 < len(dates) else None,
                    "source_id": source_id,
                }
            )
        return self.silver.upsert_trade_calendar(records)

    def collect_daily_bar(
        self,
        *,
        source_id: str,
        start_date: str,
        end_date: str,
        instruments: list[str],
        adjust: str = "",
    ) -> int:
        if not instruments:
            raise ValueError("daily_bar backfill requires symbol batches")
        akshare = __import__("akshare")
        start_iso = _date_to_iso(start_date)
        start_compact = _date_to_compact(start_date)
        end_compact = _date_to_compact(end_date)
        records = []
        for raw_instrument in instruments:
            instrument = normalize_instrument(raw_instrument)
            df = akshare.stock_zh_a_daily(
                symbol=instrument_to_akshare_daily_symbol(instrument),
                start_date=start_compact,
                end_date=end_compact,
                adjust=adjust,
            )
            provider_records = _records(df)
            self._write_source_objects(
                dataset="daily_bar",
                source_id=source_id,
                partition_value=start_iso,
                stem=f"{instrument}_{start_compact}_{end_compact}",
                raw_payload={
                    "function": "stock_zh_a_daily",
                    "params": {
                        "instrument": instrument,
                        "start_date": start_compact,
                        "end_date": end_compact,
                        "adjust": adjust,
                    },
                    "records": provider_records,
                },
                bronze_records=provider_records,
            )
            for row in provider_records:
                trade_date = _date_to_iso(_first_present(row, ("date", "日期", "trade_date")))
                open_price = _as_float(_first_present(row, ("open", "开盘")))
                close = _as_float(_first_present(row, ("close", "收盘")))
                volume = _as_float(_first_present(row, ("volume", "成交量")))
                amount = _as_float(_first_present(row, ("amount", "成交额")))
                records.append(
                    {
                        "trade_date": trade_date,
                        "instrument": instrument,
                        "open": open_price,
                        "high": _as_float(_first_present(row, ("high", "最高"))),
                        "low": _as_float(_first_present(row, ("low", "最低"))),
                        "close": close,
                        "pre_close": _as_float(_first_present(row, ("pre_close", "昨收"))),
                        "volume": volume,
                        "amount": amount,
                        "vwap": _safe_vwap(amount=amount, volume=volume),
                        "source_id": source_id,
                    }
                )
        return self.silver.upsert_daily_bar(records)

    def collect_adj_factor(
        self,
        *,
        source_id: str,
        start_date: str,
        end_date: str,
        instruments: list[str],
    ) -> int:
        if not instruments:
            raise ValueError("adj_factor backfill requires symbol batches")
        akshare = __import__("akshare")
        start_iso = _date_to_iso(start_date)
        end_iso = _date_to_iso(end_date)
        start_compact = start_iso.replace("-", "")
        end_compact = end_iso.replace("-", "")
        records = []
        for raw_instrument in instruments:
            instrument = normalize_instrument(raw_instrument)
            raw_df = akshare.stock_zh_a_daily(
                symbol=instrument_to_akshare_daily_symbol(instrument),
                start_date=start_compact,
                end_date=end_compact,
                adjust="",
            )
            qfq_df = akshare.stock_zh_a_daily(
                symbol=instrument_to_akshare_daily_symbol(instrument),
                start_date=start_compact,
                end_date=end_compact,
                adjust="qfq",
            )
            raw_records = _records(raw_df)
            qfq_records = _records(qfq_df)
            self._write_source_objects(
                dataset="adj_factor",
                source_id=source_id,
                partition_value=start_iso,
                stem=f"{instrument}_{start_compact}_{end_compact}",
                raw_payload={
                    "function": "stock_zh_a_daily",
                    "derivation": "qfq_close_div_unadjusted_close",
                    "params": {
                        "instrument": instrument,
                        "start_date": start_compact,
                        "end_date": end_compact,
                        "adjust_pair": ["", "qfq"],
                    },
                    "raw_records": raw_records,
                    "qfq_records": qfq_records,
                },
                bronze_records=_combine_adjustment_bronze(raw_records=raw_records, qfq_records=qfq_records),
            )
            qfq_by_date = {
                _date_to_iso(_first_present(row, ("date", "日期", "trade_date"))): row
                for row in qfq_records
                if _first_present(row, ("date", "日期", "trade_date")) is not None
            }
            for raw_row in raw_records:
                trade_date = _date_to_iso(_first_present(raw_row, ("date", "日期", "trade_date")))
                if trade_date < start_iso or trade_date > end_iso:
                    continue
                qfq_row = qfq_by_date.get(trade_date)
                if qfq_row is None:
                    continue
                raw_close = _as_float(_first_present(raw_row, ("close", "收盘")))
                qfq_close = _as_float(_first_present(qfq_row, ("close", "收盘")))
                if raw_close in (None, 0) or qfq_close is None:
                    continue
                records.append(
                    {
                        "trade_date": trade_date,
                        "instrument": instrument,
                        "adj_factor": round(qfq_close / raw_close, 10),
                        "factor_type": "qfq_close_ratio_v0_inferred",
                        "source_id": source_id,
                    }
                )
        return self.silver.upsert_adj_factor(records)

    def collect_price_limit(
        self,
        *,
        source_id: str,
        start_date: str,
        end_date: str,
        instruments: list[str],
        adjust: str = "",
        lookback_days: int = 14,
    ) -> int:
        if not instruments:
            raise ValueError("price_limit backfill requires symbol batches")
        akshare = __import__("akshare")
        start_iso = _date_to_iso(start_date)
        end_iso = _date_to_iso(end_date)
        query_start = _date_to_compact(
            datetime.strptime(start_iso, "%Y-%m-%d").date() - timedelta(days=lookback_days)
        )
        end_compact = end_iso.replace("-", "")
        records = []
        for raw_instrument in instruments:
            instrument = normalize_instrument(raw_instrument)
            df = akshare.stock_zh_a_daily(
                symbol=instrument_to_akshare_daily_symbol(instrument),
                start_date=query_start,
                end_date=end_compact,
                adjust=adjust,
            )
            provider_records = _records(df)
            self._write_source_objects(
                dataset="price_limit",
                source_id=source_id,
                partition_value=start_iso,
                stem=f"{instrument}_{query_start}_{end_compact}",
                raw_payload={
                    "function": "stock_zh_a_daily",
                    "derivation": "prev_close_rule_based_limit",
                    "params": {
                        "instrument": instrument,
                        "start_date": query_start,
                        "end_date": end_compact,
                        "adjust": adjust,
                    },
                    "records": provider_records,
                },
                bronze_records=provider_records,
            )
            rows = sorted(
                provider_records,
                key=lambda row: _date_to_iso(_first_present(row, ("date", "日期", "trade_date"))),
            )
            rate, limit_rule = _limit_rate(instrument)
            for index, row in enumerate(rows):
                trade_date = _date_to_iso(_first_present(row, ("date", "日期", "trade_date")))
                if trade_date < start_iso or trade_date > end_iso or index == 0:
                    continue
                prev_close = _as_float(_first_present(rows[index - 1], ("close", "收盘")))
                if prev_close is None:
                    continue
                records.append(
                    {
                        "trade_date": trade_date,
                        "instrument": instrument,
                        "limit_up": round(prev_close * (1 + rate), 2),
                        "limit_down": round(prev_close * (1 - rate), 2),
                        "prev_close": prev_close,
                        "limit_rule": limit_rule,
                        "source_id": source_id,
                    }
                )
        return self.silver.upsert_price_limit(records)

    def collect_trade_status(
        self,
        *,
        source_id: str,
        start_date: str,
        end_date: str,
    ) -> int:
        akshare = __import__("akshare")
        records = []
        for query_date in _date_range(start_date=start_date, end_date=end_date):
            df = akshare.stock_tfp_em(date=query_date.replace("-", ""))
            provider_records = _records(df)
            self._write_source_objects(
                dataset="trade_status",
                source_id=source_id,
                partition_value=query_date,
                stem=f"stock_tfp_em_{query_date}",
                raw_payload={
                    "function": "stock_tfp_em",
                    "params": {"date": query_date.replace("-", "")},
                    "records": provider_records,
                },
                bronze_records=provider_records,
            )
            for row in provider_records:
                raw_symbol = _get_value(row, ("代码", "SECURITY_CODE", "symbol", "code"), 1)
                if raw_symbol is None:
                    continue
                try:
                    instrument = normalize_instrument(str(raw_symbol))
                except ValueError:
                    continue
                records.append(
                    {
                        "trade_date": query_date,
                        "instrument": instrument,
                        "trade_status": "halted",
                        "halt_reason": _json_safe(
                            _get_value(row, ("停牌原因", "SUSPEND_REASON", "halt_reason"), 6)
                        ),
                        "source_update_time": _optional_date_to_iso(
                            _get_value(
                                row,
                                ("预计复牌时间", "PREDICT_RESUME_DATE", "source_update_time"),
                                8,
                            )
                        ),
                        "source_id": source_id,
                    }
                )
        return self.silver.upsert_trade_status(records)

    def collect_announcements(
        self,
        *,
        source_id: str,
        start_date: str,
        end_date: str,
        instruments: list[str] | None = None,
    ) -> int:
        akshare = __import__("akshare")
        instrument_filter = {normalize_instrument(item) for item in instruments or []}
        records = []
        for query_date in _date_range(start_date=start_date, end_date=end_date):
            df = akshare.stock_notice_report(symbol="全部", date=query_date.replace("-", ""))
            provider_records = _records(df)
            self._write_source_objects(
                dataset="announcement",
                source_id=source_id,
                partition_value=query_date,
                stem=f"stock_notice_report_{query_date}",
                raw_payload={
                    "function": "stock_notice_report",
                    "params": {"symbol": "全部", "date": query_date.replace("-", "")},
                    "records": provider_records,
                },
                bronze_records=provider_records,
            )
            for row in provider_records:
                raw_code = _first_present(row, ("代码", "股票代码", "证券代码", "code", "symbol"))
                if raw_code is None:
                    continue
                try:
                    instrument = normalize_instrument(str(raw_code))
                except ValueError:
                    continue
                if instrument_filter and instrument not in instrument_filter:
                    continue
                publish_date = _optional_date_to_iso(
                    _first_present(row, ("公告日期", "日期", "publish_date", "date"))
                )
                if publish_date is None or publish_date < _date_to_iso(start_date) or publish_date > _date_to_iso(end_date):
                    continue
                title = _first_present(row, ("公告标题", "标题", "title"))
                if title is None:
                    continue
                url = _first_present(row, ("公告链接", "链接", "url"))
                records.append(
                    {
                        "announcement_id": _stable_id(
                            "announcement", source_id, instrument, publish_date, title, url
                        ),
                        "publish_date": publish_date,
                        "instrument": instrument,
                        "title": str(title),
                        "url": _json_safe(url),
                        "source_id": source_id,
                    }
                )
        return self.silver.upsert_announcements(records)

    def collect_news(
        self,
        *,
        source_id: str,
        start_date: str,
        end_date: str,
        instruments: list[str],
    ) -> int:
        if not instruments:
            raise ValueError("news backfill requires symbol batches")
        akshare = __import__("akshare")
        records = []
        start_iso = _date_to_iso(start_date)
        end_iso = _date_to_iso(end_date)
        for raw_instrument in instruments:
            instrument = normalize_instrument(raw_instrument)
            symbol = instrument_to_symbol(instrument)
            try:
                df = akshare.stock_news_em(symbol=symbol)
            except Exception as exc:
                self._write_source_objects(
                    dataset="news",
                    source_id=source_id,
                    partition_value=start_iso,
                    stem=f"stock_news_em_{instrument}_{start_iso}_{end_iso}_error",
                    raw_payload={
                        "function": "stock_news_em",
                        "params": {
                            "symbol": symbol,
                            "start_date": start_iso,
                            "end_date": end_iso,
                        },
                        "error": str(exc),
                    },
                    bronze_records=[],
                )
                continue
            provider_records = _records(df)
            self._write_source_objects(
                dataset="news",
                source_id=source_id,
                partition_value=start_iso,
                stem=f"stock_news_em_{instrument}_{start_iso}_{end_iso}",
                raw_payload={
                    "function": "stock_news_em",
                    "params": {"symbol": symbol, "start_date": start_iso, "end_date": end_iso},
                    "records": provider_records,
                },
                bronze_records=provider_records,
            )
            for row in provider_records:
                raw_publish_time = _first_present(
                    row, ("发布时间", "日期", "publish_date", "date", "time")
                )
                publish_time = _optional_datetime_to_iso(raw_publish_time)
                publish_date = publish_time[:10] if publish_time else None
                if publish_date is None or publish_date < start_iso or publish_date > end_iso:
                    continue
                title = _first_present(row, ("新闻标题", "标题", "title"))
                if title is None:
                    continue
                url = _first_present(row, ("新闻链接", "链接", "url"))
                records.append(
                    {
                        "news_id": _stable_id("news", source_id, instrument, publish_date, title, url),
                        "publish_date": publish_date,
                        "publish_time": publish_time,
                        "instrument": instrument,
                        "title": str(title),
                        "url": _json_safe(url),
                        "source_id": source_id,
                        "source_record_id": _json_safe(url) or str(title),
                    }
                )
        if not records:
            records.extend(
                self._collect_global_news_fallback(
                    akshare=akshare,
                    source_id=source_id,
                    start_iso=start_iso,
                    end_iso=end_iso,
                    instruments=[normalize_instrument(item) for item in instruments],
                )
            )
        return self.silver.upsert_news(records)

    def _collect_global_news_fallback(
        self,
        *,
        akshare: Any,
        source_id: str,
        start_iso: str,
        end_iso: str,
        instruments: list[str],
    ) -> list[dict[str, Any]]:
        aliases_by_instrument = _news_aliases_by_instrument(
            akshare=akshare,
            instruments=instruments,
        )
        if not aliases_by_instrument:
            return []
        records = []
        for function_name in (
            "stock_info_global_cls",
            "stock_info_global_em",
            "stock_info_global_ths",
            "stock_info_global_sina",
            "stock_info_global_futu",
        ):
            function = getattr(akshare, function_name, None)
            if function is None:
                continue
            try:
                provider_records = _records(function())
            except Exception as exc:
                self._write_source_objects(
                    dataset="news",
                    source_id=source_id,
                    partition_value=start_iso,
                    stem=f"{function_name}_{start_iso}_{end_iso}_error",
                    raw_payload={
                        "function": function_name,
                        "params": {"start_date": start_iso, "end_date": end_iso},
                        "error": str(exc),
                    },
                    bronze_records=[],
                )
                continue
            self._write_source_objects(
                dataset="news",
                source_id=source_id,
                partition_value=start_iso,
                stem=f"{function_name}_{start_iso}_{end_iso}",
                raw_payload={
                    "function": function_name,
                    "params": {"start_date": start_iso, "end_date": end_iso},
                    "records": provider_records,
                },
                bronze_records=provider_records,
            )
            for row in provider_records:
                publish_time = _global_news_publish_time(row)
                publish_date = publish_time[:10] if publish_time else None
                if publish_date is None or publish_date < start_iso or publish_date > end_iso:
                    continue
                title = _global_news_title(row)
                if title is None:
                    continue
                url = _first_present(row, ("新闻链接", "链接", "url", "URL"))
                text = _global_news_text(row)
                for instrument in _matched_news_instruments(
                    text=text,
                    aliases_by_instrument=aliases_by_instrument,
                ):
                    records.append(
                        {
                            "news_id": _stable_id(
                                "news",
                                source_id,
                                function_name,
                                instrument,
                                publish_date,
                                title,
                                url,
                            ),
                            "publish_date": publish_date,
                            "publish_time": publish_time,
                            "instrument": instrument,
                            "title": title,
                            "url": _json_safe(url),
                            "source_id": source_id,
                            "source_record_id": _json_safe(url) or title,
                        }
                    )
        return list({str(record["news_id"]): record for record in records}.values())

    def _write_source_objects(
        self,
        *,
        dataset: str,
        source_id: str,
        partition_value: str,
        stem: str,
        raw_payload: dict[str, Any],
        bronze_records: list[dict[str, Any]],
    ) -> None:
        self.objects.put_json(
            dataset=dataset,
            source_id=source_id,
            partition_value=partition_value,
            stem=stem,
            payload=raw_payload,
        )
        self.objects.put_bronze_parquet(
            dataset=dataset,
            source_id=source_id,
            partition_value=partition_value,
            stem=stem,
            records=bronze_records,
        )


class EastmoneySilverCollector(AkshareSilverCollector):
    """Collect core A-share daily datasets from Eastmoney endpoints through AkShare."""

    def collect_daily_bar(
        self,
        *,
        source_id: str,
        start_date: str,
        end_date: str,
        instruments: list[str],
        adjust: str = "",
        lookback_days: int = 14,
    ) -> int:
        if not instruments:
            raise ValueError("daily_bar backfill requires symbol batches")
        akshare = __import__("akshare")
        start_iso = _date_to_iso(start_date)
        end_iso = _date_to_iso(end_date)
        query_start = _date_to_compact(
            datetime.strptime(start_iso, "%Y-%m-%d").date() - timedelta(days=lookback_days)
        )
        end_compact = end_iso.replace("-", "")
        records = []
        for raw_instrument in instruments:
            instrument = normalize_instrument(raw_instrument)
            provider_records = _eastmoney_hist_records(
                akshare=akshare,
                instrument=instrument,
                start_date=query_start,
                end_date=end_compact,
                adjust=adjust,
            )
            self._write_source_objects(
                dataset="daily_bar",
                source_id=source_id,
                partition_value=start_iso,
                stem=f"{instrument}_{query_start}_{end_compact}",
                raw_payload={
                    "function": "stock_zh_a_hist",
                    "params": {
                        "instrument": instrument,
                        "start_date": query_start,
                        "end_date": end_compact,
                        "adjust": adjust,
                    },
                    "records": provider_records,
                },
                bronze_records=provider_records,
            )
            rows = sorted(provider_records, key=_provider_trade_date)
            for index, row in enumerate(rows):
                trade_date = _provider_trade_date(row)
                if trade_date < start_iso or trade_date > end_iso:
                    continue
                open_price = _as_float(_first_present(row, ("open", "开盘")))
                close = _as_float(_first_present(row, ("close", "收盘")))
                volume = _as_float(_first_present(row, ("volume", "成交量")))
                amount = _as_float(_first_present(row, ("amount", "成交额")))
                prev_close = (
                    _as_float(_first_present(rows[index - 1], ("close", "收盘")))
                    if index > 0
                    else None
                )
                records.append(
                    {
                        "trade_date": trade_date,
                        "instrument": instrument,
                        "open": open_price,
                        "high": _as_float(_first_present(row, ("high", "最高"))),
                        "low": _as_float(_first_present(row, ("low", "最低"))),
                        "close": close,
                        "pre_close": prev_close,
                        "volume": volume,
                        "amount": amount,
                        "vwap": _safe_vwap(amount=amount, volume=volume),
                        "source_id": source_id,
                    }
                )
        return self.silver.upsert_daily_bar(records)

    def collect_adj_factor(
        self,
        *,
        source_id: str,
        start_date: str,
        end_date: str,
        instruments: list[str],
    ) -> int:
        if not instruments:
            raise ValueError("adj_factor backfill requires symbol batches")
        akshare = __import__("akshare")
        start_iso = _date_to_iso(start_date)
        end_iso = _date_to_iso(end_date)
        start_compact = start_iso.replace("-", "")
        end_compact = end_iso.replace("-", "")
        records = []
        for raw_instrument in instruments:
            instrument = normalize_instrument(raw_instrument)
            raw_records = _eastmoney_hist_records(
                akshare=akshare,
                instrument=instrument,
                start_date=start_compact,
                end_date=end_compact,
                adjust="",
            )
            qfq_records = _eastmoney_hist_records(
                akshare=akshare,
                instrument=instrument,
                start_date=start_compact,
                end_date=end_compact,
                adjust="qfq",
            )
            self._write_source_objects(
                dataset="adj_factor",
                source_id=source_id,
                partition_value=start_iso,
                stem=f"{instrument}_{start_compact}_{end_compact}",
                raw_payload={
                    "function": "stock_zh_a_hist",
                    "derivation": "qfq_close_div_unadjusted_close",
                    "params": {
                        "instrument": instrument,
                        "start_date": start_compact,
                        "end_date": end_compact,
                        "adjust_pair": ["", "qfq"],
                    },
                    "raw_records": raw_records,
                    "qfq_records": qfq_records,
                },
                bronze_records=_combine_adjustment_bronze(
                    raw_records=raw_records,
                    qfq_records=qfq_records,
                ),
            )
            qfq_by_date = {_provider_trade_date(row): row for row in qfq_records}
            for raw_row in raw_records:
                trade_date = _provider_trade_date(raw_row)
                if trade_date < start_iso or trade_date > end_iso:
                    continue
                qfq_row = qfq_by_date.get(trade_date)
                if qfq_row is None:
                    continue
                raw_close = _as_float(_first_present(raw_row, ("close", "收盘")))
                qfq_close = _as_float(_first_present(qfq_row, ("close", "收盘")))
                if raw_close in (None, 0) or qfq_close is None:
                    continue
                records.append(
                    {
                        "trade_date": trade_date,
                        "instrument": instrument,
                        "adj_factor": round(qfq_close / raw_close, 10),
                        "factor_type": "eastmoney_qfq_close_ratio_v0_inferred",
                        "source_id": source_id,
                    }
                )
        return self.silver.upsert_adj_factor(records)

    def collect_price_limit(
        self,
        *,
        source_id: str,
        start_date: str,
        end_date: str,
        instruments: list[str],
        adjust: str = "",
        lookback_days: int = 14,
    ) -> int:
        if not instruments:
            raise ValueError("price_limit backfill requires symbol batches")
        akshare = __import__("akshare")
        start_iso = _date_to_iso(start_date)
        end_iso = _date_to_iso(end_date)
        query_start = _date_to_compact(
            datetime.strptime(start_iso, "%Y-%m-%d").date() - timedelta(days=lookback_days)
        )
        end_compact = end_iso.replace("-", "")
        records = []
        for raw_instrument in instruments:
            instrument = normalize_instrument(raw_instrument)
            provider_records = _eastmoney_hist_records(
                akshare=akshare,
                instrument=instrument,
                start_date=query_start,
                end_date=end_compact,
                adjust=adjust,
            )
            self._write_source_objects(
                dataset="price_limit",
                source_id=source_id,
                partition_value=start_iso,
                stem=f"{instrument}_{query_start}_{end_compact}",
                raw_payload={
                    "function": "stock_zh_a_hist",
                    "derivation": "prev_close_rule_based_limit",
                    "params": {
                        "instrument": instrument,
                        "start_date": query_start,
                        "end_date": end_compact,
                        "adjust": adjust,
                    },
                    "records": provider_records,
                },
                bronze_records=provider_records,
            )
            rows = sorted(provider_records, key=_provider_trade_date)
            rate, limit_rule = _limit_rate(instrument)
            for index, row in enumerate(rows):
                trade_date = _provider_trade_date(row)
                if trade_date < start_iso or trade_date > end_iso or index == 0:
                    continue
                prev_close = _as_float(_first_present(rows[index - 1], ("close", "收盘")))
                if prev_close is None:
                    continue
                records.append(
                    {
                        "trade_date": trade_date,
                        "instrument": instrument,
                        "limit_up": round(prev_close * (1 + rate), 2),
                        "limit_down": round(prev_close * (1 - rate), 2),
                        "prev_close": prev_close,
                        "limit_rule": limit_rule,
                        "source_id": source_id,
                    }
                )
        return self.silver.upsert_price_limit(records)


def _records(df: Any) -> list[dict[str, Any]]:
    return [{str(key): _json_safe(value) for key, value in row.items()} for row in df.to_dict("records")]


def _eastmoney_hist_records(
    *,
    akshare: Any,
    instrument: str,
    start_date: str,
    end_date: str,
    adjust: str,
) -> list[dict[str, Any]]:
    df = akshare.stock_zh_a_hist(
        symbol=instrument_to_symbol(instrument),
        period="daily",
        start_date=start_date,
        end_date=end_date,
        adjust=adjust,
    )
    return _records(df)


def _provider_trade_date(row: dict[str, Any]) -> str:
    return _date_to_iso(_first_present(row, ("date", "日期", "trade_date")))


def _first_present(record: dict[str, Any], names: tuple[str, ...]) -> Any:
    for name in names:
        if name in record and record[name] not in (None, ""):
            return record[name]
    return None


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
    text = str(_json_safe(value)).strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return datetime.strptime(text[:10], "%Y-%m-%d").date().isoformat()
    compact = text.replace("-", "").replace("/", "")[:8]
    return datetime.strptime(compact, "%Y%m%d").date().isoformat()


def _date_to_compact(value: Any) -> str:
    return _date_to_iso(value).replace("-", "")


def _as_float(value: Any) -> float | None:
    value = _json_safe(value)
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_vwap(*, amount: float | None, volume: float | None) -> float | None:
    if amount is None or volume in (None, 0):
        return None
    return amount / volume


def _combine_adjustment_bronze(
    *,
    raw_records: list[dict[str, Any]],
    qfq_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    qfq_by_date = {
        _date_to_iso(_first_present(row, ("date", "日期", "trade_date"))): row
        for row in qfq_records
        if _first_present(row, ("date", "日期", "trade_date")) is not None
    }
    records = []
    for raw_row in raw_records:
        raw_date = _first_present(raw_row, ("date", "日期", "trade_date"))
        if raw_date is None:
            continue
        trade_date = _date_to_iso(raw_date)
        qfq_row = qfq_by_date.get(trade_date, {})
        records.append(
            {
                "date": trade_date,
                "raw_close": _as_float(_first_present(raw_row, ("close", "收盘"))),
                "qfq_close": _as_float(_first_present(qfq_row, ("close", "收盘"))),
                "raw_payload_json": json.dumps(raw_row, ensure_ascii=False, sort_keys=True),
                "qfq_payload_json": json.dumps(qfq_row, ensure_ascii=False, sort_keys=True),
            }
        )
    return records


def _combine_universe_bronze(
    *,
    constituents: list[dict[str, Any]],
    weights: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    weight_by_symbol = {
        _digits_only(_first_present(row, ("成分券代码", "样本代码", "证券代码", "股票代码", "代码", "code"))): _as_float(
            _first_present(row, ("权重", "权重(%)", "weight", "WEIGHT"))
        )
        for row in weights
        if _first_present(row, ("成分券代码", "样本代码", "证券代码", "股票代码", "代码", "code")) is not None
    }
    records = []
    for row in constituents:
        raw_code = _first_present(row, ("成分券代码", "样本代码", "证券代码", "股票代码", "代码", "code"))
        if raw_code is None:
            continue
        code = _digits_only(raw_code)
        records.append(
            {
                "symbol": code,
                "name": _first_present(row, ("成分券名称", "证券简称", "股票简称", "名称", "name")),
                "weight": weight_by_symbol.get(code),
                "raw_payload_json": json.dumps(row, ensure_ascii=False, sort_keys=True),
            }
        )
    return records


def _date_range(*, start_date: str, end_date: str) -> list[str]:
    start = datetime.strptime(_date_to_iso(start_date), "%Y-%m-%d").date()
    end = datetime.strptime(_date_to_iso(end_date), "%Y-%m-%d").date()
    if end < start:
        raise ValueError("end_date must be greater than or equal to start_date")
    days = []
    current = start
    while current <= end:
        days.append(current.isoformat())
        current += timedelta(days=1)
    return days


def _optional_date_to_iso(value: Any) -> str | None:
    value = _json_safe(value)
    if value in (None, "", "NaT"):
        return None
    try:
        return _date_to_iso(value)
    except ValueError:
        return None


def _optional_datetime_to_iso(value: Any) -> str | None:
    value = _json_safe(value)
    if value in (None, "", "NaT"):
        return None
    text = str(value).strip().replace("T", " ")
    try:
        if len(text) >= 19 and text[4] == "-" and text[7] == "-":
            return datetime.fromisoformat(text[:19]).replace(microsecond=0).isoformat(sep=" ")
        if len(text) == 10 and text[4] == "-" and text[7] == "-":
            return f"{_date_to_iso(text)} 00:00:00"
        if re.fullmatch(r"\d{8}", text):
            return f"{_date_to_iso(text)} 00:00:00"
        if re.fullmatch(r"\d{10,13}", text):
            timestamp = float(text)
            if timestamp > 10_000_000_000:
                timestamp = timestamp / 1000
            return datetime.fromtimestamp(timestamp).replace(microsecond=0).isoformat(sep=" ")
        return f"{_date_to_iso(text)} 00:00:00"
    except (TypeError, ValueError):
        return None


def _limit_rate(instrument: str) -> tuple[float, str]:
    symbol = instrument_to_symbol(instrument)
    if symbol.startswith(("4", "8", "9")):
        return 0.30, "bse_normal_30pct_v0_inferred"
    if symbol.startswith(("300", "301", "688", "689")):
        return 0.20, "registration_board_normal_20pct_v0_inferred"
    return 0.10, "main_board_normal_10pct_v0_inferred"


def _get_value(record: dict[str, Any], aliases: tuple[str, ...], index: int) -> Any:
    for alias in aliases:
        if alias in record:
            return record[alias]
    values = list(record.values())
    if index < len(values):
        return values[index]
    return None


def _safe_index_weight_records(*, akshare: Any, index_symbol: str) -> list[dict[str, Any]]:
    if not hasattr(akshare, "index_stock_cons_weight_csindex"):
        return []
    try:
        return _records(akshare.index_stock_cons_weight_csindex(symbol=index_symbol))
    except Exception:
        return []


def _news_aliases_by_instrument(
    *,
    akshare: Any,
    instruments: list[str],
) -> dict[str, set[str]]:
    names_by_symbol = _safe_stock_names_by_symbol(akshare=akshare)
    aliases = {}
    for raw_instrument in instruments:
        instrument = normalize_instrument(raw_instrument)
        symbol = instrument_to_symbol(instrument)
        values = {
            instrument,
            symbol,
            f"{symbol}.SH" if instrument.startswith("SH") else f"{symbol}.SZ",
            f"{symbol}.SS" if instrument.startswith("SH") else f"{symbol}.SZ",
        }
        name = names_by_symbol.get(symbol)
        if name:
            values.add(name)
        aliases[instrument] = {value for value in values if len(value) >= 2}
    return aliases


def _safe_stock_names_by_symbol(*, akshare: Any) -> dict[str, str]:
    function = getattr(akshare, "stock_info_a_code_name", None)
    if function is None:
        return {}
    try:
        records = _records(function())
    except Exception:
        return {}
    names = {}
    for row in records:
        raw_code = _first_present(row, ("code", "证券代码", "股票代码", "代码"))
        name = _first_present(row, ("name", "证券简称", "股票简称", "名称"))
        if raw_code is None or not name:
            continue
        try:
            names[_digits_only(raw_code)] = str(name).strip()
        except ValueError:
            continue
    return names


def _global_news_publish_time(row: dict[str, Any]) -> str | None:
    date_value = _first_present(row, ("发布日期", "日期", "date", "publish_date"))
    time_value = _first_present(row, ("发布时间", "时间", "datetime", "time"))
    if date_value is not None and time_value is not None:
        date_text = _optional_date_to_iso(date_value)
        time_text = str(_json_safe(time_value)).strip()
        if date_text and re.fullmatch(r"\d{1,2}:\d{2}(:\d{2})?", time_text):
            if len(time_text.split(":")) == 2:
                time_text = f"{time_text}:00"
            return f"{date_text} {time_text}"
    if date_value is not None:
        return _optional_datetime_to_iso(date_value)
    if time_value is not None:
        return _optional_datetime_to_iso(time_value)
    return None


def _global_news_title(row: dict[str, Any]) -> str | None:
    title = _first_present(row, ("新闻标题", "标题", "title"))
    if title:
        return str(title).strip()
    content = _first_present(row, ("内容", "摘要", "summary", "content"))
    if not content:
        return None
    text = str(content).strip()
    if text.startswith("【") and "】" in text:
        return text[1 : text.index("】")].strip()
    return text[:80].strip()


def _global_news_text(row: dict[str, Any]) -> str:
    return " ".join(
        str(value)
        for value in (
            _first_present(row, ("新闻标题", "标题", "title")),
            _first_present(row, ("内容", "摘要", "summary", "content")),
        )
        if value not in (None, "")
    )


def _matched_news_instruments(
    *,
    text: str,
    aliases_by_instrument: dict[str, set[str]],
) -> list[str]:
    if not text:
        return []
    matched = []
    normalized_text = text.upper()
    for instrument, aliases in aliases_by_instrument.items():
        if any(alias.upper() in normalized_text for alias in aliases):
            matched.append(instrument)
    return matched


def _digits_only(value: Any) -> str:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if not digits:
        raise ValueError(f"missing digits in index constituent code: {value}")
    return digits[-6:].zfill(6)


def _stable_id(*parts: Any) -> str:
    text = "|".join("" if part is None else str(part) for part in parts)
    return sha256(text.encode("utf-8")).hexdigest()
