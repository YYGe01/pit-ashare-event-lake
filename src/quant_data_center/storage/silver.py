"""Writers for qdc_silver research tables."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from quant_data_center.settings import QdcSettings
from quant_data_center.storage.database import QdcDatabase


class SilverStore:
    """Upsert normalized research records into DuckDB silver tables."""

    def __init__(self, settings: QdcSettings) -> None:
        self.settings = settings
        self.database = QdcDatabase(settings)

    def upsert_stock_basic(self, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        now = _now()
        rows = [
            [
                _required(record, "instrument"),
                _required(record, "symbol"),
                _required(record, "exchange"),
                record.get("name"),
                record.get("list_date"),
                record.get("delist_date"),
                record.get("is_active"),
                record.get("industry"),
                _required(record, "source_id"),
                record.get("updated_at") or now,
            ]
            for record in records
        ]
        with self.database.connect() as conn:
            conn.executemany(
                "delete from qdc_silver.stock_basic where instrument = ?",
                [[row[0]] for row in rows],
            )
            conn.executemany(
                """
                insert into qdc_silver.stock_basic (
                  instrument, symbol, exchange, name, list_date, delist_date,
                  is_active, industry, source_id, updated_at
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return len(rows)

    def upsert_universe_constituents(self, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        now = _now()
        rows = [
            [
                _required(record, "universe"),
                _required(record, "snapshot_date"),
                _required(record, "instrument"),
                _required(record, "symbol"),
                _required(record, "exchange"),
                record.get("name"),
                record.get("weight"),
                _required(record, "source_id"),
                record.get("updated_at") or now,
            ]
            for record in records
        ]
        with self.database.connect() as conn:
            conn.executemany(
                """
                delete from qdc_silver.universe_constituent
                where universe = ? and snapshot_date = ? and instrument = ?
                """,
                [[row[0], row[1], row[2]] for row in rows],
            )
            conn.executemany(
                """
                insert into qdc_silver.universe_constituent (
                  universe, snapshot_date, instrument, symbol, exchange, name,
                  weight, source_id, updated_at
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return len(rows)

    def upsert_trade_calendar(self, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        now = _now()
        rows = [
            [
                _required(record, "calendar_id"),
                _required(record, "trade_date"),
                bool(_required(record, "is_open")),
                record.get("pre_trade_date"),
                record.get("next_trade_date"),
                _required(record, "source_id"),
                record.get("updated_at") or now,
            ]
            for record in records
        ]
        with self.database.connect() as conn:
            conn.executemany(
                """
                delete from qdc_silver.trade_calendar
                where calendar_id = ? and trade_date = ?
                """,
                [[row[0], row[1]] for row in rows],
            )
            conn.executemany(
                """
                insert into qdc_silver.trade_calendar (
                  calendar_id, trade_date, is_open, pre_trade_date, next_trade_date,
                  source_id, updated_at
                )
                values (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return len(rows)

    def upsert_daily_bar(self, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        now = _now()
        rows = [
            [
                _required(record, "trade_date"),
                _required(record, "instrument"),
                record.get("open"),
                record.get("high"),
                record.get("low"),
                record.get("close"),
                record.get("pre_close"),
                record.get("volume"),
                record.get("amount"),
                record.get("vwap"),
                _required(record, "source_id"),
                record.get("updated_at") or now,
            ]
            for record in records
        ]
        with self.database.connect() as conn:
            conn.executemany(
                """
                delete from qdc_silver.daily_bar
                where trade_date = ? and instrument = ?
                """,
                [[row[0], row[1]] for row in rows],
            )
            conn.executemany(
                """
                insert into qdc_silver.daily_bar (
                  trade_date, instrument, open, high, low, close, pre_close,
                  volume, amount, vwap, source_id, updated_at
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return len(rows)

    def upsert_adj_factor(self, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        now = _now()
        rows = [
            [
                _required(record, "trade_date"),
                _required(record, "instrument"),
                record.get("adj_factor"),
                record.get("factor_type"),
                _required(record, "source_id"),
                record.get("updated_at") or now,
            ]
            for record in records
        ]
        with self.database.connect() as conn:
            conn.executemany(
                """
                delete from qdc_silver.adj_factor
                where trade_date = ? and instrument = ?
                """,
                [[row[0], row[1]] for row in rows],
            )
            conn.executemany(
                """
                insert into qdc_silver.adj_factor (
                  trade_date, instrument, adj_factor, factor_type, source_id, updated_at
                )
                values (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return len(rows)

    def upsert_price_limit(self, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        now = _now()
        rows = [
            [
                _required(record, "trade_date"),
                _required(record, "instrument"),
                record.get("limit_up"),
                record.get("limit_down"),
                record.get("prev_close"),
                record.get("limit_rule"),
                _required(record, "source_id"),
                record.get("updated_at") or now,
            ]
            for record in records
        ]
        with self.database.connect() as conn:
            conn.executemany(
                """
                delete from qdc_silver.price_limit
                where trade_date = ? and instrument = ?
                """,
                [[row[0], row[1]] for row in rows],
            )
            conn.executemany(
                """
                insert into qdc_silver.price_limit (
                  trade_date, instrument, limit_up, limit_down, prev_close,
                  limit_rule, source_id, updated_at
                )
                values (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return len(rows)

    def upsert_trade_status(self, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        deduped_records = {}
        for record in records:
            key = (str(_required(record, "trade_date")), str(_required(record, "instrument")))
            deduped_records.setdefault(key, record)
        records = list(deduped_records.values())
        now = _now()
        rows = [
            [
                _required(record, "trade_date"),
                _required(record, "instrument"),
                _required(record, "trade_status"),
                record.get("halt_reason"),
                record.get("source_update_time"),
                _required(record, "source_id"),
                record.get("updated_at") or now,
            ]
            for record in records
        ]
        with self.database.connect() as conn:
            conn.executemany(
                """
                delete from qdc_silver.trade_status
                where trade_date = ? and instrument = ?
                """,
                [[row[0], row[1]] for row in rows],
            )
            conn.executemany(
                """
                insert into qdc_silver.trade_status (
                  trade_date, instrument, trade_status, halt_reason,
                  source_update_time, source_id, updated_at
                )
                values (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return len(rows)

    def upsert_announcements(self, records: list[dict[str, Any]]) -> int:
        return self._upsert_document_table(
            table="announcement",
            id_field="announcement_id",
            records=records,
        )

    def upsert_news(self, records: list[dict[str, Any]]) -> int:
        return self._upsert_document_table(
            table="news",
            id_field="news_id",
            records=records,
        )

    def upsert_daily_news_factor(self, records: list[dict[str, Any]]) -> int:
        return self._upsert_count_factor(
            table="daily_news_factor",
            value_field="news_count",
            records=records,
        )

    def upsert_daily_announcement_factor(self, records: list[dict[str, Any]]) -> int:
        return self._upsert_count_factor(
            table="daily_announcement_factor",
            value_field="announcement_count",
            records=records,
        )

    def _upsert_document_table(
        self,
        *,
        table: str,
        id_field: str,
        records: list[dict[str, Any]],
    ) -> int:
        if not records:
            return 0
        deduped = {str(_required(record, id_field)): record for record in records}
        now = _now()
        rows = [
            [
                _required(record, id_field),
                _required(record, "publish_date"),
                _required(record, "instrument"),
                _required(record, "title"),
                record.get("url"),
                _required(record, "source_id"),
                record.get("updated_at") or now,
            ]
            for record in deduped.values()
        ]
        with self.database.connect() as conn:
            conn.executemany(
                f"delete from qdc_silver.{table} where {id_field} = ?",
                [[row[0]] for row in rows],
            )
            conn.executemany(
                f"""
                insert into qdc_silver.{table} (
                  {id_field}, publish_date, instrument, title, url, source_id, updated_at
                )
                values (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return len(rows)

    def _upsert_count_factor(
        self,
        *,
        table: str,
        value_field: str,
        records: list[dict[str, Any]],
    ) -> int:
        if not records:
            return 0
        now = _now()
        rows = [
            [
                _required(record, "trade_date"),
                _required(record, "instrument"),
                float(_required(record, value_field)),
                _required(record, "source_id"),
                record.get("updated_at") or now,
            ]
            for record in records
        ]
        with self.database.connect() as conn:
            conn.executemany(
                f"""
                delete from qdc_silver.{table}
                where trade_date = ? and instrument = ?
                """,
                [[row[0], row[1]] for row in rows],
            )
            conn.executemany(
                f"""
                insert into qdc_silver.{table} (
                  trade_date, instrument, {value_field}, source_id, updated_at
                )
                values (?, ?, ?, ?, ?)
                """,
                rows,
            )
        return len(rows)


def _required(record: dict[str, Any], key: str) -> Any:
    value = record.get(key)
    if value is None or value == "":
        raise ValueError(f"missing required silver field: {key}")
    return value


def _now() -> datetime:
    return datetime.now().replace(microsecond=0)
