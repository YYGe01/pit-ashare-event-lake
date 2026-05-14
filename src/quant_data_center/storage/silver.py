"""Writers for qdc_silver research tables."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from quant_data_center.settings import QdcSettings
from quant_data_center.storage.database import QdcDatabase


DOCUMENT_OPTIONAL_FIELDS = {
    "announcement": [
        "publish_time",
        "source_record_id",
        "source_sec_code",
        "source_sec_name",
        "adjunct_url",
        "observed_at",
        "collect_time",
        "pdf_url",
        "pdf_sha256",
        "pdf_size_bytes",
        "pdf_object_id",
        "pdf_download_status",
        "pdf_error_message",
        "raw_object_id",
        "parser_version",
    ],
    "news": [
        "publish_time",
        "source_record_id",
        "observed_at",
        "collect_time",
        "body_text",
        "body_download_status",
        "body_error_message",
        "body_size_bytes",
        "raw_object_id",
        "parser_version",
    ],
    "research_report": [
        "publish_time",
        "source_record_id",
        "source_sec_code",
        "source_sec_name",
        "institution",
        "analyst",
        "rating",
        "rating_change",
        "industry",
        "report_type",
        "observed_at",
        "collect_time",
        "pdf_url",
        "pdf_sha256",
        "pdf_size_bytes",
        "pdf_object_id",
        "pdf_download_status",
        "pdf_error_message",
        "raw_object_id",
        "parser_version",
    ],
    "investor_interaction": [
        "publish_time",
        "source_record_id",
        "source_sec_code",
        "source_sec_name",
        "question_text",
        "question_time",
        "answer_text",
        "answer_time",
        "reply_status",
        "reply_delay_hours",
        "questioner",
        "industry",
        "channel",
        "topic_tags",
        "sentiment_score",
        "observed_at",
        "collect_time",
        "raw_object_id",
        "parser_version",
    ],
    "public_sentiment": [
        "publish_time",
        "source_record_id",
        "source_sec_code",
        "source_sec_name",
        "platform",
        "sentiment_type",
        "hot_rank",
        "hot_score",
        "rank_change",
        "keyword_text",
        "keyword_count",
        "risk_topic_count",
        "new_business_topic_count",
        "sentiment_score",
        "observed_at",
        "collect_time",
        "raw_object_id",
        "parser_version",
    ],
}
PDF_METADATA_FIELDS = {"pdf_sha256", "pdf_size_bytes", "pdf_object_id"}


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

    def upsert_research_reports(self, records: list[dict[str, Any]]) -> int:
        return self._upsert_document_table(
            table="research_report",
            id_field="research_report_id",
            records=records,
        )

    def upsert_investor_interactions(self, records: list[dict[str, Any]]) -> int:
        return self._upsert_document_table(
            table="investor_interaction",
            id_field="investor_interaction_id",
            records=records,
        )

    def upsert_public_sentiment(self, records: list[dict[str, Any]]) -> int:
        return self._upsert_document_table(
            table="public_sentiment",
            id_field="public_sentiment_id",
            records=records,
        )

    def upsert_daily_news_factor(self, records: list[dict[str, Any]]) -> int:
        return self._upsert_factor_table(
            table="daily_news_factor",
            factor_fields=[
                "news_count",
                "news_sentiment_mean",
                "news_positive_count",
                "news_negative_count",
                "news_growth_count",
                "news_risk_count",
                "news_financing_count",
                "news_weighted_sentiment_sum",
                "news_importance_sum",
                "news_contract_count",
                "news_buyback_count",
                "news_shareholder_change_count",
                "news_regulatory_count",
                "news_litigation_count",
                "news_performance_count",
            ],
            records=records,
        )

    def upsert_daily_announcement_factor(self, records: list[dict[str, Any]]) -> int:
        return self._upsert_factor_table(
            table="daily_announcement_factor",
            factor_fields=[
                "announcement_count",
                "announcement_growth_count",
                "announcement_risk_count",
                "announcement_financing_count",
                "announcement_operation_count",
                "announcement_sentiment_mean",
                "announcement_positive_count",
                "announcement_negative_count",
                "announcement_weighted_sentiment_sum",
                "announcement_importance_sum",
                "announcement_contract_count",
                "announcement_buyback_count",
                "announcement_shareholder_change_count",
                "announcement_regulatory_count",
                "announcement_litigation_count",
                "announcement_performance_count",
            ],
            records=records,
        )

    def upsert_daily_research_report_factor(self, records: list[dict[str, Any]]) -> int:
        return self._upsert_factor_table(
            table="daily_research_report_factor",
            factor_fields=[
                "research_report_count",
                "research_institution_count",
                "research_analyst_count",
                "research_rating_positive_count",
                "research_rating_neutral_count",
                "research_rating_negative_count",
                "research_risk_count",
                "research_topic_strength",
                "research_sentiment_mean",
            ],
            records=records,
        )

    def upsert_daily_investor_interaction_factor(self, records: list[dict[str, Any]]) -> int:
        return self._upsert_factor_table(
            table="daily_investor_interaction_factor",
            factor_fields=[
                "question_count",
                "reply_count",
                "reply_delay_hours_mean",
                "risk_topic_count",
                "new_business_topic_count",
                "sentiment_mean",
            ],
            records=records,
        )

    def upsert_daily_public_sentiment_factor(self, records: list[dict[str, Any]]) -> int:
        return self._upsert_factor_table(
            table="daily_public_sentiment_factor",
            factor_fields=[
                "public_sentiment_count",
                "public_sentiment_heat_mean",
                "public_sentiment_rank_best",
                "public_sentiment_keyword_count",
                "public_sentiment_risk_topic_count",
                "public_sentiment_new_business_topic_count",
                "public_sentiment_sentiment_mean",
            ],
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
        optional_fields = DOCUMENT_OPTIONAL_FIELDS.get(table, [])
        now = _now()
        field_sql = ", ".join([id_field, "publish_date", "instrument", "title", "url", "source_id"])
        if optional_fields:
            field_sql = f"{field_sql}, {', '.join(optional_fields)}"
        field_sql = f"{field_sql}, updated_at"
        placeholders = ", ".join("?" for _ in range(7 + len(optional_fields)))
        with self.database.connect() as conn:
            existing = _load_existing_optional_fields(
                conn=conn,
                table=table,
                id_field=id_field,
                record_ids=list(deduped),
                optional_fields=optional_fields,
            )
            rows = [
                [
                    _required(record, id_field),
                    _required(record, "publish_date"),
                    _required(record, "instrument"),
                    _required(record, "title"),
                    record.get("url"),
                    _required(record, "source_id"),
                    *[
                        _optional_document_value(
                            record=record,
                            existing=existing.get(str(_required(record, id_field)), {}),
                            field=field,
                        )
                        for field in optional_fields
                    ],
                    record.get("updated_at") or now,
                ]
                for record in deduped.values()
            ]
            conn.executemany(
                f"delete from qdc_silver.{table} where {id_field} = ?",
                [[row[0]] for row in rows],
            )
            conn.executemany(
                f"""
                insert into qdc_silver.{table} ({field_sql})
                values ({placeholders})
                """,
                rows,
            )
        return len(rows)

    def _upsert_factor_table(
        self,
        *,
        table: str,
        factor_fields: list[str],
        records: list[dict[str, Any]],
    ) -> int:
        if not records:
            return 0
        now = _now()
        rows = [
            [
                _required(record, "trade_date"),
                _required(record, "instrument"),
                *[float(record.get(field, 0) or 0) for field in factor_fields],
                _required(record, "source_id"),
                record.get("updated_at") or now,
            ]
            for record in records
        ]
        field_sql = ", ".join(factor_fields)
        placeholders = ", ".join("?" for _ in range(len(factor_fields) + 4))
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
                  trade_date, instrument, {field_sql}, source_id, updated_at
                )
                values ({placeholders})
                """,
                rows,
            )
        return len(rows)


def _required(record: dict[str, Any], key: str) -> Any:
    value = record.get(key)
    if value is None or value == "":
        raise ValueError(f"missing required silver field: {key}")
    return value


def _load_existing_optional_fields(
    *,
    conn: Any,
    table: str,
    id_field: str,
    record_ids: list[str],
    optional_fields: list[str],
) -> dict[str, dict[str, Any]]:
    if not record_ids or not optional_fields:
        return {}
    placeholders = ", ".join("?" for _ in record_ids)
    fields_sql = ", ".join(optional_fields)
    rows = conn.execute(
        f"""
        select {id_field}, {fields_sql}
        from qdc_silver.{table}
        where {id_field} in ({placeholders})
        """,
        record_ids,
    ).fetchall()
    result = {}
    for row in rows:
        record_id = str(row[0])
        result[record_id] = {
            field: value for field, value in zip(optional_fields, row[1:], strict=True)
        }
    return result


def _optional_document_value(
    *,
    record: dict[str, Any],
    existing: dict[str, Any],
    field: str,
) -> Any:
    incoming = record.get(field)
    current = existing.get(field)
    if field == "observed_at" and current not in (None, ""):
        return current
    if field in PDF_METADATA_FIELDS and incoming in (None, "") and current not in (None, ""):
        return current
    if field in {"body_text", "body_size_bytes"} and incoming in (None, "") and current not in (
        None,
        "",
    ):
        return current
    if field == "body_download_status" and incoming in (None, "") and existing.get("body_text"):
        return current or "success"
    if field == "body_error_message" and existing.get("body_text") and record.get(
        "body_download_status"
    ) != "failed":
        return current
    if field == "pdf_download_status":
        incoming_status = str(incoming or "")
        if incoming_status != "success" and existing.get("pdf_sha256"):
            return current or "success"
    if field == "pdf_error_message" and existing.get("pdf_sha256") and record.get(
        "pdf_download_status"
    ) != "success":
        return current
    return incoming


def _now() -> datetime:
    return datetime.now().replace(microsecond=0)
