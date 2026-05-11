"""Title-level text factors for news and announcements."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from quant_data_center.factor_engine.calendar_align import TradeDayAligner, date_minus_days
from quant_data_center.factor_engine.text_events import (
    BUYBACK_EVENTS,
    CONTRACT_EVENTS,
    FINANCING_EVENTS,
    GROWTH_EVENTS,
    LITIGATION_EVENTS,
    OPERATION_EVENTS,
    PERFORMANCE_EVENTS,
    REGULATORY_EVENTS,
    RISK_EVENTS,
    SHAREHOLDER_CHANGE_EVENTS,
    RuleBasedTextEventClassifier,
    TextEventResult,
    event_matches_any,
)
from quant_data_center.storage.database import QdcDatabase

DOCUMENT_TABLES = {"announcement", "news"}
DOCUMENT_LOOKBACK_DAYS = 15

NEWS_FACTOR_FIELDS = (
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
)
ANNOUNCEMENT_FACTOR_FIELDS = (
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
)


def build_news_factor_rows(
    database: QdcDatabase,
    *,
    start_date: str,
    end_date: str,
    source_id: str,
) -> list[dict[str, Any]]:
    documents = _load_documents(database, table="news", start_date=start_date, end_date=end_date)
    classifier = RuleBasedTextEventClassifier()
    grouped: dict[tuple[str, str], dict[str, Any]] = defaultdict(_news_factor_seed)
    for document in documents:
        key = (document["trade_date"], document["instrument"])
        row = grouped[key]
        result = classifier.classify(title=document["title"], document_type="news")
        score = result.sentiment_score
        row["news_count"] += 1.0
        row["_sentiment_sum"] += score
        row["news_positive_count"] += 1.0 if score > 0 else 0.0
        row["news_negative_count"] += 1.0 if score < 0 else 0.0
        _add_text_event_fields(row, result, prefix="news")
    rows = []
    for trade_date, instrument in sorted(grouped):
        factor_row = grouped[(trade_date, instrument)]
        factor_row["trade_date"] = trade_date
        factor_row["instrument"] = instrument
        factor_row["source_id"] = source_id
        factor_row["news_sentiment_mean"] = (
            factor_row["_sentiment_sum"] / factor_row["news_count"]
            if factor_row["news_count"]
            else 0.0
        )
        del factor_row["_sentiment_sum"]
        rows.append(factor_row)
    return rows


def build_announcement_factor_rows(
    database: QdcDatabase,
    *,
    start_date: str,
    end_date: str,
    source_id: str,
) -> list[dict[str, Any]]:
    documents = _load_documents(
        database, table="announcement", start_date=start_date, end_date=end_date
    )
    classifier = RuleBasedTextEventClassifier()
    grouped: dict[tuple[str, str], dict[str, Any]] = defaultdict(_announcement_factor_seed)
    for document in documents:
        key = (document["trade_date"], document["instrument"])
        row = grouped[key]
        result = classifier.classify(title=document["title"], document_type="announcement")
        score = result.sentiment_score
        row["announcement_count"] += 1.0
        row["_sentiment_sum"] += score
        row["announcement_positive_count"] += 1.0 if score > 0 else 0.0
        row["announcement_negative_count"] += 1.0 if score < 0 else 0.0
        _add_text_event_fields(row, result, prefix="announcement")
    rows = []
    for trade_date, instrument in sorted(grouped):
        factor_row = grouped[(trade_date, instrument)]
        factor_row["trade_date"] = trade_date
        factor_row["instrument"] = instrument
        factor_row["source_id"] = source_id
        factor_row["announcement_sentiment_mean"] = (
            factor_row["_sentiment_sum"] / factor_row["announcement_count"]
            if factor_row["announcement_count"]
            else 0.0
        )
        del factor_row["_sentiment_sum"]
        rows.append(factor_row)
    return rows


def _load_documents(
    database: QdcDatabase,
    *,
    table: str,
    start_date: str,
    end_date: str,
) -> list[dict[str, str]]:
    if table not in DOCUMENT_TABLES:
        raise ValueError(f"unsupported document table: {table}")
    publish_start = date_minus_days(start_date, DOCUMENT_LOOKBACK_DAYS)
    with database.connect() as conn:
        aligner = TradeDayAligner.from_connection(conn)
        rows = conn.execute(
            f"""
            select publish_date, instrument, title
            from qdc_silver.{table}
            where publish_date >= ? and publish_date <= ?
            order by publish_date, instrument
            """,
            [publish_start, end_date],
        ).fetchall()
    documents = []
    for publish_date, instrument, title in rows:
        trade_date = aligner.align(publish_date)
        if start_date <= trade_date <= end_date:
            documents.append(
                {
                    "trade_date": trade_date,
                    "instrument": str(instrument),
                    "title": str(title),
                }
            )
    return documents


def _news_factor_seed() -> dict[str, float]:
    return {
        "news_count": 0.0,
        "news_sentiment_mean": 0.0,
        "news_positive_count": 0.0,
        "news_negative_count": 0.0,
        "news_growth_count": 0.0,
        "news_risk_count": 0.0,
        "news_financing_count": 0.0,
        "news_weighted_sentiment_sum": 0.0,
        "news_importance_sum": 0.0,
        "news_contract_count": 0.0,
        "news_buyback_count": 0.0,
        "news_shareholder_change_count": 0.0,
        "news_regulatory_count": 0.0,
        "news_litigation_count": 0.0,
        "news_performance_count": 0.0,
        "_sentiment_sum": 0.0,
    }


def _announcement_factor_seed() -> dict[str, float]:
    return {
        "announcement_count": 0.0,
        "announcement_growth_count": 0.0,
        "announcement_risk_count": 0.0,
        "announcement_financing_count": 0.0,
        "announcement_operation_count": 0.0,
        "announcement_sentiment_mean": 0.0,
        "announcement_positive_count": 0.0,
        "announcement_negative_count": 0.0,
        "announcement_weighted_sentiment_sum": 0.0,
        "announcement_importance_sum": 0.0,
        "announcement_contract_count": 0.0,
        "announcement_buyback_count": 0.0,
        "announcement_shareholder_change_count": 0.0,
        "announcement_regulatory_count": 0.0,
        "announcement_litigation_count": 0.0,
        "announcement_performance_count": 0.0,
        "_sentiment_sum": 0.0,
    }


def _add_text_event_fields(
    row: dict[str, float],
    result: TextEventResult,
    *,
    prefix: str,
) -> None:
    row[f"{prefix}_growth_count"] += _event_count(result, GROWTH_EVENTS)
    row[f"{prefix}_risk_count"] += _event_count(result, RISK_EVENTS)
    row[f"{prefix}_financing_count"] += _event_count(result, FINANCING_EVENTS)
    if prefix == "announcement":
        row[f"{prefix}_operation_count"] += _event_count(result, OPERATION_EVENTS)
    row[f"{prefix}_weighted_sentiment_sum"] += result.weighted_sentiment
    row[f"{prefix}_importance_sum"] += result.importance_score
    row[f"{prefix}_contract_count"] += _event_count(result, CONTRACT_EVENTS)
    row[f"{prefix}_buyback_count"] += _event_count(result, BUYBACK_EVENTS)
    row[f"{prefix}_shareholder_change_count"] += _event_count(
        result, SHAREHOLDER_CHANGE_EVENTS
    )
    row[f"{prefix}_regulatory_count"] += _event_count(result, REGULATORY_EVENTS)
    row[f"{prefix}_litigation_count"] += _event_count(result, LITIGATION_EVENTS)
    row[f"{prefix}_performance_count"] += _event_count(result, PERFORMANCE_EVENTS)


def _event_count(result: TextEventResult, candidates: set[str]) -> float:
    return 1.0 if event_matches_any(result, candidates) else 0.0
