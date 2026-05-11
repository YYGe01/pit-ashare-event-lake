"""Title-level text factors for news and announcements."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from quant_data_center.factor_engine.calendar_align import TradeDayAligner, date_minus_days
from quant_data_center.storage.database import QdcDatabase

DOCUMENT_TABLES = {"announcement", "news"}
DOCUMENT_LOOKBACK_DAYS = 15

POSITIVE_KEYWORDS = (
    "增长",
    "预增",
    "上升",
    "提升",
    "中标",
    "签订",
    "突破",
    "创新高",
    "增持",
    "回购",
    "盈利",
    "扭亏",
    "订单",
    "获批",
)
NEGATIVE_KEYWORDS = (
    "亏损",
    "预亏",
    "下滑",
    "下降",
    "减持",
    "处罚",
    "立案",
    "调查",
    "诉讼",
    "仲裁",
    "违规",
    "退市",
    "ST",
    "风险",
    "违约",
    "冻结",
    "质押",
)
GROWTH_EVENT_KEYWORDS = (
    "增长",
    "预增",
    "中标",
    "签订",
    "订单",
    "获批",
    "扩产",
    "投产",
    "突破",
)
RISK_EVENT_KEYWORDS = (
    "风险",
    "处罚",
    "立案",
    "调查",
    "诉讼",
    "仲裁",
    "违规",
    "退市",
    "亏损",
    "违约",
    "冻结",
    "质押",
)
FINANCING_EVENT_KEYWORDS = (
    "定增",
    "增发",
    "配股",
    "可转债",
    "融资",
    "募集资金",
    "发行股票",
)
OPERATION_EVENT_KEYWORDS = (
    "权益分派",
    "分红",
    "回购",
    "股权激励",
    "并购",
    "重组",
    "重大合同",
    "中标",
)

NEWS_FACTOR_FIELDS = (
    "news_count",
    "news_sentiment_mean",
    "news_positive_count",
    "news_negative_count",
    "news_growth_count",
    "news_risk_count",
    "news_financing_count",
)
ANNOUNCEMENT_FACTOR_FIELDS = (
    "announcement_count",
    "announcement_risk_count",
    "announcement_financing_count",
    "announcement_operation_count",
)


def build_news_factor_rows(
    database: QdcDatabase,
    *,
    start_date: str,
    end_date: str,
    source_id: str,
) -> list[dict[str, Any]]:
    documents = _load_documents(database, table="news", start_date=start_date, end_date=end_date)
    grouped: dict[tuple[str, str], dict[str, Any]] = defaultdict(_news_factor_seed)
    for document in documents:
        key = (document["trade_date"], document["instrument"])
        row = grouped[key]
        title = document["title"]
        score = _sentiment_score(title)
        row["news_count"] += 1.0
        row["_sentiment_sum"] += score
        row["news_positive_count"] += 1.0 if score > 0 else 0.0
        row["news_negative_count"] += 1.0 if score < 0 else 0.0
        row["news_growth_count"] += _has_keyword(title, GROWTH_EVENT_KEYWORDS)
        row["news_risk_count"] += _has_keyword(title, RISK_EVENT_KEYWORDS)
        row["news_financing_count"] += _has_keyword(title, FINANCING_EVENT_KEYWORDS)
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
    grouped: dict[tuple[str, str], dict[str, Any]] = defaultdict(_announcement_factor_seed)
    for document in documents:
        key = (document["trade_date"], document["instrument"])
        row = grouped[key]
        title = document["title"]
        row["announcement_count"] += 1.0
        row["announcement_risk_count"] += _has_keyword(title, RISK_EVENT_KEYWORDS)
        row["announcement_financing_count"] += _has_keyword(title, FINANCING_EVENT_KEYWORDS)
        row["announcement_operation_count"] += _has_keyword(title, OPERATION_EVENT_KEYWORDS)
    rows = []
    for trade_date, instrument in sorted(grouped):
        factor_row = grouped[(trade_date, instrument)]
        factor_row["trade_date"] = trade_date
        factor_row["instrument"] = instrument
        factor_row["source_id"] = source_id
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
        "_sentiment_sum": 0.0,
    }


def _announcement_factor_seed() -> dict[str, float]:
    return {
        "announcement_count": 0.0,
        "announcement_risk_count": 0.0,
        "announcement_financing_count": 0.0,
        "announcement_operation_count": 0.0,
    }


def _sentiment_score(title: str) -> float:
    positive_hits = _keyword_hits(title, POSITIVE_KEYWORDS)
    negative_hits = _keyword_hits(title, NEGATIVE_KEYWORDS)
    if positive_hits > negative_hits:
        return 1.0
    if negative_hits > positive_hits:
        return -1.0
    return 0.0


def _has_keyword(title: str, keywords: tuple[str, ...]) -> float:
    return 1.0 if _keyword_hits(title, keywords) > 0 else 0.0


def _keyword_hits(title: str, keywords: tuple[str, ...]) -> int:
    normalized = title.upper()
    return sum(1 for keyword in keywords if keyword.upper() in normalized)
