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

DOCUMENT_TABLES = {
    "announcement",
    "news",
    "research_report",
    "investor_interaction",
    "public_sentiment",
}
DOCUMENT_LOOKBACK_DAYS = 15
DOCUMENT_FACTOR_SOURCE_IDS = {
    "announcement": ("cninfo_announcement", "sse_announcement"),
    "news": (
        "sina_finance_news",
        "eastmoney_roll_news",
        "nbd_company_news",
        "sina",
        "wallstreetcn",
        "10jqka",
        "eastmoney",
        "yuncaijing",
        "fenghuang",
        "jinrongjie",
        "cls",
        "yicai",
    ),
    "research_report": ("eastmoney_research_report",),
    "investor_interaction": ("cninfo_investor_interaction",),
    "public_sentiment": ("eastmoney_public_sentiment",),
}

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
RESEARCH_REPORT_FACTOR_FIELDS = (
    "research_report_count",
    "research_institution_count",
    "research_analyst_count",
    "research_rating_positive_count",
    "research_rating_neutral_count",
    "research_rating_negative_count",
    "research_risk_count",
    "research_topic_strength",
    "research_sentiment_mean",
)
INVESTOR_INTERACTION_FACTOR_FIELDS = (
    "question_count",
    "reply_count",
    "reply_delay_hours_mean",
    "risk_topic_count",
    "new_business_topic_count",
    "sentiment_mean",
)
PUBLIC_SENTIMENT_FACTOR_FIELDS = (
    "public_sentiment_count",
    "public_sentiment_heat_mean",
    "public_sentiment_rank_best",
    "public_sentiment_keyword_count",
    "public_sentiment_risk_topic_count",
    "public_sentiment_new_business_topic_count",
    "public_sentiment_sentiment_mean",
)
NEW_BUSINESS_KEYWORDS = (
    "AI",
    "人工智能",
    "大模型",
    "算力",
    "数据中心",
    "机器人",
    "低空经济",
    "卫星",
    "芯片",
    "半导体",
    "新能源",
    "储能",
    "光伏",
    "氢能",
    "无人驾驶",
    "智能驾驶",
    "车联网",
    "数字化",
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
        result = classifier.classify(
            title=document["title"],
            body=document.get("body"),
            document_type="news",
        )
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
        result = classifier.classify(
            title=document["title"],
            body=document.get("body"),
            document_type="announcement",
        )
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


def build_research_report_factor_rows(
    database: QdcDatabase,
    *,
    start_date: str,
    end_date: str,
    source_id: str,
) -> list[dict[str, Any]]:
    documents = _load_research_reports(database, start_date=start_date, end_date=end_date)
    classifier = RuleBasedTextEventClassifier()
    grouped: dict[tuple[str, str], dict[str, Any]] = defaultdict(_research_report_factor_seed)
    distinct_institutions: dict[tuple[str, str], set[str]] = defaultdict(set)
    distinct_analysts: dict[tuple[str, str], set[str]] = defaultdict(set)
    for document in documents:
        key = (document["trade_date"], document["instrument"])
        row = grouped[key]
        result = classifier.classify(
            title=document["title"],
            body=None,
            document_type="news",
        )
        rating_bucket = _rating_bucket(document.get("rating"))
        row["research_report_count"] += 1.0
        row["_sentiment_sum"] += result.sentiment_score
        row["research_topic_strength"] += result.importance_score
        row["research_risk_count"] += _event_count(result, RISK_EVENTS)
        if rating_bucket:
            row[f"research_rating_{rating_bucket}_count"] += 1.0
        institution = str(document.get("institution") or "").strip()
        if institution:
            distinct_institutions[key].add(institution)
        for analyst in _analyst_names(document.get("analyst")):
            distinct_analysts[key].add(analyst)
    rows = []
    for trade_date, instrument in sorted(grouped):
        key = (trade_date, instrument)
        factor_row = grouped[key]
        factor_row["trade_date"] = trade_date
        factor_row["instrument"] = instrument
        factor_row["source_id"] = source_id
        factor_row["research_institution_count"] = float(len(distinct_institutions[key]))
        factor_row["research_analyst_count"] = float(len(distinct_analysts[key]))
        factor_row["research_sentiment_mean"] = (
            factor_row["_sentiment_sum"] / factor_row["research_report_count"]
            if factor_row["research_report_count"]
            else 0.0
        )
        del factor_row["_sentiment_sum"]
        rows.append(factor_row)
    return rows


def build_investor_interaction_factor_rows(
    database: QdcDatabase,
    *,
    start_date: str,
    end_date: str,
    source_id: str,
) -> list[dict[str, Any]]:
    documents = _load_investor_interactions(database, start_date=start_date, end_date=end_date)
    classifier = RuleBasedTextEventClassifier()
    grouped: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        _investor_interaction_factor_seed
    )
    reply_delay_sums: dict[tuple[str, str], float] = defaultdict(float)
    reply_delay_counts: dict[tuple[str, str], int] = defaultdict(int)
    for document in documents:
        key = (document["trade_date"], document["instrument"])
        row = grouped[key]
        result = classifier.classify(
            title=document["question_text"],
            body=document.get("answer_text"),
            document_type="investor_interaction",
        )
        row["question_count"] += 1.0
        row["_sentiment_sum"] += result.sentiment_score
        if document.get("answer_text"):
            row["reply_count"] += 1.0
        delay_hours = _float_or_none(document.get("reply_delay_hours"))
        if delay_hours is not None and delay_hours >= 0:
            reply_delay_sums[key] += delay_hours
            reply_delay_counts[key] += 1
        row["risk_topic_count"] += _event_count(result, RISK_EVENTS)
        row["new_business_topic_count"] += (
            1.0 if _contains_new_business_topic(document["question_text"], document.get("answer_text")) else 0.0
        )
    rows = []
    for trade_date, instrument in sorted(grouped):
        key = (trade_date, instrument)
        factor_row = grouped[key]
        factor_row["trade_date"] = trade_date
        factor_row["instrument"] = instrument
        factor_row["source_id"] = source_id
        factor_row["reply_delay_hours_mean"] = (
            reply_delay_sums[key] / reply_delay_counts[key]
            if reply_delay_counts[key]
            else 0.0
        )
        factor_row["sentiment_mean"] = (
            factor_row["_sentiment_sum"] / factor_row["question_count"]
            if factor_row["question_count"]
            else 0.0
        )
        del factor_row["_sentiment_sum"]
        rows.append(factor_row)
    return rows


def build_public_sentiment_factor_rows(
    database: QdcDatabase,
    *,
    start_date: str,
    end_date: str,
    source_id: str,
) -> list[dict[str, Any]]:
    documents = _load_public_sentiment(database, start_date=start_date, end_date=end_date)
    grouped: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        _public_sentiment_factor_seed
    )
    heat_sums: dict[tuple[str, str], float] = defaultdict(float)
    heat_counts: dict[tuple[str, str], int] = defaultdict(int)
    sentiment_sums: dict[tuple[str, str], float] = defaultdict(float)
    sentiment_counts: dict[tuple[str, str], int] = defaultdict(int)
    rank_best: dict[tuple[str, str], float] = {}
    for document in documents:
        key = (document["trade_date"], document["instrument"])
        row = grouped[key]
        row["public_sentiment_count"] += 1.0
        heat_score = _float_or_none(document.get("hot_score"))
        if heat_score is not None:
            heat_sums[key] += heat_score
            heat_counts[key] += 1
        hot_rank = _float_or_none(document.get("hot_rank"))
        if hot_rank is not None and hot_rank > 0:
            rank_best[key] = min(rank_best.get(key, hot_rank), hot_rank)
        row["public_sentiment_keyword_count"] += float(
            document.get("keyword_count") or 0
        )
        row["public_sentiment_risk_topic_count"] += float(
            document.get("risk_topic_count") or 0
        )
        row["public_sentiment_new_business_topic_count"] += float(
            document.get("new_business_topic_count") or 0
        )
        sentiment_score = _float_or_none(document.get("sentiment_score"))
        if sentiment_score is not None:
            sentiment_sums[key] += sentiment_score
            sentiment_counts[key] += 1
    rows = []
    for trade_date, instrument in sorted(grouped):
        key = (trade_date, instrument)
        factor_row = grouped[key]
        factor_row["trade_date"] = trade_date
        factor_row["instrument"] = instrument
        factor_row["source_id"] = source_id
        factor_row["public_sentiment_heat_mean"] = (
            heat_sums[key] / heat_counts[key] if heat_counts[key] else 0.0
        )
        factor_row["public_sentiment_rank_best"] = rank_best.get(key, 0.0)
        factor_row["public_sentiment_sentiment_mean"] = (
            sentiment_sums[key] / sentiment_counts[key] if sentiment_counts[key] else 0.0
        )
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
    source_ids = DOCUMENT_FACTOR_SOURCE_IDS.get(table, ())
    if not source_ids:
        return []
    placeholders = ", ".join("?" for _ in source_ids)
    with database.connect() as conn:
        aligner = TradeDayAligner.from_connection(conn)
        columns = {
            str(row[0])
            for row in conn.execute(
                """
                select column_name
                from information_schema.columns
                where table_schema = 'qdc_silver' and table_name = ?
                """,
                [table],
            ).fetchall()
        }
        body_expr = "body_text" if "body_text" in columns else "null as body_text"
        rows = conn.execute(
            f"""
            select publish_date, instrument, title, source_id, {body_expr}
            from qdc_silver.{table}
            where publish_date >= ? and publish_date <= ?
              and source_id in ({placeholders})
            order by publish_date, instrument, source_id
            """,
            [publish_start, end_date, *source_ids],
        ).fetchall()
    documents = []
    seen_keys = set()
    for publish_date, instrument, title, source_id_value, body_text in rows:
        trade_date = aligner.align(publish_date)
        if start_date <= trade_date <= end_date:
            key = (
                str(publish_date),
                str(instrument),
                _dedupe_title_key(str(title)),
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            documents.append(
                {
                    "trade_date": trade_date,
                    "instrument": str(instrument),
                    "title": str(title),
                    "body": str(body_text) if body_text else None,
                    "source_id": str(source_id_value),
                }
            )
    return documents


def _load_research_reports(
    database: QdcDatabase,
    *,
    start_date: str,
    end_date: str,
) -> list[dict[str, str]]:
    publish_start = date_minus_days(start_date, DOCUMENT_LOOKBACK_DAYS)
    source_ids = DOCUMENT_FACTOR_SOURCE_IDS["research_report"]
    placeholders = ", ".join("?" for _ in source_ids)
    with database.connect() as conn:
        aligner = TradeDayAligner.from_connection(conn)
        columns = {
            str(row[0])
            for row in conn.execute(
                """
                select column_name
                from information_schema.columns
                where table_schema = 'qdc_silver' and table_name = 'research_report'
                """
            ).fetchall()
        }
        if not columns:
            return []
        institution_expr = "institution" if "institution" in columns else "null as institution"
        analyst_expr = "analyst" if "analyst" in columns else "null as analyst"
        rating_expr = "rating" if "rating" in columns else "null as rating"
        rows = conn.execute(
            f"""
            select publish_date, instrument, title, source_id,
                   {institution_expr}, {analyst_expr}, {rating_expr}
            from qdc_silver.research_report
            where publish_date >= ? and publish_date <= ?
              and source_id in ({placeholders})
            order by publish_date, instrument, source_id
            """,
            [publish_start, end_date, *source_ids],
        ).fetchall()
    documents = []
    seen_keys = set()
    for publish_date, instrument, title, source_id_value, institution, analyst, rating in rows:
        trade_date = aligner.align(publish_date)
        if start_date <= trade_date <= end_date:
            key = (
                str(publish_date),
                str(instrument),
                str(institution or ""),
                _dedupe_title_key(str(title)),
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            documents.append(
                {
                    "trade_date": trade_date,
                    "instrument": str(instrument),
                    "title": str(title),
                    "source_id": str(source_id_value),
                    "institution": str(institution) if institution else "",
                    "analyst": str(analyst) if analyst else "",
                    "rating": str(rating) if rating else "",
                }
            )
    return documents


def _load_investor_interactions(
    database: QdcDatabase,
    *,
    start_date: str,
    end_date: str,
) -> list[dict[str, Any]]:
    publish_start = date_minus_days(start_date, DOCUMENT_LOOKBACK_DAYS)
    source_ids = DOCUMENT_FACTOR_SOURCE_IDS["investor_interaction"]
    placeholders = ", ".join("?" for _ in source_ids)
    with database.connect() as conn:
        aligner = TradeDayAligner.from_connection(conn)
        columns = {
            str(row[0])
            for row in conn.execute(
                """
                select column_name
                from information_schema.columns
                where table_schema = 'qdc_silver' and table_name = 'investor_interaction'
                """
            ).fetchall()
        }
        if not columns:
            return []
        answer_expr = "answer_text" if "answer_text" in columns else "null as answer_text"
        delay_expr = (
            "reply_delay_hours"
            if "reply_delay_hours" in columns
            else "null as reply_delay_hours"
        )
        rows = conn.execute(
            f"""
            select publish_date, instrument, title, source_id,
                   {answer_expr}, {delay_expr}
            from qdc_silver.investor_interaction
            where publish_date >= ? and publish_date <= ?
              and source_id in ({placeholders})
            order by publish_date, instrument, source_id
            """,
            [publish_start, end_date, *source_ids],
        ).fetchall()
    documents = []
    seen_keys = set()
    for publish_date, instrument, title, source_id_value, answer_text, reply_delay_hours in rows:
        trade_date = aligner.align(publish_date)
        if start_date <= trade_date <= end_date:
            key = (
                str(publish_date),
                str(instrument),
                _dedupe_title_key(str(title)),
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            documents.append(
                {
                    "trade_date": trade_date,
                    "instrument": str(instrument),
                    "question_text": str(title),
                    "answer_text": str(answer_text) if answer_text else "",
                    "reply_delay_hours": reply_delay_hours,
                    "source_id": str(source_id_value),
                }
            )
    return documents


def _load_public_sentiment(
    database: QdcDatabase,
    *,
    start_date: str,
    end_date: str,
) -> list[dict[str, Any]]:
    publish_start = date_minus_days(start_date, DOCUMENT_LOOKBACK_DAYS)
    source_ids = DOCUMENT_FACTOR_SOURCE_IDS["public_sentiment"]
    placeholders = ", ".join("?" for _ in source_ids)
    with database.connect() as conn:
        aligner = TradeDayAligner.from_connection(conn)
        columns = {
            str(row[0])
            for row in conn.execute(
                """
                select column_name
                from information_schema.columns
                where table_schema = 'qdc_silver' and table_name = 'public_sentiment'
                """
            ).fetchall()
        }
        if not columns:
            return []
        optional_exprs = [
            _optional_select_expr(columns, field)
            for field in (
                "hot_score",
                "hot_rank",
                "keyword_count",
                "risk_topic_count",
                "new_business_topic_count",
                "sentiment_score",
            )
        ]
        rows = conn.execute(
            f"""
            select publish_date, instrument, title, source_id, {", ".join(optional_exprs)}
            from qdc_silver.public_sentiment
            where publish_date >= ? and publish_date <= ?
              and source_id in ({placeholders})
            order by publish_date, instrument, source_id
            """,
            [publish_start, end_date, *source_ids],
        ).fetchall()
    documents = []
    seen_keys = set()
    for (
        publish_date,
        instrument,
        title,
        source_id_value,
        hot_score,
        hot_rank,
        keyword_count,
        risk_topic_count,
        new_business_topic_count,
        sentiment_score,
    ) in rows:
        trade_date = aligner.align(publish_date)
        if start_date <= trade_date <= end_date:
            key = (
                str(publish_date),
                str(instrument),
                _dedupe_title_key(str(title)),
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            documents.append(
                {
                    "trade_date": trade_date,
                    "instrument": str(instrument),
                    "title": str(title),
                    "source_id": str(source_id_value),
                    "hot_score": hot_score,
                    "hot_rank": hot_rank,
                    "keyword_count": keyword_count,
                    "risk_topic_count": risk_topic_count,
                    "new_business_topic_count": new_business_topic_count,
                    "sentiment_score": sentiment_score,
                }
            )
    return documents


def _optional_select_expr(columns: set[str], field: str) -> str:
    if field in columns:
        return field
    return f"null as {field}"


def _dedupe_title_key(title: str) -> str:
    return "".join(ch for ch in title.lower() if ch.isalnum())


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


def _research_report_factor_seed() -> dict[str, float]:
    return {
        "research_report_count": 0.0,
        "research_institution_count": 0.0,
        "research_analyst_count": 0.0,
        "research_rating_positive_count": 0.0,
        "research_rating_neutral_count": 0.0,
        "research_rating_negative_count": 0.0,
        "research_risk_count": 0.0,
        "research_topic_strength": 0.0,
        "research_sentiment_mean": 0.0,
        "_sentiment_sum": 0.0,
    }


def _investor_interaction_factor_seed() -> dict[str, float]:
    return {
        "question_count": 0.0,
        "reply_count": 0.0,
        "reply_delay_hours_mean": 0.0,
        "risk_topic_count": 0.0,
        "new_business_topic_count": 0.0,
        "sentiment_mean": 0.0,
        "_sentiment_sum": 0.0,
    }


def _public_sentiment_factor_seed() -> dict[str, float]:
    return {
        "public_sentiment_count": 0.0,
        "public_sentiment_heat_mean": 0.0,
        "public_sentiment_rank_best": 0.0,
        "public_sentiment_keyword_count": 0.0,
        "public_sentiment_risk_topic_count": 0.0,
        "public_sentiment_new_business_topic_count": 0.0,
        "public_sentiment_sentiment_mean": 0.0,
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


def _rating_bucket(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if any(token in text for token in ("买入", "增持", "推荐", "跑赢", "优于", "强烈")):
        return "positive"
    if any(token in text for token in ("卖出", "减持", "低于", "弱于", "回避")):
        return "negative"
    if any(token in text for token in ("中性", "持有", "无评级", "谨慎")):
        return "neutral"
    return "neutral"


def _analyst_names(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    for separator in ("；", ";", "，", ",", "、", "/", "|"):
        text = text.replace(separator, " ")
    return [item.strip() for item in text.split() if item.strip()]


def _contains_new_business_topic(question_text: str, answer_text: Any) -> bool:
    text = f"{question_text or ''} {answer_text or ''}".upper()
    return any(keyword.upper() in text for keyword in NEW_BUSINESS_KEYWORDS)


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
