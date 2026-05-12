"""Basic quality checks for QDC research tables."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd

from quant_data_center.settings import QdcSettings
from quant_data_center.storage.database import QdcDatabase


SUPPORTED_QUALITY_DATASETS = {
    "stock_basic",
    "universe_constituent",
    "trade_calendar",
    "daily_bar",
    "adj_factor",
    "price_limit",
    "trade_status",
    "announcement",
    "news",
    "daily_news_factor",
    "daily_announcement_factor",
}
DOCUMENT_PUBLISH_TIME_REQUIRED_SOURCES = {
    "cninfo_announcement",
    "sse_announcement",
    "sina_finance_news",
    "eastmoney_roll_news",
}


class QualityChecker:
    """Run deterministic local quality checks against qdc_silver tables."""

    def __init__(self, settings: QdcSettings) -> None:
        self.settings = settings
        self.database = QdcDatabase(settings)

    def run(
        self,
        *,
        dataset: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, Any]:
        datasets = [dataset] if dataset else sorted(SUPPORTED_QUALITY_DATASETS)
        unknown = [name for name in datasets if name not in SUPPORTED_QUALITY_DATASETS]
        if unknown:
            raise ValueError(f"unsupported quality dataset: {', '.join(unknown)}")

        results = []
        issue_count = 0
        checked_count = 0
        for dataset_name in datasets:
            frame = self._load_dataset(dataset_name, start_date=start_date, end_date=end_date)
            issues = self._check_dataset(dataset_name, frame)
            for issue in issues:
                self.database.insert_quality_issue(**issue)
            results.append(
                {
                    "dataset": dataset_name,
                    "checked_count": int(len(frame)),
                    "issue_count": len(issues),
                }
            )
            checked_count += int(len(frame))
            issue_count += len(issues)
        self.database.record_job_run(
            job_type="quality",
            status="success" if issue_count == 0 else "failed",
            dataset=dataset or "all",
            source_id="qdc",
            start_date=start_date,
            end_date=end_date,
            parameters={"checked_count": checked_count, "issue_count": issue_count},
        )
        return {
            "status": "ok" if issue_count == 0 else "fail",
            "checked_count": checked_count,
            "issue_count": issue_count,
            "results": results,
        }

    def _load_dataset(
        self,
        dataset: str,
        *,
        start_date: str | None,
        end_date: str | None,
    ) -> pd.DataFrame:
        filters = []
        params: list[Any] = []
        date_field = "publish_date" if dataset in {"announcement", "news"} else "trade_date"
        if dataset not in {"stock_basic", "universe_constituent"}:
            if start_date:
                filters.append(f"{date_field} >= ?")
                params.append(start_date)
            if end_date:
                filters.append(f"{date_field} <= ?")
                params.append(end_date)
        where_clause = f"where {' and '.join(filters)}" if filters else ""
        order_by = "instrument" if dataset == "stock_basic" else "trade_date, instrument"
        if dataset == "universe_constituent":
            order_by = "snapshot_date, universe, instrument"
        if dataset == "trade_calendar":
            order_by = "trade_date, calendar_id"
        if dataset in {"announcement", "news"}:
            order_by = "publish_date, instrument"
        with self.database.connect() as conn:
            return conn.execute(
                f"select * from qdc_silver.{dataset} {where_clause} order by {order_by}",
                params,
            ).fetchdf()

    def _check_dataset(self, dataset: str, frame: pd.DataFrame) -> list[dict[str, Any]]:
        checks = {
            "stock_basic": _check_stock_basic,
            "universe_constituent": _check_universe_constituent,
            "trade_calendar": _check_trade_calendar,
            "daily_bar": _check_daily_bar,
            "adj_factor": _check_adj_factor,
            "price_limit": _check_price_limit,
            "trade_status": _check_trade_status,
            "announcement": _check_document_table,
            "news": _check_document_table,
            "daily_news_factor": _check_count_factor,
            "daily_announcement_factor": _check_count_factor,
        }
        return checks[dataset](frame)


def _check_stock_basic(frame: pd.DataFrame) -> list[dict[str, Any]]:
    issues = []
    for row in frame.to_dict("records"):
        if not row.get("instrument") or not row.get("symbol") or not row.get("exchange"):
            issues.append(
                _issue(
                    dataset="stock_basic",
                    source_id=row.get("source_id"),
                    issue_type="missing_stock_identity",
                    entity_key=str(row.get("instrument") or row.get("symbol") or "<unknown>"),
                    message="stock_basic requires instrument, symbol and exchange",
                    observed=row,
                )
            )
    return issues


def _check_universe_constituent(frame: pd.DataFrame) -> list[dict[str, Any]]:
    issues = []
    for row in frame.to_dict("records"):
        if not row.get("universe") or not row.get("instrument"):
            issues.append(
                _issue(
                    dataset="universe_constituent",
                    source_id=row.get("source_id"),
                    issue_type="missing_universe_identity",
                    entity_key=str(row.get("instrument") or "<unknown>"),
                    message="universe_constituent requires universe and instrument",
                    observed=row,
                )
            )
        weight = row.get("weight")
        if not _is_null(weight) and float(weight) < 0:
            issues.append(
                _issue(
                    dataset="universe_constituent",
                    source_id=row.get("source_id"),
                    issue_type="negative_weight",
                    entity_key=f"{row.get('universe')}|{row.get('instrument')}",
                    message="universe_constituent weight must not be negative",
                    observed=row,
                )
            )
    return issues


def _check_trade_calendar(frame: pd.DataFrame) -> list[dict[str, Any]]:
    issues = []
    for row in frame.to_dict("records"):
        if row.get("is_open") is None:
            issues.append(
                _issue(
                    dataset="trade_calendar",
                    source_id=row.get("source_id"),
                    issue_type="missing_open_flag",
                    entity_key=f"{row.get('calendar_id')}|{row.get('trade_date')}",
                    message="trade_calendar requires is_open",
                    observed=row,
                )
            )
    return issues


def _check_daily_bar(frame: pd.DataFrame) -> list[dict[str, Any]]:
    issues = []
    for row in frame.to_dict("records"):
        entity_key = f"{row.get('trade_date')}|{row.get('instrument')}"
        if any(_is_null(row.get(field)) for field in ("open", "high", "low", "close")):
            issues.append(
                _issue(
                    dataset="daily_bar",
                    source_id=row.get("source_id"),
                    issue_type="missing_ohlc",
                    entity_key=entity_key,
                    message="daily_bar requires open, high, low and close",
                    observed=row,
                )
            )
            continue
        low = float(row["low"])
        high = float(row["high"])
        close = float(row["close"])
        if high < low:
            issues.append(
                _issue(
                    dataset="daily_bar",
                    source_id=row.get("source_id"),
                    issue_type="invalid_price_range",
                    entity_key=entity_key,
                    message="daily_bar high is lower than low",
                    observed=row,
                )
            )
        if close < low or close > high:
            issues.append(
                _issue(
                    dataset="daily_bar",
                    source_id=row.get("source_id"),
                    issue_type="close_outside_range",
                    entity_key=entity_key,
                    message="daily_bar close is outside low/high range",
                    observed=row,
                )
            )
        if not _is_null(row.get("volume")) and float(row["volume"]) < 0:
            issues.append(
                _issue(
                    dataset="daily_bar",
                    source_id=row.get("source_id"),
                    issue_type="negative_volume",
                    entity_key=entity_key,
                    message="daily_bar volume must not be negative",
                    observed=row,
                )
            )
    return issues


def _check_adj_factor(frame: pd.DataFrame) -> list[dict[str, Any]]:
    issues = []
    for row in frame.to_dict("records"):
        value = row.get("adj_factor")
        if _is_null(value) or float(value) <= 0:
            issues.append(
                _issue(
                    dataset="adj_factor",
                    source_id=row.get("source_id"),
                    issue_type="invalid_adj_factor",
                    entity_key=f"{row.get('trade_date')}|{row.get('instrument')}",
                    message="adj_factor must be positive",
                    observed=row,
                )
            )
    return issues


def _check_price_limit(frame: pd.DataFrame) -> list[dict[str, Any]]:
    issues = []
    for row in frame.to_dict("records"):
        limit_up = row.get("limit_up")
        limit_down = row.get("limit_down")
        if _is_null(limit_up) or _is_null(limit_down) or float(limit_up) < float(limit_down):
            issues.append(
                _issue(
                    dataset="price_limit",
                    source_id=row.get("source_id"),
                    issue_type="invalid_price_limit",
                    entity_key=f"{row.get('trade_date')}|{row.get('instrument')}",
                    message="price_limit requires limit_up >= limit_down",
                    observed=row,
                )
            )
    return issues


def _check_trade_status(frame: pd.DataFrame) -> list[dict[str, Any]]:
    allowed = {"normal", "halted", "suspended", "resume", "unknown"}
    issues = []
    for row in frame.to_dict("records"):
        if str(row.get("trade_status")).lower() not in allowed:
            issues.append(
                _issue(
                    dataset="trade_status",
                    source_id=row.get("source_id"),
                    issue_type="unknown_trade_status",
                    entity_key=f"{row.get('trade_date')}|{row.get('instrument')}",
                    message="trade_status is not in the known status set",
                    observed=row,
                )
            )
    return issues


def _check_document_table(frame: pd.DataFrame) -> list[dict[str, Any]]:
    issues = []
    dataset = "announcement" if "announcement_id" in frame.columns else "news"
    for row in frame.to_dict("records"):
        entity_key = str(row.get("instrument") or "<unknown>")
        if not row.get("instrument") or not row.get("title"):
            issues.append(
                _issue(
                    dataset=dataset,
                    source_id=row.get("source_id"),
                    issue_type="missing_document_identity",
                    entity_key=entity_key,
                    message=f"{dataset} requires instrument and title",
                    observed=row,
                )
            )
        source_id = str(row.get("source_id") or "")
        if (
            "publish_time" in frame.columns
            and source_id in DOCUMENT_PUBLISH_TIME_REQUIRED_SOURCES
            and _is_null(row.get("publish_time"))
        ):
            issues.append(
                _issue(
                    dataset=dataset,
                    source_id=row.get("source_id"),
                    issue_type="missing_publish_time",
                    entity_key=entity_key,
                    message=f"{dataset} requires explicit publish_time",
                    observed=row,
                )
            )
    return issues


def _check_count_factor(frame: pd.DataFrame) -> list[dict[str, Any]]:
    issues = []
    dataset = "daily_news_factor" if "news_count" in frame.columns else "daily_announcement_factor"
    count_fields = [column for column in frame.columns if column.endswith("_count")]
    sentiment_fields = [column for column in frame.columns if column.endswith("_sentiment_mean")]
    for row in frame.to_dict("records"):
        for value_field in count_fields:
            if _is_null(row.get(value_field)) or float(row[value_field]) < 0:
                issues.append(
                    _issue(
                        dataset=dataset,
                        source_id=row.get("source_id"),
                        issue_type="invalid_count_factor",
                        entity_key=f"{row.get('trade_date')}|{row.get('instrument')}",
                        message=f"{dataset}.{value_field} must not be negative",
                        observed=row,
                    )
                )
        for value_field in sentiment_fields:
            if _is_null(row.get(value_field)) or not -1 <= float(row[value_field]) <= 1:
                issues.append(
                    _issue(
                        dataset=dataset,
                        source_id=row.get("source_id"),
                        issue_type="invalid_sentiment_factor",
                        entity_key=f"{row.get('trade_date')}|{row.get('instrument')}",
                        message=f"{dataset}.{value_field} must be between -1 and 1",
                        observed=row,
                    )
                )
    return issues


def _issue(
    *,
    dataset: str,
    source_id: Any,
    issue_type: str,
    entity_key: str,
    message: str,
    observed: dict[str, Any],
) -> dict[str, Any]:
    return {
        "dataset": dataset,
        "source_id": str(source_id) if source_id is not None else None,
        "severity": "error",
        "issue_type": issue_type,
        "entity_key": entity_key,
        "message": message,
        "observed_value": json.dumps(observed, ensure_ascii=False, sort_keys=True, default=str),
    }


def _is_null(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False
