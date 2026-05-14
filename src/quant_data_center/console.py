"""Read-only local web console for quant_data_center."""

from __future__ import annotations

import json
import math
import mimetypes
import os
import posixpath
import subprocess
import sys
import threading
from collections import deque
from datetime import datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlparse
from uuid import uuid4

import duckdb

from quant_data_center.crawlers.registry import CRAWL_DAILY_SOURCE_IDS
from quant_data_center.exports.qlib import inspect_qlib_provider
from quant_data_center.factor_engine.calendar_align import TradeDayAligner, date_minus_days
from quant_data_center.settings import QdcSettings
from quant_data_center.storage.schema import (
    CONTROL_SCHEMA,
    CONTROL_TABLES,
    SILVER_SCHEMA,
    SILVER_TABLES,
)
from quant_data_center.utils.instruments import instrument_to_symbol, normalize_instrument


STATIC_ROOT = Path(__file__).with_name("console_static")
DEFAULT_LIMIT = 100
MAX_LIMIT = 500
MAX_OFFSET = 10000
MAX_INSTRUMENT_OPTION_LIMIT = 6000
RUN_LOG_LIMIT = 200
MAX_POST_BODY_BYTES = 65536
RAW_OBJECT_SCAN_LIMIT = 1000
DOCUMENT_DETAIL_LOOKBACK_DAYS = 15
DOCUMENT_DETAIL_LIMIT = 1000
DAILY_DOCUMENT_DETAIL_LIMIT = 10000
DAILY_DOCUMENT_SOURCE_IDS = {
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
}
DOCUMENT_SOURCE_PRIORITY = {
    "cninfo_announcement": 0,
    "sse_announcement": 1,
    "sina_finance_news": 0,
    "eastmoney_roll_news": 1,
    "nbd_company_news": 2,
    "sina": 3,
    "wallstreetcn": 4,
    "10jqka": 5,
    "eastmoney": 6,
    "yuncaijing": 7,
    "fenghuang": 8,
    "jinrongjie": 9,
    "cls": 10,
    "yicai": 11,
}
DOCUMENT_LOCAL_CONTENT_FIELDS = ("body_text", "content", "正文", "summary", "摘要")
DOCUMENT_OBJECT_FIELDS = (
    "raw_object_id",
    "body_download_status",
    "body_error_message",
    "body_size_bytes",
    "pdf_url",
    "pdf_object_id",
    "pdf_download_status",
    "pdf_size_bytes",
    "pdf_error_message",
)
DOCUMENT_OBJECT_LAYERS = {"raw", "raw_file", "raw_manifest", "raw_records"}
STALE_RUNNING_MINUTES = 15
COVERAGE_INSTRUMENT_LIMIT = 500
DAILY_PREVIEW_LIMIT = 6000
REQUIRED_DAILY_COVERAGE_DATASETS = ("daily_bar", "adj_factor", "price_limit")
DAILY_COLLECTION_DATASETS = (
    "daily_bar",
    "adj_factor",
    "price_limit",
    "trade_status",
    "announcement",
    "news",
    "daily_news_factor",
    "daily_announcement_factor",
)
DAILY_BATCH_DATASETS = ("daily_bar", "adj_factor", "price_limit", "news")
DAILY_SOURCE_DIMENSIONS = {
    "daily_bar": ("open", "high", "low", "close", "pre_close", "volume", "amount", "vwap"),
    "adj_factor": ("adj_factor",),
    "price_limit": ("limit_up", "limit_down", "prev_close"),
    "trade_status": ("trade_status",),
}
DAILY_DOCUMENT_DIMENSIONS = {
    "announcement": ("publish_time", "title"),
    "news": ("publish_time", "title"),
}
DAILY_STAGE_DATASETS = ("daily_bar", "adj_factor", "price_limit")
DAILY_JOB_STAGES = ("build_factors", "sync_parquet", "quality", "export_qlib")
DAILY_CONSOLE_SOURCE_IDS = (*CRAWL_DAILY_SOURCE_IDS, "nbd_company_news")
QUALITY_DIMENSION_LABELS = {
    "completeness": "完整性 (Completeness)",
    "freshness": "及时性 (Freshness)",
    "validity": "有效性 (Validity)",
    "source": "供应商可靠性 (Vendor reliability)",
}
QUALITY_ISSUE_DIMENSIONS = {
    "missing_ohlc": "completeness",
    "missing_document_identity": "completeness",
    "missing_publish_time": "freshness",
    "invalid_adj_factor": "validity",
    "invalid_price_limit": "validity",
    "invalid_price_range": "validity",
    "close_outside_range": "validity",
    "negative_volume": "validity",
    "unknown_trade_status": "validity",
    "invalid_count_factor": "validity",
    "invalid_sentiment_factor": "validity",
}
DAILY_RAW_WIDE_TABLES = {
    "daily_bar": [
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "volume",
        "amount",
        "vwap",
    ],
    "adj_factor": ["adj_factor", "factor_type"],
    "price_limit": ["limit_up", "limit_down", "prev_close", "limit_rule"],
    "trade_status": ["trade_status", "halt_reason", "source_update_time"],
}
DAILY_FACTOR_WIDE_TABLES = {
    "daily_bar": [
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "volume",
        "amount",
        "vwap",
    ],
    "adj_factor": ["adj_factor", "factor_type"],
    "price_limit": ["limit_up", "limit_down", "prev_close", "limit_rule"],
    "trade_status": ["trade_status", "halt_reason", "source_update_time"],
    "daily_news_factor": [
        "news_count",
        "news_sentiment_mean",
        "news_positive_count",
        "news_negative_count",
        "news_weighted_sentiment_sum",
        "news_importance_sum",
        "news_growth_count",
        "news_risk_count",
        "news_financing_count",
        "news_contract_count",
        "news_buyback_count",
        "news_shareholder_change_count",
        "news_regulatory_count",
        "news_litigation_count",
        "news_performance_count",
    ],
    "daily_announcement_factor": [
        "announcement_count",
        "announcement_sentiment_mean",
        "announcement_positive_count",
        "announcement_negative_count",
        "announcement_weighted_sentiment_sum",
        "announcement_importance_sum",
        "announcement_growth_count",
        "announcement_risk_count",
        "announcement_financing_count",
        "announcement_operation_count",
        "announcement_contract_count",
        "announcement_buyback_count",
        "announcement_shareholder_change_count",
        "announcement_regulatory_count",
        "announcement_litigation_count",
        "announcement_performance_count",
    ],
}
RAW_PREVIEW_DATASETS = (
    "stock_basic",
    "universe_constituent",
    "daily_bar",
    "adj_factor",
    "price_limit",
    "trade_status",
    "announcement",
    "news",
)
INSTRUMENT_COVERAGE_DATASETS = (
    "stock_basic",
    "universe_constituent",
    "daily_bar",
    "adj_factor",
    "price_limit",
    "trade_status",
    "announcement",
    "news",
    "daily_news_factor",
    "daily_announcement_factor",
)
OBSERVED_REFERENCE_DATASETS = (
    "daily_bar",
    "adj_factor",
    "price_limit",
    "announcement",
    "news",
    "daily_news_factor",
    "daily_announcement_factor",
)
ALL_INSTRUMENT_COVERAGE_DIMENSIONS = (
    "stock_basic",
    "universe_constituent",
    "daily_bar",
    "adj_factor",
    "price_limit",
    "trade_status",
    "news",
    "announcement",
    "daily_news_factor",
    "daily_announcement_factor",
)
INSTRUMENT_TIMELINE_TABLES = {
    "daily_bar": [
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "volume",
        "amount",
        "vwap",
    ],
    "adj_factor": ["adj_factor", "factor_type"],
    "price_limit": ["limit_up", "limit_down", "prev_close", "limit_rule"],
    "trade_status": ["trade_status", "halt_reason", "source_update_time"],
    "daily_news_factor": [
        "news_count",
        "news_sentiment_mean",
        "news_positive_count",
        "news_negative_count",
        "news_weighted_sentiment_sum",
        "news_importance_sum",
        "news_growth_count",
        "news_risk_count",
        "news_financing_count",
        "news_contract_count",
        "news_buyback_count",
        "news_shareholder_change_count",
        "news_regulatory_count",
        "news_litigation_count",
        "news_performance_count",
    ],
    "daily_announcement_factor": [
        "announcement_count",
        "announcement_sentiment_mean",
        "announcement_positive_count",
        "announcement_negative_count",
        "announcement_weighted_sentiment_sum",
        "announcement_importance_sum",
        "announcement_growth_count",
        "announcement_risk_count",
        "announcement_financing_count",
        "announcement_operation_count",
        "announcement_contract_count",
        "announcement_buyback_count",
        "announcement_shareholder_change_count",
        "announcement_regulatory_count",
        "announcement_litigation_count",
        "announcement_performance_count",
    ],
}
RAW_FACTOR_INPUT_PURPOSES = {
    "stock_basic": "标的基础资料，用来识别代码、名称、交易所和行业",
    "universe_constituent": "股票池成分，用来确定研究范围和参考标的",
    "daily_bar": "行情价格输入，用来生成开高低收、成交量和成交额因子",
    "adj_factor": "复权输入，用来处理分红送转后的价格连续性",
    "price_limit": "涨跌停输入，用来生成涨停价、跌停价和交易限制特征",
    "trade_status": "交易状态输入，用来识别正常交易、停牌和异常状态",
    "announcement": "公告文本输入，用来生成公告数量、情绪和事件类型因子",
    "news": "新闻文本输入，用来生成新闻数量、情绪和事件类型因子",
}


class QdcConsoleData:
    """Read-only query layer used by the local console HTTP handlers."""

    def __init__(self, settings: QdcSettings) -> None:
        self.settings = settings

    def overview(self) -> dict[str, Any]:
        if not self.settings.database_path.exists():
            return self._empty_payload()

        with self._connect() as conn:
            control_tables = self._existing_tables(conn, CONTROL_SCHEMA)
            silver_tables = self._existing_tables(conn, SILVER_SCHEMA)
            table_counts = self._table_counts(conn, CONTROL_SCHEMA, CONTROL_TABLES, control_tables)
            silver_counts = self._table_counts(conn, SILVER_SCHEMA, SILVER_TABLES, silver_tables)
            return {
                "status": "ok",
                "database_exists": True,
                "database_path": str(self.settings.database_path),
                "settings": self.settings.as_dict(),
                "table_counts": table_counts,
                "silver_table_counts": silver_counts,
                "backfill_status_counts": self._status_counts(
                    conn,
                    "backfill_task",
                    control_tables,
                ),
                "job_status_counts": self._status_counts(conn, "job_run", control_tables),
                "quality_status_counts": self._status_counts(
                    conn,
                    "quality_issue",
                    control_tables,
                ),
                "source_layer_counts": self._source_layer_counts(conn, control_tables),
                "latest_job_runs": self._job_runs(conn, control_tables, limit=8),
                "latest_backfill_tasks": self._backfill_tasks(
                    conn,
                    control_tables,
                    status=None,
                    dataset=None,
                    limit=8,
                ),
                "backfill_progress": self._backfill_progress(conn, control_tables),
                "watermarks": self._watermarks(conn, control_tables),
                "latest_qlib_exports": self._qlib_exports(conn, control_tables, limit=5),
                "qlib_provider_status": self.qlib_provider_status(),
                "data_coverage": self._data_coverage(conn, silver_tables),
            }

    def backfill_tasks(
        self,
        *,
        status: str | None = None,
        dataset: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        if not self.settings.database_path.exists():
            return {"status": "ok", "task_count": 0, "tasks": []}
        with self._connect() as conn:
            control_tables = self._existing_tables(conn, CONTROL_SCHEMA)
            tasks = self._backfill_tasks(
                conn,
                control_tables,
                status=status,
                dataset=dataset,
                limit=_clamp_limit(limit),
            )
        return {"status": "ok", "task_count": len(tasks), "tasks": tasks}

    def job_runs(self, *, limit: int | None = None) -> dict[str, Any]:
        if not self.settings.database_path.exists():
            return {"status": "ok", "job_count": 0, "jobs": []}
        with self._connect() as conn:
            control_tables = self._existing_tables(conn, CONTROL_SCHEMA)
            jobs = self._job_runs(conn, control_tables, limit=_clamp_limit(limit))
        return {"status": "ok", "job_count": len(jobs), "jobs": jobs}

    def watermarks(self) -> dict[str, Any]:
        if not self.settings.database_path.exists():
            return {"status": "ok", "watermark_count": 0, "watermarks": []}
        with self._connect() as conn:
            control_tables = self._existing_tables(conn, CONTROL_SCHEMA)
            watermarks = self._watermarks(conn, control_tables)
        return {
            "status": "ok",
            "watermark_count": len(watermarks),
            "watermarks": watermarks,
        }

    def quality_issues(
        self,
        *,
        dataset: str | None = None,
        status: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        if not self.settings.database_path.exists():
            return {"status": "ok", "issue_count": 0, "issues": []}
        with self._connect() as conn:
            control_tables = self._existing_tables(conn, CONTROL_SCHEMA)
            issues = self._quality_issues(
                conn,
                control_tables,
                dataset=dataset,
                status=status,
                limit=_clamp_limit(limit),
            )
        return {"status": "ok", "issue_count": len(issues), "issues": issues}

    def source_objects(
        self,
        *,
        dataset: str | None = None,
        layer: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        if not self.settings.database_path.exists():
            return {"status": "ok", "object_count": 0, "objects": []}
        with self._connect() as conn:
            control_tables = self._existing_tables(conn, CONTROL_SCHEMA)
            objects = self._source_objects(
                conn,
                control_tables,
                dataset=dataset,
                layer=layer,
                limit=_clamp_limit(limit),
            )
        return {"status": "ok", "object_count": len(objects), "objects": objects}

    def qlib_provider_status(self) -> dict[str, Any]:
        return inspect_qlib_provider(self.settings)

    def instruments(
        self,
        *,
        query: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        row_limit = _clamp_instrument_limit(limit, default=500)
        if not self.settings.database_path.exists():
            rows = _configured_instrument_options(self.settings, query=query, limit=row_limit)
            return {"status": "ok", "instrument_count": len(rows), "instruments": rows}

        with self._connect() as conn:
            silver_tables = self._existing_tables(conn, SILVER_SCHEMA)
            rows = self._instrument_options(
                conn,
                silver_tables=silver_tables,
                query=query,
                limit=row_limit,
            )
        return {"status": "ok", "instrument_count": len(rows), "instruments": rows}

    def daily_collection_status(self, *, date: str | None = None) -> dict[str, Any]:
        qlib_provider_status = self.qlib_provider_status()
        if not self.settings.database_path.exists():
            resolved_date = date or datetime.now().date().isoformat()
            payload = _empty_daily_collection_status(
                date=resolved_date,
                database_path=str(self.settings.database_path),
            )
            payload["qlib_provider_status"] = qlib_provider_status
            payload["verdict"] = _daily_verdict(
                database_exists=False,
                expected_count=0,
                collection=payload["collection"],
                batches=payload["batches"],
                quality_summary=payload["quality_summary"],
                source_summary_rows=[],
                qlib_provider_status=qlib_provider_status,
            )
            return payload

        with self._connect() as conn:
            control_tables = self._existing_tables(conn, CONTROL_SCHEMA)
            silver_tables = self._existing_tables(conn, SILVER_SCHEMA)
            resolved_date = self._resolve_daily_status_date(
                conn,
                silver_tables=silver_tables,
                control_tables=control_tables,
                requested_date=date,
            )
            reference_rows, reference_source = self._daily_reference_rows(
                conn,
                silver_tables=silver_tables,
                date=resolved_date,
            )
            dataset_rows = self._daily_dataset_rows(
                conn,
                silver_tables=silver_tables,
                date=resolved_date,
                expected_count=len(reference_rows),
            )
            required_sets = {
                dataset: self._daily_instrument_set(
                    conn,
                    silver_tables=silver_tables,
                    dataset=dataset,
                    date=resolved_date,
                )
                for dataset in REQUIRED_DAILY_COVERAGE_DATASETS
            }
            batch_rows = self._daily_batch_rows(
                conn,
                control_tables=control_tables,
                date=resolved_date,
            )
            batch_task_rows = self._daily_batch_task_rows(
                conn,
                control_tables=control_tables,
                date=resolved_date,
            )
            crawl_task_rows = self._daily_crawl_task_rows(
                conn,
                control_tables=control_tables,
                date=resolved_date,
            )
            source_dimension_rows = self._daily_source_dimension_rows(
                conn,
                control_tables=control_tables,
                silver_tables=silver_tables,
                date=resolved_date,
                expected_count=len(reference_rows),
            )
            quality_issue_rows = self._daily_quality_issue_rows(
                conn,
                control_tables=control_tables,
                date=resolved_date,
            )
            source_summary_rows = self._daily_source_summary_rows(
                conn,
                control_tables=control_tables,
                silver_tables=silver_tables,
                date=resolved_date,
                source_dimension_rows=source_dimension_rows,
            )
            job_stage_rows = self._daily_job_stage_rows(
                conn,
                control_tables=control_tables,
                date=resolved_date,
            )
        issue_rows = _daily_issue_rows(reference_rows, required_sets)
        daily_bar_count = len(required_sets.get("daily_bar", set()))
        core_complete_count = len(set.intersection(*required_sets.values())) if required_sets else 0
        expected_count = len(reference_rows)
        batch_summary = _daily_batch_summary(batch_rows)
        collection = {
            "collected_instrument_count": daily_bar_count,
            "remaining_instrument_count": max(expected_count - daily_bar_count, 0),
            "collection_percent": _percent(daily_bar_count, expected_count),
            "core_complete_instrument_count": core_complete_count,
            "core_complete_percent": _percent(core_complete_count, expected_count),
            "problem_instrument_count": len(issue_rows),
        }
        quality_summary = _daily_quality_summary(
            collection=collection,
            batches=batch_summary,
            source_dimension_rows=source_dimension_rows,
            quality_issue_rows=quality_issue_rows,
        )
        stage_rows = _daily_stage_rows(
            expected_count=expected_count,
            collection=collection,
            batches=batch_summary,
            dataset_rows=dataset_rows,
            source_summary_rows=source_summary_rows,
            quality_summary=quality_summary,
            job_stage_rows=job_stage_rows,
            qlib_provider_status=qlib_provider_status,
        )
        verdict = _daily_verdict(
            database_exists=True,
            expected_count=expected_count,
            collection=collection,
            batches=batch_summary,
            quality_summary=quality_summary,
            source_summary_rows=source_summary_rows,
            qlib_provider_status=qlib_provider_status,
        )
        return {
            "status": "ok",
            "date": resolved_date,
            "database_exists": True,
            "database_path": str(self.settings.database_path),
            "updated_at": datetime.now().replace(microsecond=0).isoformat(),
            "reference": {
                "source": reference_source,
                "expected_instrument_count": expected_count,
            },
            "verdict": verdict,
            "collection": collection,
            "batches": batch_summary,
            "stage_rows": stage_rows,
            "quality_summary": quality_summary,
            "qlib_provider_status": qlib_provider_status,
            "source_summary_rows": source_summary_rows,
            "batch_rows": batch_rows,
            "batch_task_rows": batch_task_rows,
            "crawl_task_rows": crawl_task_rows,
            "dataset_rows": dataset_rows,
            "source_dimension_rows": source_dimension_rows,
            "quality_issue_rows": quality_issue_rows,
            "issue_rows": issue_rows,
        }

    def daily_wide_preview(
        self,
        *,
        date: str | None = None,
        mode: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        preview_mode = mode if mode in {"raw", "factor"} else "raw"
        row_limit = _clamp_instrument_limit(limit, default=DAILY_PREVIEW_LIMIT)
        if not self.settings.database_path.exists():
            resolved_date = date or datetime.now().date().isoformat()
            return {
                "status": "ok",
                "date": resolved_date,
                "mode": preview_mode,
                "row_count": 0,
                "hidden_count": 0,
                "columns": _daily_preview_columns(preview_mode),
                "rows": [],
            }

        with self._connect() as conn:
            control_tables = self._existing_tables(conn, CONTROL_SCHEMA)
            silver_tables = self._existing_tables(conn, SILVER_SCHEMA)
            resolved_date = self._resolve_daily_status_date(
                conn,
                silver_tables=silver_tables,
                control_tables=control_tables,
                requested_date=date,
            )
            reference_rows, reference_source = self._daily_reference_rows(
                conn,
                silver_tables=silver_tables,
                date=resolved_date,
            )
            rows = self._daily_wide_rows(
                conn,
                control_tables=control_tables,
                silver_tables=silver_tables,
                date=resolved_date,
                mode=preview_mode,
                reference_rows=reference_rows,
                limit=row_limit,
            )
        return {
            "status": "ok",
            "date": resolved_date,
            "mode": preview_mode,
            "reference_source": reference_source,
            "row_count": len(rows),
            "hidden_count": max(len(reference_rows) - len(rows), 0),
            "columns": _daily_preview_columns(preview_mode),
            "rows": rows,
        }

    def _resolve_daily_status_date(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        silver_tables: set[str],
        control_tables: set[str],
        requested_date: str | None,
    ) -> str:
        if requested_date:
            return requested_date
        if "daily_bar" in silver_tables:
            value = conn.execute(f"select max(trade_date) from {SILVER_SCHEMA}.daily_bar").fetchone()[0]
            if value:
                return str(value)
        if "job_run" in control_tables:
            value = conn.execute(
                f"""
                select max(end_date)
                from {CONTROL_SCHEMA}.job_run
                where job_type in ('daily', 'daily_pipeline')
                """
            ).fetchone()[0]
            if value:
                return str(value)
        return datetime.now().date().isoformat()

    def _daily_reference_rows(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        silver_tables: set[str],
        date: str,
    ) -> tuple[list[dict[str, Any]], str]:
        if "stock_basic" in silver_tables:
            rows = _query_dicts(
                conn,
                f"""
                select instrument, symbol, exchange, name, industry, is_active
                from {SILVER_SCHEMA}.stock_basic
                where coalesce(is_active, true) = true
                order by instrument
                """,
            )
            if rows:
                return rows, "stock_basic_active"
        if "universe_constituent" in silver_tables:
            snapshot = conn.execute(
                f"""
                select max(snapshot_date)
                from {SILVER_SCHEMA}.universe_constituent
                where snapshot_date <= ?
                """,
                [date],
            ).fetchone()[0]
            if snapshot:
                rows = _query_dicts(
                    conn,
                    f"""
                    select instrument, symbol, exchange, name, null as industry, true as is_active
                    from {SILVER_SCHEMA}.universe_constituent
                    where snapshot_date = ?
                    order by instrument
                    """,
                    [snapshot],
                )
                if rows:
                    return rows, f"universe_constituent:{snapshot}"
        observed = self._observed_instruments(
            conn,
            silver_tables=silver_tables,
            date=date,
            datasets=OBSERVED_REFERENCE_DATASETS,
        )
        if observed:
            identity = self._instrument_identity_by_instrument(
                conn,
                silver_tables=silver_tables,
                instruments=observed,
            )
            return [identity[instrument] for instrument in observed], f"observed:{date}"
        rows = [_default_instrument_identity(instrument) for instrument in _configured_instruments(self.settings)]
        return rows, "config"

    def _daily_dataset_rows(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        silver_tables: set[str],
        date: str,
        expected_count: int,
    ) -> list[dict[str, Any]]:
        rows = []
        for dataset in DAILY_COLLECTION_DATASETS:
            if dataset not in silver_tables:
                rows.append(
                    {
                        "dataset": dataset,
                        "row_count": 0,
                        "instrument_count": 0,
                        "expected_instrument_count": expected_count
                        if dataset in REQUIRED_DAILY_COVERAGE_DATASETS
                        else None,
                        "missing_instrument_count": expected_count
                        if dataset in REQUIRED_DAILY_COVERAGE_DATASETS
                        else None,
                        "coverage_percent": 0
                        if dataset in REQUIRED_DAILY_COVERAGE_DATASETS
                        else None,
                        "latest_updated_at": None,
                    }
                )
                continue
            columns = self._columns(conn, SILVER_SCHEMA, dataset)
            date_column = _preferred_date_column(columns)
            if date_column not in {"trade_date", "publish_date", "snapshot_date"}:
                date_column = None
            filters = []
            params: list[Any] = []
            if date_column:
                filters.append(f"{date_column} = ?")
                params.append(date)
            document_source_ids = DAILY_DOCUMENT_SOURCE_IDS.get(dataset, ())
            if document_source_ids and "source_id" in columns:
                filters.append(
                    "source_id in (" + ", ".join("?" for _ in document_source_ids) + ")"
                )
                params.extend(document_source_ids)
            where_clause = f"where {' and '.join(filters)}" if filters else ""
            row_count = int(
                conn.execute(
                    f"select count(*) from {SILVER_SCHEMA}.{dataset} {where_clause}",
                    params,
                ).fetchone()[0]
                or 0
            )
            instrument_count = None
            if "instrument" in columns:
                instrument_count = int(
                    conn.execute(
                        f"""
                        select count(distinct instrument)
                        from {SILVER_SCHEMA}.{dataset}
                        {where_clause}
                        """,
                        params,
                    ).fetchone()[0]
                    or 0
                )
            latest_updated_at = None
            if "updated_at" in columns:
                latest_updated_at = conn.execute(
                    f"select max(updated_at) from {SILVER_SCHEMA}.{dataset} {where_clause}",
                    params,
                ).fetchone()[0]
            expected = expected_count if dataset in REQUIRED_DAILY_COVERAGE_DATASETS else None
            missing = max(expected - int(instrument_count or 0), 0) if expected is not None else None
            rows.append(
                {
                    "dataset": dataset,
                    "row_count": row_count,
                    "instrument_count": instrument_count,
                    "expected_instrument_count": expected,
                    "missing_instrument_count": missing,
                    "coverage_percent": _percent(int(instrument_count or 0), expected)
                    if expected is not None
                    else None,
                    "latest_updated_at": latest_updated_at,
                }
            )
        return rows

    def _daily_source_dimension_rows(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        control_tables: set[str],
        silver_tables: set[str],
        date: str,
        expected_count: int,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for dataset, dimensions in DAILY_SOURCE_DIMENSIONS.items():
            source_ids = self._daily_dataset_source_ids(
                conn,
                control_tables=control_tables,
                silver_tables=silver_tables,
                dataset=dataset,
                date=date,
            )
            if not source_ids:
                source_ids = [""]
            task_stats = self._daily_task_failure_stats(
                conn,
                control_tables=control_tables,
                dataset=dataset,
                date=date,
            )
            columns = self._columns(conn, SILVER_SCHEMA, dataset) if dataset in silver_tables else []
            for source_id in source_ids:
                source_task_stats = task_stats.get(source_id, {})
                for dimension in dimensions:
                    success_count = 0
                    if (
                        dataset in silver_tables
                        and "source_id" in columns
                        and "instrument" in columns
                        and "trade_date" in columns
                        and dimension in columns
                        and source_id
                    ):
                        success_count = int(
                            conn.execute(
                                f"""
                                select count(distinct instrument)
                                from {SILVER_SCHEMA}.{dataset}
                                where trade_date = ?
                                  and source_id = ?
                                  and {dimension} is not null
                                """,
                                [date, source_id],
                            ).fetchone()[0]
                            or 0
                        )
                    failed_count = max(expected_count - success_count, 0)
                    explicit_failed = int(source_task_stats.get("failed_symbol_count") or 0)
                    if explicit_failed:
                        failed_count = max(failed_count, explicit_failed)
                    rows.append(
                        {
                            "dataset": dataset,
                            "dimension": dimension,
                            "source_id": source_id or "-",
                            "expected_instrument_count": expected_count,
                            "success_instrument_count": success_count,
                            "failed_instrument_count": failed_count,
                            "timeout_count": int(source_task_stats.get("timeout_count") or 0),
                            "error_count": int(source_task_stats.get("error_count") or 0),
                            "failed_task_count": int(source_task_stats.get("failed_task_count") or 0),
                            "last_error": source_task_stats.get("last_error"),
                        }
                    )

        for dataset, dimensions in DAILY_DOCUMENT_DIMENSIONS.items():
            source_ids = self._daily_dataset_source_ids(
                conn,
                control_tables=control_tables,
                silver_tables=silver_tables,
                dataset=dataset,
                date=date,
            )
            task_stats = self._daily_crawl_failure_stats(
                conn,
                control_tables=control_tables,
                dataset=dataset,
                date=date,
            )
            columns = self._columns(conn, SILVER_SCHEMA, dataset) if dataset in silver_tables else []
            for source_id in source_ids or sorted(task_stats):
                source_task_stats = task_stats.get(source_id, {})
                for dimension in dimensions:
                    present_count = None
                    if (
                        dataset in silver_tables
                        and "source_id" in columns
                        and "publish_date" in columns
                        and dimension in columns
                        and source_id
                    ):
                        present_count = int(
                            conn.execute(
                                f"""
                                select count(*)
                                from {SILVER_SCHEMA}.{dataset}
                                where publish_date = ?
                                  and source_id = ?
                                  and {dimension} is not null
                                """,
                                [date, source_id],
                            ).fetchone()[0]
                            or 0
                        )
                    rows.append(
                        {
                            "dataset": dataset,
                            "dimension": dimension,
                            "source_id": source_id or "-",
                            "expected_instrument_count": None,
                            "success_instrument_count": present_count,
                            "failed_instrument_count": None,
                            "timeout_count": int(source_task_stats.get("timeout_count") or 0),
                            "error_count": int(source_task_stats.get("error_count") or 0),
                            "failed_task_count": int(source_task_stats.get("failed_task_count") or 0),
                            "last_error": source_task_stats.get("last_error"),
                        }
                    )
        rows.sort(
            key=lambda row: (
                -(int(row.get("failed_instrument_count") or 0)),
                -(int(row.get("failed_task_count") or 0)),
                str(row.get("dataset") or ""),
                str(row.get("dimension") or ""),
                str(row.get("source_id") or ""),
            )
        )
        return rows

    def _daily_quality_issue_rows(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        control_tables: set[str],
        date: str,
    ) -> list[dict[str, Any]]:
        if "quality_issue" not in control_tables:
            return []
        return _query_dicts(
            conn,
            f"""
            select *
            from {CONTROL_SCHEMA}.quality_issue
            where status <> 'closed'
              and (
                entity_key like ?
                or observed_value like ?
              )
            order by created_at desc, severity, dataset, issue_type
            limit 50
            """,
            [f"{date}|%", f"%{date}%"],
        )

    def _daily_source_summary_rows(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        control_tables: set[str],
        silver_tables: set[str],
        date: str,
        source_dimension_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        raw_counts = self._daily_source_object_counts(
            conn,
            control_tables=control_tables,
            date=date,
        )
        document_metrics = self._daily_document_source_metrics(
            conn,
            control_tables=control_tables,
            date=date,
        )
        crawl_counts = self._daily_crawl_run_counts(
            conn,
            control_tables=control_tables,
            date=date,
        )
        silver_counts = self._daily_silver_source_counts(
            conn,
            silver_tables=silver_tables,
            date=date,
        )

        by_dataset: dict[tuple[str, str], dict[str, Any]] = {}
        for row in source_dimension_rows:
            source_id = str(row.get("source_id") or "-")
            dataset = str(row.get("dataset") or "-")
            item = by_dataset.setdefault(
                (source_id, dataset),
                {
                    "source_id": source_id,
                    "dataset": dataset,
                    "expected_instrument_count": None,
                    "success_instrument_count": 0,
                    "failed_instrument_count": 0,
                    "timeout_count": 0,
                    "error_count": 0,
                    "failed_task_count": 0,
                    "last_error": None,
                },
            )
            expected = row.get("expected_instrument_count")
            if expected is not None:
                item["expected_instrument_count"] = max(
                    int(item.get("expected_instrument_count") or 0),
                    int(expected or 0),
                )
            item["success_instrument_count"] = max(
                int(item.get("success_instrument_count") or 0),
                int(row.get("success_instrument_count") or 0),
            )
            item["failed_instrument_count"] = max(
                int(item.get("failed_instrument_count") or 0),
                int(row.get("failed_instrument_count") or 0),
            )
            item["timeout_count"] = max(
                int(item.get("timeout_count") or 0),
                int(row.get("timeout_count") or 0),
            )
            item["error_count"] = max(
                int(item.get("error_count") or 0),
                int(row.get("error_count") or 0),
            )
            item["failed_task_count"] = max(
                int(item.get("failed_task_count") or 0),
                int(row.get("failed_task_count") or 0),
            )
            if row.get("last_error") and not item.get("last_error"):
                item["last_error"] = row.get("last_error")

        summaries: dict[str, dict[str, Any]] = {}

        def ensure(source_id: str) -> dict[str, Any]:
            return summaries.setdefault(
                source_id,
                {
                    "source_id": source_id,
                    "datasets": set(),
                    "dataset_count": 0,
                    "expected_instrument_count": None,
                    "success_instrument_count": 0,
                    "failed_instrument_count": 0,
                    "timeout_count": 0,
                    "error_count": 0,
                    "failed_task_count": 0,
                    "raw_object_count": 0,
                    "source_object_count": 0,
                    "silver_row_count": 0,
                    "document_count": 0,
                    "planned_count": 0,
                    "provider_record_count": 0,
                    "manifest_count": 0,
                    "empty_result_count": 0,
                    "empty_result_rate": None,
                    "duplicate_record_count": 0,
                    "duplicate_rate": None,
                    "parse_failed_count": 0,
                    "parse_failed_rate": None,
                    "parsed_unique_record_count": 0,
                    "mapped_source_record_count": 0,
                    "mapping_failed_count": 0,
                    "mapping_rate": None,
                    "last_error": None,
                },
            )

        for (source_id, dataset), item in by_dataset.items():
            summary = ensure(source_id)
            summary["datasets"].add(dataset)
            if item.get("expected_instrument_count") is not None:
                summary["expected_instrument_count"] = max(
                    int(summary.get("expected_instrument_count") or 0),
                    int(item.get("expected_instrument_count") or 0),
                )
            summary["success_instrument_count"] = max(
                int(summary.get("success_instrument_count") or 0),
                int(item.get("success_instrument_count") or 0),
            )
            summary["failed_instrument_count"] = max(
                int(summary.get("failed_instrument_count") or 0),
                int(item.get("failed_instrument_count") or 0),
            )
            summary["timeout_count"] += int(item.get("timeout_count") or 0)
            summary["error_count"] += int(item.get("error_count") or 0)
            summary["failed_task_count"] += int(item.get("failed_task_count") or 0)
            if item.get("last_error") and not summary.get("last_error"):
                summary["last_error"] = item.get("last_error")

        for (source_id, dataset), counts in raw_counts.items():
            summary = ensure(source_id)
            summary["datasets"].add(dataset)
            summary["raw_object_count"] += int(counts.get("raw_object_count") or 0)
            summary["source_object_count"] += int(counts.get("source_object_count") or 0)

        for (source_id, dataset), counts in crawl_counts.items():
            summary = ensure(source_id)
            summary["datasets"].add(dataset)
            summary["planned_count"] += int(counts.get("planned_count") or 0)
            summary["document_count"] += int(counts.get("document_count") or 0)
            summary["raw_object_count"] += int(counts.get("raw_object_count") or 0)
            summary["error_count"] += int(counts.get("failed_count") or 0)
            if counts.get("last_error") and not summary.get("last_error"):
                summary["last_error"] = counts.get("last_error")

        for source_id, metrics in document_metrics.items():
            summary = ensure(source_id)
            if metrics.get("dataset"):
                summary["datasets"].add(str(metrics["dataset"]))
            for key in (
                "provider_record_count",
                "manifest_count",
                "empty_result_count",
                "duplicate_record_count",
                "parse_failed_count",
                "parsed_unique_record_count",
                "mapped_source_record_count",
                "mapping_failed_count",
            ):
                summary[key] += int(metrics.get(key) or 0)

        for (source_id, dataset), row_count in silver_counts.items():
            summary = ensure(source_id)
            summary["datasets"].add(dataset)
            summary["silver_row_count"] += int(row_count or 0)

        for source_id in DAILY_CONSOLE_SOURCE_IDS:
            ensure(source_id)

        rows = []
        for summary in summaries.values():
            source_id = str(summary.get("source_id") or "-")
            dataset_names = sorted(summary.pop("datasets"))
            expected = summary.get("expected_instrument_count")
            success = int(summary.get("success_instrument_count") or 0)
            failed = int(summary.get("failed_instrument_count") or 0)
            timeout = int(summary.get("timeout_count") or 0)
            error = int(summary.get("error_count") or 0)
            failed_task = int(summary.get("failed_task_count") or 0)
            provider_records = int(summary.get("provider_record_count") or 0)
            manifest_count = int(summary.get("manifest_count") or 0)
            summary["empty_result_rate"] = _ratio(
                int(summary.get("empty_result_count") or 0),
                manifest_count,
            )
            summary["duplicate_rate"] = _ratio(
                int(summary.get("duplicate_record_count") or 0),
                provider_records,
            )
            summary["parse_failed_rate"] = _ratio(
                int(summary.get("parse_failed_count") or 0),
                provider_records,
            )
            summary["mapping_rate"] = _ratio(
                int(summary.get("mapped_source_record_count") or 0),
                int(summary.get("parsed_unique_record_count") or 0),
            )
            if timeout or error or failed_task:
                state = "failed"
            elif source_id == "nbd_company_news" and not (
                success or summary.get("raw_object_count") or summary.get("silver_row_count")
            ):
                state = "manual"
            elif summary.get("empty_result_count") and not summary.get("silver_row_count"):
                state = "empty"
            elif failed:
                state = "partial"
            elif success or summary.get("raw_object_count") or summary.get("silver_row_count"):
                state = "success"
            else:
                state = "empty"
            rows.append(
                {
                    **summary,
                    "datasets": dataset_names,
                    "dataset_count": len(dataset_names),
                    "state": state,
                    "success_percent": _percent(success, expected) if expected else None,
                }
            )
        rows.sort(
            key=lambda row: (
                str(row.get("state") or "") != "failed",
                _document_source_priority(row.get("source_id")),
                -(int(row.get("failed_instrument_count") or 0)),
                -(int(row.get("timeout_count") or 0)),
                -(int(row.get("error_count") or 0)),
                str(row.get("source_id") or ""),
            )
        )
        return rows

    def _daily_document_source_metrics(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        control_tables: set[str],
        date: str,
    ) -> dict[str, dict[str, Any]]:
        if "source_object" not in control_tables:
            return {}
        rows = _query_dicts(
            conn,
            f"""
            select source_id, dataset, uri, created_at
            from {CONTROL_SCHEMA}.source_object
            where layer = 'raw_manifest'
              and dataset in ('announcement', 'news')
              and (uri like ? or uri like ?)
            order by created_at desc
            limit ?
            """,
            [f"%documents\\{date}\\%", f"%documents/{date}/%", RAW_OBJECT_SCAN_LIMIT],
        )
        metrics: dict[str, dict[str, Any]] = {}
        for row in rows:
            uri = str(row.get("uri") or "")
            if f"/documents/{date}/" not in uri.replace("\\", "/"):
                continue
            path = Path(uri)
            if not path.is_file():
                continue
            try:
                manifest = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if str(manifest.get("partition_value") or "") != date:
                continue
            source_id = str(manifest.get("source_id") or row.get("source_id") or "-")
            item = metrics.setdefault(
                source_id,
                {
                    "source_id": source_id,
                    "dataset": manifest.get("dataset") or row.get("dataset"),
                    "provider_record_count": 0,
                    "manifest_count": 0,
                    "empty_result_count": 0,
                    "duplicate_record_count": 0,
                    "parse_failed_count": 0,
                    "parsed_unique_record_count": 0,
                    "mapped_source_record_count": 0,
                    "mapping_failed_count": 0,
                },
            )
            item["manifest_count"] += 1
            for key in (
                "provider_record_count",
                "empty_result_count",
                "duplicate_record_count",
                "parse_failed_count",
                "parsed_unique_record_count",
                "mapped_source_record_count",
                "mapping_failed_count",
            ):
                item[key] += int(manifest.get(key) or 0)
        return metrics

    def _daily_source_object_counts(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        control_tables: set[str],
        date: str,
    ) -> dict[tuple[str, str], dict[str, int]]:
        if "source_object" not in control_tables:
            return {}
        rows = _query_dicts(
            conn,
            f"""
            select
              source_id,
              dataset,
              count(*) as source_object_count,
              sum(case when layer like 'raw%' then 1 else 0 end) as raw_object_count
            from {CONTROL_SCHEMA}.source_object
            where cast(created_at as date) = ?
            group by source_id, dataset
            """,
            [date],
        )
        return {
            (str(row.get("source_id") or "-"), str(row.get("dataset") or "-")): {
                "source_object_count": int(row.get("source_object_count") or 0),
                "raw_object_count": int(row.get("raw_object_count") or 0),
            }
            for row in rows
        }

    def _daily_crawl_run_counts(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        control_tables: set[str],
        date: str,
    ) -> dict[tuple[str, str], dict[str, Any]]:
        if "crawl_run" not in control_tables:
            return {}
        rows = _query_dicts(
            conn,
            f"""
            select
              source_id,
              dataset,
              sum(planned_count) as planned_count,
              sum(success_count) as success_count,
              sum(failed_count) as failed_count,
              sum(document_count) as document_count,
              sum(raw_object_count) as raw_object_count,
              max(error_message) as last_error
            from {CONTROL_SCHEMA}.crawl_run
            where crawl_date = ?
            group by source_id, dataset
            """,
            [date],
        )
        return {
            (str(row.get("source_id") or "-"), str(row.get("dataset") or "-")): row
            for row in rows
        }

    def _daily_silver_source_counts(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        silver_tables: set[str],
        date: str,
    ) -> dict[tuple[str, str], int]:
        counts: dict[tuple[str, str], int] = {}
        for dataset in DAILY_COLLECTION_DATASETS:
            if dataset not in silver_tables:
                continue
            columns = self._columns(conn, SILVER_SCHEMA, dataset)
            date_column = _preferred_date_column(columns)
            if "source_id" not in columns or date_column not in {"trade_date", "publish_date"}:
                continue
            rows = conn.execute(
                f"""
                select source_id, count(*) as row_count
                from {SILVER_SCHEMA}.{dataset}
                where {date_column} = ?
                group by source_id
                """,
                [date],
            ).fetchall()
            for source_id, row_count in rows:
                counts[(str(source_id or "-"), dataset)] = int(row_count or 0)
        return counts

    def _daily_job_stage_rows(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        control_tables: set[str],
        date: str,
    ) -> dict[str, dict[str, Any]]:
        if "job_run" not in control_tables:
            return {}
        placeholders = ", ".join("?" for _ in DAILY_JOB_STAGES)
        rows = _query_dicts(
            conn,
            f"""
            select *
            from {CONTROL_SCHEMA}.job_run
            where job_type in ({placeholders})
              and (start_date is null or start_date <= ?)
              and (end_date is null or end_date >= ?)
            order by start_at desc, created_at desc, job_id
            """,
            [*DAILY_JOB_STAGES, date, date],
        )
        latest: dict[str, dict[str, Any]] = {}
        for row in rows:
            job_type = str(row.get("job_type") or "")
            latest.setdefault(job_type, row)
        return latest

    def _daily_dataset_source_ids(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        control_tables: set[str],
        silver_tables: set[str],
        dataset: str,
        date: str,
    ) -> list[str]:
        source_ids: set[str] = set()
        if dataset in silver_tables:
            columns = self._columns(conn, SILVER_SCHEMA, dataset)
            date_column = "publish_date" if dataset in DAILY_DOCUMENT_DIMENSIONS else "trade_date"
            if "source_id" in columns and date_column in columns:
                rows = conn.execute(
                    f"""
                    select distinct source_id
                    from {SILVER_SCHEMA}.{dataset}
                    where {date_column} = ?
                      and source_id is not null
                    order by source_id
                    """,
                    [date],
                ).fetchall()
                source_ids.update(str(row[0]) for row in rows if row[0])
        if "backfill_task" in control_tables and dataset in DAILY_SOURCE_DIMENSIONS:
            rows = conn.execute(
                f"""
                select distinct source_id
                from {CONTROL_SCHEMA}.backfill_task
                where start_date <= ?
                  and end_date >= ?
                  and dataset = ?
                  and source_id is not null
                order by source_id
                """,
                [date, date, dataset],
            ).fetchall()
            source_ids.update(str(row[0]) for row in rows if row[0])
        if "crawl_task" in control_tables and dataset in DAILY_DOCUMENT_DIMENSIONS:
            rows = conn.execute(
                f"""
                select distinct source_id
                from {CONTROL_SCHEMA}.crawl_task
                where crawl_date = ?
                  and dataset = ?
                  and source_id is not null
                order by source_id
                """,
                [date, dataset],
            ).fetchall()
            source_ids.update(str(row[0]) for row in rows if row[0])
        return sorted(source_ids)

    def _daily_task_failure_stats(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        control_tables: set[str],
        dataset: str,
        date: str,
    ) -> dict[str, dict[str, Any]]:
        if "backfill_task" not in control_tables:
            return {}
        tasks = _query_dicts(
            conn,
            f"""
            select source_id, symbol_batch_json, status, last_error, updated_at
            from {CONTROL_SCHEMA}.backfill_task
            where start_date <= ?
              and end_date >= ?
              and dataset = ?
              and status <> 'superseded'
            order by updated_at desc
            """,
            [date, date, dataset],
        )
        stats: dict[str, dict[str, Any]] = {}
        for task in tasks:
            source_id = str(task.get("source_id") or "")
            item = stats.setdefault(
                source_id,
                {
                    "failed_task_count": 0,
                    "failed_symbol_count": 0,
                    "timeout_count": 0,
                    "error_count": 0,
                    "last_error": None,
                },
            )
            if task.get("status") != "failed":
                continue
            symbols = task.get("symbol_batch_json") or []
            if not isinstance(symbols, list):
                symbols = []
            error = str(task.get("last_error") or "")
            item["failed_task_count"] += 1
            item["failed_symbol_count"] += len(symbols)
            if _looks_like_timeout(error):
                item["timeout_count"] += 1
            else:
                item["error_count"] += 1
            if error and not item.get("last_error"):
                item["last_error"] = error[:300]
        return stats

    def _daily_crawl_failure_stats(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        control_tables: set[str],
        dataset: str,
        date: str,
    ) -> dict[str, dict[str, Any]]:
        if "crawl_task" not in control_tables:
            return {}
        tasks = _query_dicts(
            conn,
            f"""
            select source_id, status, last_error, updated_at
            from {CONTROL_SCHEMA}.crawl_task
            where crawl_date = ?
              and dataset = ?
            order by updated_at desc
            """,
            [date, dataset],
        )
        stats: dict[str, dict[str, Any]] = {}
        for task in tasks:
            source_id = str(task.get("source_id") or "")
            item = stats.setdefault(
                source_id,
                {"failed_task_count": 0, "timeout_count": 0, "error_count": 0, "last_error": None},
            )
            if task.get("status") != "failed":
                continue
            error = str(task.get("last_error") or "")
            item["failed_task_count"] += 1
            if _looks_like_timeout(error):
                item["timeout_count"] += 1
            else:
                item["error_count"] += 1
            if error and not item.get("last_error"):
                item["last_error"] = error[:300]
        return stats

    def _daily_instrument_set(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        silver_tables: set[str],
        dataset: str,
        date: str,
    ) -> set[str]:
        if dataset not in silver_tables:
            return set()
        columns = self._columns(conn, SILVER_SCHEMA, dataset)
        if "instrument" not in columns or "trade_date" not in columns:
            return set()
        rows = conn.execute(
            f"""
            select distinct instrument
            from {SILVER_SCHEMA}.{dataset}
            where trade_date = ?
            """,
            [date],
        ).fetchall()
        return {str(row[0]) for row in rows}

    def _daily_batch_rows(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        control_tables: set[str],
        date: str,
    ) -> list[dict[str, Any]]:
        if "backfill_task" not in control_tables:
            return []
        placeholders = ", ".join("?" for _ in DAILY_BATCH_DATASETS)
        stale_threshold = _now_for_console_minutes_ago(STALE_RUNNING_MINUTES)
        tasks = _query_dicts(
            conn,
            f"""
            select *
            from {CONTROL_SCHEMA}.backfill_task
            where start_date <= ?
              and end_date >= ?
              and status <> 'superseded'
              and dataset in ({placeholders})
            order by dataset, created_at, task_id
            """,
            [date, date, *DAILY_BATCH_DATASETS],
        )
        groups: dict[str, dict[str, Any]] = {}
        for task in tasks:
            dataset = str(task.get("dataset") or "")
            group = groups.setdefault(
                dataset,
                {
                    "dataset": dataset,
                    "total_batch_count": 0,
                    "success_count": 0,
                    "running_count": 0,
                    "pending_count": 0,
                    "failed_count": 0,
                    "stale_running_count": 0,
                    "symbol_count": 0,
                    "latest_updated_at": None,
                },
            )
            status = str(task.get("status") or "")
            symbols = task.get("symbol_batch_json") or []
            if not isinstance(symbols, list):
                symbols = []
            group["total_batch_count"] += 1
            group["symbol_count"] += len(symbols)
            if status == "success":
                group["success_count"] += 1
            elif status == "running":
                group["running_count"] += 1
                if str(task.get("updated_at") or "") <= stale_threshold:
                    group["stale_running_count"] += 1
            elif status == "pending":
                group["pending_count"] += 1
            elif status == "failed":
                group["failed_count"] += 1
            updated_at = task.get("updated_at")
            if updated_at and (
                not group["latest_updated_at"]
                or str(updated_at) > str(group["latest_updated_at"])
            ):
                group["latest_updated_at"] = updated_at
        rows = []
        for dataset in DAILY_BATCH_DATASETS:
            row = groups.get(
                dataset,
                {
                    "dataset": dataset,
                    "total_batch_count": 0,
                    "success_count": 0,
                    "running_count": 0,
                    "pending_count": 0,
                    "failed_count": 0,
                    "stale_running_count": 0,
                    "symbol_count": 0,
                    "latest_updated_at": None,
                },
            )
            total = int(row["total_batch_count"] or 0)
            success = int(row["success_count"] or 0)
            row["complete_percent"] = _percent(success, total)
            row["state"] = _progress_state(
                total=total,
                success=success,
                failed=int(row["failed_count"] or 0),
                running=int(row["running_count"] or 0),
                pending=int(row["pending_count"] or 0),
                stale=int(row["stale_running_count"] or 0),
            )
            rows.append(row)
        return rows

    def _daily_batch_task_rows(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        control_tables: set[str],
        date: str,
    ) -> list[dict[str, Any]]:
        if "backfill_task" not in control_tables:
            return []
        placeholders = ", ".join("?" for _ in DAILY_BATCH_DATASETS)
        stale_threshold = _now_for_console_minutes_ago(STALE_RUNNING_MINUTES)
        rows = _query_dicts(
            conn,
            f"""
            select task_id, dataset, source_id, universe, start_date, end_date,
                   symbol_batch_json, status, attempt_count, last_error, created_at, updated_at
            from {CONTROL_SCHEMA}.backfill_task
            where start_date <= ?
              and end_date >= ?
              and status <> 'superseded'
              and dataset in ({placeholders})
            order by dataset, source_id, created_at, task_id
            """,
            [date, date, *DAILY_BATCH_DATASETS],
        )
        task_rows = []
        for row in rows:
            symbols = row.get("symbol_batch_json") or []
            if not isinstance(symbols, list):
                symbols = []
            state = _task_state(
                status=str(row.get("status") or "pending"),
                updated_at=row.get("updated_at"),
                stale_threshold=stale_threshold,
            )
            task_rows.append(
                {
                    **row,
                    "state": state,
                    "symbol_count": len(symbols),
                    "symbol_preview": _symbol_preview(symbols),
                    "progress_percent": _task_progress_percent(state),
                    "is_stale": state == "stale",
                }
            )
        return task_rows

    def _daily_crawl_task_rows(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        control_tables: set[str],
        date: str,
    ) -> list[dict[str, Any]]:
        if "crawl_task" not in control_tables:
            return []
        stale_threshold = _now_for_console_minutes_ago(STALE_RUNNING_MINUTES)
        rows = _query_dicts(
            conn,
            f"""
            select task_id, source_id, dataset, crawl_date, partition_key, status,
                   attempt_count, last_error, created_at, updated_at
            from {CONTROL_SCHEMA}.crawl_task
            where crawl_date = ?
            order by dataset, source_id, created_at, task_id
            """,
            [date],
        )
        task_rows = []
        for row in rows:
            state = _task_state(
                status=str(row.get("status") or "pending"),
                updated_at=row.get("updated_at"),
                stale_threshold=stale_threshold,
            )
            task_rows.append(
                {
                    **row,
                    "state": state,
                    "progress_percent": _task_progress_percent(state),
                    "is_stale": state == "stale",
                }
            )
        return task_rows

    def _daily_wide_rows(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        control_tables: set[str],
        silver_tables: set[str],
        date: str,
        mode: str,
        reference_rows: list[dict[str, Any]],
        limit: int,
    ) -> list[dict[str, Any]]:
        table_fields = DAILY_FACTOR_WIDE_TABLES if mode == "factor" else DAILY_RAW_WIDE_TABLES
        table_maps = {
            table: self._daily_table_rows(
                conn,
                silver_tables=silver_tables,
                table=table,
                date=date,
                fields=fields,
            )
            for table, fields in table_fields.items()
        }
        news_groups = self._daily_document_groups(
            conn,
            control_tables=control_tables,
            silver_tables=silver_tables,
            table="news",
            date=date,
        )
        announcement_groups = self._daily_document_groups(
            conn,
            control_tables=control_tables,
            silver_tables=silver_tables,
            table="announcement",
            date=date,
        )
        rows = []
        for identity in reference_rows[:limit]:
            instrument = str(identity.get("instrument") or "")
            row = {
                "instrument": instrument,
                "symbol": identity.get("symbol") or _instrument_symbol(instrument),
                "exchange": identity.get("exchange") or _instrument_exchange(instrument),
                "name": identity.get("name"),
                "industry": identity.get("industry"),
            }
            for table, fields in table_fields.items():
                table_row = table_maps.get(table, {}).get(instrument, {})
                for field in fields:
                    if field in table_row:
                        row[field] = table_row.get(field)
                if table_row.get("source_id"):
                    row[f"{table}_source_id"] = table_row.get("source_id")
                if table_row.get("updated_at"):
                    row[f"{table}_updated_at"] = table_row.get("updated_at")
            news = news_groups.get(instrument, {"count": 0, "documents": []})
            announcements = announcement_groups.get(instrument, {"count": 0, "documents": []})
            row["raw_news_count" if mode == "factor" else "news_count"] = news["count"]
            row["raw_announcement_count" if mode == "factor" else "announcement_count"] = announcements["count"]
            row["_news_documents"] = news["documents"]
            row["_announcement_documents"] = announcements["documents"]
            rows.append(row)
        return rows

    def _daily_table_rows(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        silver_tables: set[str],
        table: str,
        date: str,
        fields: list[str],
    ) -> dict[str, dict[str, Any]]:
        if table not in silver_tables:
            return {}
        columns = self._columns(conn, SILVER_SCHEMA, table)
        if "instrument" not in columns or "trade_date" not in columns:
            return {}
        selected_fields = [field for field in fields if field in columns]
        source_expr = ", source_id" if "source_id" in columns else ""
        updated_expr = ", updated_at" if "updated_at" in columns else ""
        if selected_fields:
            select_expr = ", " + ", ".join(selected_fields)
        else:
            select_expr = ""
        rows = _query_dicts(
            conn,
            f"""
            select instrument{select_expr}{source_expr}{updated_expr}
            from {SILVER_SCHEMA}.{table}
            where trade_date = ?
            order by instrument
            """,
            [date],
        )
        return {str(row["instrument"]): row for row in rows}

    def _daily_document_groups(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        control_tables: set[str],
        silver_tables: set[str],
        table: str,
        date: str,
    ) -> dict[str, dict[str, Any]]:
        if table not in silver_tables:
            return {}
        source_ids = DAILY_DOCUMENT_SOURCE_IDS.get(table, ())
        if not source_ids:
            return {}
        id_field = "news_id" if table == "news" else "announcement_id"
        columns = self._columns(conn, SILVER_SCHEMA, table)
        optional_fields = [
            field
            for field in (*DOCUMENT_LOCAL_CONTENT_FIELDS, *DOCUMENT_OBJECT_FIELDS)
            if field in columns
        ]
        select_fields = [id_field, "publish_date", "instrument", "title", "url", "source_id"]
        for field in optional_fields:
            if field not in select_fields:
                select_fields.append(field)
        placeholders = ", ".join("?" for _ in source_ids)
        raw_object_ids = self._daily_source_raw_object_ids(
            conn,
            control_tables=control_tables,
            dataset=table,
            source_ids=source_ids,
            date=date,
        )
        rows = _query_dicts(
            conn,
            f"""
            select {", ".join(select_fields)}
            from {SILVER_SCHEMA}.{table}
            where publish_date = ?
              and source_id in ({placeholders})
            order by publish_date desc, instrument, {id_field}
            limit ?
            """,
            [date, *source_ids, DAILY_DOCUMENT_DETAIL_LIMIT],
        )
        groups: dict[str, dict[str, Any]] = {}
        for row in rows:
            instrument = str(row.get("instrument") or "")
            group = groups.setdefault(instrument, {"documents_by_key": {}})
            documents_by_key = group["documents_by_key"]
            key = _daily_document_key(row)
            existing = documents_by_key.get(key)
            item = _annotated_document_row(row, raw_object_ids=raw_object_ids)
            if existing:
                source_ids = set(existing.get("source_ids") or [existing.get("source_id")])
                source_ids.add(item.get("source_id"))
                preferred = _preferred_document_row(existing, item)
                preferred["source_ids"] = _ordered_source_ids(source_ids)
                preferred["source_id"] = ", ".join(preferred["source_ids"])
                preferred = _annotated_document_row(preferred, raw_object_ids=raw_object_ids)
                documents_by_key[key] = preferred
            else:
                source_id = item.get("source_id")
                item["source_ids"] = [source_id] if source_id else []
                documents_by_key[key] = item
        return {
            instrument: {
                "count": len(group["documents_by_key"]),
                "documents": list(group["documents_by_key"].values())[:80],
            }
            for instrument, group in groups.items()
        }

    def _daily_source_raw_object_ids(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        control_tables: set[str],
        dataset: str,
        source_ids: tuple[str, ...],
        date: str,
    ) -> dict[str, str]:
        if "source_object" not in control_tables or not source_ids:
            return {}
        placeholders = ", ".join("?" for _ in source_ids)
        layer_placeholders = ", ".join("?" for _ in DOCUMENT_OBJECT_LAYERS)
        rows = _query_dicts(
            conn,
            f"""
            select source_id, object_id, layer, created_at
            from {CONTROL_SCHEMA}.source_object
            where dataset = ?
              and layer in ({layer_placeholders})
              and source_id in ({placeholders})
              and uri like ?
            order by
              case layer
                when 'raw_records' then 0
                when 'raw' then 1
                when 'raw_manifest' then 2
                when 'raw_file' then 3
                else 9
              end,
              created_at desc,
              object_id desc
            """,
            [dataset, *DOCUMENT_OBJECT_LAYERS, *source_ids, f"%{date}%"],
        )
        object_ids: dict[str, str] = {}
        for row in rows:
            source_id = str(row.get("source_id") or "")
            if source_id and source_id not in object_ids:
                object_ids[source_id] = str(row.get("object_id") or "")
        return object_ids

    def source_object_file(self, *, object_id: str) -> dict[str, Any]:
        object_id = str(object_id or "").strip()
        if not object_id:
            raise ValueError("source object id is required")
        if not self.settings.database_path.exists():
            raise ValueError("database does not exist")
        with self._connect() as conn:
            control_tables = self._existing_tables(conn, CONTROL_SCHEMA)
            if "source_object" not in control_tables:
                raise ValueError("source_object table does not exist")
            row = _query_dicts(
                conn,
                f"""
                select object_id, dataset, source_id, layer, uri, size_bytes
                from {CONTROL_SCHEMA}.source_object
                where object_id = ?
                """,
                [object_id],
            )
        if not row:
            raise ValueError(f"source object not found: {object_id}")
        item = row[0]
        dataset = str(item.get("dataset") or "")
        layer = str(item.get("layer") or "")
        if dataset not in {"announcement", "news"} or layer not in DOCUMENT_OBJECT_LAYERS:
            raise ValueError(f"source object is not a previewable document object: {object_id}")
        path = _resolve_source_object_path(self.settings, item.get("uri"))
        if not path.is_file():
            raise ValueError(f"source object file does not exist: {path}")
        body = path.read_bytes()
        content_type = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
        return {
            "body": body,
            "content_type": content_type,
            "filename": path.name,
            "size_bytes": len(body),
        }

    def raw_instrument_preview(
        self,
        *,
        instrument: str,
        start: str | None = None,
        end: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        instrument = _normalize_preview_instrument(instrument)
        row_limit = _clamp_limit(limit)
        if not self.settings.database_path.exists():
            return _empty_raw_instrument_preview(instrument, start=start, end=end, limit=row_limit)

        with self._connect() as conn:
            control_tables = self._existing_tables(conn, CONTROL_SCHEMA)
            objects = self._raw_source_objects(
                conn,
                control_tables=control_tables,
                scan_limit=RAW_OBJECT_SCAN_LIMIT,
            )
        sections = _raw_preview_sections(
            objects=objects,
            instrument=instrument,
            start=start,
            end=end,
            limit=row_limit,
        )
        return {
            "status": "ok",
            "instrument": instrument,
            "start": start,
            "end": end,
            "limit": row_limit,
            "summary": _raw_preview_summary(sections),
            "sections": sections,
        }

    def _dataset_preview_summary(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        dataset: str,
        columns: list[str],
        date_column: str | None,
        where_clause: str,
        params: list[Any],
    ) -> dict[str, Any]:
        total_row_count = int(
            conn.execute(f"select count(*) from {SILVER_SCHEMA}.{dataset}").fetchone()[0]
            or 0
        )
        filtered_row_count = int(
            conn.execute(
                f"select count(*) from {SILVER_SCHEMA}.{dataset} {where_clause}",
                params,
            ).fetchone()[0]
            or 0
        )
        summary: dict[str, Any] = {
            "total_row_count": total_row_count,
            "filtered_row_count": filtered_row_count,
            "date_column": date_column,
            "supports_instrument_filter": "instrument" in columns,
            "supports_date_filter": date_column is not None,
            "min_date": None,
            "max_date": None,
            "date_count": None,
            "instrument_count": None,
            "source_ids": [],
        }
        if date_column:
            row = conn.execute(
                f"""
                select min({date_column}), max({date_column}), count(distinct {date_column})
                from {SILVER_SCHEMA}.{dataset}
                {where_clause}
                """,
                params,
            ).fetchone()
            summary.update(
                {
                    "min_date": row[0],
                    "max_date": row[1],
                    "date_count": int(row[2] or 0),
                }
            )
        if "instrument" in columns:
            summary["instrument_count"] = int(
                conn.execute(
                    f"""
                    select count(distinct instrument)
                    from {SILVER_SCHEMA}.{dataset}
                    {where_clause}
                    """,
                    params,
                ).fetchone()[0]
                or 0
            )
        if "source_id" in columns:
            summary["source_ids"] = _query_dicts(
                conn,
                f"""
                select source_id, count(*) as row_count
                from {SILVER_SCHEMA}.{dataset}
                {where_clause}
                group by source_id
                order by row_count desc, source_id
                limit 8
                """,
                params,
            )
        return summary

    def _timeline_rows(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        silver_tables: set[str],
        instrument: str,
        start: str | None,
        end: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        rows_by_date: dict[str, dict[str, Any]] = {}
        for table, configured_fields in INSTRUMENT_TIMELINE_TABLES.items():
            if table not in silver_tables:
                continue
            columns = self._columns(conn, SILVER_SCHEMA, table)
            fields = [field for field in configured_fields if field in columns]
            if "trade_date" not in columns or "instrument" not in columns or not fields:
                continue
            select_fields = ", ".join(fields)
            source_expr = ", source_id" if "source_id" in columns else ""
            date_clause, params = _instrument_date_filter(
                instrument=instrument,
                date_column="trade_date",
                start=start,
                end=end,
            )
            table_rows = _query_dicts(
                conn,
                f"""
                select trade_date, instrument, {select_fields}{source_expr}
                from {SILVER_SCHEMA}.{table}
                where {date_clause}
                order by trade_date desc
                limit ?
                """,
                [*params, limit],
            )
            for table_row in table_rows:
                trade_date = str(table_row["trade_date"])
                target = rows_by_date.setdefault(
                    trade_date,
                    {"trade_date": trade_date, "instrument": instrument},
                )
                for field in fields:
                    target[field] = table_row.get(field)
                if "source_id" in table_row:
                    target[f"{table}_source_id"] = table_row.get("source_id")
        return [
            rows_by_date[trade_date]
            for trade_date in sorted(rows_by_date, reverse=True)[:limit]
        ]

    def _document_rows(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        table: str,
        silver_tables: set[str],
        instrument: str,
        trade_dates: list[str],
    ) -> list[dict[str, Any]]:
        if table not in silver_tables or not trade_dates:
            return []
        source_ids = DAILY_DOCUMENT_SOURCE_IDS.get(table, ())
        if not source_ids:
            return []
        id_field = "news_id" if table == "news" else "announcement_id"
        target_trade_dates = {str(trade_date) for trade_date in trade_dates if trade_date}
        if not target_trade_dates:
            return []
        publish_start = date_minus_days(min(target_trade_dates), DOCUMENT_DETAIL_LOOKBACK_DAYS)
        publish_end = max(target_trade_dates)
        aligner = TradeDayAligner.from_connection(conn)
        placeholders = ", ".join("?" for _ in source_ids)
        rows = _query_dicts(
            conn,
            f"""
            select {id_field}, publish_date, instrument, title, url, source_id
            from {SILVER_SCHEMA}.{table}
            where instrument = ?
              and publish_date >= ?
              and publish_date <= ?
              and source_id in ({placeholders})
            order by publish_date desc, {id_field}
            limit ?
            """,
            [instrument, publish_start, publish_end, *source_ids, DOCUMENT_DETAIL_LIMIT],
        )
        aligned_rows = []
        for row in rows:
            trade_date = aligner.align(row["publish_date"])
            if trade_date not in target_trade_dates:
                continue
            row["trade_date"] = trade_date
            aligned_rows.append(row)
        return aligned_rows

    def dataset_preview(
        self,
        *,
        dataset: str,
        instrument: str | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        if dataset not in SILVER_TABLES:
            supported = ", ".join(SILVER_TABLES)
            raise ValueError(f"unsupported preview dataset: {dataset}; supported: {supported}")
        if not self.settings.database_path.exists():
            return _empty_dataset_preview(dataset)

        with self._connect() as conn:
            silver_tables = self._existing_tables(conn, SILVER_SCHEMA)
            if dataset not in silver_tables:
                return _empty_dataset_preview(dataset)

            columns = self._columns(conn, SILVER_SCHEMA, dataset)
            date_column = _preferred_date_column(columns)
            filters = []
            params: list[Any] = []
            if instrument:
                if "instrument" not in columns:
                    raise ValueError(f"dataset has no instrument filter: {dataset}")
                filters.append("instrument = ?")
                params.append(instrument)
            if start:
                if not date_column:
                    raise ValueError(f"dataset has no date filter: {dataset}")
                filters.append(f"{date_column} >= ?")
                params.append(start)
            if end:
                if not date_column:
                    raise ValueError(f"dataset has no date filter: {dataset}")
                filters.append(f"{date_column} <= ?")
                params.append(end)

            where_clause = f"where {' and '.join(filters)}" if filters else ""
            order_clause = _order_clause(columns, date_column)
            row_limit = _clamp_limit(limit)
            rows = _query_dicts(
                conn,
                f"""
                select *
                from {SILVER_SCHEMA}.{dataset}
                {where_clause}
                {order_clause}
                limit ?
                """,
                [*params, row_limit],
            )
            summary = self._dataset_preview_summary(
                conn,
                dataset=dataset,
                columns=columns,
                date_column=date_column,
                where_clause=where_clause,
                params=params,
            )
        return {
            "status": "ok",
            "dataset": dataset,
            "columns": columns,
            "date_column": date_column,
            "supports_instrument_filter": "instrument" in columns,
            "supports_date_filter": date_column is not None,
            "row_count": len(rows),
            "filtered_row_count": summary["filtered_row_count"],
            "total_row_count": summary["total_row_count"],
            "summary": summary,
            "rows": rows,
        }

    def instrument_timeline(
        self,
        *,
        instrument: str,
        start: str | None = None,
        end: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> dict[str, Any]:
        instrument = _normalize_preview_instrument(instrument)
        if not self.settings.database_path.exists():
            return _empty_instrument_timeline(
                instrument,
                start=start,
                end=end,
                limit=_clamp_limit(limit),
                offset=_clamp_offset(offset),
            )

        with self._connect() as conn:
            silver_tables = self._existing_tables(conn, SILVER_SCHEMA)
            row_limit = _clamp_limit(limit)
            row_offset = _clamp_offset(offset)
            timeline_candidates = self._timeline_rows(
                conn,
                silver_tables=silver_tables,
                instrument=instrument,
                start=start,
                end=end,
                limit=row_limit + row_offset + 1,
            )
            timeline_rows = timeline_candidates[row_offset : row_offset + row_limit]
            current_trade_dates = [str(row["trade_date"]) for row in timeline_rows]
            news_rows = self._document_rows(
                conn,
                table="news",
                silver_tables=silver_tables,
                instrument=instrument,
                trade_dates=current_trade_dates,
            )
            announcement_rows = self._document_rows(
                conn,
                table="announcement",
                silver_tables=silver_tables,
                instrument=instrument,
                trade_dates=current_trade_dates,
            )
        return {
            "status": "ok",
            "instrument": instrument,
            "start": start,
            "end": end,
            "limit": row_limit,
            "offset": row_offset,
            "page": (row_offset // row_limit) + 1,
            "page_size": row_limit,
            "has_previous": row_offset > 0,
            "has_next": len(timeline_candidates) > row_offset + row_limit,
            "summary": _instrument_timeline_summary(
                timeline_rows=timeline_rows,
                news_rows=news_rows,
                announcement_rows=announcement_rows,
            ),
            "timeline_rows": timeline_rows,
            "news_rows": news_rows,
            "announcement_rows": announcement_rows,
        }

    def _empty_payload(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "database_exists": False,
            "database_path": str(self.settings.database_path),
            "settings": self.settings.as_dict(),
            "table_counts": {table: 0 for table in CONTROL_TABLES},
            "silver_table_counts": {table: 0 for table in SILVER_TABLES},
            "backfill_status_counts": {},
            "job_status_counts": {},
            "quality_status_counts": {},
            "source_layer_counts": {},
            "latest_job_runs": [],
            "latest_backfill_tasks": [],
            "backfill_progress": [],
            "watermarks": [],
            "latest_qlib_exports": [],
            "qlib_provider_status": self.qlib_provider_status(),
            "data_coverage": _empty_data_coverage(),
        }

    def locked_payload(self, message: str) -> dict[str, Any]:
        payload = self._empty_payload()
        payload.update(
            {
                "status": "busy",
                "database_exists": self.settings.database_path.exists(),
                "database_busy": True,
                "message": message,
            }
        )
        return payload

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.settings.database_path), read_only=True)

    def _existing_tables(
        self,
        conn: duckdb.DuckDBPyConnection,
        schema_name: str,
    ) -> set[str]:
        rows = conn.execute(
            """
            select table_name
            from information_schema.tables
            where table_schema = ?
            """,
            [schema_name],
        ).fetchall()
        return {str(row[0]) for row in rows}

    def _table_counts(
        self,
        conn: duckdb.DuckDBPyConnection,
        schema_name: str,
        tables: list[str],
        existing_tables: set[str],
    ) -> dict[str, int]:
        counts = {}
        for table in tables:
            if table not in existing_tables:
                counts[table] = 0
                continue
            value = conn.execute(f"select count(*) from {schema_name}.{table}").fetchone()[0]
            counts[table] = int(value)
        return counts

    def _status_counts(
        self,
        conn: duckdb.DuckDBPyConnection,
        table: str,
        control_tables: set[str],
    ) -> dict[str, int]:
        if table not in control_tables:
            return {}
        rows = conn.execute(
            f"""
            select status, count(*) as row_count
            from {CONTROL_SCHEMA}.{table}
            group by status
            order by status
            """
        ).fetchall()
        return {str(status): int(row_count) for status, row_count in rows}

    def _source_layer_counts(
        self,
        conn: duckdb.DuckDBPyConnection,
        control_tables: set[str],
    ) -> dict[str, int]:
        if "source_object" not in control_tables:
            return {}
        rows = conn.execute(
            f"""
            select layer, count(*) as row_count
            from {CONTROL_SCHEMA}.source_object
            group by layer
            order by layer
            """
        ).fetchall()
        return {str(layer): int(row_count) for layer, row_count in rows}

    def _job_runs(
        self,
        conn: duckdb.DuckDBPyConnection,
        control_tables: set[str],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        if "job_run" not in control_tables:
            return []
        return _query_dicts(
            conn,
            f"""
            select *
            from {CONTROL_SCHEMA}.job_run
            order by created_at desc, start_at desc, job_id
            limit ?
            """,
            [limit],
        )

    def _qlib_exports(
        self,
        conn: duckdb.DuckDBPyConnection,
        control_tables: set[str],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        if "job_run" not in control_tables:
            return []
        return _query_dicts(
            conn,
            f"""
            select *
            from {CONTROL_SCHEMA}.job_run
            where job_type = 'export_qlib'
            order by created_at desc, start_at desc, job_id
            limit ?
            """,
            [limit],
        )

    def _backfill_tasks(
        self,
        conn: duckdb.DuckDBPyConnection,
        control_tables: set[str],
        *,
        status: str | None,
        dataset: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        if "backfill_task" not in control_tables:
            return []
        filters = []
        params: list[Any] = []
        if status:
            filters.append("status = ?")
            params.append(status)
        if dataset:
            filters.append("dataset = ?")
            params.append(dataset)
        where_clause = f"where {' and '.join(filters)}" if filters else ""
        return _query_dicts(
            conn,
            f"""
            select *
            from {CONTROL_SCHEMA}.backfill_task
            {where_clause}
            order by updated_at desc, created_at desc, task_id
            limit ?
            """,
            [*params, limit],
        )

    def _backfill_progress(
        self,
        conn: duckdb.DuckDBPyConnection,
        control_tables: set[str],
    ) -> list[dict[str, Any]]:
        if "backfill_task" not in control_tables:
            return []
        stale_threshold = _now_for_console_minutes_ago(STALE_RUNNING_MINUTES)
        rows = _query_dicts(
            conn,
            f"""
            select
              dataset,
              source_id,
              coalesce(universe, '') as universe,
              min(start_date) as min_date,
              max(end_date) as max_date,
              count(*) as total_task_count,
              sum(case when status = 'success' then 1 else 0 end) as success_count,
              sum(case when status = 'failed' then 1 else 0 end) as failed_count,
              sum(case when status = 'running' then 1 else 0 end) as running_count,
              sum(case when status = 'pending' then 1 else 0 end) as pending_count,
              sum(
                case when status = 'running' and updated_at <= ? then 1 else 0 end
              ) as stale_running_count,
              max(updated_at) as latest_updated_at
            from {CONTROL_SCHEMA}.backfill_task
            where status <> 'superseded'
            group by dataset, source_id, coalesce(universe, '')
            order by latest_updated_at desc, dataset, source_id, universe
            """,
            [stale_threshold],
        )
        for row in rows:
            total = int(row.get("total_task_count") or 0)
            success = int(row.get("success_count") or 0)
            failed = int(row.get("failed_count") or 0)
            running = int(row.get("running_count") or 0)
            pending = int(row.get("pending_count") or 0)
            stale = int(row.get("stale_running_count") or 0)
            row["success_percent"] = round((success / total) * 100, 2) if total else 0
            row["blocked_count"] = failed + stale
            row["unresolved_count"] = failed + running + pending
            row["state"] = _progress_state(
                total=total,
                success=success,
                failed=failed,
                running=running,
                pending=pending,
                stale=stale,
            )
        return rows

    def _watermarks(
        self,
        conn: duckdb.DuckDBPyConnection,
        control_tables: set[str],
    ) -> list[dict[str, Any]]:
        if "dataset_watermark" not in control_tables:
            return []
        return _query_dicts(
            conn,
            f"""
            select *
            from {CONTROL_SCHEMA}.dataset_watermark
            order by updated_at desc, dataset, source_id, universe
            """,
        )

    def _quality_issues(
        self,
        conn: duckdb.DuckDBPyConnection,
        control_tables: set[str],
        *,
        dataset: str | None,
        status: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        if "quality_issue" not in control_tables:
            return []
        filters = []
        params: list[Any] = []
        if dataset:
            filters.append("dataset = ?")
            params.append(dataset)
        if status:
            filters.append("status = ?")
            params.append(status)
        where_clause = f"where {' and '.join(filters)}" if filters else ""
        return _query_dicts(
            conn,
            f"""
            select *
            from {CONTROL_SCHEMA}.quality_issue
            {where_clause}
            order by created_at desc, severity, dataset, issue_type
            limit ?
            """,
            [*params, limit],
        )

    def _source_objects(
        self,
        conn: duckdb.DuckDBPyConnection,
        control_tables: set[str],
        *,
        dataset: str | None,
        layer: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        if "source_object" not in control_tables:
            return []
        filters = []
        params: list[Any] = []
        if dataset:
            filters.append("dataset = ?")
            params.append(dataset)
        if layer:
            filters.append("layer = ?")
            params.append(layer)
        where_clause = f"where {' and '.join(filters)}" if filters else ""
        return _query_dicts(
            conn,
            f"""
            select *
            from {CONTROL_SCHEMA}.source_object
            {where_clause}
            order by created_at desc, dataset, layer, uri
            limit ?
            """,
            [*params, limit],
        )

    def _raw_source_objects(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        control_tables: set[str],
        scan_limit: int,
    ) -> list[dict[str, Any]]:
        if "source_object" not in control_tables:
            return []
        placeholders = ", ".join("?" for _ in RAW_PREVIEW_DATASETS)
        return _query_dicts(
            conn,
            f"""
            select *
            from {CONTROL_SCHEMA}.source_object
            where layer = 'raw'
              and dataset in ({placeholders})
            order by created_at desc, dataset, uri
            limit ?
            """,
            [*RAW_PREVIEW_DATASETS, scan_limit],
        )

    def _instrument_options(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        silver_tables: set[str],
        query: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        candidates: set[str] = set()
        normalized_query = (query or "").strip().upper()
        like_query = f"%{normalized_query}%"
        if "stock_basic" in silver_tables:
            filters = ""
            params: list[Any] = []
            if normalized_query:
                filters = """
                where upper(instrument) like ?
                   or upper(symbol) like ?
                   or upper(coalesce(name, '')) like ?
                   or upper(coalesce(industry, '')) like ?
                """
                params.extend([like_query, like_query, like_query, like_query])
            rows = conn.execute(
                f"""
                select instrument
                from {SILVER_SCHEMA}.stock_basic
                {filters}
                order by instrument
                limit ?
                """,
                [*params, max(limit * 4, limit)],
            ).fetchall()
            candidates.update(str(row[0]) for row in rows)

        if "universe_constituent" in silver_tables:
            filters = ""
            params = []
            if normalized_query:
                filters = """
                where upper(c.instrument) like ?
                   or upper(c.symbol) like ?
                   or upper(coalesce(c.name, '')) like ?
                   or upper(c.universe) like ?
                """
                params.extend([like_query, like_query, like_query, like_query])
            rows = conn.execute(
                f"""
                with latest as (
                  select universe, max(snapshot_date) as snapshot_date
                  from {SILVER_SCHEMA}.universe_constituent
                  group by universe
                )
                select distinct c.instrument
                from {SILVER_SCHEMA}.universe_constituent c
                join latest l
                  on c.universe = l.universe
                 and c.snapshot_date = l.snapshot_date
                {filters}
                order by c.instrument
                limit ?
                """,
                [*params, max(limit * 4, limit)],
            ).fetchall()
            candidates.update(str(row[0]) for row in rows)

        candidates.update(
            self._observed_instruments(
                conn,
                silver_tables=silver_tables,
                query=normalized_query,
                limit=max(limit * 4, limit),
            )
        )

        if not candidates:
            reference_instruments, _source = self._reference_instruments(conn, silver_tables)
            candidates.update(reference_instruments)

        candidates.update(_configured_instruments(self.settings))
        filtered = [
            instrument
            for instrument in sorted(candidates)
            if not normalized_query
            or normalized_query in instrument.upper()
            or normalized_query in _instrument_symbol(instrument).upper()
        ]
        if normalized_query and len(filtered) < limit:
            filtered = sorted(candidates)
        identity = self._instrument_identity_by_instrument(
            conn,
            silver_tables=silver_tables,
            instruments=filtered[: max(limit * 4, limit)],
        )
        rows = []
        for instrument in filtered:
            item = identity.get(instrument, _default_instrument_identity(instrument))
            option = _instrument_option_from_identity(item)
            if normalized_query and not _instrument_option_matches(option, normalized_query):
                continue
            rows.append(option)
            if len(rows) >= limit:
                break
        return rows

    def _data_coverage(
        self,
        conn: duckdb.DuckDBPyConnection,
        silver_tables: set[str],
    ) -> dict[str, Any]:
        reference = self._coverage_reference(conn, silver_tables)
        dataset_rows = [
            self._dataset_coverage_row(
                conn,
                table=table,
                silver_tables=silver_tables,
                reference=reference,
            )
            for table in SILVER_TABLES
        ]
        instrument_coverage = self._instrument_coverage(conn, silver_tables, reference)
        return {
            "status": "ok",
            "reference": _public_coverage_reference(reference),
            "required_dimensions": list(REQUIRED_DAILY_COVERAGE_DATASETS),
            "dataset_rows": dataset_rows,
            "instrument_summary": instrument_coverage["summary"],
            "instrument_rows": instrument_coverage["rows"],
            "hidden_instrument_count": instrument_coverage["hidden_count"],
        }

    def _coverage_reference(
        self,
        conn: duckdb.DuckDBPyConnection,
        silver_tables: set[str],
    ) -> dict[str, Any]:
        instruments, instrument_source = self._reference_instruments(conn, silver_tables)
        trade_dates, trade_date_source = self._reference_trade_dates(conn, silver_tables)
        return {
            "instrument_source": instrument_source,
            "trade_date_source": trade_date_source,
            "instrument_count": len(instruments),
            "trade_date_count": len(trade_dates),
            "min_trade_date": trade_dates[0] if trade_dates else None,
            "max_trade_date": trade_dates[-1] if trade_dates else None,
            "instruments": instruments,
            "trade_dates": trade_dates,
        }

    def _reference_instruments(
        self,
        conn: duckdb.DuckDBPyConnection,
        silver_tables: set[str],
    ) -> tuple[list[str], str]:
        if "universe_constituent" in silver_tables:
            rows = conn.execute(
                f"""
                with latest as (
                  select universe, max(snapshot_date) as snapshot_date
                  from {SILVER_SCHEMA}.universe_constituent
                  group by universe
                )
                select distinct c.instrument
                from {SILVER_SCHEMA}.universe_constituent c
                join latest l
                  on c.universe = l.universe
                 and c.snapshot_date = l.snapshot_date
                order by c.instrument
                """
            ).fetchall()
            instruments = [str(row[0]) for row in rows]
            if instruments:
                return instruments, "universe_constituent"

        observed = self._observed_instruments(
            conn,
            silver_tables=silver_tables,
            datasets=OBSERVED_REFERENCE_DATASETS,
        )
        if observed:
            return observed, "silver.instrument_union"

        configured = sorted(
            {
                instrument
                for symbols in self.settings.universes.values()
                for instrument in symbols
                if instrument
            }
        )
        if configured:
            return configured, "config.universes"
        return [], "none"

    def _observed_instruments(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        silver_tables: set[str],
        date: str | None = None,
        query: str | None = None,
        limit: int | None = None,
        datasets: tuple[str, ...] = INSTRUMENT_COVERAGE_DATASETS,
    ) -> list[str]:
        instruments: set[str] = set()
        normalized_query = (query or "").strip().upper()
        like_query = f"%{normalized_query}%"
        for table in datasets:
            if table not in silver_tables:
                continue
            columns = self._columns(conn, SILVER_SCHEMA, table)
            if "instrument" not in columns:
                continue
            filters = []
            params: list[Any] = []
            if date:
                date_column = _preferred_date_column(columns)
                if date_column not in {"trade_date", "publish_date", "snapshot_date"}:
                    continue
                filters.append(f"{date_column} = ?")
                params.append(date)
            if normalized_query:
                search_columns = ["instrument"]
                if "symbol" in columns:
                    search_columns.append("symbol")
                if "name" in columns:
                    search_columns.append("coalesce(name, '')")
                filters.append(
                    "("
                    + " or ".join(f"upper({column}) like ?" for column in search_columns)
                    + ")"
                )
                params.extend([like_query] * len(search_columns))
            where_clause = f"where {' and '.join(filters)}" if filters else ""
            limit_clause = "limit ?" if limit else ""
            if limit:
                params.append(limit)
            rows = conn.execute(
                f"""
                select distinct instrument
                from {SILVER_SCHEMA}.{table}
                {where_clause}
                order by instrument
                {limit_clause}
                """,
                params,
            ).fetchall()
            instruments.update(str(row[0]) for row in rows)
        return sorted(instruments)

    def _reference_trade_dates(
        self,
        conn: duckdb.DuckDBPyConnection,
        silver_tables: set[str],
    ) -> tuple[list[Any], str]:
        if "trade_calendar" in silver_tables:
            rows = conn.execute(
                f"""
                select trade_date
                from {SILVER_SCHEMA}.trade_calendar
                where is_open = true
                order by trade_date
                """
            ).fetchall()
            trade_dates = [row[0] for row in rows]
            if trade_dates:
                return trade_dates, "trade_calendar"

        dates: set[Any] = set()
        for table in REQUIRED_DAILY_COVERAGE_DATASETS:
            if table not in silver_tables:
                continue
            rows = conn.execute(
                f"""
                select distinct trade_date
                from {SILVER_SCHEMA}.{table}
                order by trade_date
                """
            ).fetchall()
            dates.update(row[0] for row in rows)
        return sorted(dates), "required_daily_union"

    def _dataset_coverage_row(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        table: str,
        silver_tables: set[str],
        reference: dict[str, Any],
    ) -> dict[str, Any]:
        if table not in silver_tables:
            return _empty_dataset_coverage_row(table, "missing_table")

        columns = self._columns(conn, SILVER_SCHEMA, table)
        date_column = _preferred_date_column(columns)
        row_count = int(
            conn.execute(f"select count(*) from {SILVER_SCHEMA}.{table}").fetchone()[0]
        )
        sources = self._dataset_sources(conn, table, columns)
        date_stats = self._dataset_date_stats(conn, table, date_column)
        instrument_stats = self._dataset_instrument_stats(conn, table, columns, reference)
        expected = self._dataset_expected_stats(
            conn,
            table=table,
            columns=columns,
            reference=reference,
        )
        return {
            "dataset": table,
            "coverage_kind": _coverage_kind(table),
            "row_count": row_count,
            "source_ids": sources,
            **date_stats,
            **instrument_stats,
            **expected,
        }

    def _dataset_sources(
        self,
        conn: duckdb.DuckDBPyConnection,
        table: str,
        columns: list[str],
    ) -> list[dict[str, Any]]:
        if "source_id" not in columns:
            return []
        return _query_dicts(
            conn,
            f"""
            select source_id, count(*) as row_count
            from {SILVER_SCHEMA}.{table}
            group by source_id
            order by row_count desc, source_id
            limit 8
            """,
        )

    def _dataset_date_stats(
        self,
        conn: duckdb.DuckDBPyConnection,
        table: str,
        date_column: str | None,
    ) -> dict[str, Any]:
        if not date_column:
            return {
                "date_column": None,
                "min_date": None,
                "max_date": None,
                "date_count": None,
            }
        row = conn.execute(
            f"""
            select min({date_column}), max({date_column}), count(distinct {date_column})
            from {SILVER_SCHEMA}.{table}
            """
        ).fetchone()
        return {
            "date_column": date_column,
            "min_date": row[0],
            "max_date": row[1],
            "date_count": int(row[2] or 0),
        }

    def _dataset_instrument_stats(
        self,
        conn: duckdb.DuckDBPyConnection,
        table: str,
        columns: list[str],
        reference: dict[str, Any],
    ) -> dict[str, Any]:
        reference_instruments = set(reference["instruments"])
        if "instrument" not in columns:
            return {
                "instrument_count": None,
                "reference_instrument_count": len(reference_instruments),
                "instruments_with_rows": None,
                "instruments_missing": None,
                "instrument_coverage_percent": None,
            }
        rows = conn.execute(
            f"""
            select distinct instrument
            from {SILVER_SCHEMA}.{table}
            order by instrument
            """
        ).fetchall()
        instruments = {str(row[0]) for row in rows}
        with_rows = len(reference_instruments & instruments) if reference_instruments else len(instruments)
        missing = max(len(reference_instruments) - with_rows, 0)
        percent = _percent(with_rows, len(reference_instruments)) if reference_instruments else None
        return {
            "instrument_count": len(instruments),
            "reference_instrument_count": len(reference_instruments),
            "instruments_with_rows": with_rows,
            "instruments_missing": missing,
            "instrument_coverage_percent": percent,
        }

    def _dataset_expected_stats(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        table: str,
        columns: list[str],
        reference: dict[str, Any],
    ) -> dict[str, Any]:
        reference_instruments = reference["instruments"]
        trade_dates = reference["trade_dates"]
        if (
            table not in REQUIRED_DAILY_COVERAGE_DATASETS
            or "instrument" not in columns
            or "trade_date" not in columns
            or not reference_instruments
            or not trade_dates
        ):
            return {
                "expected_daily_rows": None,
                "present_daily_rows": None,
                "missing_daily_rows": None,
                "daily_coverage_percent": None,
            }
        expected_rows = len(reference_instruments) * len(trade_dates)
        present_rows = self._count_distinct_daily_rows(
            conn,
            table=table,
            instruments=reference_instruments,
            min_date=trade_dates[0],
            max_date=trade_dates[-1],
        )
        present_rows = min(present_rows, expected_rows)
        return {
            "expected_daily_rows": expected_rows,
            "present_daily_rows": present_rows,
            "missing_daily_rows": max(expected_rows - present_rows, 0),
            "daily_coverage_percent": _percent(present_rows, expected_rows),
        }

    def _instrument_coverage(
        self,
        conn: duckdb.DuckDBPyConnection,
        silver_tables: set[str],
        reference: dict[str, Any],
    ) -> dict[str, Any]:
        instruments = reference["instruments"]
        trade_dates = reference["trade_dates"]
        expected_date_count = len(trade_dates)
        if not instruments or not trade_dates:
            return {
                "summary": {
                    "total_instruments": len(instruments),
                    "expected_trade_dates": expected_date_count,
                    "complete_instruments": 0,
                    "missing_instruments": len(instruments),
                    "missing_daily_rows": 0,
                    "complete_percent": 0,
                    "missing_by_dimension": {
                        dataset: len(instruments)
                        for dataset in REQUIRED_DAILY_COVERAGE_DATASETS
                    },
                    "available_by_dimension": {
                        dataset: 0 for dataset in ALL_INSTRUMENT_COVERAGE_DIMENSIONS
                    },
                },
                "rows": [],
                "hidden_count": 0,
            }

        identity_by_instrument = self._instrument_identity_by_instrument(
            conn,
            silver_tables=silver_tables,
            instruments=instruments,
        )
        counts_by_dataset = {
            dataset: self._instrument_daily_counts(
                conn,
                table=dataset,
                silver_tables=silver_tables,
                min_date=trade_dates[0],
                max_date=trade_dates[-1],
            )
            for dataset in REQUIRED_DAILY_COVERAGE_DATASETS
        }
        trade_status_counts = self._instrument_daily_counts(
            conn,
            table="trade_status",
            silver_tables=silver_tables,
            min_date=trade_dates[0],
            max_date=trade_dates[-1],
        )
        news_row_counts = self._instrument_record_counts(
            conn,
            table="news",
            silver_tables=silver_tables,
            date_column="publish_date",
            min_date=trade_dates[0],
            max_date=trade_dates[-1],
        )
        announcement_row_counts = self._instrument_record_counts(
            conn,
            table="announcement",
            silver_tables=silver_tables,
            date_column="publish_date",
            min_date=trade_dates[0],
            max_date=trade_dates[-1],
        )
        news_factor_day_counts = self._instrument_daily_counts(
            conn,
            table="daily_news_factor",
            silver_tables=silver_tables,
            min_date=trade_dates[0],
            max_date=trade_dates[-1],
        )
        announcement_factor_day_counts = self._instrument_daily_counts(
            conn,
            table="daily_announcement_factor",
            silver_tables=silver_tables,
            min_date=trade_dates[0],
            max_date=trade_dates[-1],
        )
        news_factor_sums = self._instrument_numeric_sums(
            conn,
            table="daily_news_factor",
            field="news_count",
            silver_tables=silver_tables,
            min_date=trade_dates[0],
            max_date=trade_dates[-1],
        )
        announcement_factor_sums = self._instrument_numeric_sums(
            conn,
            table="daily_announcement_factor",
            field="announcement_count",
            silver_tables=silver_tables,
            min_date=trade_dates[0],
            max_date=trade_dates[-1],
        )
        missing_by_dimension = {dataset: 0 for dataset in REQUIRED_DAILY_COVERAGE_DATASETS}
        available_by_dimension = {
            dataset: 0 for dataset in ALL_INSTRUMENT_COVERAGE_DIMENSIONS
        }
        rows = []
        complete_count = 0
        for instrument in instruments:
            identity = identity_by_instrument.get(
                instrument,
                _default_instrument_identity(instrument),
            )
            dimension_counts = {
                dataset: min(int(counts.get(instrument, 0)), expected_date_count)
                for dataset, counts in counts_by_dataset.items()
            }
            missing_dimensions = [
                dataset
                for dataset, count in dimension_counts.items()
                if count < expected_date_count
            ]
            for dataset in missing_dimensions:
                missing_by_dimension[dataset] += 1
            complete = not missing_dimensions
            complete_count += 1 if complete else 0
            trade_status_days = min(
                int(trade_status_counts.get(instrument, 0)),
                expected_date_count,
            )
            daily_news_factor_days = min(
                int(news_factor_day_counts.get(instrument, 0)),
                expected_date_count,
            )
            daily_announcement_factor_days = min(
                int(announcement_factor_day_counts.get(instrument, 0)),
                expected_date_count,
            )
            news_rows = int(news_row_counts.get(instrument, 0))
            announcement_rows = int(announcement_row_counts.get(instrument, 0))
            factor_news_count = float(news_factor_sums.get(instrument, 0))
            factor_announcement_count = float(
                announcement_factor_sums.get(instrument, 0)
            )
            all_dimension_counts = {
                "stock_basic": 1 if identity.get("stock_basic_present") else 0,
                "universe_constituent": 1
                if identity.get("universe_constituent_present")
                else 0,
                **dimension_counts,
                "trade_status": trade_status_days,
                "news": news_rows,
                "announcement": announcement_rows,
                "daily_news_factor": daily_news_factor_days,
                "daily_announcement_factor": daily_announcement_factor_days,
            }
            dimension_statuses = {
                "stock_basic": _dimension_status(
                    observed=all_dimension_counts["stock_basic"],
                    expected=1,
                    unit="项",
                ),
                "universe_constituent": _dimension_status(
                    observed=all_dimension_counts["universe_constituent"],
                    expected=1,
                    unit="项",
                ),
                "daily_bar": _dimension_status(
                    observed=dimension_counts["daily_bar"],
                    expected=expected_date_count,
                    unit="天",
                ),
                "adj_factor": _dimension_status(
                    observed=dimension_counts["adj_factor"],
                    expected=expected_date_count,
                    unit="天",
                ),
                "price_limit": _dimension_status(
                    observed=dimension_counts["price_limit"],
                    expected=expected_date_count,
                    unit="天",
                ),
                "trade_status": _dimension_status(
                    observed=trade_status_days,
                    expected=None,
                    unit="天",
                    note="事件/状态类维度，只统计已有记录",
                ),
                "news": _dimension_status(
                    observed=news_rows,
                    expected=None,
                    unit="条",
                    note="事件明细维度，只统计已有记录",
                ),
                "announcement": _dimension_status(
                    observed=announcement_rows,
                    expected=None,
                    unit="条",
                    note="事件明细维度，只统计已有记录",
                ),
                "daily_news_factor": _dimension_status(
                    observed=daily_news_factor_days,
                    expected=None,
                    unit="天",
                    event_count=factor_news_count,
                    event_unit="条",
                    note="文本因子维度，只统计已有因子行",
                ),
                "daily_announcement_factor": _dimension_status(
                    observed=daily_announcement_factor_days,
                    expected=None,
                    unit="天",
                    event_count=factor_announcement_count,
                    event_unit="条",
                    note="文本因子维度，只统计已有因子行",
                ),
            }
            available_dimensions = [
                dataset
                for dataset in ALL_INSTRUMENT_COVERAGE_DIMENSIONS
                if all_dimension_counts.get(dataset, 0) > 0
            ]
            for dataset in available_dimensions:
                available_by_dimension[dataset] += 1
            core_missing_days = sum(
                max(expected_date_count - count, 0)
                for count in dimension_counts.values()
            )
            raw_missing_daily_rows = core_missing_days
            rows.append(
                {
                    "instrument": instrument,
                    "symbol": identity.get("symbol"),
                    "exchange": identity.get("exchange"),
                    "name": identity.get("name"),
                    "industry": identity.get("industry"),
                    "is_active": identity.get("is_active"),
                    "universes": identity.get("universes", []),
                    "stock_basic_present": bool(identity.get("stock_basic_present")),
                    "universe_constituent_present": bool(
                        identity.get("universe_constituent_present")
                    ),
                    "complete": complete,
                    "missing_dimensions": missing_dimensions,
                    "dimension_counts": dimension_counts,
                    "dimension_statuses": dimension_statuses,
                    "all_dimension_counts": all_dimension_counts,
                    "available_dimensions": available_dimensions,
                    "observed_dimension_count": len(available_dimensions),
                    "raw_missing_daily_rows": raw_missing_daily_rows,
                    "core_missing_days": core_missing_days,
                    "trade_status_days": trade_status_days,
                    "news_rows": news_rows,
                    "announcement_rows": announcement_rows,
                    "daily_news_factor_days": daily_news_factor_days,
                    "daily_announcement_factor_days": daily_announcement_factor_days,
                    "factor_news_count": factor_news_count,
                    "factor_announcement_count": factor_announcement_count,
                    "expected_trade_dates": expected_date_count,
                }
            )

        missing_daily_rows = sum(int(row["raw_missing_daily_rows"]) for row in rows)
        rows.sort(
            key=lambda row: (
                row["complete"],
                -row["raw_missing_daily_rows"],
                -len(row["missing_dimensions"]),
                -row["observed_dimension_count"],
                row["instrument"],
            )
        )
        visible_rows = rows[:COVERAGE_INSTRUMENT_LIMIT]
        return {
            "summary": {
                "total_instruments": len(instruments),
                "expected_trade_dates": expected_date_count,
                "complete_instruments": complete_count,
                "missing_instruments": len(instruments) - complete_count,
                "missing_daily_rows": missing_daily_rows,
                "complete_percent": _percent(complete_count, len(instruments)),
                "missing_by_dimension": missing_by_dimension,
                "available_by_dimension": available_by_dimension,
            },
            "rows": visible_rows,
            "hidden_count": max(len(rows) - len(visible_rows), 0),
        }

    def _instrument_identity_by_instrument(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        silver_tables: set[str],
        instruments: list[str],
    ) -> dict[str, dict[str, Any]]:
        identity = {
            instrument: _default_instrument_identity(instrument)
            for instrument in instruments
        }
        if not instruments:
            return identity

        placeholders = ", ".join("?" for _ in instruments)
        if "stock_basic" in silver_tables:
            rows = _query_dicts(
                conn,
                f"""
                select instrument, symbol, exchange, name, industry, is_active
                from {SILVER_SCHEMA}.stock_basic
                where instrument in ({placeholders})
                order by instrument
                """,
                instruments,
            )
            for row in rows:
                target = identity.setdefault(
                    str(row["instrument"]),
                    _default_instrument_identity(str(row["instrument"])),
                )
                target.update(
                    {
                        "symbol": row.get("symbol") or target["symbol"],
                        "exchange": row.get("exchange") or target["exchange"],
                        "name": row.get("name") or target["name"],
                        "industry": row.get("industry"),
                        "is_active": row.get("is_active"),
                        "stock_basic_present": True,
                    }
                )

        if "universe_constituent" in silver_tables:
            rows = _query_dicts(
                conn,
                f"""
                with latest as (
                  select universe, max(snapshot_date) as snapshot_date
                  from {SILVER_SCHEMA}.universe_constituent
                  group by universe
                )
                select c.universe, c.snapshot_date, c.instrument, c.symbol,
                       c.exchange, c.name, c.weight
                from {SILVER_SCHEMA}.universe_constituent c
                join latest l
                  on c.universe = l.universe
                 and c.snapshot_date = l.snapshot_date
                where c.instrument in ({placeholders})
                order by c.instrument, c.universe
                """,
                instruments,
            )
            for row in rows:
                target = identity.setdefault(
                    str(row["instrument"]),
                    _default_instrument_identity(str(row["instrument"])),
                )
                target["symbol"] = row.get("symbol") or target.get("symbol")
                target["exchange"] = row.get("exchange") or target.get("exchange")
                target["name"] = target.get("name") or row.get("name")
                target["universe_constituent_present"] = True
                if row.get("universe"):
                    universes = target.setdefault("universes", [])
                    universe = str(row["universe"])
                    if universe not in universes:
                        universes.append(universe)
        return identity

    def _instrument_daily_counts(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        table: str,
        silver_tables: set[str],
        min_date: Any,
        max_date: Any,
    ) -> dict[str, int]:
        if table not in silver_tables:
            return {}
        rows = conn.execute(
            f"""
            select instrument, count(distinct trade_date) as date_count
            from {SILVER_SCHEMA}.{table}
            where trade_date between ? and ?
            group by instrument
            order by instrument
            """,
            [min_date, max_date],
        ).fetchall()
        return {str(instrument): int(date_count) for instrument, date_count in rows}

    def _instrument_record_counts(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        table: str,
        silver_tables: set[str],
        date_column: str,
        min_date: Any,
        max_date: Any,
    ) -> dict[str, int]:
        if table not in silver_tables:
            return {}
        columns = self._columns(conn, SILVER_SCHEMA, table)
        if "instrument" not in columns or date_column not in columns:
            return {}
        rows = conn.execute(
            f"""
            select instrument, count(*) as row_count
            from {SILVER_SCHEMA}.{table}
            where {date_column} between ? and ?
            group by instrument
            order by instrument
            """,
            [min_date, max_date],
        ).fetchall()
        return {str(instrument): int(row_count) for instrument, row_count in rows}

    def _instrument_numeric_sums(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        table: str,
        field: str,
        silver_tables: set[str],
        min_date: Any,
        max_date: Any,
    ) -> dict[str, float]:
        if table not in silver_tables:
            return {}
        columns = self._columns(conn, SILVER_SCHEMA, table)
        if "instrument" not in columns or "trade_date" not in columns or field not in columns:
            return {}
        rows = conn.execute(
            f"""
            select instrument, coalesce(sum({field}), 0) as field_sum
            from {SILVER_SCHEMA}.{table}
            where trade_date between ? and ?
            group by instrument
            order by instrument
            """,
            [min_date, max_date],
        ).fetchall()
        return {str(instrument): float(field_sum or 0) for instrument, field_sum in rows}

    def _count_distinct_daily_rows(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        table: str,
        instruments: list[str],
        min_date: Any,
        max_date: Any,
    ) -> int:
        instrument_filter = ""
        params: list[Any] = [min_date, max_date]
        if instruments:
            placeholders = ", ".join("?" for _ in instruments)
            instrument_filter = f"and instrument in ({placeholders})"
            params.extend(instruments)
        value = conn.execute(
            f"""
            select count(*)
            from (
              select distinct trade_date, instrument
              from {SILVER_SCHEMA}.{table}
              where trade_date between ? and ?
                {instrument_filter}
            ) daily_rows
            """,
            params,
        ).fetchone()[0]
        return int(value or 0)

    def _columns(
        self,
        conn: duckdb.DuckDBPyConnection,
        schema_name: str,
        table: str,
    ) -> list[str]:
        rows = conn.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = ?
              and table_name = ?
            order by ordinal_position
            """,
            [schema_name, table],
        ).fetchall()
        return [str(row[0]) for row in rows]


class DailyPipelineProcessManager:
    """Start at most one local collection process for the console session."""

    def __init__(self, settings: QdcSettings) -> None:
        self.settings = settings
        self._lock = threading.Lock()
        self._run: dict[str, Any] | None = None

    def start(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._refresh_locked()
            if self._run and self._run["status"] == "running":
                snapshot = self._snapshot_locked()
                snapshot["accepted"] = False
                snapshot["message"] = "控制台采集任务已在运行"
                return snapshot

            command = _daily_pipeline_command(self.settings, payload)
            run_id = str(uuid4())
            now = datetime.now().replace(microsecond=0).isoformat()
            run = {
                "run_id": run_id,
                "status": "running",
                "command": command,
                "start_at": now,
                "end_at": None,
                "return_code": None,
                "stop_requested_at": None,
                "logs": deque(maxlen=RUN_LOG_LIMIT),
                "stdout": deque(maxlen=RUN_LOG_LIMIT),
                "stderr": deque(maxlen=RUN_LOG_LIMIT),
                "process": None,
            }
            process = subprocess.Popen(
                command,
                cwd=str(self.settings.project_root),
                env=_daily_pipeline_process_env(self.settings),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
            run["process"] = process
            self._run = run

        self._start_reader(run_id, process.stdout, "stdout")
        self._start_reader(run_id, process.stderr, "stderr")
        threading.Thread(target=self._watch_process, args=(run_id,), daemon=True).start()

        snapshot = self.snapshot()
        snapshot["accepted"] = True
        return snapshot

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            self._refresh_locked()
            return self._snapshot_locked()

    def stop(self) -> dict[str, Any]:
        with self._lock:
            self._refresh_locked()
            if not self._run or self._run["status"] != "running":
                snapshot = self._snapshot_locked()
                snapshot["accepted"] = False
                snapshot["message"] = "当前没有运行中的控制台采集任务"
                return snapshot
            process: subprocess.Popen[str] | None = self._run.get("process")
            self._run["stop_requested_at"] = datetime.now().replace(microsecond=0).isoformat()

        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=8)

        snapshot = self.snapshot()
        snapshot["accepted"] = True
        return snapshot

    def _start_reader(self, run_id: str, stream: Any, stream_name: str) -> None:
        if stream is None:
            return
        threading.Thread(
            target=self._read_stream,
            args=(run_id, stream, stream_name),
            daemon=True,
        ).start()

    def _read_stream(self, run_id: str, stream: Any, stream_name: str) -> None:
        try:
            for line in iter(stream.readline, ""):
                text = line.rstrip("\n")
                with self._lock:
                    if not self._run or self._run.get("run_id") != run_id:
                        return
                    self._run[stream_name].append(text)
                    self._run["logs"].append({"stream": stream_name, "text": text})
        finally:
            try:
                stream.close()
            except Exception:
                pass

    def _watch_process(self, run_id: str) -> None:
        process: subprocess.Popen[str] | None = None
        with self._lock:
            if self._run and self._run.get("run_id") == run_id:
                process = self._run.get("process")
        if process is None:
            return
        process.wait()
        with self._lock:
            self._refresh_locked()

    def _refresh_locked(self) -> None:
        if not self._run or self._run["status"] != "running":
            return
        process: subprocess.Popen[str] | None = self._run.get("process")
        if process is None:
            return
        return_code = process.poll()
        if return_code is None:
            return
        self._run["return_code"] = int(return_code)
        if self._run.get("stop_requested_at") and return_code != 0:
            self._run["status"] = "stopped"
        else:
            self._run["status"] = "success" if return_code == 0 else "failed"
        self._run["end_at"] = datetime.now().replace(microsecond=0).isoformat()

    def _snapshot_locked(self) -> dict[str, Any]:
        if not self._run:
            return {"status": "ok", "running": False, "run": None}
        run = self._run
        return {
            "status": "ok",
            "running": run["status"] == "running",
            "run": {
                "run_id": run["run_id"],
                "status": run["status"],
                "return_code": run["return_code"],
                "start_at": run["start_at"],
                "end_at": run["end_at"],
                "stop_requested_at": run.get("stop_requested_at"),
                "command": [str(part) for part in run["command"]],
                "log_tail": list(run.get("logs") or [])[-80:],
                "stdout_tail": list(run["stdout"])[-40:],
                "stderr_tail": list(run["stderr"])[-80:],
            },
        }


def _daily_pipeline_command(settings: QdcSettings, payload: dict[str, Any]) -> list[str]:
    workflow = _payload_text(payload, "workflow") or "daily_pipeline"
    if workflow in {"crawl_daily", "crawl-daily"}:
        return _crawl_daily_command(settings, payload)
    if workflow not in {"daily_pipeline", "daily-pipeline"}:
        raise ValueError("workflow must be one of: crawl_daily, daily_pipeline")

    command = [
        sys.executable,
        "-m",
        "quant_data_center.cli",
        "--config",
        str(settings.config_path),
        "daily-pipeline",
        "--watch",
    ]
    run_date = _payload_text(payload, "date")
    if run_date:
        _validate_run_date(run_date)
        command.extend(["--date", run_date])

    symbols = _payload_symbols(payload.get("symbols"))
    if symbols:
        command.extend(["--symbols", symbols])

    batch_size = _payload_int(payload, "batch_size")
    if batch_size is not None:
        if batch_size <= 0 or batch_size > 10000:
            raise ValueError("batch_size must be between 1 and 10000")
        command.extend(["--batch-size", str(batch_size)])

    if _payload_bool(payload, "control_only") is True:
        command.append("--control-only")

    _append_optional_bool(command, payload, key="crawl_documents", flag="crawl-documents")
    _append_refresh_stock_basic(command, payload)
    _append_optional_bool(
        command,
        payload,
        key="skip_crawl_pdf_download",
        flag="skip-crawl-pdf-download",
    )
    for key, flag in (
        ("skip_factors", "skip-factors"),
        ("skip_sync", "skip-sync"),
        ("skip_quality", "skip-quality"),
        ("skip_export", "skip-export"),
    ):
        _append_optional_bool(command, payload, key=key, flag=flag)
    return command


def _crawl_daily_command(settings: QdcSettings, payload: dict[str, Any]) -> list[str]:
    command = [
        sys.executable,
        "-m",
        "quant_data_center.cli",
        "--config",
        str(settings.config_path),
        "crawl-daily",
    ]
    run_date = _payload_text(payload, "date")
    if run_date:
        _validate_run_date(run_date)
        command.extend(["--date", run_date])

    source_id = _payload_text(payload, "source_id")
    if source_id:
        command.extend(["--source-id", source_id])

    symbols = _payload_symbols(payload.get("symbols"))
    if symbols:
        command.extend(["--symbols", symbols])

    for key, flag, upper_bound in (
        ("page_size", "page-size", 10000),
        ("max_pages", "max-pages", 10000),
        ("parallel_sources", "parallel-sources", 64),
    ):
        value = _payload_int(payload, key)
        if value is None:
            continue
        if value <= 0 or value > upper_bound:
            raise ValueError(f"{key} must be between 1 and {upper_bound}")
        command.extend([f"--{flag}", str(value)])

    if _payload_bool(payload, "download_pdfs") is True:
        command.append("--download-pdfs")

    if _payload_bool(payload, "control_only") is True:
        command.append("--control-only")
    return command


def _daily_pipeline_process_env(settings: QdcSettings) -> dict[str, str]:
    env = dict(os.environ)
    src_path = str(settings.project_root / "src")
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = src_path if not existing else f"{src_path}{os.pathsep}{existing}"
    return env


def _payload_text(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _payload_symbols(value: Any) -> str | None:
    if value is None:
        return None
    symbols = [str(item).strip().upper() for item in str(value).split(",") if str(item).strip()]
    if not symbols:
        return None
    if len(symbols) > 10000:
        raise ValueError("symbols is too large")
    return ",".join(symbols)


def _payload_int(payload: dict[str, Any], key: str) -> int | None:
    value = payload.get(key)
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be an integer") from exc


def _payload_bool(payload: dict[str, Any], key: str) -> bool | None:
    if key not in payload:
        return None
    value = payload.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "on"}:
            return True
        if lowered in {"false", "0", "no", "off"}:
            return False
    raise ValueError(f"{key} must be a boolean")


def _append_optional_bool(
    command: list[str],
    payload: dict[str, Any],
    *,
    key: str,
    flag: str,
) -> None:
    value = _payload_bool(payload, key)
    if value is None:
        return
    command.append(f"--{flag}" if value else f"--no-{flag}")


def _append_refresh_stock_basic(command: list[str], payload: dict[str, Any]) -> None:
    value = _payload_bool(payload, "refresh_stock_basic")
    if value is None:
        return
    command.append("--no-skip-stock-basic-refresh" if value else "--skip-stock-basic-refresh")


def _validate_run_date(value: str) -> None:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("date must use YYYY-MM-DD") from exc


def run_console(settings: QdcSettings, *, host: str = "127.0.0.1", port: int = 8765) -> None:
    """Start the blocking local HTTP server."""

    handler = _make_handler(settings=settings, static_root=STATIC_ROOT)
    server = ThreadingHTTPServer((host, port), handler)
    url = f"http://{host}:{port}/"
    print(f"QDC console listening on {url}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:  # pragma: no cover - interactive shutdown
        print("\nQDC console stopped", flush=True)
    finally:
        server.server_close()


def _make_handler(
    *,
    settings: QdcSettings,
    static_root: Path,
) -> type[BaseHTTPRequestHandler]:
    data = QdcConsoleData(settings)
    pipeline_runner = DailyPipelineProcessManager(settings)

    class QdcConsoleHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path.startswith("/api/"):
                self._handle_api(parsed.path, parse_qs(parsed.query))
                return
            self._serve_static(parsed.path)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path.startswith("/api/"):
                self._handle_post_api(parsed.path)
                return
            self._send_json(
                {"status": "fail", "error": f"unsupported path: {parsed.path}"},
                status=HTTPStatus.NOT_FOUND,
            )

        def log_message(self, format: str, *args: Any) -> None:
            return

        def _handle_api(self, path: str, query: dict[str, list[str]]) -> None:
            try:
                if path == "/api/overview":
                    self._send_json(data.overview())
                elif path == "/api/daily-collection-status":
                    self._send_json(
                        data.daily_collection_status(date=_query_value(query, "date"))
                    )
                elif path == "/api/daily-pipeline-run":
                    self._send_json(pipeline_runner.snapshot())
                elif path == "/api/daily-wide-preview":
                    raw_limit = _query_value(query, "limit")
                    self._send_json(
                        data.daily_wide_preview(
                            date=_query_value(query, "date"),
                            mode=_query_value(query, "mode"),
                            limit=int(raw_limit) if raw_limit else None,
                        )
                    )
                elif path == "/api/backfill-tasks":
                    self._send_json(
                        data.backfill_tasks(
                            status=_query_value(query, "status"),
                            dataset=_query_value(query, "dataset"),
                            limit=_query_limit(query),
                        )
                    )
                elif path == "/api/job-runs":
                    self._send_json(data.job_runs(limit=_query_limit(query)))
                elif path == "/api/watermarks":
                    self._send_json(data.watermarks())
                elif path == "/api/quality-issues":
                    self._send_json(
                        data.quality_issues(
                            dataset=_query_value(query, "dataset"),
                            status=_query_value(query, "status"),
                            limit=_query_limit(query),
                        )
                    )
                elif path == "/api/source-objects":
                    self._send_json(
                        data.source_objects(
                            dataset=_query_value(query, "dataset"),
                            layer=_query_value(query, "layer"),
                            limit=_query_limit(query),
                        )
                    )
                elif path == "/api/source-object-file":
                    self._send_bytes(
                        data.source_object_file(
                            object_id=_query_value(query, "object_id") or ""
                        )
                    )
                elif path == "/api/instruments":
                    self._send_json(
                        data.instruments(
                            query=_query_value(query, "query"),
                            limit=_query_limit(query),
                        )
                    )
                elif path == "/api/raw-instrument-preview":
                    self._send_json(
                        data.raw_instrument_preview(
                            instrument=_query_value(query, "instrument") or "",
                            start=_query_value(query, "start"),
                            end=_query_value(query, "end"),
                            limit=_query_limit(query),
                        )
                    )
                elif path == "/api/dataset-preview":
                    dataset = _query_value(query, "dataset") or "daily_bar"
                    self._send_json(
                        data.dataset_preview(
                            dataset=dataset,
                            instrument=_query_value(query, "instrument"),
                            start=_query_value(query, "start"),
                            end=_query_value(query, "end"),
                            limit=_query_limit(query),
                        )
                    )
                elif path in {"/api/instrument-timeline", "/api/factor-preview"}:
                    self._send_json(
                        data.instrument_timeline(
                            instrument=_query_value(query, "instrument") or "",
                            start=_query_value(query, "start"),
                            end=_query_value(query, "end"),
                            limit=_query_limit(query),
                            offset=_query_offset(query),
                        )
                    )
                else:
                    self._send_json(
                        {"status": "fail", "error": f"unknown api endpoint: {path}"},
                        status=HTTPStatus.NOT_FOUND,
                    )
            except ValueError as exc:
                self._send_json(
                    {"status": "fail", "error": str(exc)},
                    status=HTTPStatus.BAD_REQUEST,
                )
            except duckdb.Error as exc:
                if _is_duckdb_lock_error(exc):
                    self._send_json(_locked_api_payload(data, path, query, exc))
                else:
                    self._send_json(
                        {"status": "fail", "error": f"duckdb read error: {exc}"},
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                    )
            except Exception as exc:  # pragma: no cover - defensive HTTP guard
                self._send_json(
                    {"status": "fail", "error": str(exc)},
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )

        def _handle_post_api(self, path: str) -> None:
            try:
                if path == "/api/daily-pipeline-run":
                    self._send_json(pipeline_runner.start(self._read_json_body()))
                elif path == "/api/daily-pipeline-stop":
                    self._send_json(pipeline_runner.stop())
                else:
                    self._send_json(
                        {"status": "fail", "error": f"unknown api endpoint: {path}"},
                        status=HTTPStatus.NOT_FOUND,
                    )
            except ValueError as exc:
                self._send_json(
                    {"status": "fail", "error": str(exc)},
                    status=HTTPStatus.BAD_REQUEST,
                )
            except Exception as exc:  # pragma: no cover - defensive HTTP guard
                self._send_json(
                    {"status": "fail", "error": str(exc)},
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )

        def _read_json_body(self) -> dict[str, Any]:
            raw_length = self.headers.get("Content-Length", "0")
            try:
                length = int(raw_length or "0")
            except ValueError as exc:
                raise ValueError("invalid Content-Length") from exc
            if length > MAX_POST_BODY_BYTES:
                raise ValueError("request body too large")
            if length <= 0:
                return {}
            body = self.rfile.read(length)
            try:
                payload = json.loads(body.decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise ValueError("request body must be JSON") from exc
            if not isinstance(payload, dict):
                raise ValueError("request body must be a JSON object")
            return payload

        def _serve_static(self, request_path: str) -> None:
            relative_path = "index.html" if request_path in {"", "/"} else request_path.lstrip("/")
            relative_path = posixpath.normpath(unquote(relative_path))
            if relative_path.startswith("../") or relative_path == "..":
                self._send_json(
                    {"status": "fail", "error": "invalid static path"},
                    status=HTTPStatus.NOT_FOUND,
                )
                return

            file_path = static_root / relative_path
            if not file_path.is_file():
                file_path = static_root / "index.html"
            if not file_path.is_file():
                self._send_json(
                    {"status": "fail", "error": "console static assets missing"},
                    status=HTTPStatus.NOT_FOUND,
                )
                return

            body = file_path.read_bytes()
            content_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _send_bytes(self, payload: dict[str, Any]) -> None:
            body = bytes(payload["body"])
            filename = str(payload.get("filename") or "source-object")
            content_type = str(payload.get("content_type") or "application/octet-stream")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Disposition", f"inline; filename={quote(filename)}")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _send_json(
            self,
            payload: dict[str, Any],
            *,
            status: HTTPStatus = HTTPStatus.OK,
        ) -> None:
            body = json.dumps(
                _json_safe(payload),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

    return QdcConsoleHandler


def _locked_api_payload(
    data: QdcConsoleData,
    path: str,
    query: dict[str, list[str]],
    exc: BaseException,
) -> dict[str, Any]:
    message = (
        "DuckDB 正在被采集任务写入，控制台暂时显示降级状态并会继续刷新。"
    )
    if path == "/api/overview":
        return data.locked_payload(message)
    if path == "/api/daily-collection-status":
        payload = _empty_daily_collection_status(
            date=_query_value(query, "date") or datetime.now().date().isoformat(),
            database_path=str(data.settings.database_path),
        )
        payload.update({"status": "busy", "database_busy": True, "message": message})
        return payload
    if path == "/api/daily-wide-preview":
        preview_mode = _query_value(query, "mode")
        payload = {
            "status": "busy",
            "database_busy": True,
            "message": message,
            "date": _query_value(query, "date") or datetime.now().date().isoformat(),
            "mode": preview_mode if preview_mode in {"raw", "factor"} else "raw",
            "row_count": 0,
            "hidden_count": 0,
            "columns": _daily_preview_columns(
                preview_mode if preview_mode in {"raw", "factor"} else "raw"
            ),
            "rows": [],
        }
        return payload
    if path == "/api/job-runs":
        return {"status": "busy", "database_busy": True, "message": message, "job_count": 0, "jobs": []}
    if path == "/api/quality-issues":
        return {
            "status": "busy",
            "database_busy": True,
            "message": message,
            "issue_count": 0,
            "issues": [],
        }
    if path == "/api/watermarks":
        return {
            "status": "busy",
            "database_busy": True,
            "message": message,
            "watermark_count": 0,
            "watermarks": [],
        }
    if path == "/api/source-objects":
        return {
            "status": "busy",
            "database_busy": True,
            "message": message,
            "object_count": 0,
            "objects": [],
        }
    if path == "/api/source-object-file":
        return {
            "status": "busy",
            "database_busy": True,
            "message": message,
        }
    if path == "/api/backfill-tasks":
        return {
            "status": "busy",
            "database_busy": True,
            "message": message,
            "task_count": 0,
            "tasks": [],
        }
    if path == "/api/instruments":
        row_limit = _clamp_instrument_limit(_query_limit(query), default=500)
        rows = _configured_instrument_options(
            data.settings,
            query=_query_value(query, "query"),
            limit=row_limit,
        )
        return {
            "status": "busy",
            "database_busy": True,
            "message": message,
            "instrument_count": len(rows),
            "instruments": rows,
        }
    if path == "/api/dataset-preview":
        payload = _empty_dataset_preview(_query_value(query, "dataset") or "daily_bar")
        payload.update({"status": "busy", "database_busy": True, "message": message})
        return payload
    if path == "/api/raw-instrument-preview":
        payload = _empty_raw_instrument_preview(
            _query_value(query, "instrument") or "",
            start=_query_value(query, "start"),
            end=_query_value(query, "end"),
            limit=_clamp_limit(_query_limit(query)),
        )
        payload.update({"status": "busy", "database_busy": True, "message": message})
        return payload
    if path in {"/api/instrument-timeline", "/api/factor-preview"}:
        payload = _empty_instrument_timeline(
            _query_value(query, "instrument") or "",
            start=_query_value(query, "start"),
            end=_query_value(query, "end"),
            limit=_clamp_limit(_query_limit(query)),
            offset=_query_offset(query),
        )
        payload.update({"status": "busy", "database_busy": True, "message": message})
        return payload
    return {
        "status": "busy",
        "database_busy": True,
        "message": f"{message} ({exc})",
    }


def _is_duckdb_lock_error(exc: BaseException) -> bool:
    message = str(exc).lower()
    return "could not set lock" in message or "conflicting lock" in message


def _annotated_document_row(
    row: dict[str, Any],
    *,
    raw_object_ids: dict[str, str],
) -> dict[str, Any]:
    item = dict(row)
    source_id = str(item.get("source_id") or "")
    raw_object_id = str(item.get("raw_object_id") or raw_object_ids.get(source_id) or "")
    metadata_object_id = str(raw_object_ids.get(source_id) or raw_object_id)
    if raw_object_id:
        item["raw_object_id"] = raw_object_id
    if metadata_object_id:
        item["metadata_object_id"] = metadata_object_id
    body_text = _first_document_text(item)
    pdf_object_id = str(item.get("pdf_object_id") or "")
    pdf_status = str(item.get("pdf_download_status") or "")
    has_pdf = bool(pdf_object_id and pdf_status == "success")
    item["has_body"] = bool(body_text)
    item["has_pdf"] = has_pdf
    item["body_text"] = body_text
    if body_text:
        item["content_status"] = "local_text"
        item["content_label"] = (
            "已保存新闻正文预览文本"
            if item.get("news_id")
            else "已保存正文文本"
        )
        if metadata_object_id:
            item["local_object_id"] = metadata_object_id
            item["local_object_kind"] = "metadata_records"
            item["local_url"] = _source_object_url(metadata_object_id)
        return item
    if has_pdf:
        item["content_status"] = "local_pdf"
        item["content_label"] = "已保存 PDF 原文，尚未抽取正文文本"
        item["local_object_id"] = pdf_object_id
        item["local_object_kind"] = "pdf"
        item["local_url"] = _source_object_url(pdf_object_id)
        return item
    if metadata_object_id:
        item["content_status"] = "local_metadata"
        pdf_note = f"，PDF 状态：{pdf_status}" if pdf_status else ""
        item["content_label"] = f"本地仅保存列表/元数据 JSON，未保存正文或 PDF{pdf_note}"
        item["local_object_id"] = metadata_object_id
        item["local_object_kind"] = "metadata_records"
        item["local_url"] = _source_object_url(metadata_object_id)
        return item
    item["content_status"] = "missing_local_content"
    item["content_label"] = "本地未保存正文、PDF 或原始 JSON"
    item["local_object_id"] = None
    item["local_object_kind"] = None
    item["local_url"] = None
    return item


def _first_document_text(row: dict[str, Any]) -> str | None:
    for field in DOCUMENT_LOCAL_CONTENT_FIELDS:
        value = row.get(field)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text[:5000]
    return None


def _source_object_url(object_id: str) -> str:
    return f"/api/source-object-file?object_id={quote(str(object_id), safe='')}"


def _resolve_source_object_path(settings: QdcSettings, uri: Any) -> Path:
    value = str(uri or "").strip()
    if not value:
        raise ValueError("source object uri is empty")
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    resolved = path.resolve()
    roots = [
        _resolve_settings_path(settings.data_root),
        _resolve_settings_path(settings.raw_root),
        _resolve_settings_path(settings.parquet_root),
    ]
    if not any(_is_relative_to(resolved, root) for root in roots):
        raise ValueError(f"source object path is outside configured data roots: {resolved}")
    return resolved


def _resolve_settings_path(path: Path) -> Path:
    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    return candidate.resolve()


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _daily_document_key(row: dict[str, Any]) -> str:
    title_key = _normalize_document_title(row.get("title"))
    if title_key:
        return title_key
    url = str(row.get("url") or "").strip()
    if url:
        return url
    return str(row.get("news_id") or row.get("announcement_id") or "")


def _normalize_document_title(value: Any) -> str:
    text = str(value or "").strip()
    for separator in (":", "："):
        if separator in text:
            prefix, suffix = text.split(separator, 1)
            if 0 < len(prefix.strip()) <= 12 and suffix.strip():
                text = suffix.strip()
                break
    return "".join(text.split()).lower()


def _preferred_document_row(
    current: dict[str, Any],
    candidate: dict[str, Any],
) -> dict[str, Any]:
    current_priority = _document_source_priority(current.get("source_id"))
    candidate_priority = _document_source_priority(candidate.get("source_id"))
    if candidate_priority < current_priority:
        return dict(candidate)
    return dict(current)


def _ordered_source_ids(source_ids: set[Any]) -> list[str]:
    values = [str(source_id) for source_id in source_ids if source_id]
    return sorted(values, key=lambda source_id: (_document_source_priority(source_id), source_id))


def _document_source_priority(source_id: Any) -> int:
    return DOCUMENT_SOURCE_PRIORITY.get(str(source_id or ""), 100)


def _query_dicts(
    conn: duckdb.DuckDBPyConnection,
    query: str,
    params: list[Any] | None = None,
) -> list[dict[str, Any]]:
    rows = conn.execute(query, params or []).fetchall()
    columns = [item[0] for item in conn.description]
    return [_row_to_dict(columns, row) for row in rows]


def _row_to_dict(columns: list[str], row: Any) -> dict[str, Any]:
    result = {}
    for column, value in zip(columns, row, strict=True):
        if isinstance(value, str) and column.endswith("_json"):
            result[column] = json.loads(value)
        elif hasattr(value, "isoformat"):
            result[column] = value.isoformat()
        else:
            result[column] = value
    return result


def _json_safe(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        return _json_safe(value.item())
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def _query_value(query: dict[str, list[str]], key: str) -> str | None:
    value = query.get(key, [""])[0].strip()
    return value or None


def _query_limit(query: dict[str, list[str]]) -> int | None:
    value = _query_value(query, "limit")
    if value is None:
        return None
    return int(value)


def _query_offset(query: dict[str, list[str]]) -> int | None:
    value = _query_value(query, "offset")
    if value is None:
        return None
    return int(value)


def _clamp_limit(value: int | None, *, default: int = DEFAULT_LIMIT) -> int:
    if value is None or value <= 0:
        return default
    return min(value, MAX_LIMIT)


def _clamp_instrument_limit(value: int | None, *, default: int = 500) -> int:
    if value is None or value <= 0:
        return default
    return min(value, MAX_INSTRUMENT_OPTION_LIMIT)


def _clamp_offset(value: int | None) -> int:
    if value is None or value <= 0:
        return 0
    return min(value, MAX_OFFSET)


def _normalize_preview_instrument(instrument: str) -> str:
    instrument = instrument.strip()
    if not instrument:
        raise ValueError("instrument preview requires instrument")
    return normalize_instrument(instrument)


def _configured_instruments(settings: QdcSettings) -> list[str]:
    instruments = {
        normalize_instrument(instrument)
        for symbols in settings.universes.values()
        for instrument in symbols
        if instrument
    }
    return sorted(instruments)


def _configured_instrument_options(
    settings: QdcSettings,
    *,
    query: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    normalized_query = (query or "").strip().upper()
    rows = []
    for instrument in _configured_instruments(settings):
        identity = _default_instrument_identity(instrument)
        option = _instrument_option_from_identity(identity)
        if normalized_query and not _instrument_option_matches(option, normalized_query):
            continue
        rows.append(option)
        if len(rows) >= limit:
            break
    return rows


def _instrument_option_from_identity(identity: dict[str, Any]) -> dict[str, Any]:
    return {
        "instrument": identity.get("instrument"),
        "symbol": identity.get("symbol"),
        "exchange": identity.get("exchange"),
        "name": identity.get("name"),
        "industry": identity.get("industry"),
        "universes": identity.get("universes", []),
        "label": _instrument_option_label(identity),
    }


def _instrument_option_label(identity: dict[str, Any]) -> str:
    parts = [
        str(identity.get("instrument") or ""),
        str(identity.get("name") or "").strip(),
        str(identity.get("industry") or "").strip(),
    ]
    universes = identity.get("universes") or []
    if universes:
        parts.append(",".join(str(item) for item in universes))
    return " / ".join(part for part in parts if part)


def _instrument_option_matches(option: dict[str, Any], query: str) -> bool:
    haystack = " ".join(
        str(value)
        for value in [
            option.get("instrument"),
            option.get("symbol"),
            option.get("exchange"),
            option.get("name"),
            option.get("industry"),
            " ".join(option.get("universes") or []),
        ]
        if value
    ).upper()
    return query in haystack


def _empty_raw_instrument_preview(
    instrument: str,
    *,
    start: str | None,
    end: str | None,
    limit: int,
) -> dict[str, Any]:
    return {
        "status": "ok",
        "instrument": instrument,
        "start": start,
        "end": end,
        "limit": limit,
        "summary": {
            "dataset_count": 0,
            "object_count": 0,
            "row_count": 0,
            "datasets": [],
        },
        "sections": [],
    }


def _raw_preview_sections(
    *,
    objects: list[dict[str, Any]],
    instrument: str,
    start: str | None,
    end: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    sections: dict[str, dict[str, Any]] = {}
    for source_object in objects:
        dataset = str(source_object.get("dataset") or "")
        payload = _read_raw_payload(source_object)
        object_matches = _raw_payload_matches_instrument(
            payload,
            source_object=source_object,
            instrument=instrument,
        )
        rows, columns = _raw_rows_from_payload(
            payload,
            dataset=dataset,
            instrument=instrument,
            start=start,
            end=end,
            object_matches=object_matches,
            limit=limit,
        )
        has_error = bool(payload.get("error")) and object_matches
        if not rows and not has_error:
            continue
        section = sections.setdefault(
            dataset,
            {
                "dataset": dataset,
                "objects": [],
                "columns": [],
                "rows": [],
            },
        )
        section["objects"].append(_raw_object_meta(source_object, payload, len(rows)))
        for column in columns:
            if column not in section["columns"]:
                section["columns"].append(column)
        available = max(limit - len(section["rows"]), 0)
        if available:
            section["rows"].extend(rows[:available])

    return [
        {
            **section,
            "object_count": len(section["objects"]),
            "row_count": len(section["rows"]),
        }
        for _dataset, section in sorted(sections.items())
    ]


def _read_raw_payload(source_object: dict[str, Any]) -> dict[str, Any]:
    uri = str(source_object.get("uri") or "")
    path = Path(uri)
    if not path.exists():
        return {
            "function": None,
            "params": {},
            "records": [],
            "error": f"raw 文件不存在：{uri}",
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
        if isinstance(payload, list):
            return {"function": None, "params": {}, "records": payload}
        return {"function": None, "params": {}, "records": [{"value": payload}]}
    except json.JSONDecodeError as exc:
        return {
            "function": None,
            "params": {},
            "records": [],
            "error": f"raw JSON 解析失败：{exc}",
        }


def _raw_payload_matches_instrument(
    payload: dict[str, Any],
    *,
    source_object: dict[str, Any],
    instrument: str,
) -> bool:
    params = payload.get("params") or {}
    if isinstance(params, dict):
        for value in params.values():
            if _raw_value_matches_instrument(value, instrument):
                return True
    uri = str(source_object.get("uri") or "")
    return instrument in uri.upper() or instrument_to_symbol(instrument) in uri


def _raw_rows_from_payload(
    payload: dict[str, Any],
    *,
    dataset: str,
    instrument: str,
    start: str | None,
    end: str | None,
    object_matches: bool,
    limit: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    if dataset == "adj_factor":
        return _raw_adj_factor_rows_from_payload(
            payload,
            instrument=instrument,
            start=start,
            end=end,
            object_matches=object_matches,
            limit=limit,
        )
    rows = []
    columns: list[str] = []
    for record_set, records in _raw_record_sets(payload):
        for row_index, record in enumerate(records, start=1):
            raw_record = record if isinstance(record, dict) else {"value": record}
            if not object_matches and not _raw_record_matches_instrument(raw_record, instrument):
                continue
            if not _raw_record_matches_date(raw_record, start=start, end=end):
                continue
            public_row = _raw_factor_input_row(
                dataset=dataset,
                record_set=record_set,
                row_index=row_index,
                record=raw_record,
                instrument=instrument,
            )
            if not public_row:
                continue
            for column in public_row:
                if column not in columns:
                    columns.append(column)
            rows.append(public_row)
            if len(rows) >= limit:
                return rows, columns
    return rows, columns


def _raw_adj_factor_rows_from_payload(
    payload: dict[str, Any],
    *,
    instrument: str,
    start: str | None,
    end: str | None,
    object_matches: bool,
    limit: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    raw_records = payload.get("raw_records")
    if not isinstance(raw_records, list):
        raw_records = payload.get("records")
    if not isinstance(raw_records, list):
        raw_records = []
    qfq_records = payload.get("qfq_records")
    if not isinstance(qfq_records, list):
        qfq_records = []
    qfq_by_date = {
        _raw_record_date(record): record
        for record in qfq_records
        if isinstance(record, dict) and _raw_record_date(record)
    }
    rows = []
    columns: list[str] = []
    for record in raw_records:
        raw_record = record if isinstance(record, dict) else {"value": record}
        if not object_matches and not _raw_record_matches_instrument(raw_record, instrument):
            continue
        if not _raw_record_matches_date(raw_record, start=start, end=end):
            continue
        trade_date = _raw_record_date(raw_record)
        qfq_record = qfq_by_date.get(trade_date, {})
        raw_close = _raw_first_value(raw_record, ("close", "收盘", "收盘价", "最新价"))
        qfq_close = _raw_first_value(qfq_record, ("close", "收盘", "收盘价", "最新价"))
        raw_close_number = _raw_number(raw_close)
        qfq_close_number = _raw_number(qfq_close)
        adj_factor = (
            round(qfq_close_number / raw_close_number, 10)
            if raw_close_number not in (None, 0) and qfq_close_number is not None
            else None
        )
        public_row = _drop_empty_values(
            {
                "factor_input": RAW_FACTOR_INPUT_PURPOSES["adj_factor"],
                "trade_date": trade_date,
                "instrument": _raw_record_instrument(raw_record) or instrument,
                "symbol": _raw_first_value(raw_record, _raw_symbol_keys()),
                "adj_factor": adj_factor,
                "factor_type": "qfq_close_ratio_v0_inferred" if adj_factor is not None else None,
                "raw_close": raw_close,
                "qfq_close": qfq_close,
            }
        )
        if not {"adj_factor", "raw_close", "qfq_close"} & set(public_row):
            continue
        for column in public_row:
            if column not in columns:
                columns.append(column)
        rows.append(public_row)
        if len(rows) >= limit:
            return rows, columns
    return rows, columns


def _raw_factor_input_row(
    *,
    dataset: str,
    record_set: str,
    row_index: int,
    record: dict[str, Any],
    instrument: str,
) -> dict[str, Any]:
    if dataset == "stock_basic":
        row = {
            "factor_input": RAW_FACTOR_INPUT_PURPOSES[dataset],
            "instrument": _raw_record_instrument(record) or instrument,
            "symbol": _raw_first_value(record, _raw_symbol_keys()),
            "exchange": _raw_first_value(record, ("exchange", "市场", "交易所")),
            "name": _raw_first_value(
                record,
                ("name", "名称", "股票简称", "证券简称", "成分券名称"),
            ),
            "industry": _raw_first_value(record, ("industry", "行业", "所属行业")),
            "list_date": _raw_date_from_keys(record, ("list_date", "上市日期")),
            "delist_date": _raw_date_from_keys(record, ("delist_date", "退市日期")),
        }
    elif dataset == "universe_constituent":
        row = {
            "factor_input": RAW_FACTOR_INPUT_PURPOSES[dataset],
            "snapshot_date": _raw_date_from_keys(record, ("snapshot_date", "日期", "调整日期")),
            "instrument": _raw_record_instrument(record) or instrument,
            "symbol": _raw_first_value(record, _raw_symbol_keys()),
            "name": _raw_first_value(record, ("name", "成分券名称", "证券简称", "股票简称")),
            "weight": _raw_first_value(record, ("weight", "权重", "权重(%)")),
        }
    elif dataset in {"daily_bar", "adj_factor", "price_limit", "trade_status"}:
        row = _raw_daily_input_row(dataset, record, instrument)
    elif dataset in {"news", "announcement"}:
        row = _raw_document_input_row(dataset, record, instrument)
    else:
        row = {
            "factor_input": RAW_FACTOR_INPUT_PURPOSES.get(dataset, "后续因子加工输入"),
            "record_set": record_set,
            "row_index": row_index,
        }
    public_row = _drop_empty_values(row)
    if dataset in {"daily_bar", "adj_factor", "price_limit", "trade_status"}:
        value_fields = {
            "daily_bar": (
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "volume",
                "amount",
                "vwap",
                "turnover_rate",
                "outstanding_share",
            ),
            "adj_factor": ("adj_factor", "factor_type"),
            "price_limit": ("limit_up", "limit_down", "prev_close", "limit_rule"),
            "trade_status": (
                "trade_status",
                "halt_reason",
                "halt_start_date",
                "halt_end_date",
                "halt_period",
                "expected_resume_date",
                "market",
            ),
        }[dataset]
        if not any(field in public_row for field in value_fields):
            return {}
    useful_keys = set(public_row) - {"factor_input", "instrument", "symbol"}
    return public_row if useful_keys else {}


def _raw_daily_input_row(
    dataset: str,
    record: dict[str, Any],
    instrument: str,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "factor_input": RAW_FACTOR_INPUT_PURPOSES[dataset],
        "trade_date": _raw_record_date(record),
        "instrument": _raw_record_instrument(record) or instrument,
        "symbol": _raw_first_value(record, _raw_symbol_keys()),
    }
    if dataset == "daily_bar":
        row.update(
            {
                "open": _raw_first_value(record, ("open", "开盘", "开盘价")),
                "high": _raw_first_value(record, ("high", "最高", "最高价")),
                "low": _raw_first_value(record, ("low", "最低", "最低价")),
                "close": _raw_first_value(record, ("close", "收盘", "收盘价", "最新价")),
                "pre_close": _raw_first_value(record, ("pre_close", "昨收", "昨收价")),
                "volume": _raw_first_value(record, ("volume", "成交量")),
                "amount": _raw_first_value(record, ("amount", "成交额")),
                "vwap": _raw_first_value(record, ("vwap", "成交均价", "均价")),
                "turnover_rate": _raw_first_value(record, ("turnover_rate", "turnover", "换手率")),
                "outstanding_share": _raw_first_value(
                    record,
                    ("outstanding_share", "流通股本", "流通股"),
                ),
            }
        )
    if dataset == "adj_factor":
        row.update(
            {
                "adj_factor": _raw_first_value(record, ("adj_factor", "复权因子", "factor")),
                "factor_type": _raw_first_value(record, ("factor_type", "复权类型")),
            }
        )
    if dataset == "price_limit":
        row.update(
            {
                "limit_up": _raw_first_value(record, ("limit_up", "涨停价")),
                "limit_down": _raw_first_value(record, ("limit_down", "跌停价")),
                "prev_close": _raw_first_value(record, ("prev_close", "pre_close", "前收盘价", "昨收")),
                "limit_rule": _raw_first_value(record, ("limit_rule", "涨跌停规则", "规则")),
            }
        )
    if dataset == "trade_status":
        row.update(
            {
                "trade_status": _raw_first_value(record, ("trade_status", "交易状态", "停牌状态")),
                "halt_reason": _raw_first_value(record, ("halt_reason", "停牌原因", "原因")),
                "halt_start_date": _raw_date_from_keys(record, ("halt_start_date", "停牌时间")),
                "halt_end_date": _raw_date_from_keys(record, ("halt_end_date", "停牌截止时间")),
                "halt_period": _raw_first_value(record, ("halt_period", "停牌期限")),
                "expected_resume_date": _raw_date_from_keys(
                    record,
                    ("expected_resume_date", "预计复牌时间", "source_update_time"),
                ),
                "market": _raw_first_value(record, ("market", "所属市场")),
            }
        )
    return row


def _raw_document_input_row(
    dataset: str,
    record: dict[str, Any],
    instrument: str,
) -> dict[str, Any]:
    return {
        "factor_input": RAW_FACTOR_INPUT_PURPOSES[dataset],
        "publish_date": _raw_record_date(record),
        "instrument": _raw_record_instrument(record) or instrument,
        "symbol": _raw_first_value(record, _raw_symbol_keys()),
        "title": _raw_first_value(record, ("title", "新闻标题", "公告标题", "标题")),
        "url": _raw_first_value(record, ("url", "链接", "公告链接", "新闻链接", "网址", "URL")),
        "source": _raw_first_value(record, ("source", "来源", "新闻来源", "文章来源")),
        "document_type": _raw_first_value(record, ("document_type", "公告类型", "类型", "category")),
        "keyword": _raw_first_value(record, ("keyword", "关键词")),
    }


def _raw_record_instrument(record: dict[str, Any]) -> str | None:
    value = _raw_first_value(record, _raw_symbol_keys())
    if value is None:
        return None
    try:
        return normalize_instrument(str(value))
    except ValueError:
        return None


def _raw_symbol_keys() -> tuple[str, ...]:
    return (
        "instrument",
        "symbol",
        "code",
        "股票代码",
        "证券代码",
        "代码",
        "成分券代码",
        "样本代码",
        "SECURITY_CODE",
    )


def _raw_first_value(record: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key not in record:
            continue
        value = _raw_public_value(record.get(key))
        if value is not None and value != "":
            return value
    return None


def _raw_date_from_keys(record: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        if key in record:
            return _normalize_date_text(record.get(key))
    return None


def _raw_public_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list, tuple, set)):
        return None
    if isinstance(value, str):
        return value.strip()
    return value


def _raw_number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _drop_empty_values(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in row.items()
        if value is not None and value != ""
    }


def _raw_record_sets(payload: dict[str, Any]) -> list[tuple[str, list[Any]]]:
    record_sets = []
    for key in ("records", "raw_records", "qfq_records"):
        records = payload.get(key)
        if isinstance(records, list):
            record_sets.append((key, records))
    return record_sets


def _raw_record_matches_instrument(record: dict[str, Any], instrument: str) -> bool:
    for key in (
        "instrument",
        "symbol",
        "code",
        "股票代码",
        "证券代码",
        "代码",
        "成分券代码",
        "样本代码",
        "SECURITY_CODE",
    ):
        if key in record and _raw_value_matches_instrument(record.get(key), instrument):
            return True
    return False


def _raw_value_matches_instrument(value: Any, instrument: str) -> bool:
    if value is None:
        return False
    text = str(value).strip().upper()
    if not text:
        return False
    if text == instrument or text == instrument_to_symbol(instrument):
        return True
    try:
        return normalize_instrument(text) == instrument
    except ValueError:
        return False


def _raw_record_matches_date(
    record: dict[str, Any],
    *,
    start: str | None,
    end: str | None,
) -> bool:
    date_text = _raw_record_date(record)
    if not date_text:
        return True
    if start and date_text < start:
        return False
    if end and date_text > end:
        return False
    return True


def _raw_record_date(record: dict[str, Any]) -> str | None:
    for key in (
        "trade_date",
        "publish_date",
        "date",
        "日期",
        "公告日期",
        "新闻时间",
        "发布时间",
        "公告时间",
        "time",
    ):
        if key in record:
            return _normalize_date_text(record.get(key))
    return None


def _normalize_date_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    return None


def _raw_object_meta(
    source_object: dict[str, Any],
    payload: dict[str, Any],
    row_count: int,
) -> dict[str, Any]:
    return {
        "object_id": source_object.get("object_id"),
        "dataset": source_object.get("dataset"),
        "source_id": source_object.get("source_id"),
        "function": payload.get("function"),
        "parameter_summary": _raw_parameter_summary(payload.get("params") or {}),
        "record_count": _raw_payload_record_count(payload),
        "error": payload.get("error"),
        "row_count": row_count,
        "uri": source_object.get("uri"),
        "created_at": source_object.get("created_at"),
        "observed_at": source_object.get("observed_at"),
        "size_bytes": source_object.get("size_bytes"),
    }


def _raw_parameter_summary(params: Any) -> str:
    if not isinstance(params, dict) or not params:
        return "-"
    pairs = []
    for key, value in params.items():
        display = _raw_parameter_value(value)
        if display:
            pairs.append(f"{key}={display}")
        if len(pairs) >= 6:
            break
    return "；".join(pairs) if pairs else "-"


def _raw_parameter_value(value: Any) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    if isinstance(value, (list, tuple, set)):
        scalars = [
            str(item)
            for item in list(value)[:5]
            if isinstance(item, (str, int, float, bool))
        ]
        suffix = "..." if len(value) > 5 else ""
        return ",".join(scalars) + suffix
    return type(value).__name__


def _raw_payload_record_count(payload: dict[str, Any]) -> int:
    return sum(len(records) for _record_set, records in _raw_record_sets(payload))


def _raw_preview_summary(sections: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "dataset_count": len(sections),
        "object_count": sum(len(section.get("objects") or []) for section in sections),
        "row_count": sum(int(section.get("row_count") or 0) for section in sections),
        "datasets": [section.get("dataset") for section in sections],
    }


def _empty_dataset_preview(dataset: str) -> dict[str, Any]:
    return {
        "status": "ok",
        "dataset": dataset,
        "columns": [],
        "date_column": None,
        "supports_instrument_filter": False,
        "supports_date_filter": False,
        "row_count": 0,
        "filtered_row_count": 0,
        "total_row_count": 0,
        "summary": {
            "total_row_count": 0,
            "filtered_row_count": 0,
            "date_column": None,
            "supports_instrument_filter": False,
            "supports_date_filter": False,
            "min_date": None,
            "max_date": None,
            "date_count": None,
            "instrument_count": None,
            "source_ids": [],
        },
        "rows": [],
    }


def _empty_instrument_timeline(
    instrument: str,
    *,
    start: str | None,
    end: str | None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
) -> dict[str, Any]:
    return {
        "status": "ok",
        "instrument": instrument,
        "start": start,
        "end": end,
        "limit": limit,
        "offset": offset,
        "page": (offset // limit) + 1 if limit else 1,
        "page_size": limit,
        "has_previous": offset > 0,
        "has_next": False,
        "summary": _instrument_timeline_summary(
            timeline_rows=[],
            news_rows=[],
            announcement_rows=[],
        ),
        "timeline_rows": [],
        "news_rows": [],
        "announcement_rows": [],
    }


def _instrument_timeline_summary(
    *,
    timeline_rows: list[dict[str, Any]],
    news_rows: list[dict[str, Any]],
    announcement_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    core_complete_days = sum(
        1
        for row in timeline_rows
        if row.get("close") is not None
        and row.get("adj_factor") is not None
        and row.get("limit_up") is not None
        and row.get("limit_down") is not None
    )
    trade_dates = [row.get("trade_date") for row in timeline_rows if row.get("trade_date")]
    return {
        "trade_date_count": len(timeline_rows),
        "core_complete_days": core_complete_days,
        "news_rows": len(news_rows),
        "announcement_rows": len(announcement_rows),
        "factor_news_count": _sum_numeric(timeline_rows, "news_count"),
        "factor_announcement_count": _sum_numeric(timeline_rows, "announcement_count"),
        "min_trade_date": min(trade_dates) if trade_dates else None,
        "max_trade_date": max(trade_dates) if trade_dates else None,
    }


def _sum_numeric(rows: list[dict[str, Any]], field: str) -> float:
    total = 0.0
    for row in rows:
        value = row.get(field)
        if value is not None:
            total += float(value)
    return total


def _instrument_date_filter(
    *,
    instrument: str,
    date_column: str,
    start: str | None,
    end: str | None,
) -> tuple[str, list[Any]]:
    filters = ["instrument = ?"]
    params: list[Any] = [instrument]
    if start:
        filters.append(f"{date_column} >= ?")
        params.append(start)
    if end:
        filters.append(f"{date_column} <= ?")
        params.append(end)
    return " and ".join(filters), params


def _default_instrument_identity(instrument: str) -> dict[str, Any]:
    return {
        "instrument": instrument,
        "symbol": _instrument_symbol(instrument),
        "exchange": _instrument_exchange(instrument),
        "name": None,
        "industry": None,
        "is_active": None,
        "universes": [],
        "stock_basic_present": False,
        "universe_constituent_present": False,
    }


def _dimension_status(
    *,
    observed: int | float,
    expected: int | None,
    unit: str,
    event_count: float | None = None,
    event_unit: str | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    observed_number = float(observed or 0)
    observed_value: int | float = (
        int(observed_number) if observed_number.is_integer() else observed_number
    )
    if expected is None:
        status = "observed" if observed_number > 0 else "empty"
        complete = None
        missing = None
    else:
        missing = max(int(expected) - int(observed_number), 0)
        complete = missing == 0
        status = "complete" if complete else "missing"
    return {
        "status": status,
        "observed": observed_value,
        "expected": expected,
        "missing": missing,
        "complete": complete,
        "unit": unit,
        "event_count": event_count,
        "event_unit": event_unit,
        "note": note,
    }


def _instrument_symbol(instrument: str) -> str:
    if len(instrument) > 2 and instrument[:2] in {"SH", "SZ", "BJ"}:
        return instrument[2:]
    return instrument


def _instrument_exchange(instrument: str) -> str | None:
    if len(instrument) > 2 and instrument[:2] in {"SH", "SZ", "BJ"}:
        return instrument[:2]
    return None


def _empty_daily_collection_status(*, date: str, database_path: str) -> dict[str, Any]:
    return {
        "status": "ok",
        "date": date,
        "database_exists": False,
        "database_path": database_path,
        "updated_at": datetime.now().replace(microsecond=0).isoformat(),
        "reference": {"source": "none", "expected_instrument_count": 0},
        "verdict": _daily_verdict(
            database_exists=False,
            expected_count=0,
            collection={
                "collected_instrument_count": 0,
                "remaining_instrument_count": 0,
                "collection_percent": 0,
                "core_complete_instrument_count": 0,
                "core_complete_percent": 0,
                "problem_instrument_count": 0,
            },
            batches={
                "total_batch_count": 0,
                "success_count": 0,
                "running_count": 0,
                "pending_count": 0,
                "failed_count": 0,
                "stale_running_count": 0,
                "complete_percent": 0,
                "state": "empty",
            },
            quality_summary=_empty_daily_quality_summary(),
            source_summary_rows=[],
            qlib_provider_status={"status": "pending", "issues": []},
        ),
        "collection": {
            "collected_instrument_count": 0,
            "remaining_instrument_count": 0,
            "collection_percent": 0,
            "core_complete_instrument_count": 0,
            "core_complete_percent": 0,
            "problem_instrument_count": 0,
        },
        "batches": {
            "total_batch_count": 0,
            "success_count": 0,
            "running_count": 0,
            "pending_count": 0,
            "failed_count": 0,
            "stale_running_count": 0,
            "complete_percent": 0,
            "state": "empty",
        },
        "stage_rows": [],
        "quality_summary": _empty_daily_quality_summary(),
        "source_summary_rows": [],
        "batch_rows": [],
        "batch_task_rows": [],
        "crawl_task_rows": [],
        "dataset_rows": [],
        "source_dimension_rows": [],
        "quality_issue_rows": [],
        "issue_rows": [],
    }


def _daily_issue_rows(
    reference_rows: list[dict[str, Any]],
    required_sets: dict[str, set[str]],
) -> list[dict[str, Any]]:
    rows = []
    for identity in reference_rows:
        instrument = str(identity.get("instrument") or "")
        missing = [
            dataset for dataset, instruments in required_sets.items() if instrument not in instruments
        ]
        if not missing:
            continue
        rows.append(
            {
                "instrument": instrument,
                "symbol": identity.get("symbol") or _instrument_symbol(instrument),
                "exchange": identity.get("exchange") or _instrument_exchange(instrument),
                "name": identity.get("name"),
                "industry": identity.get("industry"),
                "missing_dimensions": missing,
                "daily_bar": "missing" if "daily_bar" in missing else "ok",
                "adj_factor": "missing" if "adj_factor" in missing else "ok",
                "price_limit": "missing" if "price_limit" in missing else "ok",
            }
        )
    return rows


def _daily_batch_summary(batch_rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = sum(int(row.get("total_batch_count") or 0) for row in batch_rows)
    success = sum(int(row.get("success_count") or 0) for row in batch_rows)
    running = sum(int(row.get("running_count") or 0) for row in batch_rows)
    pending = sum(int(row.get("pending_count") or 0) for row in batch_rows)
    failed = sum(int(row.get("failed_count") or 0) for row in batch_rows)
    stale = sum(int(row.get("stale_running_count") or 0) for row in batch_rows)
    return {
        "total_batch_count": total,
        "success_count": success,
        "running_count": running,
        "pending_count": pending,
        "failed_count": failed,
        "stale_running_count": stale,
        "complete_percent": _percent(success, total),
        "state": _progress_state(
            total=total,
            success=success,
            failed=failed,
            running=running,
            pending=pending,
            stale=stale,
        ),
    }


def _daily_quality_summary(
    *,
    collection: dict[str, Any],
    batches: dict[str, Any],
    source_dimension_rows: list[dict[str, Any]],
    quality_issue_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    dimension_rows = {
        dimension: {
            "dimension": dimension,
            "label": QUALITY_DIMENSION_LABELS[dimension],
            "status": "success",
            "affected_count": 0,
            "issue_count": 0,
            "sample": None,
        }
        for dimension in QUALITY_DIMENSION_LABELS
    }

    completeness = int(collection.get("problem_instrument_count") or 0)
    if completeness:
        dimension_rows["completeness"].update(
            {
                "status": "failed",
                "affected_count": completeness,
                "sample": "存在核心日频缺失标的",
            }
        )

    stale = int(batches.get("stale_running_count") or 0)
    if stale:
        dimension_rows["freshness"].update(
            {
                "status": "failed",
                "affected_count": stale,
                "sample": "存在疑似卡住批次",
            }
        )

    source_totals = _daily_source_failure_totals(source_dimension_rows)
    source_failures = source_totals["timeout_count"] + source_totals["error_count"]
    if source_failures:
        dimension_rows["source"].update(
            {
                "status": "failed",
                "affected_count": source_failures,
                "sample": "存在供应商超时或错误",
            }
        )

    for issue in quality_issue_rows:
        issue_type = str(issue.get("issue_type") or "")
        dimension = QUALITY_ISSUE_DIMENSIONS.get(issue_type, "validity")
        row = dimension_rows[dimension]
        row["issue_count"] = int(row.get("issue_count") or 0) + 1
        row["affected_count"] = int(row.get("affected_count") or 0) + 1
        row["status"] = "failed"
        if not row.get("sample"):
            row["sample"] = issue.get("message") or issue_type

    rows = list(dimension_rows.values())
    failed_count = sum(1 for row in rows if row.get("status") == "failed")
    return {
        "status": "failed" if failed_count else "success",
        "failed_dimension_count": failed_count,
        "open_issue_count": len(quality_issue_rows),
        "source_timeout_count": source_totals["timeout_count"],
        "source_error_count": source_totals["error_count"],
        "rows": rows,
    }


def _empty_daily_quality_summary() -> dict[str, Any]:
    return {
        "status": "success",
        "failed_dimension_count": 0,
        "open_issue_count": 0,
        "source_timeout_count": 0,
        "source_error_count": 0,
        "rows": [
            {
                "dimension": dimension,
                "label": label,
                "status": "success",
                "affected_count": 0,
                "issue_count": 0,
                "sample": None,
            }
            for dimension, label in QUALITY_DIMENSION_LABELS.items()
        ],
    }


def _daily_source_failure_totals(rows: list[dict[str, Any]]) -> dict[str, int]:
    by_source_dataset: dict[tuple[str, str], dict[str, int]] = {}
    for row in rows:
        key = (str(row.get("source_id") or "-"), str(row.get("dataset") or "-"))
        item = by_source_dataset.setdefault(
            key,
            {"timeout_count": 0, "error_count": 0, "failed_task_count": 0},
        )
        item["timeout_count"] = max(
            item["timeout_count"],
            int(row.get("timeout_count") or 0),
        )
        item["error_count"] = max(item["error_count"], int(row.get("error_count") or 0))
        item["failed_task_count"] = max(
            item["failed_task_count"],
            int(row.get("failed_task_count") or 0),
        )
    return {
        "timeout_count": sum(item["timeout_count"] for item in by_source_dataset.values()),
        "error_count": sum(item["error_count"] for item in by_source_dataset.values()),
        "failed_task_count": sum(
            item["failed_task_count"] for item in by_source_dataset.values()
        ),
    }


def _daily_stage_rows(
    *,
    expected_count: int,
    collection: dict[str, Any],
    batches: dict[str, Any],
    dataset_rows: list[dict[str, Any]],
    source_summary_rows: list[dict[str, Any]],
    quality_summary: dict[str, Any],
    job_stage_rows: dict[str, dict[str, Any]],
    qlib_provider_status: dict[str, Any],
) -> list[dict[str, Any]]:
    dataset_by_name = {str(row.get("dataset") or ""): row for row in dataset_rows}
    qlib_issues = qlib_provider_status.get("issues") or []
    qlib_latest = qlib_provider_status.get("calendar_latest_date") or "-"
    qlib_expected = qlib_provider_status.get("expected_latest_date") or "-"
    rows = [
        {
            "stage_id": "qlib_provider",
            "label": "Qlib Provider",
            "status": qlib_provider_status.get("status") or "pending",
            "progress_percent": 100 if qlib_provider_status.get("status") == "ok" else 0,
            "primary": f"最新日历 {qlib_latest}",
            "secondary": (
                f"预期 {qlib_expected}；"
                f"样本 {len(qlib_provider_status.get('sample_instruments') or [])}；"
                f"问题 {len(qlib_issues)}"
            ),
        },
        {
            "stage_id": "universe",
            "label": "标的清单 (Universe)",
            "status": "success" if expected_count else "pending",
            "progress_percent": 100 if expected_count else 0,
            "primary": f"应采 {expected_count} 只标的",
            "secondary": "来自 stock_basic / universe / 当日观测标的",
        },
        {
            "stage_id": "daily_pipeline",
            "label": "采集任务 (Daily pipeline)",
            "status": batches.get("state") or "pending",
            "progress_percent": batches.get("complete_percent") or 0,
            "primary": (
                f"成功 {int(batches.get('success_count') or 0)} / "
                f"总批次 {int(batches.get('total_batch_count') or 0)}"
            ),
            "secondary": (
                f"待执行 {int(batches.get('pending_count') or 0)}，"
                f"失败 {int(batches.get('failed_count') or 0)}，"
                f"卡住 {int(batches.get('stale_running_count') or 0)}"
            ),
        },
    ]
    for dataset in DAILY_STAGE_DATASETS:
        item = dataset_by_name.get(dataset, {})
        coverage = item.get("coverage_percent")
        missing = int(item.get("missing_instrument_count") or 0)
        rows.append(
            {
                "stage_id": dataset,
                "label": _stage_dataset_label(dataset),
                "status": "success" if coverage == 100 else "failed" if missing else "pending",
                "progress_percent": coverage or 0,
                "primary": (
                    f"覆盖 {int(item.get('instrument_count') or 0)} / "
                    f"{int(item.get('expected_instrument_count') or expected_count)}"
                ),
                "secondary": f"缺失 {missing} 只标的",
            }
        )

    announcement_rows = int(dataset_by_name.get("announcement", {}).get("row_count") or 0)
    news_rows = int(dataset_by_name.get("news", {}).get("row_count") or 0)
    vendor_failures = sum(
        int(row.get("timeout_count") or 0) + int(row.get("error_count") or 0)
        for row in source_summary_rows
    )
    rows.append(
        {
            "stage_id": "documents",
            "label": "公告新闻 (Documents)",
            "status": "failed" if vendor_failures else "success" if announcement_rows or news_rows else "pending",
            "progress_percent": None,
            "primary": f"公告 {announcement_rows} 行，新闻 {news_rows} 行",
            "secondary": f"供应商错误/超时 {vendor_failures} 次；无事件不算缺失",
        }
    )
    rows.append(_job_stage_row("build_factors", "文本因子 (Build factors)", job_stage_rows))
    rows.append(
        {
            "stage_id": "quality",
            "label": "质量检查 (Quality check)",
            "status": quality_summary.get("status") or "pending",
            "progress_percent": 100 if quality_summary.get("status") == "success" else 0,
            "primary": f"未关闭问题 {int(quality_summary.get('open_issue_count') or 0)} 条",
            "secondary": f"失败维度 {int(quality_summary.get('failed_dimension_count') or 0)} 个",
        }
    )
    rows.append(_job_stage_row("sync_parquet", "Parquet 同步 (Sync)", job_stage_rows))
    rows.append(_job_stage_row("export_qlib", "Qlib 导出 (Export)", job_stage_rows))
    return rows


def _job_stage_row(
    job_type: str,
    label: str,
    job_stage_rows: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    job = job_stage_rows.get(job_type)
    if not job:
        return {
            "stage_id": job_type,
            "label": label,
            "status": "pending",
            "progress_percent": 0,
            "primary": "未看到当天成功记录",
            "secondary": "等待流水线执行",
        }
    status = str(job.get("status") or "pending")
    return {
        "stage_id": job_type,
        "label": label,
        "status": "success" if status == "success" else "failed" if status == "failed" else status,
        "progress_percent": 100 if status == "success" else 0,
        "primary": f"最近状态 {status}",
        "secondary": str(job.get("error_message") or job.get("end_at") or job.get("start_at") or "-"),
    }


def _stage_dataset_label(dataset: str) -> str:
    labels = {
        "daily_bar": "日线行情 (Daily bar)",
        "adj_factor": "复权因子 (Adjustment factor)",
        "price_limit": "涨跌停 (Price limit)",
    }
    return labels.get(dataset, dataset)


def _daily_verdict(
    *,
    database_exists: bool,
    expected_count: int,
    collection: dict[str, Any],
    batches: dict[str, Any],
    quality_summary: dict[str, Any],
    source_summary_rows: list[dict[str, Any]],
    qlib_provider_status: dict[str, Any],
) -> dict[str, Any]:
    if not database_exists:
        return {
            "level": "warning",
            "title": "还没有初始化数据库",
            "summary": "控制台没有读到 qdc.duckdb，只能展示 Qlib provider 文件检查。",
            "next_action": "先运行 qdc init，再执行 crawl-daily 和 build-factors。",
            "readiness_percent": 0,
        }

    core_percent = float(collection.get("core_complete_percent") or 0)
    failed_batches = int(batches.get("failed_count") or 0)
    stale_batches = int(batches.get("stale_running_count") or 0)
    vendor_failures = sum(
        int(row.get("timeout_count") or 0) + int(row.get("error_count") or 0)
        for row in source_summary_rows
    )
    quality_failed = quality_summary.get("status") == "failed"
    qlib_status = str(qlib_provider_status.get("status") or "pending")
    qlib_issue = (qlib_provider_status.get("issues") or [{}])[0] if qlib_status != "ok" else {}
    if failed_batches or stale_batches:
        return {
            "level": "danger",
            "title": "采集卡住或失败",
            "summary": f"失败批次 {failed_batches} 个，疑似卡住 {stale_batches} 个。",
            "next_action": "先看采集任务和供应商状态，再重试失败任务。",
            "readiness_percent": core_percent,
        }
    if vendor_failures:
        return {
            "level": "danger",
            "title": "供应商采集有异常",
            "summary": f"供应商错误或超时 {vendor_failures} 次，可能影响当天覆盖。",
            "next_action": "先看供应商采集质量，确认是否需要切换备用源。",
            "readiness_percent": core_percent,
        }
    if quality_failed:
        return {
            "level": "danger",
            "title": "采完了但质量未通过",
            "summary": "质量检查发现未关闭问题，数据暂不建议直接用于研究。",
            "next_action": "先处理质量体检里的失败维度。",
            "readiness_percent": core_percent,
        }
    if qlib_status == "fail":
        return {
            "level": "warning",
            "title": "Qlib 基础 provider 不可用",
            "summary": str(qlib_issue.get("message") or "基础行情 provider 文件检查未通过。"),
            "next_action": "先在 Qlib 仓库侧更新或修复 cn_data，再回来验证 external factors。",
            "readiness_percent": 0,
        }
    if qlib_status == "warning":
        return {
            "level": "warning",
            "title": "Qlib 基础 provider 需要更新",
            "summary": str(qlib_issue.get("message") or "基础行情 provider 可能不是最新完整交易日。"),
            "next_action": "QDC 可以继续采集当日文本因子，但训练底座需先更新 Qlib provider。",
            "readiness_percent": max(core_percent, 50),
        }
    if expected_count <= 0:
        return {
            "level": "warning",
            "title": "等待每日文档和因子记录",
            "summary": "当前日期还没有可用于观察的公告、新闻或因子行。",
            "next_action": "运行 crawl-daily，然后运行 build-factors、sync-parquet 和 quality。",
            "readiness_percent": 0,
        }
    if core_percent >= 100:
        return {
            "level": "success",
            "title": "每日文档因子链路可观察",
            "summary": "Qlib provider 文件检查通过，文档采集、因子和质量状态没有阻塞问题。",
            "next_action": "可以查看公告新闻明细、因子宽表或执行 verify-qlib 抽样读取。",
            "readiness_percent": core_percent,
        }
    return {
        "level": "running",
        "title": "等待文档因子链路推进",
        "summary": (
            "基础行情可用性由 Qlib provider 检查给出；"
            f"当前结构化诊断完整率 {core_percent:.2f}%。"
        ),
        "next_action": "继续运行 crawl-daily、build-factors、sync-parquet 和 quality。",
        "readiness_percent": core_percent,
    }


def _looks_like_timeout(message: str) -> bool:
    text = str(message or "").lower()
    return any(token in text for token in ("timeout", "timed out", "read timed", "connect timed", "超时"))


def _daily_preview_columns(mode: str) -> list[str]:
    base = ["instrument", "symbol", "exchange", "name", "industry"]
    if mode == "factor":
        fields = [
            field
            for table_fields in DAILY_FACTOR_WIDE_TABLES.values()
            for field in table_fields
        ]
        return [
            *base,
            *fields,
            "raw_news_count",
            "raw_announcement_count",
        ]
    fields = [field for table_fields in DAILY_RAW_WIDE_TABLES.values() for field in table_fields]
    return [*base, *fields, "news_count", "announcement_count"]


def _empty_data_coverage() -> dict[str, Any]:
    return {
        "status": "ok",
        "reference": {
            "instrument_source": "none",
            "trade_date_source": "none",
            "instrument_count": 0,
            "trade_date_count": 0,
            "min_trade_date": None,
            "max_trade_date": None,
        },
        "required_dimensions": list(REQUIRED_DAILY_COVERAGE_DATASETS),
        "dataset_rows": [
            _empty_dataset_coverage_row(table, "missing_database") for table in SILVER_TABLES
        ],
        "instrument_summary": {
            "total_instruments": 0,
            "expected_trade_dates": 0,
            "complete_instruments": 0,
            "missing_instruments": 0,
            "missing_daily_rows": 0,
            "complete_percent": 0,
            "missing_by_dimension": {
                dataset: 0 for dataset in REQUIRED_DAILY_COVERAGE_DATASETS
            },
            "available_by_dimension": {
                dataset: 0 for dataset in ALL_INSTRUMENT_COVERAGE_DIMENSIONS
            },
        },
        "instrument_rows": [],
        "hidden_instrument_count": 0,
    }


def _empty_dataset_coverage_row(dataset: str, coverage_kind: str) -> dict[str, Any]:
    return {
        "dataset": dataset,
        "coverage_kind": coverage_kind,
        "row_count": 0,
        "source_ids": [],
        "date_column": None,
        "min_date": None,
        "max_date": None,
        "date_count": None,
        "instrument_count": None,
        "reference_instrument_count": 0,
        "instruments_with_rows": None,
        "instruments_missing": None,
        "instrument_coverage_percent": None,
        "expected_daily_rows": None,
        "present_daily_rows": None,
        "missing_daily_rows": None,
        "daily_coverage_percent": None,
    }


def _public_coverage_reference(reference: dict[str, Any]) -> dict[str, Any]:
    return {
        "instrument_source": reference["instrument_source"],
        "trade_date_source": reference["trade_date_source"],
        "instrument_count": reference["instrument_count"],
        "trade_date_count": reference["trade_date_count"],
        "min_trade_date": reference["min_trade_date"],
        "max_trade_date": reference["max_trade_date"],
    }


def _coverage_kind(dataset: str) -> str:
    if dataset in REQUIRED_DAILY_COVERAGE_DATASETS:
        return "required_daily"
    if dataset in {"trade_status", "announcement", "news"}:
        return "sparse_source"
    if dataset in {"daily_news_factor", "daily_announcement_factor"}:
        return "sparse_factor"
    return "metadata"


def _percent(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0
    return round((numerator / denominator) * 100, 2)


def _ratio(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 6)


def _now_for_console_minutes_ago(minutes: int) -> str:
    return (datetime.now().replace(microsecond=0) - timedelta(minutes=minutes)).isoformat()


def _task_state(*, status: str, updated_at: Any, stale_threshold: str) -> str:
    if status == "running" and str(updated_at or "") <= stale_threshold:
        return "stale"
    return status or "pending"


def _task_progress_percent(state: str) -> int:
    if state == "success":
        return 100
    if state == "running":
        return 50
    if state in {"failed", "stale"}:
        return 100
    return 0


def _symbol_preview(symbols: list[Any], *, max_items: int = 8) -> str:
    values = [str(symbol) for symbol in symbols]
    if not values:
        return "-"
    shown = values[:max_items]
    suffix = f"...(+{len(values) - max_items})" if len(values) > max_items else ""
    return ",".join(shown) + suffix


def _progress_state(
    *,
    total: int,
    success: int,
    failed: int,
    running: int,
    pending: int,
    stale: int,
) -> str:
    if stale:
        return "blocked"
    if running:
        return "running"
    if pending:
        return "partial" if success or failed else "pending"
    if failed:
        return "blocked"
    if total and success == total:
        return "complete"
    return "empty"


def _preferred_date_column(columns: list[str]) -> str | None:
    for column in ("trade_date", "publish_date", "snapshot_date", "updated_at"):
        if column in columns:
            return column
    return None


def _order_clause(columns: list[str], date_column: str | None) -> str:
    parts = []
    if date_column:
        parts.append(f"{date_column} desc")
    if "instrument" in columns:
        parts.append("instrument")
    if not parts:
        return ""
    return f"order by {', '.join(parts)}"
