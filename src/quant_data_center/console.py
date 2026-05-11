"""Read-only local web console for quant_data_center."""

from __future__ import annotations

import json
import math
import mimetypes
import posixpath
from datetime import datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import duckdb

from quant_data_center.settings import QdcSettings
from quant_data_center.storage.schema import (
    CONTROL_SCHEMA,
    CONTROL_TABLES,
    SILVER_SCHEMA,
    SILVER_TABLES,
)


STATIC_ROOT = Path(__file__).with_name("console_static")
DEFAULT_LIMIT = 100
MAX_LIMIT = 500
STALE_RUNNING_MINUTES = 15
COVERAGE_INSTRUMENT_LIMIT = 500
REQUIRED_DAILY_COVERAGE_DATASETS = ("daily_bar", "adj_factor", "price_limit")
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
            return {
                "status": "ok",
                "dataset": dataset,
                "columns": [],
                "row_count": 0,
                "rows": [],
            }

        with self._connect() as conn:
            silver_tables = self._existing_tables(conn, SILVER_SCHEMA)
            if dataset not in silver_tables:
                return {
                    "status": "ok",
                    "dataset": dataset,
                    "columns": [],
                    "row_count": 0,
                    "rows": [],
                }

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
        return {
            "status": "ok",
            "dataset": dataset,
            "columns": columns,
            "row_count": len(rows),
            "rows": rows,
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
            "data_coverage": _empty_data_coverage(),
        }

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

        instruments: set[str] = set()
        for table in INSTRUMENT_COVERAGE_DATASETS:
            if table not in silver_tables:
                continue
            columns = self._columns(conn, SILVER_SCHEMA, table)
            if "instrument" not in columns:
                continue
            rows = conn.execute(
                f"""
                select distinct instrument
                from {SILVER_SCHEMA}.{table}
                order by instrument
                """
            ).fetchall()
            instruments.update(str(row[0]) for row in rows)
        return sorted(instruments), "silver.instrument_union"

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
                    "complete_percent": 0,
                    "missing_by_dimension": {
                        dataset: len(instruments)
                        for dataset in REQUIRED_DAILY_COVERAGE_DATASETS
                    },
                },
                "rows": [],
                "hidden_count": 0,
            }

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
        missing_by_dimension = {dataset: 0 for dataset in REQUIRED_DAILY_COVERAGE_DATASETS}
        rows = []
        complete_count = 0
        for instrument in instruments:
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
            rows.append(
                {
                    "instrument": instrument,
                    "complete": complete,
                    "missing_dimensions": missing_dimensions,
                    "dimension_counts": dimension_counts,
                    "expected_trade_dates": expected_date_count,
                }
            )

        rows.sort(
            key=lambda row: (
                row["complete"],
                -len(row["missing_dimensions"]),
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
                "complete_percent": _percent(complete_count, len(instruments)),
                "missing_by_dimension": missing_by_dimension,
            },
            "rows": visible_rows,
            "hidden_count": max(len(rows) - len(visible_rows), 0),
        }

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

    class QdcConsoleHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path.startswith("/api/"):
                self._handle_api(parsed.path, parse_qs(parsed.query))
                return
            self._serve_static(parsed.path)

        def log_message(self, format: str, *args: Any) -> None:
            return

        def _handle_api(self, path: str, query: dict[str, list[str]]) -> None:
            try:
                if path == "/api/overview":
                    self._send_json(data.overview())
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
                self._send_json(
                    {"status": "fail", "error": f"duckdb read error: {exc}"},
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                )
            except Exception as exc:  # pragma: no cover - defensive HTTP guard
                self._send_json(
                    {"status": "fail", "error": str(exc)},
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )

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


def _clamp_limit(value: int | None, *, default: int = DEFAULT_LIMIT) -> int:
    if value is None or value <= 0:
        return default
    return min(value, MAX_LIMIT)


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
            "complete_percent": 0,
            "missing_by_dimension": {
                dataset: 0 for dataset in REQUIRED_DAILY_COVERAGE_DATASETS
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


def _now_for_console_minutes_ago(minutes: int) -> str:
    return (datetime.now().replace(microsecond=0) - timedelta(minutes=minutes)).isoformat()


def _progress_state(
    *,
    total: int,
    success: int,
    failed: int,
    running: int,
    pending: int,
    stale: int,
) -> str:
    if failed or stale:
        return "blocked"
    if running:
        return "running"
    if pending:
        return "pending"
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
