"""DuckDB control-plane database."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

import duckdb

from quant_data_center.settings import QdcSettings
from quant_data_center.storage.schema import (
    CONTROL_SCHEMA,
    CONTROL_SCHEMA_SQL,
    CONTROL_TABLES,
    SILVER_SCHEMA_MIGRATIONS,
    SILVER_SCHEMA,
    SILVER_SCHEMA_SQL,
    SILVER_TABLES,
)


DEFAULT_DUCKDB_LOCK_TIMEOUT_SECONDS = 60.0
DUCKDB_LOCK_RETRY_INTERVAL_SECONDS = 0.25


class QdcDatabase:
    """Small DuckDB wrapper for migration-phase control tables."""

    def __init__(self, settings: QdcSettings) -> None:
        self.settings = settings

    def connect(self, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        self.settings.database_path.parent.mkdir(parents=True, exist_ok=True)
        return _connect_duckdb_with_retry(
            self.settings.database_path,
            read_only=read_only,
            timeout_seconds=_duckdb_lock_timeout_seconds(),
        )

    def init_schema(self) -> None:
        for directory in self.settings.required_directories():
            directory.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.execute(CONTROL_SCHEMA_SQL)
            conn.execute(SILVER_SCHEMA_SQL)
            self._migrate_silver_schema(conn)

    def _migrate_silver_schema(self, conn: duckdb.DuckDBPyConnection) -> None:
        for table, columns in SILVER_SCHEMA_MIGRATIONS.items():
            for column, definition in columns.items():
                exists = conn.execute(
                    """
                    select 1
                    from information_schema.columns
                    where table_schema = ?
                      and table_name = ?
                      and column_name = ?
                    """,
                    [SILVER_SCHEMA, table, column],
                ).fetchone()
                if not exists:
                    conn.execute(
                        f"alter table {SILVER_SCHEMA}.{table} add column {column} {definition}"
                    )

    def table_counts(self) -> dict[str, int]:
        with self.connect() as conn:
            counts = {}
            for table in CONTROL_TABLES:
                value = conn.execute(f"select count(*) from {CONTROL_SCHEMA}.{table}").fetchone()[0]
                counts[table] = int(value)
        return counts

    def db_info(self) -> dict[str, Any]:
        return {
            "database_path": str(self.settings.database_path),
            "table_counts": self.table_counts(),
            "silver_table_counts": self.silver_table_counts(),
        }

    def silver_table_counts(self) -> dict[str, int]:
        with self.connect() as conn:
            counts = {}
            for table in SILVER_TABLES:
                value = conn.execute(f"select count(*) from {SILVER_SCHEMA}.{table}").fetchone()[0]
                counts[table] = int(value)
        return counts

    def record_job_run(
        self,
        *,
        job_type: str,
        status: str,
        dataset: str | None = None,
        source_id: str | None = None,
        universe: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        parameters: dict[str, Any] | None = None,
        error_message: str | None = None,
    ) -> str:
        job_id = str(uuid4())
        now = datetime.now().replace(microsecond=0)
        with self.connect() as conn:
            conn.execute(
                """
                insert into qdc_meta.job_run (
                  job_id, job_type, status, dataset, source_id, universe,
                  start_date, end_date, start_at, end_at, parameters_json, error_message, created_at
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    job_id,
                    job_type,
                    status,
                    dataset,
                    source_id,
                    universe,
                    start_date,
                    end_date,
                    now,
                    now,
                    json.dumps(parameters or {}, ensure_ascii=False, sort_keys=True),
                    error_message,
                    now,
                ],
            )
        return job_id

    def insert_backfill_task(
        self,
        *,
        dataset: str,
        source_id: str,
        universe: str,
        start_date: str,
        end_date: str,
        symbols: list[str],
    ) -> tuple[str, bool]:
        symbol_batch_json = _json_dumps(symbols)
        with self.connect() as conn:
            existing = conn.execute(
                """
                select task_id
                from qdc_meta.backfill_task
                where dataset = ?
                  and source_id = ?
                  and coalesce(universe, '') = ?
                  and start_date = ?
                  and end_date = ?
                  and symbol_batch_json = ?
                limit 1
                """,
                [dataset, source_id, universe, start_date, end_date, symbol_batch_json],
            ).fetchone()
            if existing:
                return str(existing[0]), False

            task_id = str(uuid4())
            now = _now()
            conn.execute(
                """
                insert into qdc_meta.backfill_task (
                  task_id, dataset, source_id, universe, start_date, end_date,
                  symbol_batch_json, status, attempt_count, created_at, updated_at
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    task_id,
                    dataset,
                    source_id,
                    universe,
                    start_date,
                    end_date,
                    symbol_batch_json,
                    "pending",
                    0,
                    now,
                    now,
                ],
            )
        return task_id, True

    def list_backfill_tasks(
        self,
        *,
        status: str | None = None,
        dataset: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        filters = []
        params: list[Any] = []
        if status:
            filters.append("status = ?")
            params.append(status)
        if dataset:
            filters.append("dataset = ?")
            params.append(dataset)
        where_clause = f"where {' and '.join(filters)}" if filters else ""
        limit_clause = "limit ?" if limit and limit > 0 else ""
        if limit_clause:
            params.append(int(limit))
        query = f"""
            select *
            from qdc_meta.backfill_task
            {where_clause}
            order by created_at, start_date, end_date, symbol_batch_json, task_id
            {limit_clause}
        """
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
            columns = [item[0] for item in conn.description]
        return [_row_to_dict(columns, row) for row in rows]

    def fetch_backfill_tasks_by_ids(self, task_ids: list[str]) -> list[dict[str, Any]]:
        if not task_ids:
            return []
        placeholders = ", ".join("?" for _ in task_ids)
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                select *
                from qdc_meta.backfill_task
                where task_id in ({placeholders})
                order by created_at, start_date, end_date, symbol_batch_json, task_id
                """,
                task_ids,
            ).fetchall()
            columns = [item[0] for item in conn.description]
        return [_row_to_dict(columns, row) for row in rows]

    def mark_backfill_task_running(self, task_id: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                update qdc_meta.backfill_task
                set status = 'running',
                    attempt_count = attempt_count + 1,
                    updated_at = ?
                where task_id = ?
                """,
                [_now(), task_id],
            )

    def finish_backfill_task(
        self,
        *,
        task_id: str,
        status: str,
        last_error: str | None = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                update qdc_meta.backfill_task
                set status = ?,
                    last_error = ?,
                    updated_at = ?
                where task_id = ?
                """,
                [status, last_error, _now(), task_id],
            )

    def recover_running_backfill_tasks(
        self,
        *,
        dataset: str | None = None,
        older_than_minutes: int = 15,
        limit: int | None = None,
        reason: str | None = None,
    ) -> list[dict[str, Any]]:
        threshold = _now() - timedelta(minutes=max(0, older_than_minutes))
        filters = ["status = 'running'", "updated_at <= ?"]
        params: list[Any] = [threshold]
        if dataset:
            filters.append("dataset = ?")
            params.append(dataset)
        limit_clause = "limit ?" if limit and limit > 0 else ""
        if limit_clause:
            params.append(int(limit))
        failure_reason = reason or (
            f"recovered stale running task older than {older_than_minutes} minutes"
        )
        now = _now()
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                select *
                from qdc_meta.backfill_task
                where {' and '.join(filters)}
                order by updated_at, created_at, task_id
                {limit_clause}
                """,
                params,
            ).fetchall()
            columns = [item[0] for item in conn.description]
            tasks = [_row_to_dict(columns, row) for row in rows]
            for task in tasks:
                conn.execute(
                    """
                    update qdc_meta.backfill_task
                    set status = 'failed',
                        last_error = ?,
                        updated_at = ?
                    where task_id = ?
                    """,
                    [failure_reason, now, task["task_id"]],
                )
                task["status"] = "failed"
                task["last_error"] = failure_reason
                task["updated_at"] = now.isoformat()
        return tasks

    def split_backfill_task(
        self,
        *,
        task_id: str,
        batch_size: int,
    ) -> dict[str, Any]:
        if batch_size <= 0:
            raise ValueError("split-backfill requires --batch-size > 0")
        tasks = self.fetch_backfill_tasks_by_ids([task_id])
        if not tasks:
            raise ValueError(f"unknown backfill task_id: {task_id}")
        task = tasks[0]
        if task.get("status") not in {"pending", "failed"}:
            raise ValueError(
                f"only pending or failed backfill tasks can be split: {task_id} "
                f"status={task.get('status')}"
            )
        symbols = list(task.get("symbol_batch_json") or [])
        if not symbols:
            raise ValueError(f"backfill task has no symbol batch to split: {task_id}")
        if len(symbols) <= batch_size:
            raise ValueError(
                f"backfill task symbol count {len(symbols)} is <= batch_size {batch_size}"
            )

        chunks = [symbols[index : index + batch_size] for index in range(0, len(symbols), batch_size)]
        subtasks = []
        for chunk in chunks:
            subtask_id, inserted = self.insert_backfill_task(
                dataset=str(task["dataset"]),
                source_id=str(task["source_id"]),
                universe=str(task.get("universe") or ""),
                start_date=str(task["start_date"]),
                end_date=str(task["end_date"]),
                symbols=chunk,
            )
            subtasks.append(
                {
                    "task_id": subtask_id,
                    "inserted": inserted,
                    "symbols": chunk,
                }
            )
        self.finish_backfill_task(
            task_id=task_id,
            status="superseded",
            last_error=f"split into {len(subtasks)} subtasks with batch_size {batch_size}",
        )
        return {
            "original_task": task,
            "subtask_count": len(subtasks),
            "inserted_count": sum(1 for item in subtasks if item["inserted"]),
            "subtasks": subtasks,
        }

    def upsert_dataset_watermark(
        self,
        *,
        dataset: str,
        source_id: str,
        universe: str,
        start_date: str,
        end_date: str,
        job_id: str,
    ) -> None:
        now = _now()
        with self.connect() as conn:
            existing = conn.execute(
                """
                select min_date, max_date
                from qdc_meta.dataset_watermark
                where dataset = ? and source_id = ? and universe = ?
                """,
                [dataset, source_id, universe],
            ).fetchone()
            if existing:
                current_min, current_max = existing
                next_min = min(str(current_min), start_date) if current_min else start_date
                next_max = max(str(current_max), end_date) if current_max else end_date
                conn.execute(
                    """
                    update qdc_meta.dataset_watermark
                    set min_date = ?,
                        max_date = ?,
                        last_success_at = ?,
                        last_job_id = ?,
                        updated_at = ?
                    where dataset = ? and source_id = ? and universe = ?
                    """,
                    [
                        next_min,
                        next_max,
                        now,
                        job_id,
                        now,
                        dataset,
                        source_id,
                        universe,
                    ],
                )
            else:
                conn.execute(
                    """
                    insert into qdc_meta.dataset_watermark (
                      dataset, source_id, universe, min_date, max_date,
                      last_success_at, last_job_id, updated_at
                    )
                    values (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [dataset, source_id, universe, start_date, end_date, now, job_id, now],
                )

    def fetch_dataset_watermarks(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                select *
                from qdc_meta.dataset_watermark
                order by dataset, source_id, universe
                """
            ).fetchall()
            columns = [item[0] for item in conn.description]
        return [_row_to_dict(columns, row) for row in rows]

    def latest_universe_symbols(self, universe: str) -> list[str]:
        with self.connect() as conn:
            latest = conn.execute(
                """
                select max(snapshot_date)
                from qdc_silver.universe_constituent
                where universe = ?
                """,
                [universe],
            ).fetchone()[0]
            if latest is None:
                return []
            rows = conn.execute(
                """
                select instrument
                from qdc_silver.universe_constituent
                where universe = ? and snapshot_date = ?
                order by instrument
                """,
                [universe, latest],
            ).fetchall()
        return [str(row[0]) for row in rows]

    def stock_basic_instruments(self, *, active_only: bool = True) -> list[str]:
        filters = []
        if active_only:
            filters.append("coalesce(is_active, true) = true")
        where_clause = f"where {' and '.join(filters)}" if filters else ""
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                select instrument
                from qdc_silver.stock_basic
                {where_clause}
                order by instrument
                """
            ).fetchall()
        return [str(row[0]) for row in rows]

    def upsert_crawler_source(self, source: dict[str, Any]) -> None:
        now = _now()
        with self.connect() as conn:
            existing = conn.execute(
                """
                select source_id
                from qdc_meta.crawler_source
                where source_id = ?
                """,
                [source["source_id"]],
            ).fetchone()
            values = [
                source["source_id"],
                source["source_type"],
                source["dataset"],
                source["base_url"],
                bool(source["enabled"]),
                source.get("robots_url"),
                source["robots_status"],
                source["terms_review_status"],
                source["copyright_policy"],
                int(source["rate_limit_per_minute"]),
                float(source["min_delay_seconds"]),
                int(source["max_retry"]),
                source["parser_version"],
                source.get("notes"),
                now,
            ]
            if existing:
                conn.execute(
                    """
                    update qdc_meta.crawler_source
                    set source_type = ?,
                        dataset = ?,
                        base_url = ?,
                        enabled = ?,
                        robots_url = ?,
                        robots_status = ?,
                        terms_review_status = ?,
                        copyright_policy = ?,
                        rate_limit_per_minute = ?,
                        min_delay_seconds = ?,
                        max_retry = ?,
                        parser_version = ?,
                        notes = ?,
                        updated_at = ?
                    where source_id = ?
                    """,
                    values[1:] + [source["source_id"]],
                )
            else:
                conn.execute(
                    """
                    insert into qdc_meta.crawler_source (
                      source_id, source_type, dataset, base_url, enabled,
                      robots_url, robots_status, terms_review_status,
                      copyright_policy, rate_limit_per_minute,
                      min_delay_seconds, max_retry, parser_version, notes, updated_at
                    )
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    values,
                )

    def insert_crawl_task(
        self,
        *,
        source_id: str,
        dataset: str,
        crawl_date: str,
        partition_key: str,
        request: dict[str, Any],
    ) -> tuple[str, bool]:
        request_json = _json_dumps(request)
        with self.connect() as conn:
            existing = conn.execute(
                """
                select task_id
                from qdc_meta.crawl_task
                where source_id = ?
                  and dataset = ?
                  and crawl_date = ?
                  and partition_key = ?
                  and request_json = ?
                limit 1
                """,
                [source_id, dataset, crawl_date, partition_key, request_json],
            ).fetchone()
            if existing:
                return str(existing[0]), False

            task_id = str(uuid4())
            now = _now()
            conn.execute(
                """
                insert into qdc_meta.crawl_task (
                  task_id, source_id, dataset, crawl_date, partition_key,
                  request_json, status, attempt_count, created_at, updated_at
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    task_id,
                    source_id,
                    dataset,
                    crawl_date,
                    partition_key,
                    request_json,
                    "pending",
                    0,
                    now,
                    now,
                ],
            )
        return task_id, True

    def list_crawl_tasks(
        self,
        *,
        status: str | None = None,
        source_id: str | None = None,
        dataset: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        filters = []
        params: list[Any] = []
        if status:
            filters.append("status = ?")
            params.append(status)
        if source_id:
            filters.append("source_id = ?")
            params.append(source_id)
        if dataset:
            filters.append("dataset = ?")
            params.append(dataset)
        where_clause = f"where {' and '.join(filters)}" if filters else ""
        limit_clause = "limit ?" if limit and limit > 0 else ""
        if limit_clause:
            params.append(int(limit))
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                select *
                from qdc_meta.crawl_task
                {where_clause}
                order by created_at, crawl_date, source_id, partition_key, task_id
                {limit_clause}
                """,
                params,
            ).fetchall()
            columns = [item[0] for item in conn.description]
        return [_row_to_dict(columns, row) for row in rows]

    def mark_crawl_task_running(self, task_id: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                update qdc_meta.crawl_task
                set status = 'running',
                    attempt_count = attempt_count + 1,
                    updated_at = ?
                where task_id = ?
                """,
                [_now(), task_id],
            )

    def finish_crawl_task(
        self,
        *,
        task_id: str,
        status: str,
        last_error: str | None = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                update qdc_meta.crawl_task
                set status = ?,
                    last_error = ?,
                    updated_at = ?
                where task_id = ?
                """,
                [status, last_error, _now(), task_id],
            )

    def recover_running_crawl_tasks(
        self,
        *,
        source_id: str | None = None,
        older_than_minutes: int = 30,
        limit: int | None = None,
        reason: str | None = None,
    ) -> list[dict[str, Any]]:
        threshold = _now() - timedelta(minutes=max(0, older_than_minutes))
        filters = ["status = 'running'", "updated_at <= ?"]
        params: list[Any] = [threshold]
        if source_id:
            filters.append("source_id = ?")
            params.append(source_id)
        limit_clause = "limit ?" if limit and limit > 0 else ""
        if limit_clause:
            params.append(int(limit))
        failure_reason = reason or f"recovered stale crawl task older than {older_than_minutes} minutes"
        now = _now()
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                select *
                from qdc_meta.crawl_task
                where {' and '.join(filters)}
                order by updated_at, created_at, task_id
                {limit_clause}
                """,
                params,
            ).fetchall()
            columns = [item[0] for item in conn.description]
            tasks = [_row_to_dict(columns, row) for row in rows]
            for task in tasks:
                conn.execute(
                    """
                    update qdc_meta.crawl_task
                    set status = 'failed',
                        last_error = ?,
                        updated_at = ?
                    where task_id = ?
                    """,
                    [failure_reason, now, task["task_id"]],
                )
                task["status"] = "failed"
                task["last_error"] = failure_reason
                task["updated_at"] = now.isoformat()
        return tasks

    def record_crawl_run(
        self,
        *,
        status: str,
        source_id: str | None = None,
        dataset: str | None = None,
        crawl_date: str | None = None,
        planned_count: int = 0,
        success_count: int = 0,
        failed_count: int = 0,
        document_count: int = 0,
        raw_object_count: int = 0,
        parameters: dict[str, Any] | None = None,
        error_message: str | None = None,
    ) -> str:
        run_id = str(uuid4())
        now = _now()
        with self.connect() as conn:
            conn.execute(
                """
                insert into qdc_meta.crawl_run (
                  run_id, source_id, dataset, crawl_date, status,
                  planned_count, success_count, failed_count,
                  document_count, raw_object_count, start_at, end_at,
                  parameters_json, error_message, created_at
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    source_id,
                    dataset,
                    crawl_date,
                    status,
                    int(planned_count),
                    int(success_count),
                    int(failed_count),
                    int(document_count),
                    int(raw_object_count),
                    now,
                    now,
                    _json_dumps(parameters or {}),
                    error_message,
                    now,
                ],
            )
        return run_id

    def insert_source_object(
        self,
        *,
        dataset: str,
        source_id: str,
        layer: str,
        uri: str,
        content_hash: str,
        size_bytes: int,
        job_id: str | None = None,
    ) -> str:
        return self.insert_source_objects(
            [
                {
                    "dataset": dataset,
                    "source_id": source_id,
                    "layer": layer,
                    "uri": uri,
                    "content_hash": content_hash,
                    "size_bytes": size_bytes,
                    "job_id": job_id,
                }
            ]
        )[0]

    def insert_source_objects(self, objects: list[dict[str, Any]]) -> list[str]:
        if not objects:
            return []
        now = _now()
        rows = []
        object_ids = []
        for item in objects:
            object_id = str(uuid4())
            object_ids.append(object_id)
            rows.append(
                [
                    object_id,
                    item.get("job_id"),
                    item["dataset"],
                    item["source_id"],
                    item["layer"],
                    item["uri"],
                    item["content_hash"],
                    int(item["size_bytes"]),
                    now,
                    now,
                ]
            )
        with self.connect() as conn:
            conn.executemany(
                """
                insert into qdc_meta.source_object (
                  object_id, job_id, dataset, source_id, layer, uri,
                  content_hash, size_bytes, observed_at, created_at
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return object_ids

    def insert_quality_issue(
        self,
        *,
        severity: str,
        issue_type: str,
        message: str,
        dataset: str | None = None,
        source_id: str | None = None,
        job_id: str | None = None,
        entity_key: str | None = None,
        observed_value: str | None = None,
        status: str = "open",
    ) -> str:
        issue_id = str(uuid4())
        now = _now()
        with self.connect() as conn:
            conn.execute(
                """
                insert into qdc_meta.quality_issue (
                  issue_id, job_id, dataset, source_id, severity, issue_type,
                  status, entity_key, message, observed_value, created_at
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    issue_id,
                    job_id,
                    dataset,
                    source_id,
                    severity,
                    issue_type,
                    status,
                    entity_key,
                    message,
                    observed_value,
                    now,
                ],
            )
        return issue_id

    def list_quality_issues(
        self,
        *,
        dataset: str | None = None,
        status: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        filters = []
        params: list[Any] = []
        if dataset:
            filters.append("dataset = ?")
            params.append(dataset)
        if status:
            filters.append("status = ?")
            params.append(status)
        where_clause = f"where {' and '.join(filters)}" if filters else ""
        limit_clause = "limit ?" if limit and limit > 0 else ""
        if limit_clause:
            params.append(int(limit))
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                select *
                from qdc_meta.quality_issue
                {where_clause}
                order by created_at, dataset, issue_type, entity_key
                {limit_clause}
                """,
                params,
            ).fetchall()
            columns = [item[0] for item in conn.description]
        return [_row_to_dict(columns, row) for row in rows]

    def list_source_objects(
        self,
        *,
        dataset: str | None = None,
        source_id: str | None = None,
        layer: str | None = None,
    ) -> list[dict[str, Any]]:
        filters = []
        params: list[Any] = []
        if dataset:
            filters.append("dataset = ?")
            params.append(dataset)
        if source_id:
            filters.append("source_id = ?")
            params.append(source_id)
        if layer:
            filters.append("layer = ?")
            params.append(layer)
        where_clause = f"where {' and '.join(filters)}" if filters else ""
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                select *
                from qdc_meta.source_object
                {where_clause}
                order by created_at, dataset, source_id, layer, uri
                """,
                params,
            ).fetchall()
            columns = [item[0] for item in conn.description]
        return [_row_to_dict(columns, row) for row in rows]


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _connect_duckdb_with_retry(
    database_path: Any,
    *,
    read_only: bool,
    timeout_seconds: float,
) -> duckdb.DuckDBPyConnection:
    deadline = time.monotonic() + max(timeout_seconds, 0)
    interval = DUCKDB_LOCK_RETRY_INTERVAL_SECONDS
    last_error: duckdb.Error | None = None
    while True:
        try:
            return duckdb.connect(str(database_path), read_only=read_only)
        except duckdb.Error as exc:
            if not _is_duckdb_lock_error(exc):
                raise
            last_error = exc
            if time.monotonic() >= deadline:
                mode = "read-only" if read_only else "read-write"
                raise RuntimeError(
                    f"timed out waiting for DuckDB {mode} lock on {database_path}; "
                    "another qdc process is still using the database"
                ) from exc
            time.sleep(interval)
            interval = min(interval * 1.5, 2.0)

    raise RuntimeError("unreachable DuckDB lock retry state") from last_error


def _duckdb_lock_timeout_seconds() -> float:
    raw = os.getenv("QDC_DUCKDB_LOCK_TIMEOUT_SECONDS")
    if raw is None or not raw.strip():
        return DEFAULT_DUCKDB_LOCK_TIMEOUT_SECONDS
    try:
        return float(raw)
    except ValueError:
        return DEFAULT_DUCKDB_LOCK_TIMEOUT_SECONDS


def _is_duckdb_lock_error(exc: BaseException) -> bool:
    message = str(exc).lower()
    return (
        "could not set lock" in message
        or "conflicting lock" in message
        or "another process" in message
        or "另一个程序正在使用此文件" in message
        or "进程无法访问" in message
    )


def _now() -> datetime:
    return datetime.now().replace(microsecond=0)


def _row_to_dict(columns: list[str], row: Any) -> dict[str, Any]:
    result = {}
    for column, value in zip(columns, row, strict=True):
        if hasattr(value, "isoformat"):
            result[column] = value.isoformat()
        elif isinstance(value, str) and column.endswith("_json"):
            result[column] = json.loads(value)
        else:
            result[column] = value
    return result
