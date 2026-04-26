"""SQLite metadata store for the V0 crawl ledger."""

from __future__ import annotations

import sqlite3
from typing import Any
from uuid import uuid4

from pitlake.settings import ProjectSettings
from pitlake.storage.raw_store import RawWriteResult
from pitlake.utils import isoformat, stable_json_dumps

SCHEMA_SQL = """
create table if not exists crawl_run (
  run_id text primary key,
  source_id text not null,
  provider_id text not null,
  logical_dataset text not null,
  connector_name text not null,
  connector_version text not null,
  trigger_type text not null,
  start_at text not null,
  end_at text,
  status text not null,
  request_count integer default 0,
  success_count integer default 0,
  error_count integer default 0,
  new_item_count integer default 0,
  updated_item_count integer default 0,
  duplicate_count integer default 0,
  quarantine_count integer default 0,
  error_message text,
  code_git_commit text,
  created_at text not null
);

create table if not exists raw_object (
  raw_object_id text primary key,
  run_id text,
  source_id text not null,
  provider_id text not null,
  logical_dataset text not null,
  raw_uri text not null,
  storage_path text not null,
  metadata_path text not null,
  mime_type text not null,
  size_bytes integer not null,
  content_hash text not null,
  first_seen_at text not null,
  stored_at text not null,
  status text not null,
  request_hash text,
  request_url text,
  request_params_json text
);

create table if not exists raw_item_version (
  item_version_id text primary key,
  logical_dataset text not null,
  provider_id text not null,
  source_id text not null,
  source_item_key text not null,
  title text,
  source_url text,
  source_publish_time text,
  source_update_time text,
  first_seen_at text not null,
  stored_at text not null,
  raw_object_id text not null,
  content_hash text not null,
  dedup_hash text,
  quality_status text not null,
  is_backfilled integer not null default 0,
  backfill_reason text,
  observed_payload_json text
);

create table if not exists quality_check_result (
  check_id text primary key,
  run_id text,
  logical_dataset text,
  source_id text,
  check_name text not null,
  check_type text not null,
  severity text not null,
  status text not null,
  expected_value text,
  observed_value text,
  failed_count integer default 0,
  sample_failed_keys text,
  created_at text not null
);

create table if not exists collection_manifest (
  manifest_id text primary key,
  manifest_type text not null,
  manifest_date text not null,
  created_at text not null,
  status text not null,
  manifest_path text not null,
  manifest_hash text not null,
  run_count integer not null,
  raw_object_count integer not null,
  new_item_count integer not null,
  error_count integer not null
);

create table if not exists source_health (
  health_id text primary key,
  source_id text not null,
  check_time text not null,
  status text not null,
  freshness_minutes real,
  last_success_time text,
  last_error_time text,
  success_rate_24h real,
  new_items_24h integer,
  notes text
);

create table if not exists lineage_event (
  lineage_event_id text primary key,
  event_time text not null,
  job_name text not null,
  run_id text,
  input_datasets text,
  output_datasets text,
  input_manifest_ids text,
  output_manifest_ids text,
  source_code_version text,
  config_hash text,
  status text not null
);
"""


class MetadataStore:
    """SQLite-backed metadata store."""

    def __init__(self, settings: ProjectSettings) -> None:
        self.settings = settings
        self.path = settings.metadata_db

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA_SQL)

    def create_run(
        self,
        *,
        source_id: str,
        provider_id: str,
        logical_dataset: str,
        connector_name: str,
        connector_version: str,
        trigger_type: str,
        code_git_commit: str | None = None,
    ) -> str:
        run_id = str(uuid4())
        created_at = isoformat()
        with self.connect() as conn:
            conn.execute(
                """
                insert into crawl_run (
                  run_id, source_id, provider_id, logical_dataset, connector_name,
                  connector_version, trigger_type, start_at, status, code_git_commit, created_at
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    source_id,
                    provider_id,
                    logical_dataset,
                    connector_name,
                    connector_version,
                    trigger_type,
                    created_at,
                    "running",
                    code_git_commit,
                    created_at,
                ),
            )
        return run_id

    def finish_run(
        self,
        run_id: str,
        *,
        status: str,
        request_count: int = 0,
        success_count: int = 0,
        error_count: int = 0,
        new_item_count: int = 0,
        updated_item_count: int = 0,
        duplicate_count: int = 0,
        quarantine_count: int = 0,
        error_message: str | None = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                update crawl_run
                set end_at = ?, status = ?, request_count = ?, success_count = ?,
                    error_count = ?, new_item_count = ?, updated_item_count = ?,
                    duplicate_count = ?, quarantine_count = ?, error_message = ?
                where run_id = ?
                """,
                (
                    isoformat(),
                    status,
                    request_count,
                    success_count,
                    error_count,
                    new_item_count,
                    updated_item_count,
                    duplicate_count,
                    quarantine_count,
                    error_message,
                    run_id,
                ),
            )

    def insert_raw_object(
        self,
        raw: RawWriteResult,
        *,
        status: str = "stored",
        request_hash: str | None = None,
        request_url: str | None = None,
        request_params: dict[str, Any] | None = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                insert or ignore into raw_object (
                  raw_object_id, run_id, source_id, provider_id, logical_dataset,
                  raw_uri, storage_path, metadata_path, mime_type, size_bytes,
                  content_hash, first_seen_at, stored_at, status, request_hash,
                  request_url, request_params_json
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    raw.raw_object_id,
                    raw.run_id,
                    raw.source_id,
                    raw.provider_id,
                    raw.logical_dataset,
                    raw.raw_uri,
                    str(raw.storage_path),
                    str(raw.metadata_path),
                    raw.mime_type,
                    raw.size_bytes,
                    raw.content_hash,
                    raw.first_seen_at,
                    raw.stored_at,
                    status,
                    request_hash,
                    request_url,
                    stable_json_dumps(request_params or {}),
                ),
            )

    def insert_raw_item_version(
        self,
        *,
        logical_dataset: str,
        provider_id: str,
        source_id: str,
        source_item_key: str,
        first_seen_at: str,
        stored_at: str,
        raw_object_id: str,
        content_hash: str,
        title: str | None = None,
        source_url: str | None = None,
        source_publish_time: str | None = None,
        source_update_time: str | None = None,
        dedup_hash: str | None = None,
        quality_status: str = "pending",
        is_backfilled: bool = False,
        backfill_reason: str | None = None,
        observed_payload: dict[str, Any] | None = None,
    ) -> str:
        item_version_id = str(uuid4())
        with self.connect() as conn:
            conn.execute(
                """
                insert into raw_item_version (
                  item_version_id, logical_dataset, provider_id, source_id, source_item_key,
                  title, source_url, source_publish_time, source_update_time,
                  first_seen_at, stored_at, raw_object_id, content_hash, dedup_hash,
                  quality_status, is_backfilled, backfill_reason, observed_payload_json
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item_version_id,
                    logical_dataset,
                    provider_id,
                    source_id,
                    source_item_key,
                    title,
                    source_url,
                    source_publish_time,
                    source_update_time,
                    first_seen_at,
                    stored_at,
                    raw_object_id,
                    content_hash,
                    dedup_hash,
                    quality_status,
                    int(is_backfilled),
                    backfill_reason,
                    stable_json_dumps(observed_payload or {}),
                ),
            )
        return item_version_id

    def raw_item_version_exists(
        self,
        *,
        logical_dataset: str,
        provider_id: str,
        source_item_key: str,
        content_hash: str,
    ) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                """
                select 1
                from raw_item_version
                where logical_dataset = ?
                  and provider_id = ?
                  and source_item_key = ?
                  and content_hash = ?
                limit 1
                """,
                (logical_dataset, provider_id, source_item_key, content_hash),
            ).fetchone()
        return row is not None

    def insert_quality_results(self, results: list[Any]) -> None:
        if not results:
            return
        with self.connect() as conn:
            conn.executemany(
                """
                insert into quality_check_result (
                  check_id, run_id, logical_dataset, source_id, check_name, check_type,
                  severity, status, expected_value, observed_value, failed_count,
                  sample_failed_keys, created_at
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        str(uuid4()),
                        getattr(result, "run_id", None),
                        getattr(result, "logical_dataset", None),
                        getattr(result, "source_id", None),
                        result.check_name,
                        result.check_type,
                        result.severity,
                        result.status,
                        result.expected_value,
                        result.observed_value,
                        result.failed_count,
                        stable_json_dumps(result.sample_failed_keys),
                        isoformat(),
                    )
                    for result in results
                ],
            )

    def insert_manifest(self, manifest: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                insert or replace into collection_manifest (
                  manifest_id, manifest_type, manifest_date, created_at, status,
                  manifest_path, manifest_hash, run_count, raw_object_count,
                  new_item_count, error_count
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    manifest["manifest_id"],
                    manifest["manifest_type"],
                    manifest["manifest_date"],
                    manifest["created_at"],
                    manifest["status"],
                    manifest["manifest_path"],
                    manifest["manifest_hash"],
                    manifest["summary"]["run_count"],
                    manifest["summary"]["raw_object_count"],
                    manifest["summary"]["new_item_count"],
                    manifest["summary"]["error_count"],
                ),
            )

    def fetch_runs_for_day(self, manifest_date: str) -> list[dict[str, Any]]:
        return self._fetch_by_day("crawl_run", "start_at", manifest_date)

    def fetch_raw_objects_for_day(self, manifest_date: str) -> list[dict[str, Any]]:
        return self._fetch_by_day("raw_object", "stored_at", manifest_date)

    def fetch_quality_for_day(self, manifest_date: str) -> list[dict[str, Any]]:
        return self._fetch_by_day("quality_check_result", "created_at", manifest_date)

    def _fetch_by_day(self, table: str, field: str, manifest_date: str) -> list[dict[str, Any]]:
        query = f"select * from {table} where {field} like ? order by {field}, rowid"
        with self.connect() as conn:
            rows = conn.execute(query, (f"{manifest_date}%",)).fetchall()
        return [dict(row) for row in rows]
