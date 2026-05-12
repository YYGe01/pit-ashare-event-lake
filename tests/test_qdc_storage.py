from __future__ import annotations

import json
import hashlib
import math
import sys
import struct
from datetime import datetime
from types import SimpleNamespace
from pathlib import Path

import pandas as pd

from quant_data_center.cli import _print_json, main
from quant_data_center.console import QdcConsoleData, _locked_api_payload
from quant_data_center.jobs.backfill import parse_date, plan_backfill_tasks
from quant_data_center.settings import QdcSettings
from quant_data_center.storage import database as database_module
from quant_data_center.storage.database import QdcDatabase
from quant_data_center.storage.schema import CONTROL_TABLES, SILVER_TABLES
from quant_data_center.storage.silver import SilverStore


def _write_config(tmp_path: Path, extra_config: str = "") -> Path:
    config_path = tmp_path / "config" / "quant_data_center.yaml"
    config_path.parent.mkdir(parents=True)
    content = """
project:
  name: quant_data_center
  timezone: Asia/Shanghai
  phase: test
paths:
  data_root: data/quant_data_center
  database_path: data/quant_data_center/qdc.duckdb
  raw_root: data/quant_data_center/raw
  parquet_root: data/quant_data_center/parquet
  qlib_root: data/quant_data_center/qlib
  logs_dir: data/quant_data_center/logs
runtime:
  database_backend: duckdb
  file_format: parquet
policy:
  prefer_free_sources: true
  paid_providers_enabled: false
  raw_append_only: true
  unknown_copyright_policy: metadata_only
llm:
  text_event:
    provider: rule
    model: deepseek/unit-test
    api_key_file: data/quant_data_center/secrets/deepseek_api_key
    api_key_env: QDC_TEST_LLM_KEY
    temperature: 0
    max_tokens: 128
universes:
  csi300:
    symbols:
      - SH600000
      - SZ000001
      - SZ300750
""".strip()
    if extra_config.strip():
        content = f"{content}\n{extra_config.strip()}"
    config_path.write_text(content, encoding="utf-8")
    return config_path


def _seed_research_rows(tmp_path: Path) -> tuple[Path, QdcDatabase]:
    config_path = _write_config(tmp_path)
    settings = QdcSettings.from_yaml(config_path)
    database = QdcDatabase(settings)
    database.init_schema()
    silver = SilverStore(settings)
    silver.upsert_daily_bar(
        [
            {
                "trade_date": "2026-05-11",
                "instrument": "SH600000",
                "open": 10.0,
                "high": 10.5,
                "low": 9.9,
                "close": 10.2,
                "pre_close": 10.1,
                "volume": 1000,
                "amount": 10200,
                "vwap": 10.2,
                "source_id": "unit_test",
            },
            {
                "trade_date": "2026-05-12",
                "instrument": "SH600000",
                "open": 10.2,
                "high": 10.8,
                "low": 10.0,
                "close": 10.6,
                "pre_close": 10.2,
                "volume": 1200,
                "amount": 12720,
                "vwap": 10.6,
                "source_id": "unit_test",
            },
        ]
    )
    silver.upsert_adj_factor(
        [
            {
                "trade_date": "2026-05-11",
                "instrument": "SH600000",
                "adj_factor": 1.0,
                "factor_type": "unit_test",
                "source_id": "unit_test",
            },
            {
                "trade_date": "2026-05-12",
                "instrument": "SH600000",
                "adj_factor": 1.0,
                "factor_type": "unit_test",
                "source_id": "unit_test",
            },
        ]
    )
    silver.upsert_price_limit(
        [
            {
                "trade_date": "2026-05-11",
                "instrument": "SH600000",
                "limit_up": 11.11,
                "limit_down": 9.09,
                "prev_close": 10.1,
                "limit_rule": "unit_test",
                "source_id": "unit_test",
            },
            {
                "trade_date": "2026-05-12",
                "instrument": "SH600000",
                "limit_up": 11.22,
                "limit_down": 9.18,
                "prev_close": 10.2,
                "limit_rule": "unit_test",
                "source_id": "unit_test",
            },
        ]
    )
    return config_path, database


def test_qdc_init_schema_creates_control_tables(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    settings = QdcSettings.from_yaml(config_path)
    database = QdcDatabase(settings)

    database.init_schema()

    assert settings.database_path.exists()
    assert (settings.parquet_root / "bronze").is_dir()
    assert (settings.parquet_root / "silver").is_dir()
    assert (settings.parquet_root / "gold").is_dir()
    assert database.table_counts() == {table: 0 for table in CONTROL_TABLES}
    assert database.silver_table_counts() == {table: 0 for table in SILVER_TABLES}


def test_qdc_smoke_records_job_run(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)

    assert main(["--config", str(config_path), "smoke"]) == 0

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    counts = database.table_counts()
    assert counts["job_run"] == 1
    assert counts["backfill_task"] == 0


def test_cli_print_json_replaces_non_finite_floats(capsys) -> None:
    _print_json({"status": "ok", "value": math.nan, "rows": [{"value": math.inf}]})

    payload = json.loads(capsys.readouterr().out)
    assert payload == {"rows": [{"value": None}], "status": "ok", "value": None}


def test_plan_backfill_tasks_splits_dates_and_symbols() -> None:
    tasks = plan_backfill_tasks(
        dataset="daily_bar",
        source_id="akshare",
        universe="csi300",
        start_date=parse_date("2026-05-01"),
        end_date=parse_date("2026-05-05"),
        symbols=["SH600000", "SZ000001", "SZ300750"],
        batch_size=2,
        chunk_days=2,
    )

    assert len(tasks) == 6
    assert tasks[0].start_date.isoformat() == "2026-05-01"
    assert tasks[0].end_date.isoformat() == "2026-05-02"
    assert tasks[0].symbols == ["SH600000", "SZ000001"]
    assert tasks[1].symbols == ["SZ300750"]
    assert tasks[-1].start_date.isoformat() == "2026-05-05"
    assert tasks[-1].end_date.isoformat() == "2026-05-05"


def test_qdc_plan_backfill_is_idempotent(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    args = [
        "--config",
        str(config_path),
        "plan-backfill",
        "--dataset",
        "daily_bar",
        "--source-id",
        "akshare",
        "--universe",
        "csi300",
        "--start",
        "2026-05-01",
        "--end",
        "2026-05-02",
        "--symbols",
        "SH600000,SZ000001",
        "--batch-size",
        "1",
    ]

    assert main(args) == 0
    assert main(args) == 0

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    tasks = database.list_backfill_tasks()
    assert len(tasks) == 2
    assert all(task["status"] == "pending" for task in tasks)
    assert sorted(task["symbol_batch_json"][0] for task in tasks) == ["SH600000", "SZ000001"]


def test_qdc_plan_backfill_expands_configured_universe(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)

    assert (
        main(
            [
                "--config",
                str(config_path),
                "plan-backfill",
                "--dataset",
                "daily_bar",
                "--source-id",
                "akshare",
                "--universe",
                "csi300",
                "--start",
                "2026-05-01",
                "--end",
                "2026-05-01",
                "--batch-size",
                "2",
            ]
        )
        == 0
    )

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    tasks = database.list_backfill_tasks(dataset="daily_bar")
    assert len(tasks) == 2
    assert tasks[0]["symbol_batch_json"] == ["SH600000", "SZ000001"]
    assert tasks[1]["symbol_batch_json"] == ["SZ300750"]


def test_qdc_run_backfill_control_only_updates_tasks_and_watermark(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    assert (
        main(
            [
                "--config",
                str(config_path),
                "plan-backfill",
                "--dataset",
                "daily_bar",
                "--source-id",
                "akshare",
                "--universe",
                "csi300",
                "--start",
                "2026-05-01",
                "--end",
                "2026-05-03",
                "--symbols",
                "SH600000",
                "--chunk-days",
                "2",
            ]
        )
        == 0
    )

    assert (
        main(
            [
                "--config",
                str(config_path),
                "run-backfill",
                "--dataset",
                "daily_bar",
                "--limit-tasks",
                "2",
                "--control-only",
            ]
        )
        == 0
    )

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    tasks = database.list_backfill_tasks(dataset="daily_bar")
    assert [task["status"] for task in tasks] == ["success", "success"]
    counts = database.table_counts()
    assert counts["job_run"] == 2
    assert counts["dataset_watermark"] == 1
    watermark = database.fetch_dataset_watermarks()[0]
    assert watermark["dataset"] == "daily_bar"
    assert watermark["source_id"] == "akshare"
    assert watermark["universe"] == "csi300"
    assert watermark["min_date"] == "2026-05-01"
    assert watermark["max_date"] == "2026-05-03"


def test_qdc_database_connect_retries_temporary_duckdb_lock(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = _write_config(tmp_path)
    settings = QdcSettings.from_yaml(config_path)
    attempts = []
    original_connect = database_module.duckdb.connect

    def fake_connect(path: str, **kwargs):
        attempts.append((path, kwargs))
        if len(attempts) == 1:
            raise database_module.duckdb.IOException(
                'IO Error: Could not set lock on file "qdc.duckdb": Conflicting lock'
            )
        return original_connect(":memory:", **kwargs)

    monkeypatch.setattr(database_module.duckdb, "connect", fake_connect)
    monkeypatch.setattr(database_module.time, "sleep", lambda _: None)
    monkeypatch.setenv("QDC_DUCKDB_LOCK_TIMEOUT_SECONDS", "1")

    with QdcDatabase(settings).connect() as conn:
        assert conn.execute("select 1").fetchone()[0] == 1

    assert len(attempts) == 2
    assert attempts[0][0].endswith("qdc.duckdb")


def test_qdc_console_overview_reads_collection_state(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    settings = QdcSettings.from_yaml(config_path)
    database = QdcDatabase(settings)
    database.init_schema()
    database.record_job_run(
        job_type="daily",
        status="success",
        dataset="daily",
        source_id="akshare",
        start_date="2026-05-11",
        end_date="2026-05-11",
        parameters={"planned_count": 1},
    )
    database.insert_backfill_task(
        dataset="daily_bar",
        source_id="akshare",
        universe="csi300",
        start_date="2026-05-11",
        end_date="2026-05-11",
        symbols=["SH600000"],
    )

    overview = QdcConsoleData(settings).overview()

    assert overview["database_exists"] is True
    assert overview["table_counts"]["job_run"] == 1
    assert overview["table_counts"]["backfill_task"] == 1
    assert overview["job_status_counts"] == {"success": 1}
    assert overview["backfill_status_counts"] == {"pending": 1}
    assert overview["latest_job_runs"][0]["parameters_json"] == {"planned_count": 1}
    assert overview["backfill_progress"][0]["success_percent"] == 0
    assert overview["backfill_progress"][0]["state"] == "pending"


def test_qdc_console_returns_busy_payload_when_duckdb_is_locked(tmp_path: Path) -> None:
    settings = QdcSettings.from_yaml(_write_config(tmp_path))
    data = QdcConsoleData(settings)

    overview = _locked_api_payload(
        data,
        "/api/overview",
        {},
        RuntimeError("Could not set lock on file"),
    )
    jobs = _locked_api_payload(
        data,
        "/api/job-runs",
        {},
        RuntimeError("Could not set lock on file"),
    )

    assert overview["status"] == "busy"
    assert overview["database_busy"] is True
    assert overview["database_path"].endswith("qdc.duckdb")
    assert jobs == {
        "status": "busy",
        "database_busy": True,
        "message": overview["message"],
        "job_count": 0,
        "jobs": [],
    }


def test_qdc_console_overview_reports_data_coverage(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    settings = QdcSettings.from_yaml(config_path)
    database = QdcDatabase(settings)
    database.init_schema()
    silver = SilverStore(settings)
    silver.upsert_stock_basic(
        [
            {
                "instrument": "SH600000",
                "symbol": "600000",
                "exchange": "SH",
                "name": "浦发银行",
                "industry": "银行",
                "is_active": True,
                "source_id": "unit_test",
            },
            {
                "instrument": "SZ000001",
                "symbol": "000001",
                "exchange": "SZ",
                "name": "平安银行",
                "industry": "银行",
                "is_active": True,
                "source_id": "unit_test",
            },
        ]
    )
    silver.upsert_universe_constituents(
        [
            {
                "universe": "csi300",
                "snapshot_date": "2026-05-10",
                "instrument": "SH600000",
                "symbol": "600000",
                "exchange": "SH",
                "name": "浦发银行",
                "weight": 1.1,
                "source_id": "unit_test",
            },
            {
                "universe": "csi300",
                "snapshot_date": "2026-05-10",
                "instrument": "SZ000001",
                "symbol": "000001",
                "exchange": "SZ",
                "name": "平安银行",
                "weight": 0.9,
                "source_id": "unit_test",
            },
            {
                "universe": "csi300",
                "snapshot_date": "2026-05-10",
                "instrument": "SZ300750",
                "symbol": "300750",
                "exchange": "SZ",
                "name": "宁德时代",
                "weight": 2.3,
                "source_id": "unit_test",
            },
        ]
    )
    silver.upsert_trade_calendar(
        [
            {
                "calendar_id": "XSHG",
                "trade_date": "2026-05-11",
                "is_open": True,
                "source_id": "unit_test",
            },
            {
                "calendar_id": "XSHG",
                "trade_date": "2026-05-12",
                "is_open": True,
                "source_id": "unit_test",
            },
        ]
    )
    silver.upsert_daily_bar(
        [
            {
                "trade_date": "2026-05-11",
                "instrument": "SH600000",
                "close": 10.2,
                "source_id": "unit_test",
            },
            {
                "trade_date": "2026-05-12",
                "instrument": "SH600000",
                "close": 10.4,
                "source_id": "unit_test",
            },
            {
                "trade_date": "2026-05-11",
                "instrument": "SZ000001",
                "close": 9.2,
                "source_id": "unit_test",
            },
            {
                "trade_date": "2026-05-12",
                "instrument": "SZ000001",
                "close": 9.4,
                "source_id": "unit_test",
            },
        ]
    )
    silver.upsert_adj_factor(
        [
            {
                "trade_date": "2026-05-11",
                "instrument": "SH600000",
                "adj_factor": 1.0,
                "source_id": "unit_test",
            },
            {
                "trade_date": "2026-05-12",
                "instrument": "SH600000",
                "adj_factor": 1.0,
                "source_id": "unit_test",
            },
            {
                "trade_date": "2026-05-11",
                "instrument": "SZ000001",
                "adj_factor": 1.0,
                "source_id": "unit_test",
            },
        ]
    )
    silver.upsert_price_limit(
        [
            {
                "trade_date": "2026-05-11",
                "instrument": "SH600000",
                "limit_up": 11.0,
                "limit_down": 9.0,
                "source_id": "unit_test",
            },
            {
                "trade_date": "2026-05-12",
                "instrument": "SH600000",
                "limit_up": 11.2,
                "limit_down": 9.2,
                "source_id": "unit_test",
            },
        ]
    )
    silver.upsert_trade_status(
        [
            {
                "trade_date": "2026-05-11",
                "instrument": "SH600000",
                "trade_status": "normal",
                "source_id": "unit_test",
            }
        ]
    )
    silver.upsert_news(
        [
            {
                "news_id": "n1",
                "publish_date": "2026-05-11",
                "instrument": "SH600000",
                "title": "浦发银行新闻",
                "url": "https://example.com/news/1",
                "source_id": "unit_test",
            }
        ]
    )
    silver.upsert_announcements(
        [
            {
                "announcement_id": "a1",
                "publish_date": "2026-05-12",
                "instrument": "SZ000001",
                "title": "平安银行公告",
                "url": "https://example.com/announcement/1",
                "source_id": "unit_test",
            }
        ]
    )
    silver.upsert_daily_news_factor(
        [
            {
                "trade_date": "2026-05-11",
                "instrument": "SH600000",
                "news_count": 2,
                "news_sentiment_mean": 0.5,
                "source_id": "unit_test",
            }
        ]
    )
    silver.upsert_daily_announcement_factor(
        [
            {
                "trade_date": "2026-05-12",
                "instrument": "SZ000001",
                "announcement_count": 1,
                "announcement_sentiment_mean": 0.2,
                "source_id": "unit_test",
            }
        ]
    )

    coverage = QdcConsoleData(settings).overview()["data_coverage"]

    assert coverage["reference"]["instrument_count"] == 3
    assert coverage["reference"]["trade_date_count"] == 2
    dataset_rows = {row["dataset"]: row for row in coverage["dataset_rows"]}
    assert dataset_rows["daily_bar"]["missing_daily_rows"] == 2
    assert dataset_rows["adj_factor"]["missing_daily_rows"] == 3
    assert dataset_rows["price_limit"]["missing_daily_rows"] == 4
    assert dataset_rows["daily_bar"]["instruments_with_rows"] == 2
    assert dataset_rows["daily_bar"]["instruments_missing"] == 1
    summary = coverage["instrument_summary"]
    assert summary["complete_instruments"] == 1
    assert summary["missing_instruments"] == 2
    assert summary["missing_daily_rows"] == 9
    assert summary["missing_by_dimension"] == {
        "daily_bar": 1,
        "adj_factor": 2,
        "price_limit": 2,
    }
    rows = {row["instrument"]: row for row in coverage["instrument_rows"]}
    assert rows["SH600000"]["complete"] is True
    assert rows["SH600000"]["name"] == "浦发银行"
    assert rows["SH600000"]["industry"] == "银行"
    assert rows["SH600000"]["stock_basic_present"] is True
    assert rows["SH600000"]["universe_constituent_present"] is True
    assert rows["SH600000"]["raw_missing_daily_rows"] == 0
    assert rows["SH600000"]["trade_status_days"] == 1
    assert rows["SH600000"]["news_rows"] == 1
    assert rows["SH600000"]["daily_news_factor_days"] == 1
    assert rows["SH600000"]["factor_news_count"] == 2
    sh_dimensions = rows["SH600000"]["dimension_statuses"]
    assert sh_dimensions["stock_basic"]["status"] == "complete"
    assert sh_dimensions["daily_bar"]["observed"] == 2
    assert sh_dimensions["daily_bar"]["expected"] == 2
    assert sh_dimensions["daily_bar"]["missing"] == 0
    assert sh_dimensions["news"]["observed"] == 1
    assert sh_dimensions["news"]["expected"] is None
    assert sh_dimensions["daily_news_factor"]["event_count"] == 2
    assert rows["SZ000001"]["missing_dimensions"] == ["adj_factor", "price_limit"]
    assert rows["SZ000001"]["raw_missing_daily_rows"] == 3
    assert rows["SZ000001"]["announcement_rows"] == 1
    assert rows["SZ000001"]["daily_announcement_factor_days"] == 1
    assert rows["SZ000001"]["factor_announcement_count"] == 1
    sz_dimensions = rows["SZ000001"]["dimension_statuses"]
    assert sz_dimensions["adj_factor"]["missing"] == 1
    assert sz_dimensions["price_limit"]["missing"] == 2
    assert sz_dimensions["announcement"]["observed"] == 1
    assert sz_dimensions["daily_announcement_factor"]["event_count"] == 1
    assert rows["SZ300750"]["name"] == "宁德时代"
    assert rows["SZ300750"]["missing_dimensions"] == [
        "daily_bar",
        "adj_factor",
        "price_limit",
    ]
    assert rows["SZ300750"]["raw_missing_daily_rows"] == 6
    sz300_dimensions = rows["SZ300750"]["dimension_statuses"]
    assert sz300_dimensions["stock_basic"]["status"] == "missing"
    assert sz300_dimensions["stock_basic"]["missing"] == 1
    assert sz300_dimensions["universe_constituent"]["status"] == "complete"
    assert sz300_dimensions["daily_bar"]["missing"] == 2
    assert rows["SZ300750"]["available_dimensions"] == ["universe_constituent"]


def test_qdc_console_dataset_preview_filters_silver_rows(tmp_path: Path) -> None:
    config_path, _database = _seed_research_rows(tmp_path)
    settings = QdcSettings.from_yaml(config_path)

    preview = QdcConsoleData(settings).dataset_preview(
        dataset="daily_bar",
        instrument="SH600000",
        start="2026-05-12",
        end="2026-05-12",
    )

    assert preview["dataset"] == "daily_bar"
    assert preview["columns"][:2] == ["trade_date", "instrument"]
    assert preview["row_count"] == 1
    assert preview["filtered_row_count"] == 1
    assert preview["total_row_count"] == 2
    assert preview["date_column"] == "trade_date"
    assert preview["supports_instrument_filter"] is True
    assert preview["summary"]["instrument_count"] == 1
    assert preview["summary"]["source_ids"] == [{"source_id": "unit_test", "row_count": 1}]
    assert preview["rows"][0]["trade_date"] == "2026-05-12"
    assert preview["rows"][0]["close"] == 10.6


def test_qdc_console_daily_documents_use_crawler_sources_and_local_preview(
    tmp_path: Path,
) -> None:
    config_path = _write_config(tmp_path)
    settings = QdcSettings.from_yaml(config_path)
    database = QdcDatabase(settings)
    database.init_schema()
    silver = SilverStore(settings)
    silver.upsert_stock_basic(
        [
            {
                "instrument": "SH600000",
                "symbol": "600000",
                "exchange": "SH",
                "name": "浦发银行",
                "is_active": True,
                "source_id": "unit_test",
            }
        ]
    )
    announcement_raw_path = (
        settings.raw_root / "announcement" / "cninfo_announcement" / "dt=2026-05-11" / "raw.json"
    )
    announcement_raw_path.parent.mkdir(parents=True)
    announcement_raw_path.write_text('{"pages": []}', encoding="utf-8")
    announcement_raw_id = database.insert_source_object(
        dataset="announcement",
        source_id="cninfo_announcement",
        layer="raw",
        uri=str(announcement_raw_path),
        content_hash=hashlib.sha256(announcement_raw_path.read_bytes()).hexdigest(),
        size_bytes=announcement_raw_path.stat().st_size,
    )
    pdf_path = (
        settings.raw_root / "announcement" / "cninfo_announcement" / "dt=2026-05-11" / "a1.pdf"
    )
    pdf_path.write_bytes(b"%PDF-1.4\nunit test\n")
    pdf_id = database.insert_source_object(
        dataset="announcement",
        source_id="cninfo_announcement",
        layer="raw_file",
        uri=str(pdf_path),
        content_hash=hashlib.sha256(pdf_path.read_bytes()).hexdigest(),
        size_bytes=pdf_path.stat().st_size,
    )
    news_raw_path = settings.raw_root / "news" / "sina_finance_news" / "dt=2026-05-11" / "raw.json"
    news_raw_path.parent.mkdir(parents=True)
    news_raw_path.write_text('{"items": [{"title": "浦发银行新闻"}]}', encoding="utf-8")
    news_raw_id = database.insert_source_object(
        dataset="news",
        source_id="sina_finance_news",
        layer="raw",
        uri=str(news_raw_path),
        content_hash=hashlib.sha256(news_raw_path.read_bytes()).hexdigest(),
        size_bytes=news_raw_path.stat().st_size,
    )
    silver.upsert_announcements(
        [
            {
                "announcement_id": "cninfo_a1",
                "publish_date": "2026-05-11",
                "instrument": "SH600000",
                "title": "年度权益分派公告",
                "url": "https://static.cninfo.com.cn/a1.pdf",
                "source_id": "cninfo_announcement",
                "raw_object_id": announcement_raw_id,
                "pdf_object_id": pdf_id,
                "pdf_download_status": "success",
                "pdf_size_bytes": pdf_path.stat().st_size,
            },
            {
                "announcement_id": "ak_a1",
                "publish_date": "2026-05-11",
                "instrument": "SH600000",
                "title": "浦发银行:年度权益分派公告",
                "url": "https://example.test/ak/1",
                "source_id": "akshare",
            },
        ]
    )
    silver.upsert_news(
        [
            {
                "news_id": "sina_n1",
                "publish_date": "2026-05-11",
                "instrument": "SH600000",
                "title": "浦发银行新闻",
                "url": "https://finance.sina.com.cn/n1",
                "source_id": "sina_finance_news",
            },
            {
                "news_id": "ak_n1",
                "publish_date": "2026-05-11",
                "instrument": "SH600000",
                "title": "AkShare 新闻",
                "url": "https://example.test/ak/news",
                "source_id": "akshare",
            },
        ]
    )

    payload = QdcConsoleData(settings).daily_wide_preview(
        date="2026-05-11",
        mode="factor",
    )
    row = payload["rows"][0]

    assert row["raw_announcement_count"] == 1
    assert row["raw_news_count"] == 1
    announcement = row["_announcement_documents"][0]
    assert announcement["source_ids"] == ["cninfo_announcement"]
    assert announcement["has_pdf"] is True
    assert announcement["has_body"] is False
    assert announcement["local_object_id"] == pdf_id
    assert announcement["local_url"].endswith(pdf_id)
    assert "正文文本" in announcement["content_label"]
    news = row["_news_documents"][0]
    assert news["source_ids"] == ["sina_finance_news"]
    assert news["local_object_id"] == news_raw_id
    assert news["content_status"] == "local_metadata"

    local_pdf = QdcConsoleData(settings).source_object_file(object_id=pdf_id)
    assert local_pdf["body"] == b"%PDF-1.4\nunit test\n"
    assert local_pdf["content_type"] == "application/pdf"


def test_qdc_console_instruments_supports_search_by_name(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    settings = QdcSettings.from_yaml(config_path)
    database = QdcDatabase(settings)
    database.init_schema()
    silver = SilverStore(settings)
    silver.upsert_stock_basic(
        [
            {
                "instrument": "SH600000",
                "symbol": "600000",
                "exchange": "SH",
                "name": "浦发银行",
                "industry": "银行",
                "is_active": True,
                "source_id": "unit_test",
            },
            {
                "instrument": "SZ300750",
                "symbol": "300750",
                "exchange": "SZ",
                "name": "宁德时代",
                "industry": "电力设备",
                "is_active": True,
                "source_id": "unit_test",
            },
        ]
    )

    payload = QdcConsoleData(settings).instruments(query="浦发")

    assert payload["instrument_count"] == 1
    assert payload["instruments"][0]["instrument"] == "SH600000"
    assert payload["instruments"][0]["name"] == "浦发银行"

    all_payload = QdcConsoleData(settings).instruments(limit=6000)
    all_instruments = {item["instrument"] for item in all_payload["instruments"]}
    assert {"SH600000", "SZ300750"}.issubset(all_instruments)


def test_qdc_console_raw_instrument_preview_reads_raw_records(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    settings = QdcSettings.from_yaml(config_path)
    database = QdcDatabase(settings)
    database.init_schema()
    daily_raw_path = settings.raw_root / "daily_bar" / "unit_test" / "dt=2026-05-11" / "raw.json"
    daily_raw_path.parent.mkdir(parents=True)
    daily_raw_payload = {
        "function": "stock_zh_a_hist",
        "params": {"symbol": "600000", "start_date": "2026-05-11"},
        "records": [
            {
                "日期": "2026-05-11",
                "股票代码": "600000",
                "开盘": 10.0,
                "最高": 10.5,
                "最低": 9.9,
                "收盘": 10.2,
                "昨收": 10.1,
                "成交量": 1000000,
                "成交额": 10200000,
                "成交均价": 10.2,
                "换手率": 1.2,
                "流通股本": 29352174170,
            }
        ],
    }
    daily_raw_path.write_text(json.dumps(daily_raw_payload, ensure_ascii=False), encoding="utf-8")
    database.insert_source_object(
        dataset="daily_bar",
        source_id="unit_test",
        layer="raw",
        uri=str(daily_raw_path),
        content_hash="unit-test-daily",
        size_bytes=daily_raw_path.stat().st_size,
    )
    announcement_raw_path = (
        settings.raw_root / "announcement" / "unit_test" / "dt=2026-05-11" / "raw.json"
    )
    announcement_raw_path.parent.mkdir(parents=True)
    announcement_raw_payload = {
        "function": "stock_notice_report",
        "params": {"date": "20260511", "symbol": "全部"},
        "records": [
            {
                "公告日期": "2026-05-11",
                "代码": "600000",
                "公告标题": "浦发银行关于利润分配的公告",
                "公告类型": "分红派息",
                "网址": "https://example.com/announcement/1",
            }
        ],
    }
    announcement_raw_path.write_text(
        json.dumps(announcement_raw_payload, ensure_ascii=False),
        encoding="utf-8",
    )
    database.insert_source_object(
        dataset="announcement",
        source_id="unit_test",
        layer="raw",
        uri=str(announcement_raw_path),
        content_hash="unit-test-announcement",
        size_bytes=announcement_raw_path.stat().st_size,
    )
    trade_status_raw_path = (
        settings.raw_root / "trade_status" / "unit_test" / "dt=2026-05-11" / "raw.json"
    )
    trade_status_raw_path.parent.mkdir(parents=True)
    trade_status_raw_payload = {
        "function": "stock_tfp_em",
        "params": {"date": "20260511"},
        "records": [
            {
                "代码": "600000",
                "停牌原因": "重大事项",
                "停牌时间": "2026-05-11",
                "停牌截止时间": "2026-05-11",
                "停牌期限": "停牌一天",
                "预计复牌时间": "2026-05-12",
                "所属市场": "上交所主板",
            }
        ],
    }
    trade_status_raw_path.write_text(
        json.dumps(trade_status_raw_payload, ensure_ascii=False),
        encoding="utf-8",
    )
    database.insert_source_object(
        dataset="trade_status",
        source_id="unit_test",
        layer="raw",
        uri=str(trade_status_raw_path),
        content_hash="unit-test-trade-status",
        size_bytes=trade_status_raw_path.stat().st_size,
    )
    raw_path = settings.raw_root / "news" / "unit_test" / "dt=2026-05-11" / "raw.json"
    raw_path.parent.mkdir(parents=True)
    raw_payload = {
        "function": "stock_news_em",
        "params": {"symbol": "600000", "start_date": "2026-05-11"},
        "records": [
            {
                "新闻时间": "2026-05-11 09:30:00",
                "新闻标题": "浦发银行订单增长",
                "链接": "https://example.com/news/1",
                "文章来源": "测试媒体",
                "关键词": "600000",
            },
            {
                "新闻时间": "2026-05-10 09:30:00",
                "新闻标题": "日期外新闻",
                "链接": "https://example.com/news/2",
            },
        ],
    }
    raw_path.write_text(json.dumps(raw_payload, ensure_ascii=False), encoding="utf-8")
    database.insert_source_object(
        dataset="news",
        source_id="unit_test",
        layer="raw",
        uri=str(raw_path),
        content_hash="unit-test",
        size_bytes=raw_path.stat().st_size,
    )
    adj_raw_path = settings.raw_root / "adj_factor" / "unit_test" / "dt=2026-05-11" / "raw.json"
    adj_raw_path.parent.mkdir(parents=True)
    adj_raw_payload = {
        "function": "stock_zh_a_hist",
        "params": {"symbol": "600000", "start_date": "2026-05-11"},
        "raw_records": [
            {
                "日期": "2026-05-11",
                "股票代码": "600000",
                "收盘": 10.2,
            }
        ],
        "qfq_records": [
            {
                "日期": "2026-05-11",
                "股票代码": "600000",
                "收盘": 10.71,
            }
        ],
    }
    adj_raw_path.write_text(json.dumps(adj_raw_payload, ensure_ascii=False), encoding="utf-8")
    database.insert_source_object(
        dataset="adj_factor",
        source_id="unit_test",
        layer="raw",
        uri=str(adj_raw_path),
        content_hash="unit-test-adj",
        size_bytes=adj_raw_path.stat().st_size,
    )

    preview = QdcConsoleData(settings).raw_instrument_preview(
        instrument="SH600000",
        start="2026-05-11",
        end="2026-05-11",
    )

    assert preview["status"] == "ok"
    assert preview["instrument"] == "SH600000"
    assert preview["summary"]["dataset_count"] == 5
    assert preview["summary"]["object_count"] == 5
    assert preview["summary"]["row_count"] == 5
    sections = {section["dataset"]: section for section in preview["sections"]}
    daily_section = sections["daily_bar"]
    assert daily_section["rows"][0]["trade_date"] == "2026-05-11"
    assert daily_section["rows"][0]["open"] == 10.0
    assert daily_section["rows"][0]["pre_close"] == 10.1
    assert daily_section["rows"][0]["volume"] == 1000000
    assert daily_section["rows"][0]["vwap"] == 10.2
    assert daily_section["rows"][0]["turnover_rate"] == 1.2
    assert daily_section["rows"][0]["outstanding_share"] == 29352174170
    announcement_section = sections["announcement"]
    assert announcement_section["rows"][0]["document_type"] == "分红派息"
    assert announcement_section["rows"][0]["url"] == "https://example.com/announcement/1"
    trade_status_section = sections["trade_status"]
    assert trade_status_section["rows"][0]["halt_reason"] == "重大事项"
    assert trade_status_section["rows"][0]["expected_resume_date"] == "2026-05-12"
    assert "source_update_time" not in trade_status_section["columns"]
    news_section = sections["news"]
    assert news_section["objects"][0]["function"] == "stock_news_em"
    assert news_section["objects"][0]["parameter_summary"] == "symbol=600000；start_date=2026-05-11"
    assert news_section["objects"][0]["record_count"] == 2
    assert "params" not in news_section["objects"][0]
    assert news_section["columns"] == [
        "factor_input",
        "publish_date",
        "instrument",
        "title",
        "url",
        "source",
        "keyword",
    ]
    assert "新闻标题" not in news_section["columns"]
    assert news_section["rows"][0] == {
        "factor_input": "新闻文本输入，用来生成新闻数量、情绪和事件类型因子",
        "publish_date": "2026-05-11",
        "instrument": "SH600000",
        "title": "浦发银行订单增长",
        "url": "https://example.com/news/1",
        "source": "测试媒体",
        "keyword": "600000",
    }
    adj_section = sections["adj_factor"]
    assert adj_section["rows"][0]["trade_date"] == "2026-05-11"
    assert adj_section["rows"][0]["raw_close"] == 10.2
    assert adj_section["rows"][0]["qfq_close"] == 10.71
    assert adj_section["rows"][0]["adj_factor"] == 1.05


def test_qdc_console_instrument_timeline_merges_daily_and_documents(
    tmp_path: Path,
) -> None:
    config_path, _database = _seed_research_rows(tmp_path)
    settings = QdcSettings.from_yaml(config_path)
    silver = SilverStore(settings)
    silver.upsert_news(
        [
            {
                "news_id": "n1",
                "publish_date": "2026-05-11",
                "instrument": "SH600000",
                "title": "公司订单增长",
                "url": "https://example.com/news",
                "source_id": "sina_finance_news",
            }
        ]
    )
    silver.upsert_announcements(
        [
            {
                "announcement_id": "a1",
                "publish_date": "2026-05-11",
                "instrument": "SH600000",
                "title": "公司发布回购公告",
                "url": "https://example.com/announcement",
                "source_id": "cninfo_announcement",
            }
        ]
    )
    silver.upsert_daily_news_factor(
        [
            {
                "trade_date": "2026-05-11",
                "instrument": "SH600000",
                "news_count": 1,
                "news_sentiment_mean": 0.6,
                "news_positive_count": 1,
                "news_negative_count": 0,
                "news_weighted_sentiment_sum": 0.48,
                "news_importance_sum": 0.8,
                "news_growth_count": 1,
                "news_risk_count": 0,
                "news_financing_count": 0,
                "source_id": "unit_test",
            }
        ]
    )
    silver.upsert_daily_announcement_factor(
        [
            {
                "trade_date": "2026-05-11",
                "instrument": "SH600000",
                "announcement_count": 1,
                "announcement_sentiment_mean": 0.4,
                "announcement_positive_count": 1,
                "announcement_negative_count": 0,
                "announcement_weighted_sentiment_sum": 0.36,
                "announcement_importance_sum": 0.9,
                "announcement_growth_count": 0,
                "announcement_risk_count": 0,
                "announcement_financing_count": 1,
                "announcement_operation_count": 0,
                "source_id": "unit_test",
            }
        ]
    )

    timeline = QdcConsoleData(settings).instrument_timeline(
        instrument="SH600000",
        start="2026-05-11",
        end="2026-05-12",
    )

    assert timeline["status"] == "ok"
    assert timeline["summary"]["trade_date_count"] == 2
    assert timeline["summary"]["core_complete_days"] == 2
    assert timeline["summary"]["news_rows"] == 1
    assert timeline["summary"]["announcement_rows"] == 1
    assert timeline["summary"]["factor_news_count"] == 1
    assert timeline["summary"]["factor_announcement_count"] == 1
    rows = {row["trade_date"]: row for row in timeline["timeline_rows"]}
    assert rows["2026-05-11"]["close"] == 10.2
    assert rows["2026-05-11"]["adj_factor"] == 1.0
    assert rows["2026-05-11"]["limit_up"] == 11.11
    assert rows["2026-05-11"]["news_count"] == 1
    assert rows["2026-05-11"]["news_weighted_sentiment_sum"] == 0.48
    assert rows["2026-05-11"]["news_importance_sum"] == 0.8
    assert rows["2026-05-11"]["announcement_count"] == 1
    assert rows["2026-05-11"]["announcement_sentiment_mean"] == 0.4
    assert rows["2026-05-11"]["announcement_weighted_sentiment_sum"] == 0.36
    assert rows["2026-05-11"]["announcement_importance_sum"] == 0.9
    assert timeline["news_rows"][0]["trade_date"] == "2026-05-11"
    assert timeline["news_rows"][0]["title"] == "公司订单增长"
    assert timeline["announcement_rows"][0]["trade_date"] == "2026-05-11"
    assert timeline["announcement_rows"][0]["title"] == "公司发布回购公告"

    second_page = QdcConsoleData(settings).instrument_timeline(
        instrument="SH600000",
        start="2026-05-11",
        end="2026-05-12",
        limit=1,
        offset=1,
    )

    assert second_page["page"] == 2
    assert second_page["page_size"] == 1
    assert second_page["has_previous"] is True
    assert second_page["has_next"] is False
    assert [row["trade_date"] for row in second_page["timeline_rows"]] == ["2026-05-11"]


def test_qdc_console_dataset_preview_rejects_unknown_dataset(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    settings = QdcSettings.from_yaml(config_path)

    try:
        QdcConsoleData(settings).dataset_preview(dataset="daily_bar; drop table qdc_meta.job_run")
    except ValueError as exc:
        assert "unsupported preview dataset" in str(exc)
    else:  # pragma: no cover - assertion branch
        raise AssertionError("dataset preview accepted an unsafe dataset name")


def test_qdc_recover_running_marks_stale_tasks_failed(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    assert (
        main(
            [
                "--config",
                str(config_path),
                "plan-backfill",
                "--dataset",
                "daily_bar",
                "--source-id",
                "akshare",
                "--start",
                "2026-05-01",
                "--end",
                "2026-05-01",
                "--symbols",
                "SH600000",
            ]
        )
        == 0
    )

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    task = database.list_backfill_tasks(dataset="daily_bar")[0]
    database.mark_backfill_task_running(str(task["task_id"]))

    assert (
        main(
            [
                "--config",
                str(config_path),
                "recover-running",
                "--dataset",
                "daily_bar",
                "--older-than-minutes",
                "0",
                "--reason",
                "unit test recovery",
            ]
        )
        == 0
    )

    recovered = database.list_backfill_tasks(dataset="daily_bar")[0]
    assert recovered["status"] == "failed"
    assert recovered["last_error"] == "unit test recovery"


def test_qdc_split_backfill_creates_smaller_pending_tasks(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    assert (
        main(
            [
                "--config",
                str(config_path),
                "plan-backfill",
                "--dataset",
                "daily_bar",
                "--source-id",
                "akshare",
                "--start",
                "2026-05-01",
                "--end",
                "2026-05-01",
                "--symbols",
                "SH600000,SZ000001,SZ300750",
            ]
        )
        == 0
    )

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    task = database.list_backfill_tasks(dataset="daily_bar")[0]

    assert (
        main(
            [
                "--config",
                str(config_path),
                "split-backfill",
                "--task-id",
                str(task["task_id"]),
                "--batch-size",
                "1",
            ]
        )
        == 0
    )

    tasks = database.list_backfill_tasks(dataset="daily_bar")
    assert [task["status"] for task in tasks] == [
        "superseded",
        "pending",
        "pending",
        "pending",
    ]
    assert [task["symbol_batch_json"] for task in tasks[1:]] == [
        ["SH600000"],
        ["SZ000001"],
        ["SZ300750"],
    ]
    try:
        database.split_backfill_task(task_id=str(task["task_id"]), batch_size=1)
    except ValueError as exc:
        assert "only pending or failed backfill tasks can be split" in str(exc)
    else:  # pragma: no cover - assertion branch
        raise AssertionError("split-backfill accepted a superseded original task")


def test_qdc_run_backfill_can_retry_failed_tasks_control_only(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    assert (
        main(
            [
                "--config",
                str(config_path),
                "plan-backfill",
                "--dataset",
                "daily_bar",
                "--source-id",
                "akshare",
                "--start",
                "2026-05-01",
                "--end",
                "2026-05-01",
                "--symbols",
                "SH600000",
            ]
        )
        == 0
    )

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    task = database.list_backfill_tasks(dataset="daily_bar")[0]
    database.finish_backfill_task(
        task_id=str(task["task_id"]),
        status="failed",
        last_error="provider timeout",
    )

    assert (
        main(
            [
                "--config",
                str(config_path),
                "run-backfill",
                "--dataset",
                "daily_bar",
                "--control-only",
            ]
        )
        == 0
    )
    assert database.list_backfill_tasks(dataset="daily_bar")[0]["status"] == "failed"

    assert (
        main(
            [
                "--config",
                str(config_path),
                "run-backfill",
                "--dataset",
                "daily_bar",
                "--retry-failed",
                "--control-only",
            ]
        )
        == 0
    )

    retried = database.list_backfill_tasks(dataset="daily_bar")[0]
    assert retried["status"] == "success"
    assert retried["attempt_count"] == 1
    assert retried["last_error"] is None
    assert database.fetch_dataset_watermarks()[0]["dataset"] == "daily_bar"


def test_qdc_crawl_plan_is_idempotent(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    args = [
        "--config",
        str(config_path),
        "crawl-plan",
        "--source-id",
        "cninfo_announcement",
        "--date",
        "2026-05-11",
        "--control-only",
    ]

    assert main(args) == 0
    assert main(args) == 0

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    counts = database.table_counts()
    assert counts["crawler_source"] == 1
    assert counts["crawl_task"] == 1
    assert counts["crawl_run"] == 2
    task = database.list_crawl_tasks()[0]
    assert task["source_id"] == "cninfo_announcement"
    assert task["dataset"] == "announcement"
    assert task["crawl_date"] == "2026-05-11"
    assert task["partition_key"] == "date=2026-05-11"
    assert task["request_json"]["parser_version"] == "cninfo_announcement_v1"
    assert task["status"] == "pending"


def test_qdc_crawl_run_control_only_updates_tasks(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    assert (
        main(
            [
                "--config",
                str(config_path),
                "crawl-plan",
                "--source-id",
                "cninfo_announcement",
                "--date",
                "2026-05-11",
            ]
        )
        == 0
    )

    assert (
        main(
            [
                "--config",
                str(config_path),
                "crawl-run",
                "--source-id",
                "cninfo_announcement",
                "--control-only",
            ]
        )
        == 0
    )

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    task = database.list_crawl_tasks()[0]
    assert task["status"] == "success"
    assert task["attempt_count"] == 1
    assert task["last_error"] is None
    assert database.table_counts()["crawl_run"] == 2


def test_qdc_crawl_run_real_cninfo_with_fake_response(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = _write_config(tmp_path)
    assert (
        main(
            [
                "--config",
                str(config_path),
                "crawl-plan",
                "--source-id",
                "cninfo_announcement",
                "--date",
                "2026-05-11",
            ]
        )
        == 0
    )

    class FakeResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "totalpages": 1,
                "totalRecordNum": 2,
                "announcements": [
                    {
                        "secCode": "600000",
                        "secName": "浦发银行",
                        "announcementId": "1218792734",
                        "announcementTitle": "<em>年度报告</em>",
                        "announcementTime": 1704199222000,
                        "adjunctUrl": "finalpage/2024-01-02/1218792734.PDF",
                    },
                    {
                        "secCode": "000001",
                        "secName": "平安银行",
                        "announcementId": "1218792735",
                        "announcementTitle": "关于董事会决议的公告",
                        "announcementTime": 1704199222000,
                        "adjunctUrl": "finalpage/2024-01-02/1218792735.PDF",
                    },
                ],
            }

    class FakePdfResponse:
        status_code = 200
        headers = {"Content-Type": "application/pdf"}
        content = b"%PDF-1.4 unit-test"

        def raise_for_status(self) -> None:
            return None

    calls = []
    pdf_calls = []

    def fake_post(url, headers, data, timeout):
        calls.append({"url": url, "headers": headers, "data": data, "timeout": timeout})
        return FakeResponse()

    def fake_get(url, headers, timeout):
        pdf_calls.append({"url": url, "headers": headers, "timeout": timeout})
        return FakePdfResponse()

    import requests

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(requests, "get", fake_get)

    assert (
        main(
            [
                "--config",
                str(config_path),
                "crawl-run",
                "--source-id",
                "cninfo_announcement",
                "--page-size",
                "2",
                "--pdf-limit",
                "1",
            ]
        )
        == 0
    )

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    task = database.list_crawl_tasks()[0]
    assert task["status"] == "success"
    assert calls[0]["data"]["seDate"] == "2026-05-11~2026-05-11"
    assert pdf_calls == [
        {
            "url": "https://static.cninfo.com.cn/finalpage/2024-01-02/1218792734.PDF",
            "headers": {
                "User-Agent": "Mozilla/5.0 QDC-Crawler/0.1 (+local research)",
                "Referer": (
                    "https://www.cninfo.com.cn/new/commonUrl/pageOfSearch?"
                    "lastPage=index&url=disclosure/list/search"
                ),
                "Origin": "https://www.cninfo.com.cn",
                "Accept": "application/pdf,*/*",
            },
            "timeout": 45,
        }
    ]
    assert database.silver_table_counts()["announcement"] == 2
    source_objects = database.list_source_objects(
        dataset="announcement",
        source_id="cninfo_announcement",
    )
    assert {item["layer"] for item in source_objects} == {
        "bronze",
        "raw",
        "raw_file",
        "raw_manifest",
        "raw_records",
    }
    records_uri = next(item["uri"] for item in source_objects if item["layer"] == "raw_records")
    assert "/raw/documents/2026-05-11/cninfo_announcement/" in records_uri
    pdf_hash = hashlib.sha256(FakePdfResponse.content).hexdigest()
    with database.connect() as conn:
        rows = conn.execute(
            """
            select instrument, title, url, pdf_download_status, pdf_sha256, pdf_size_bytes
            from qdc_silver.announcement
            order by instrument
            """
        ).fetchall()
    assert rows == [
        (
            "SH600000",
            "年度报告",
            "https://static.cninfo.com.cn/finalpage/2024-01-02/1218792734.PDF",
            "success",
            pdf_hash,
            len(FakePdfResponse.content),
        ),
        (
            "SZ000001",
            "关于董事会决议的公告",
            "https://static.cninfo.com.cn/finalpage/2024-01-02/1218792735.PDF",
            "skipped_by_limit",
            None,
            None,
        ),
    ]


def test_qdc_crawl_run_real_sse_announcement_with_fake_response(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = _write_config(tmp_path)
    assert (
        main(
            [
                "--config",
                str(config_path),
                "crawl-plan",
                "--source-id",
                "sse_announcement",
                "--date",
                "2026-05-11",
            ]
        )
        == 0
    )

    class FakeResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "pageHelp": {"pageCount": 1},
                "result": [
                    {
                        "SECURITY_CODE": "600000",
                        "SECURITY_NAME": "浦发银行",
                        "SSEDATE": "2026-05-11",
                        "TITLE": "浦发银行2025年年度股东会决议公告",
                        "URL": "/disclosure/listedinfo/announcement/c/new/a1.pdf",
                    },
                    {
                        "SECURITY_CODE": "600001",
                        "SECURITY_NAME": "测试公司",
                        "SSEDATE": "2026-05-10",
                        "TITLE": "历史公告不应入库",
                        "URL": "/disclosure/listedinfo/announcement/c/new/a2.pdf",
                    },
                ],
            }

    calls = []

    def fake_get(url, headers, params=None, timeout=30):
        calls.append({"url": url, "headers": headers, "params": params, "timeout": timeout})
        return FakeResponse()

    import requests

    monkeypatch.setattr(requests, "get", fake_get)

    assert (
        main(
            [
                "--config",
                str(config_path),
                "crawl-run",
                "--source-id",
                "sse_announcement",
                "--page-size",
                "2",
                "--skip-pdf-download",
            ]
        )
        == 0
    )

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    task = database.list_crawl_tasks(source_id="sse_announcement")[0]
    assert task["status"] == "success"
    assert calls[0]["params"]["beginDate"] == "2026-05-11"
    assert calls[0]["params"]["endDate"] == "2026-05-11"
    assert database.silver_table_counts()["announcement"] == 1
    source_objects = database.list_source_objects(
        dataset="announcement",
        source_id="sse_announcement",
    )
    assert {item["layer"] for item in source_objects} == {
        "bronze",
        "raw",
        "raw_manifest",
        "raw_records",
    }
    with database.connect() as conn:
        row = conn.execute(
            """
            select instrument, publish_date, title, source_id, pdf_download_status
            from qdc_silver.announcement
            """
        ).fetchone()
    assert row == (
        "SH600000",
        datetime(2026, 5, 11).date(),
        "浦发银行2025年年度股东会决议公告",
        "sse_announcement",
        "skipped",
    )



def test_qdc_crawl_run_real_sina_news_with_fake_response(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = _write_config(tmp_path)
    settings = QdcSettings.from_yaml(config_path)
    database = QdcDatabase(settings)
    database.init_schema()
    SilverStore(settings).upsert_stock_basic(
        [
            {
                "instrument": "SH600000",
                "symbol": "600000",
                "exchange": "SH",
                "name": "浦发银行",
                "is_active": True,
                "source_id": "unit_test",
            },
            {
                "instrument": "SZ000001",
                "symbol": "000001",
                "exchange": "SZ",
                "name": "平安银行",
                "is_active": True,
                "source_id": "unit_test",
            },
        ]
    )
    assert (
        main(
            [
                "--config",
                str(config_path),
                "crawl-plan",
                "--source-id",
                "sina_finance_news",
                "--date",
                "2026-05-11",
            ]
        )
        == 0
    )

    class FakeResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "result": {
                    "data": [
                        {
                            "id": "news-1",
                            "title": "浦发银行签订重大合同",
                            "ctime": "2026-05-11 18:20:00",
                            "url": "https://finance.sina.com.cn/news/1.shtml",
                        },
                        {
                            "id": "news-2",
                            "title": "市场综述未提及个股",
                            "ctime": "2026-05-11 18:21:00",
                            "url": "https://finance.sina.com.cn/news/2.shtml",
                        },
                        {
                            "id": "news-3",
                            "title": "平安银行公告解读",
                            "ctime": "2026-05-10 18:21:00",
                            "url": "https://finance.sina.com.cn/news/3.shtml",
                        },
                    ]
                }
            }

    calls = []

    def fake_get(url, headers, params, timeout):
        calls.append({"url": url, "headers": headers, "params": params, "timeout": timeout})
        return FakeResponse()

    import requests

    monkeypatch.setattr(requests, "get", fake_get)

    assert (
        main(
            [
                "--config",
                str(config_path),
                "crawl-run",
                "--source-id",
                "sina_finance_news",
                "--page-size",
                "3",
                "--max-pages",
                "1",
            ]
        )
        == 0
    )

    task = database.list_crawl_tasks(source_id="sina_finance_news")[0]
    assert task["status"] == "success"
    assert calls[0]["params"] == {"pageid": "153", "lid": "1686", "num": "3", "page": "1"}
    assert database.silver_table_counts()["news"] == 2
    source_objects = database.list_source_objects(
        dataset="news",
        source_id="sina_finance_news",
    )
    assert {item["layer"] for item in source_objects} == {
        "bronze",
        "raw",
        "raw_manifest",
        "raw_records",
    }
    records_uri = next(item["uri"] for item in source_objects if item["layer"] == "raw_records")
    assert "/raw/documents/2026-05-11/sina_finance_news/" in records_uri
    with database.connect() as conn:
        rows = conn.execute(
            """
            select instrument, publish_date, publish_time, title, url, source_id
            from qdc_silver.news
            order by instrument
            """
        ).fetchall()
    assert rows == [
        (
            "SH600000",
            datetime(2026, 5, 11).date(),
            datetime(2026, 5, 11, 18, 20),
            "浦发银行签订重大合同",
            "https://finance.sina.com.cn/news/1.shtml",
            "sina_finance_news",
        ),
        (
            "SZ000001",
            datetime(2026, 5, 10).date(),
            datetime(2026, 5, 10, 18, 21),
            "平安银行公告解读",
            "https://finance.sina.com.cn/news/3.shtml",
            "sina_finance_news",
        ),
    ]


def test_qdc_crawl_run_real_eastmoney_news_with_fake_response(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = _write_config(tmp_path)
    settings = QdcSettings.from_yaml(config_path)
    database = QdcDatabase(settings)
    database.init_schema()
    SilverStore(settings).upsert_stock_basic(
        [
            {
                "instrument": "SH600000",
                "symbol": "600000",
                "exchange": "SH",
                "name": "浦发银行",
                "is_active": True,
                "source_id": "unit_test",
            }
        ]
    )
    assert (
        main(
            [
                "--config",
                str(config_path),
                "crawl-plan",
                "--source-id",
                "eastmoney_roll_news",
                "--date",
                "2026-05-11",
            ]
        )
        == 0
    )

    class FakeResponse:
        status_code = 200
        text = """
        <li><span>2026-05-11 18:20</span>[<a href="stock.html">股票</a>]
        <a href="http://stock.eastmoney.com/a/202605111111.html"
        title="浦发银行签订重大合同" target="_blank">浦发银行签订重大合同</a></li>
        <li><span>2026-05-10 18:21</span>[<a href="stock.html">股票</a>]
        <a href="http://stock.eastmoney.com/a/202605101111.html"
        title="浦发银行历史新闻" target="_blank">浦发银行历史新闻</a></li>
        """

        def raise_for_status(self) -> None:
            return None

    calls = []

    def fake_get(url, headers, timeout):
        calls.append({"url": url, "headers": headers, "timeout": timeout})
        return FakeResponse()

    import requests

    monkeypatch.setattr(requests, "get", fake_get)

    assert (
        main(
            [
                "--config",
                str(config_path),
                "crawl-run",
                "--source-id",
                "eastmoney_roll_news",
                "--page-size",
                "5",
                "--max-pages",
                "1",
            ]
        )
        == 0
    )

    task = database.list_crawl_tasks(source_id="eastmoney_roll_news")[0]
    assert task["status"] == "success"
    assert calls[0]["url"] == "https://roll.eastmoney.com/default_1.html"
    assert database.silver_table_counts()["news"] == 2
    source_objects = database.list_source_objects(
        dataset="news",
        source_id="eastmoney_roll_news",
    )
    assert {item["layer"] for item in source_objects} == {
        "bronze",
        "raw",
        "raw_manifest",
        "raw_records",
    }
    with database.connect() as conn:
        row = conn.execute(
            """
            select instrument, publish_date, publish_time, title, source_id
            from qdc_silver.news
            order by publish_date desc, title
            """
        ).fetchall()
    assert row == [
        (
            "SH600000",
            datetime(2026, 5, 11).date(),
            datetime(2026, 5, 11, 18, 20),
            "浦发银行签订重大合同",
            "eastmoney_roll_news",
        ),
        (
            "SH600000",
            datetime(2026, 5, 10).date(),
            datetime(2026, 5, 10, 18, 21),
            "浦发银行历史新闻",
            "eastmoney_roll_news",
        ),
    ]


def test_qdc_crawl_run_unsupported_real_source_fails_explicitly(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    database.init_schema()
    database.insert_crawl_task(
        source_id="unsupported_source",
        dataset="announcement",
        crawl_date="2026-05-11",
        partition_key="date=2026-05-11",
        request={"source_id": "unsupported_source"},
    )

    assert main(["--config", str(config_path), "crawl-run"]) == 1

    task = database.list_crawl_tasks()[0]
    assert task["status"] == "failed"
    assert "unsupported real crawler source_id" in str(task["last_error"])


def test_qdc_crawl_daily_control_only_plans_and_runs_default_sources(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)

    assert (
        main(
            [
                "--config",
                str(config_path),
                "crawl-daily",
                "--date",
                "2026-05-11",
                "--control-only",
            ]
        )
        == 0
    )

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    tasks = database.list_crawl_tasks()
    assert len(tasks) == 4
    assert {task["source_id"] for task in tasks} == {
        "cninfo_announcement",
        "sse_announcement",
        "sina_finance_news",
        "eastmoney_roll_news",
    }
    assert {task["status"] for task in tasks} == {"success"}
    assert database.table_counts()["crawl_run"] == 1


def test_qdc_crawl_recover_running_marks_stale_tasks_failed(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    assert (
        main(
            [
                "--config",
                str(config_path),
                "crawl-plan",
                "--source-id",
                "cninfo_announcement",
                "--date",
                "2026-05-11",
            ]
        )
        == 0
    )

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    task = database.list_crawl_tasks()[0]
    database.mark_crawl_task_running(str(task["task_id"]))

    assert (
        main(
            [
                "--config",
                str(config_path),
                "crawl-recover-running",
                "--source-id",
                "cninfo_announcement",
                "--older-than-minutes",
                "0",
                "--reason",
                "unit test crawl recovery",
            ]
        )
        == 0
    )

    recovered = database.list_crawl_tasks()[0]
    assert recovered["status"] == "failed"
    assert recovered["last_error"] == "unit test crawl recovery"


def test_qdc_daily_control_only_plans_and_runs_daily_tasks(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)

    assert (
        main(
            [
                "--config",
                str(config_path),
                "daily",
                "--date",
                "2026-05-11",
                "--universe",
                "csi300",
                "--batch-size",
                "2",
                "--control-only",
            ]
        )
        == 0
    )

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    tasks = database.list_backfill_tasks()
    assert len(tasks) == 8
    assert {task["dataset"] for task in tasks}.isdisjoint({"announcement", "news"})
    assert {task["status"] for task in tasks} == {"success"}
    assert database.table_counts()["dataset_watermark"] == 5
    job_runs = database.table_counts()["job_run"]
    assert job_runs == 9


def test_qdc_daily_all_market_uses_stock_basic_symbols(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = _write_config(tmp_path)

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            stock_info_a_code_name=lambda: pd.DataFrame(
                [{"code": "600000", "name": "浦发银行"}, {"code": "000001", "name": "平安银行"}]
            )
        ),
    )

    assert (
        main(
            [
                "--config",
                str(config_path),
                "daily",
                "--date",
                "2026-05-11",
                "--all-market",
                "--refresh-stock-basic",
                "--batch-size",
                "1",
                "--control-only",
            ]
        )
        == 0
    )

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    assert database.stock_basic_instruments() == ["SH600000", "SZ000001"]
    tasks = database.list_backfill_tasks()
    assert len(tasks) == 8
    assert {task["status"] for task in tasks} == {"success"}
    assert {task["dataset"] for task in tasks}.isdisjoint({"announcement", "news"})
    daily_bar_tasks = database.list_backfill_tasks(dataset="daily_bar")
    assert [task["symbol_batch_json"] for task in daily_bar_tasks] == [["SH600000"], ["SZ000001"]]


def test_qdc_daily_pipeline_control_only_records_pipeline_job(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = _write_config(tmp_path)

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            stock_info_a_code_name=lambda: pd.DataFrame(
                [{"code": "600000", "name": "浦发银行"}, {"code": "000001", "name": "平安银行"}]
            )
        ),
    )

    assert (
        main(
            [
                "--config",
                str(config_path),
                "daily-pipeline",
                "--date",
                "2026-05-11",
                "--batch-size",
                "1",
                "--control-only",
            ]
        )
        == 0
    )

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    assert database.stock_basic_instruments() == ["SH600000", "SZ000001"]
    tasks = database.list_backfill_tasks()
    assert len(tasks) == 8
    assert {task["dataset"] for task in tasks}.isdisjoint({"announcement", "news"})
    with database.connect() as conn:
        pipeline_job = conn.execute(
            """
            select status, dataset, universe, parameters_json
            from qdc_meta.job_run
            where job_type = 'daily_pipeline'
            """
        ).fetchone()
    assert pipeline_job is not None
    assert pipeline_job[0:3] == ("success", "daily_pipeline", "all_a")
    assert json.loads(pipeline_job[3])["symbol_count"] == 2


def test_qdc_daily_watch_outputs_backfill_progress(tmp_path: Path, capsys) -> None:
    config_path = _write_config(tmp_path)

    assert (
        main(
            [
                "--config",
                str(config_path),
                "daily",
                "--date",
                "2026-05-11",
                "--symbols",
                "SH600000",
                "--batch-size",
                "1",
                "--control-only",
                "--watch",
            ]
        )
        == 0
    )

    captured = capsys.readouterr()
    assert "[BACKFILL]" in captured.err
    assert "RUNNING" in captured.err
    assert "dataset=" in captured.err
    assert "source=" in captured.err


def test_qdc_daily_pipeline_uses_config_defaults_and_cli_overrides(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = _write_config(
        tmp_path,
        """
daily_pipeline:
  universe: all_a
  source_id: akshare
  batch_size: 1
  crawl_documents: true
  crawl_page_size: 5
  crawl_max_pages: 1
  crawl_pdf_limit: 1
""",
    )

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            stock_info_a_code_name=lambda: pd.DataFrame(
                [{"code": "600000", "name": "浦发银行"}, {"code": "000001", "name": "平安银行"}]
            )
        ),
    )

    assert (
        main(
            [
                "--config",
                str(config_path),
                "daily-pipeline",
                "--date",
                "2026-05-11",
                "--batch-size",
                "2",
                "--no-crawl-documents",
                "--control-only",
            ]
        )
        == 0
    )

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    daily_bar_tasks = database.list_backfill_tasks(dataset="daily_bar")
    assert [task["symbol_batch_json"] for task in daily_bar_tasks] == [
        ["SH600000", "SZ000001"]
    ]
    assert database.list_crawl_tasks() == []
    with database.connect() as conn:
        pipeline_job = conn.execute(
            """
            select parameters_json
            from qdc_meta.job_run
            where job_type = 'daily_pipeline'
            """
        ).fetchone()
    assert pipeline_job is not None
    assert json.loads(pipeline_job[0])["crawl_documents"] is False


def test_qdc_daily_pipeline_watch_outputs_pipeline_and_crawl_progress(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    config_path = _write_config(
        tmp_path,
        """
daily_pipeline:
  crawl_documents: true
""",
    )

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            stock_info_a_code_name=lambda: pd.DataFrame(
                [{"code": "600000", "name": "浦发银行"}, {"code": "000001", "name": "平安银行"}]
            )
        ),
    )

    assert (
        main(
            [
                "--config",
                str(config_path),
                "daily-pipeline",
                "--date",
                "2026-05-11",
                "--batch-size",
                "1",
                "--watch",
                "--control-only",
                "--crawl-page-size",
                "5",
                "--crawl-max-pages",
                "1",
                "--crawl-pdf-limit",
                "1",
            ]
        )
        == 0
    )

    captured = capsys.readouterr()
    assert "pipeline" in captured.err
    assert "crawl-documents" in captured.err
    assert "source=" in captured.err


def test_qdc_daily_pipeline_crawl_documents_control_only_runs_crawlers(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = _write_config(tmp_path)

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            stock_info_a_code_name=lambda: pd.DataFrame(
                [{"code": "600000", "name": "浦发银行"}, {"code": "000001", "name": "平安银行"}]
            )
        ),
    )

    assert (
        main(
            [
                "--config",
                str(config_path),
                "daily-pipeline",
                "--date",
                "2026-05-11",
                "--batch-size",
                "1",
                "--control-only",
                "--crawl-documents",
                "--crawl-page-size",
                "5",
                "--crawl-max-pages",
                "1",
                "--crawl-pdf-limit",
                "1",
            ]
        )
        == 0
    )

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    tasks = database.list_crawl_tasks()
    assert len(tasks) == 4
    assert {task["source_id"] for task in tasks} == {
        "cninfo_announcement",
        "sse_announcement",
        "sina_finance_news",
        "eastmoney_roll_news",
    }
    assert {task["status"] for task in tasks} == {"success"}
    with database.connect() as conn:
        crawl_run = conn.execute(
            """
            select status, planned_count, success_count, parameters_json
            from qdc_meta.crawl_run
            """
        ).fetchone()
        pipeline_job = conn.execute(
            """
            select parameters_json
            from qdc_meta.job_run
            where job_type = 'daily_pipeline'
            """
        ).fetchone()
    assert crawl_run is not None
    assert crawl_run[0:3] == ("success", 4, 4)
    assert json.loads(crawl_run[3])["command"] == "daily-pipeline"
    assert pipeline_job is not None
    parameters = json.loads(pipeline_job[0])
    assert parameters["crawl_documents"] is True
    assert parameters["crawl_status"] == "ok"
    assert parameters["crawl_planned_count"] == 4
    assert parameters["crawl_ran_count"] == 4


def test_qdc_plan_backfill_rejects_unsupported_source(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    assert (
        main(
            [
                "--config",
                str(config_path),
                "plan-backfill",
                "--dataset",
                "daily_bar",
                "--source-id",
                "unsupported_source",
                "--start",
                "2026-05-01",
                "--end",
                "2026-05-01",
                "--symbols",
                "SH600000",
            ]
        )
        == 1
    )

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    assert database.list_backfill_tasks(dataset="daily_bar") == []


def test_qdc_plan_backfill_requires_symbols_for_symbol_datasets(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    assert (
        main(
            [
                "--config",
                str(config_path),
                "plan-backfill",
                "--dataset",
                "daily_bar",
                "--source-id",
                "akshare",
                "--start",
                "2026-05-01",
                "--end",
                "2026-05-01",
            ]
        )
        == 1
    )

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    assert database.list_backfill_tasks(dataset="daily_bar") == []


def test_qdc_plan_backfill_rejects_unknown_universe(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    assert (
        main(
            [
                "--config",
                str(config_path),
                "plan-backfill",
                "--dataset",
                "daily_bar",
                "--source-id",
                "akshare",
                "--universe",
                "unknown_universe",
                "--start",
                "2026-05-01",
                "--end",
                "2026-05-01",
            ]
        )
        == 1
    )

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    assert database.list_backfill_tasks(dataset="daily_bar") == []


def test_qdc_run_backfill_real_daily_bar_with_fake_akshare(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = _write_config(tmp_path)

    def stock_zh_a_daily(**kwargs):
        assert kwargs["symbol"] == "sh600000"
        return pd.DataFrame(
            [
                {
                    "date": "2026-05-11",
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.9,
                    "close": 10.2,
                    "volume": 1000,
                    "amount": 10200,
                }
            ]
        )

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(stock_zh_a_daily=stock_zh_a_daily),
    )
    assert (
        main(
            [
                "--config",
                str(config_path),
                "plan-backfill",
                "--dataset",
                "daily_bar",
                "--source-id",
                "akshare",
                "--start",
                "2026-05-11",
                "--end",
                "2026-05-11",
                "--symbols",
                "SH600000",
            ]
        )
        == 0
    )
    assert main(["--config", str(config_path), "run-backfill", "--dataset", "daily_bar"]) == 0

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    assert database.silver_table_counts()["daily_bar"] == 1
    tasks = database.list_backfill_tasks(dataset="daily_bar")
    assert tasks[0]["status"] == "success"
    source_objects = database.list_source_objects(dataset="daily_bar", source_id="akshare")
    assert {item["layer"] for item in source_objects} == {"bronze", "raw"}
    for source_object in source_objects:
        assert Path(str(source_object["uri"])).exists()
        assert source_object["content_hash"]
        assert source_object["size_bytes"] > 0
    bronze_uri = next(item["uri"] for item in source_objects if item["layer"] == "bronze")
    assert pd.read_parquet(str(bronze_uri)).iloc[0]["close"] == 10.2
    with database.connect() as conn:
        row = conn.execute(
            """
            select instrument, close, vwap
            from qdc_silver.daily_bar
            where trade_date = '2026-05-11'
            """
        ).fetchone()
    assert row == ("SH600000", 10.2, 10.2)


def test_qdc_run_backfill_real_stock_basic_and_calendar_with_fake_akshare(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = _write_config(tmp_path)
    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            stock_info_a_code_name=lambda: pd.DataFrame(
                [{"code": "600000", "name": "浦发银行"}, {"code": "000001", "name": "平安银行"}]
            ),
            tool_trade_date_hist_sina=lambda: pd.DataFrame(
                [
                    {"trade_date": "2026-05-08"},
                    {"trade_date": "2026-05-11"},
                    {"trade_date": "2026-05-12"},
                ]
            ),
        ),
    )

    for dataset in ("stock_basic", "trade_calendar"):
        assert (
            main(
                [
                    "--config",
                    str(config_path),
                    "plan-backfill",
                    "--dataset",
                    dataset,
                    "--source-id",
                    "akshare",
                    "--start",
                    "2026-05-11",
                    "--end",
                    "2026-05-11",
                ]
            )
            == 0
        )
        assert main(["--config", str(config_path), "run-backfill", "--dataset", dataset]) == 0

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    assert database.silver_table_counts()["stock_basic"] == 2
    assert database.silver_table_counts()["trade_calendar"] == 1
    with database.connect() as conn:
        calendar_row = conn.execute(
            """
            select trade_date, pre_trade_date, next_trade_date
            from qdc_silver.trade_calendar
            where calendar_id = 'cn_ashare'
            """
        ).fetchone()
    assert tuple(str(item) for item in calendar_row) == (
        "2026-05-11",
        "2026-05-08",
        "2026-05-12",
    )


def test_qdc_run_backfill_real_stage2_market_tables_with_fake_akshare(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = _write_config(tmp_path)

    def stock_zh_a_daily(**kwargs):
        assert kwargs["symbol"] == "sh600001"
        if kwargs["adjust"] == "qfq":
            return pd.DataFrame([{"date": "2026-05-11", "close": 8.8}])
        return pd.DataFrame(
            [
                {"date": "2026-05-08", "close": 10.0},
                {"date": "2026-05-11", "close": 11.0},
            ]
        )

    def stock_tfp_em(date: str):
        assert date == "20260511"
        return pd.DataFrame(
            [
                {
                    "序号": 1,
                    "代码": "600001",
                    "名称": "样例股份",
                    "停牌时间": "2026-05-11",
                    "停牌原因": "重大事项",
                    "所属市场": "沪市",
                    "预计复牌时间": pd.NaT,
                },
                {
                    "序号": 2,
                    "代码": "600001",
                    "名称": "样例股份",
                    "停牌时间": "2026-05-11",
                    "停牌原因": "重复记录应去重",
                    "所属市场": "沪市",
                    "预计复牌时间": pd.NaT,
                },
                {
                    "序号": 3,
                    "代码": "200016",
                    "名称": "非A股样例",
                    "停牌时间": "2026-05-11",
                    "停牌原因": "非A股代码应跳过",
                    "所属市场": "深市",
                    "预计复牌时间": pd.NaT,
                }
            ]
        )

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(stock_zh_a_daily=stock_zh_a_daily, stock_tfp_em=stock_tfp_em),
    )

    for dataset in ("adj_factor", "price_limit"):
        assert (
            main(
                [
                    "--config",
                    str(config_path),
                    "plan-backfill",
                    "--dataset",
                    dataset,
                    "--source-id",
                    "akshare",
                    "--start",
                    "2026-05-11",
                    "--end",
                    "2026-05-11",
                    "--symbols",
                    "SH600001",
                ]
            )
            == 0
        )
        assert main(["--config", str(config_path), "run-backfill", "--dataset", dataset]) == 0

    assert (
        main(
            [
                "--config",
                str(config_path),
                "plan-backfill",
                "--dataset",
                "trade_status",
                "--source-id",
                "akshare",
                "--start",
                "2026-05-11",
                "--end",
                "2026-05-11",
            ]
        )
        == 0
    )
    assert main(["--config", str(config_path), "run-backfill", "--dataset", "trade_status"]) == 0

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    counts = database.silver_table_counts()
    assert counts["adj_factor"] == 1
    assert counts["price_limit"] == 1
    assert counts["trade_status"] == 1
    with database.connect() as conn:
        adj_row = conn.execute(
            """
            select instrument, adj_factor, factor_type
            from qdc_silver.adj_factor
            where trade_date = '2026-05-11'
            """
        ).fetchone()
        limit_row = conn.execute(
            """
            select instrument, limit_up, limit_down, limit_rule
            from qdc_silver.price_limit
            where trade_date = '2026-05-11'
            """
        ).fetchone()
        status_row = conn.execute(
            """
            select instrument, trade_status, halt_reason
            from qdc_silver.trade_status
            where trade_date = '2026-05-11'
            """
        ).fetchone()

    assert adj_row == ("SH600001", 0.8, "qfq_close_ratio_v0_inferred")
    assert limit_row == ("SH600001", 11.0, 9.0, "main_board_normal_10pct_v0_inferred")
    assert status_row == ("SH600001", "halted", "重大事项")


def test_qdc_run_backfill_announcement_news_and_build_factors_with_fake_akshare(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = _write_config(tmp_path)

    def stock_notice_report(symbol: str, date: str):
        assert symbol == "全部"
        assert date == "20260511"
        return pd.DataFrame(
            [
                {
                    "代码": "600000",
                    "公告日期": "2026-05-11",
                    "公告标题": "年度权益分派公告",
                    "公告链接": "https://example.test/notice/1",
                },
                {
                    "代码": "600000",
                    "公告日期": "2026-05-11",
                    "公告标题": "年度权益分派公告",
                    "公告链接": "https://example.test/notice/1",
                }
            ]
        )

    def stock_news_em(symbol: str):
        assert symbol == "600000"
        return pd.DataFrame(
            [
                {
                    "发布时间": "2026-05-11 09:30:00",
                    "新闻标题": "公司新闻标题",
                    "新闻链接": "https://example.test/news/1",
                }
            ]
        )

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(stock_notice_report=stock_notice_report, stock_news_em=stock_news_em),
    )

    assert (
        main(
            [
                "--config",
                str(config_path),
                "plan-backfill",
                "--dataset",
                "announcement",
                "--source-id",
                "akshare",
                "--start",
                "2026-05-11",
                "--end",
                "2026-05-11",
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "--config",
                str(config_path),
                "plan-backfill",
                "--dataset",
                "news",
                "--source-id",
                "akshare",
                "--start",
                "2026-05-11",
                "--end",
                "2026-05-11",
                "--symbols",
                "SH600000",
            ]
        )
        == 0
    )
    assert main(["--config", str(config_path), "run-backfill", "--dataset", "announcement"]) == 0
    assert main(["--config", str(config_path), "run-backfill", "--dataset", "news"]) == 0
    assert (
        main(
            [
                "--config",
                str(config_path),
                "build-factors",
                "--factor-set",
                "all",
                "--start",
                "2026-05-11",
                "--end",
                "2026-05-11",
            ]
        )
        == 0
    )

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    counts = database.silver_table_counts()
    assert counts["announcement"] == 1
    assert counts["news"] == 1
    assert counts["daily_news_factor"] == 0
    assert counts["daily_announcement_factor"] == 0
    with database.connect() as conn:
        row = conn.execute(
            """
            select n.news_count, a.announcement_count
            from qdc_silver.daily_news_factor n
            join qdc_silver.daily_announcement_factor a
              on n.trade_date = a.trade_date and n.instrument = a.instrument
            """
        ).fetchone()
    assert row is None


def test_qdc_news_falls_back_to_global_stream_and_maps_instrument(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = _write_config(tmp_path)

    def stock_news_em(symbol: str):
        assert symbol == "300750"
        return pd.DataFrame([])

    def stock_info_a_code_name():
        return pd.DataFrame(
            [
                {"code": "300750", "name": "宁德时代"},
                {"code": "600000", "name": "浦发银行"},
            ]
        )

    def stock_info_global_cls():
        return pd.DataFrame(
            [
                {
                    "标题": "宁德时代：控股股东捐赠股份完成过户",
                    "内容": "宁德时代(300750.SZ)公告称，控股股东股份过户完成。",
                    "发布日期": "2026-05-11",
                    "发布时间": "18:50:42",
                },
                {
                    "标题": "未命中标的的宏观新闻",
                    "内容": "这条新闻不包含测试股票名称或代码。",
                    "发布日期": "2026-05-11",
                    "发布时间": "18:51:42",
                },
            ]
        )

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            stock_news_em=stock_news_em,
            stock_info_a_code_name=stock_info_a_code_name,
            stock_info_global_cls=stock_info_global_cls,
        ),
    )

    assert (
        main(
            [
                "--config",
                str(config_path),
                "plan-backfill",
                "--dataset",
                "news",
                "--source-id",
                "akshare",
                "--start",
                "2026-05-11",
                "--end",
                "2026-05-11",
                "--symbols",
                "SZ300750",
            ]
        )
        == 0
    )
    assert main(["--config", str(config_path), "run-backfill", "--dataset", "news"]) == 0

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    assert database.silver_table_counts()["news"] == 1
    with database.connect() as conn:
        row = conn.execute(
            """
            select publish_date, instrument, title
            from qdc_silver.news
            """
        ).fetchone()
    assert (str(row[0]), row[1], row[2]) == (
        "2026-05-11",
        "SZ300750",
        "宁德时代：控股股东捐赠股份完成过户",
    )


def test_qdc_build_factors_aligns_text_titles_and_labels_events(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    settings = QdcSettings.from_yaml(config_path)
    database = QdcDatabase(settings)
    database.init_schema()
    silver = SilverStore(settings)
    silver.upsert_trade_calendar(
        [
            {
                "calendar_id": "SSE",
                "trade_date": "2026-05-08",
                "is_open": True,
                "source_id": "unit_test",
            },
            {
                "calendar_id": "SSE",
                "trade_date": "2026-05-10",
                "is_open": False,
                "source_id": "unit_test",
            },
            {
                "calendar_id": "SSE",
                "trade_date": "2026-05-11",
                "is_open": True,
                "source_id": "unit_test",
            },
        ]
    )
    silver.upsert_news(
        [
            {
                "news_id": "n1",
                "publish_date": "2026-05-10",
                "instrument": "SH600000",
                "title": "公司签订重大订单增长",
                "source_id": "sina_finance_news",
            },
            {
                "news_id": "n2",
                "publish_date": "2026-05-11",
                "instrument": "SH600000",
                "title": "公司被立案调查存在退市风险",
                "source_id": "sina_finance_news",
            },
            {
                "news_id": "n2_backup",
                "publish_date": "2026-05-11",
                "instrument": "SH600000",
                "title": "公司被立案调查存在退市风险",
                "source_id": "eastmoney_roll_news",
            },
        ]
    )
    silver.upsert_announcements(
        [
            {
                "announcement_id": "a1",
                "publish_date": "2026-05-10",
                "instrument": "SH600000",
                "title": "向特定对象发行股票募集资金",
                "source_id": "cninfo_announcement",
            },
            {
                "announcement_id": "a2",
                "publish_date": "2026-05-11",
                "instrument": "SH600000",
                "title": "年度权益分派公告",
                "source_id": "cninfo_announcement",
            },
            {
                "announcement_id": "a2_backup",
                "publish_date": "2026-05-11",
                "instrument": "SH600000",
                "title": "年度权益分派公告",
                "source_id": "sse_announcement",
            },
        ]
    )

    assert (
        main(
            [
                "--config",
                str(config_path),
                "build-factors",
                "--factor-set",
                "all",
                "--start",
                "2026-05-11",
                "--end",
                "2026-05-11",
            ]
        )
        == 0
    )

    with database.connect() as conn:
        news_row = conn.execute(
            """
            select
              trade_date,
              news_count,
              news_sentiment_mean,
              news_positive_count,
              news_negative_count,
              news_growth_count,
              news_risk_count,
              news_financing_count,
              news_contract_count,
              news_regulatory_count,
              news_performance_count
            from qdc_silver.daily_news_factor
            where instrument = 'SH600000'
            """
        ).fetchone()
        announcement_row = conn.execute(
            """
            select
              announcement_count,
              announcement_growth_count,
              announcement_risk_count,
              announcement_financing_count,
              announcement_operation_count,
              announcement_sentiment_mean
            from qdc_silver.daily_announcement_factor
            where instrument = 'SH600000'
            """
        ).fetchone()

    assert (
        news_row[0],
        news_row[1],
        round(float(news_row[2]), 3),
        news_row[3],
        news_row[4],
        news_row[5],
        news_row[6],
        news_row[7],
        news_row[8],
        news_row[9],
        news_row[10],
    ) == (
        datetime(2026, 5, 11).date(),
        2.0,
        0.007,
        1.0,
        1.0,
        1.0,
        1.0,
        0.0,
        1.0,
        1.0,
        0.0,
    )
    assert (
        announcement_row[0],
        announcement_row[1],
        announcement_row[2],
        announcement_row[3],
        announcement_row[4],
        round(float(announcement_row[5]), 2),
    ) == (2.0, 0.0, 0.0, 1.0, 1.0, 0.15)


def test_qdc_classify_text_event_rule_and_mock_litellm(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    config_path = _write_config(tmp_path)

    assert (
        main(
            [
                "--config",
                str(config_path),
                "classify-text-event",
                "--document-type",
                "announcement",
                "--title",
                "公司收到交易所监管问询函",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    classification = payload["classification"]
    assert classification["provider"] == "rule"
    assert "regulatory" in classification["event_types"]
    assert classification["sentiment_score"] < 0

    def completion(**kwargs):
        assert kwargs["model"] == "deepseek/unit-test"
        assert kwargs["api_key"] == "unit-test"
        assert kwargs["temperature"] == 0
        assert kwargs["max_tokens"] == 128
        return SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        content=json.dumps(
                            {
                                "event_types": ["buyback"],
                                "sentiment_score": 0.7,
                                "importance_score": 0.8,
                                "matched_keywords": ["回购"],
                                "evidence": "公司拟回购股份",
                            },
                            ensure_ascii=False,
                        )
                    )
                )
            ]
        )

    secret_path = tmp_path / "data" / "quant_data_center" / "secrets" / "deepseek_api_key"
    secret_path.parent.mkdir(parents=True)
    secret_path.write_text("unit-test\n", encoding="utf-8")
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace("provider: rule", "provider: llm", 1),
        encoding="utf-8",
    )
    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))

    assert (
        main(
            [
                "--config",
                str(config_path),
                "classify-text-event",
                "--document-type",
                "announcement",
                "--title",
                "公司拟回购股份",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["classification"]["provider"] == "llm"
    assert payload["classification"]["model"] == "deepseek/unit-test"
    assert payload["classification"]["event_types"] == ["buyback"]


def test_qdc_news_provider_error_is_recorded_without_failing_task(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = _write_config(tmp_path)

    def stock_news_em(symbol: str):
        assert symbol == "600000"
        raise ValueError(r"Invalid regular expression: invalid escape sequence: \u")

    monkeypatch.setitem(sys.modules, "akshare", SimpleNamespace(stock_news_em=stock_news_em))

    assert (
        main(
            [
                "--config",
                str(config_path),
                "plan-backfill",
                "--dataset",
                "news",
                "--source-id",
                "akshare",
                "--start",
                "2024-01-02",
                "--end",
                "2024-01-02",
                "--symbols",
                "SH600000",
            ]
        )
        == 0
    )
    assert main(["--config", str(config_path), "run-backfill", "--dataset", "news"]) == 0

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    assert database.silver_table_counts()["news"] == 0
    assert database.list_backfill_tasks(dataset="news")[0]["status"] == "success"
    source_objects = database.list_source_objects(dataset="news", layer="raw")
    assert len(source_objects) == 1


def test_silver_store_upserts_core_research_tables(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    settings = QdcSettings.from_yaml(config_path)
    database = QdcDatabase(settings)
    database.init_schema()
    silver = SilverStore(settings)

    assert (
        silver.upsert_stock_basic(
            [
                {
                    "instrument": "SH600000",
                    "symbol": "600000",
                    "exchange": "SSE",
                    "name": "浦发银行",
                    "list_date": "1999-11-10",
                    "is_active": True,
                    "industry": "bank",
                    "source_id": "unit_test",
                }
            ]
        )
        == 1
    )
    assert (
        silver.upsert_trade_calendar(
            [
                {
                    "calendar_id": "cn_ashare",
                    "trade_date": "2026-05-11",
                    "is_open": True,
                    "pre_trade_date": "2026-05-08",
                    "next_trade_date": "2026-05-12",
                    "source_id": "unit_test",
                }
            ]
        )
        == 1
    )
    assert (
        silver.upsert_universe_constituents(
            [
                {
                    "universe": "csi300",
                    "snapshot_date": "2026-05-11",
                    "instrument": "SH600000",
                    "symbol": "600000",
                    "exchange": "SSE",
                    "name": "浦发银行",
                    "weight": 0.8,
                    "source_id": "unit_test",
                }
            ]
        )
        == 1
    )
    assert (
        silver.upsert_daily_bar(
            [
                {
                    "trade_date": "2026-05-11",
                    "instrument": "SH600000",
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.9,
                    "close": 10.2,
                    "pre_close": 10.1,
                    "volume": 1000000,
                    "amount": 10200000,
                    "vwap": 10.2,
                    "source_id": "unit_test",
                }
            ]
        )
        == 1
    )
    assert (
        silver.upsert_adj_factor(
            [
                {
                    "trade_date": "2026-05-11",
                    "instrument": "SH600000",
                    "adj_factor": 0.8,
                    "factor_type": "qfq_close_ratio_v0_inferred",
                    "source_id": "unit_test",
                }
            ]
        )
        == 1
    )
    assert (
        silver.upsert_price_limit(
            [
                {
                    "trade_date": "2026-05-11",
                    "instrument": "SH600000",
                    "limit_up": 11.0,
                    "limit_down": 9.0,
                    "prev_close": 10.0,
                    "limit_rule": "main_board_normal_10pct_v0_inferred",
                    "source_id": "unit_test",
                }
            ]
        )
        == 1
    )
    assert (
        silver.upsert_trade_status(
            [
                {
                    "trade_date": "2026-05-11",
                    "instrument": "SH600000",
                    "trade_status": "halted",
                    "halt_reason": "重大事项",
                    "source_id": "unit_test",
                }
            ]
        )
        == 1
    )

    assert database.silver_table_counts() == {
        "stock_basic": 1,
        "universe_constituent": 1,
        "trade_calendar": 1,
        "daily_bar": 1,
        "adj_factor": 1,
        "price_limit": 1,
        "trade_status": 1,
        "announcement": 0,
        "news": 0,
        "daily_news_factor": 0,
        "daily_announcement_factor": 0,
    }

    silver.upsert_daily_bar(
        [
            {
                "trade_date": "2026-05-11",
                "instrument": "SH600000",
                "open": 10.0,
                "high": 10.5,
                "low": 9.9,
                "close": 10.3,
                "source_id": "unit_test",
            }
        ]
    )
    with database.connect() as conn:
        close_value = conn.execute(
            """
            select close
            from qdc_silver.daily_bar
            where trade_date = '2026-05-11' and instrument = 'SH600000'
            """
        ).fetchone()[0]
    assert close_value == 10.3


def test_silver_announcements_preserve_first_seen_pdf_metadata(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    settings = QdcSettings.from_yaml(config_path)
    database = QdcDatabase(settings)
    database.init_schema()
    silver = SilverStore(settings)

    silver.upsert_announcements(
        [
            {
                "announcement_id": "cninfo_a1_SH600000",
                "source_record_id": "a1",
                "publish_date": "2026-05-11",
                "publish_time": "2026-05-11 18:30:00",
                "instrument": "SH600000",
                "title": "公司公告",
                "url": "https://static.cninfo.com.cn/a1.pdf",
                "source_id": "cninfo_announcement",
                "observed_at": "2026-05-11 18:40:00",
                "collect_time": "2026-05-11 18:40:00",
                "pdf_sha256": "first_hash",
                "pdf_size_bytes": 123,
                "pdf_object_id": "obj1",
                "pdf_download_status": "success",
            }
        ]
    )
    silver.upsert_announcements(
        [
            {
                "announcement_id": "cninfo_a1_SH600000",
                "source_record_id": "a1",
                "publish_date": "2026-05-11",
                "publish_time": "2026-05-11 18:30:00",
                "instrument": "SH600000",
                "title": "公司公告",
                "url": "https://static.cninfo.com.cn/a1.pdf",
                "source_id": "cninfo_announcement",
                "observed_at": "2026-05-11 21:30:00",
                "collect_time": "2026-05-11 21:30:00",
                "pdf_download_status": "skipped",
            }
        ]
    )

    with database.connect() as conn:
        row = conn.execute(
            """
            select observed_at, collect_time, pdf_sha256, pdf_size_bytes,
                   pdf_object_id, pdf_download_status
            from qdc_silver.announcement
            where announcement_id = 'cninfo_a1_SH600000'
            """
        ).fetchone()

    assert str(row[0]) == "2026-05-11 18:40:00"
    assert str(row[1]) == "2026-05-11 21:30:00"
    assert row[2:] == ("first_hash", 123, "obj1", "success")


def test_qdc_refresh_universe_snapshot_feeds_plan_backfill(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = _write_config(tmp_path)

    def index_stock_cons_csindex(symbol: str):
        assert symbol == "000300"
        return pd.DataFrame(
            [
                {"成分券代码": "600001", "成分券名称": "样例沪市"},
                {"成分券代码": "000002", "成分券名称": "样例深市"},
            ]
        )

    def index_stock_cons_weight_csindex(symbol: str):
        assert symbol == "000300"
        return pd.DataFrame(
            [
                {"成分券代码": "600001", "权重": 1.2},
                {"成分券代码": "000002", "权重": 0.9},
            ]
        )

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            index_stock_cons_csindex=index_stock_cons_csindex,
            index_stock_cons_weight_csindex=index_stock_cons_weight_csindex,
        ),
    )

    assert (
        main(
            [
                "--config",
                str(config_path),
                "refresh-universe",
                "--universe",
                "csi300",
                "--snapshot-date",
                "2026-05-11",
            ]
        )
        == 0
    )
    assert main(["--config", str(config_path), "list-universe", "--universe", "csi300"]) == 0

    database = QdcDatabase(QdcSettings.from_yaml(config_path))
    assert database.silver_table_counts()["universe_constituent"] == 2
    assert database.latest_universe_symbols("csi300") == ["SH600001", "SZ000002"]
    assert {item["layer"] for item in database.list_source_objects(dataset="universe_constituent")} == {
        "bronze",
        "raw",
    }

    assert (
        main(
            [
                "--config",
                str(config_path),
                "plan-backfill",
                "--dataset",
                "daily_bar",
                "--source-id",
                "akshare",
                "--universe",
                "csi300",
                "--start",
                "2026-05-11",
                "--end",
                "2026-05-11",
                "--batch-size",
                "2",
            ]
        )
        == 0
    )
    tasks = database.list_backfill_tasks(dataset="daily_bar")
    assert tasks[0]["symbol_batch_json"] == ["SH600001", "SZ000002"]


def test_qdc_sync_parquet_writes_silver_and_gold_files(tmp_path: Path) -> None:
    config_path, database = _seed_research_rows(tmp_path)

    assert main(["--config", str(config_path), "sync-parquet", "--layer", "all"]) == 0

    settings = QdcSettings.from_yaml(config_path)
    silver_path = settings.parquet_root / "silver" / "daily_bar" / "part-000.parquet"
    gold_path = settings.parquet_root / "gold" / "daily_research" / "part-000.parquet"
    assert pd.read_parquet(silver_path).shape[0] == 2
    gold = pd.read_parquet(gold_path)
    assert list(gold["close"]) == [10.2, 10.6]
    source_objects = database.list_source_objects(source_id="qdc")
    assert {"silver", "gold"}.issubset({item["layer"] for item in source_objects})


def test_qdc_quality_records_issues_for_invalid_daily_bar(tmp_path: Path) -> None:
    config_path, database = _seed_research_rows(tmp_path)
    settings = QdcSettings.from_yaml(config_path)
    silver = SilverStore(settings)

    assert main(["--config", str(config_path), "quality", "--dataset", "daily_bar"]) == 0

    silver.upsert_daily_bar(
        [
            {
                "trade_date": "2026-05-13",
                "instrument": "SH600000",
                "open": 10.0,
                "high": 9.5,
                "low": 10.5,
                "close": 10.2,
                "volume": 1000,
                "source_id": "unit_test",
            }
        ]
    )

    assert main(["--config", str(config_path), "quality", "--dataset", "daily_bar"]) == 1
    issues = database.list_quality_issues(dataset="daily_bar")
    assert sorted(issue["issue_type"] for issue in issues) == [
        "close_outside_range",
        "invalid_price_range",
    ]


def test_qdc_export_qlib_writes_day_provider_files(tmp_path: Path, capsys) -> None:
    config_path, database = _seed_research_rows(tmp_path)
    silver = SilverStore(QdcSettings.from_yaml(config_path))
    silver.upsert_daily_news_factor(
        [
            {
                "trade_date": "2026-05-11",
                "instrument": "SH600000",
                "news_count": 2,
                "news_sentiment_mean": 0.5,
                "news_positive_count": 2,
                "news_negative_count": 0,
                "news_growth_count": 1,
                "news_risk_count": 0,
                "news_financing_count": 0,
                "source_id": "unit_test",
            }
        ]
    )
    silver.upsert_daily_announcement_factor(
        [
            {
                "trade_date": "2026-05-11",
                "instrument": "SH600000",
                "announcement_count": 1,
                "announcement_risk_count": 0,
                "announcement_financing_count": 1,
                "announcement_operation_count": 0,
                "source_id": "unit_test",
            }
        ]
    )
    provider_uri = tmp_path / "qlib_export"

    assert (
        main(
            [
                "--config",
                str(config_path),
                "export-qlib",
                "--provider-uri",
                str(provider_uri),
                "--market-name",
                "qdc_smoke",
                "--start",
                "2026-05-11",
                "--end",
                "2026-05-12",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["object_id_count"] == 44
    assert len(payload["object_id_sample"]) == 5
    assert "object_ids" not in payload

    assert (provider_uri / "calendars" / "day.txt").read_text(encoding="utf-8") == (
        "2026-05-11\n2026-05-12\n"
    )
    assert (provider_uri / "instruments" / "all.txt").read_text(encoding="utf-8") == (
        "sh600000\t2026-05-11\t2026-05-12\n"
    )
    assert (provider_uri / "instruments" / "qdc_smoke.txt").read_text(encoding="utf-8") == (
        "sh600000\t2026-05-11\t2026-05-12\n"
    )
    close_bin = (provider_uri / "features" / "sh600000" / "close.day.bin").read_bytes()
    assert struct.unpack("<fff", close_bin) == (0.0, 10.199999809265137, 10.600000381469727)
    vwap_bin = (provider_uri / "features" / "sh600000" / "vwap.day.bin").read_bytes()
    assert struct.unpack("<fff", vwap_bin) == (0.0, 10.199999809265137, 10.600000381469727)
    sentiment_bin = (
        provider_uri / "features" / "sh600000" / "news_sentiment_mean.day.bin"
    ).read_bytes()
    assert struct.unpack("<fff", sentiment_bin) == (0.0, 0.5, 0.0)
    qlib_objects = database.list_source_objects(dataset="qlib_export", layer="qlib")
    assert len(qlib_objects) == 44


def test_qdc_verify_qlib_reports_missing_instrument_without_db_side_effect(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    config_path = _write_config(tmp_path)
    provider_uri = tmp_path / "qlib_provider"
    (provider_uri / "features" / "sh600000").mkdir(parents=True)
    (provider_uri / "features" / "sh600000" / "close.day.bin").write_bytes(b"")

    class FakeD:
        @staticmethod
        def calendar(**kwargs):
            return [datetime(2026, 5, 11)]

        @staticmethod
        def instruments(name):
            assert name == "all"
            return name

        @staticmethod
        def list_instruments(pool, **kwargs):
            assert pool == "all"
            return ["sh600000"]

        @staticmethod
        def features(instruments, fields, **kwargs):
            assert instruments == ["SZ000001"]
            assert fields == ["$close"]
            return pd.DataFrame(columns=fields)

    monkeypatch.setitem(
        sys.modules,
        "qlib",
        SimpleNamespace(init=lambda **kwargs: None),
    )
    monkeypatch.setitem(sys.modules, "qlib.data", SimpleNamespace(D=FakeD))

    assert (
        main(
            [
                "--config",
                str(config_path),
                "verify-qlib",
                "--provider-uri",
                str(provider_uri),
                "--start",
                "2026-05-11",
                "--end",
                "2026-05-11",
                "--instruments",
                "SZ000001",
                "--fields",
                "$close",
            ]
        )
        == 1
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "fail"
    assert [issue["issue_type"] for issue in payload["issues"]] == [
        "missing_instruments",
        "empty_features",
    ]
    assert not QdcSettings.from_yaml(config_path).database_path.exists()
