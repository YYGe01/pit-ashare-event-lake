from __future__ import annotations

import sys
from pathlib import Path

from quant_data_center.console import QdcConsoleData, _daily_pipeline_command
from quant_data_center.settings import QdcSettings
from quant_data_center.storage.database import QdcDatabase
from quant_data_center.storage.silver import SilverStore


def _write_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "config" / "quant_data_center.yaml"
    config_path.parent.mkdir(parents=True)
    config_path.write_text(
        """
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
""".strip(),
        encoding="utf-8",
    )
    return config_path


def test_qdc_daily_status_returns_beginner_dashboard_sections(tmp_path: Path) -> None:
    settings = QdcSettings.from_yaml(_write_config(tmp_path))
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
    silver.upsert_daily_bar(
        [
            {
                "trade_date": "2026-05-11",
                "instrument": "SH600000",
                "open": 10.0,
                "high": 10.2,
                "low": 9.9,
                "close": 10.1,
                "source_id": "akshare",
            }
        ]
    )
    task_id, _inserted = database.insert_backfill_task(
        dataset="daily_bar",
        source_id="akshare",
        universe="all_a",
        start_date="2026-05-11",
        end_date="2026-05-11",
        symbols=["SZ000001"],
    )
    database.finish_backfill_task(
        task_id=task_id,
        status="failed",
        last_error="Read timed out",
    )
    crawl_task_id, _inserted = database.insert_crawl_task(
        source_id="cninfo_announcement",
        dataset="announcement",
        crawl_date="2026-05-11",
        partition_key="2026-05-11",
        request={"date": "2026-05-11"},
    )
    database.mark_crawl_task_running(crawl_task_id)
    database.insert_quality_issue(
        dataset="daily_bar",
        source_id="akshare",
        severity="error",
        issue_type="invalid_price_range",
        entity_key="2026-05-11|SH600000",
        message="daily_bar high is lower than low",
    )
    database.record_crawl_run(
        source_id="cninfo_announcement",
        dataset="announcement",
        crawl_date="2026-05-11",
        status="success",
        planned_count=1,
        success_count=1,
        document_count=3,
        raw_object_count=2,
    )

    payload = QdcConsoleData(settings).daily_collection_status(date="2026-05-11")

    assert payload["verdict"]["level"] == "danger"
    assert "采集" in payload["verdict"]["title"]
    assert {row["stage_id"] for row in payload["stage_rows"]}.issuperset(
        {"universe", "daily_pipeline", "daily_bar", "quality", "export_qlib"}
    )
    assert payload["quality_summary"]["status"] == "failed"
    assert payload["quality_summary"]["open_issue_count"] == 1
    assert any(
        row["dimension"] == "validity" and row["status"] == "failed"
        for row in payload["quality_summary"]["rows"]
    )
    source_rows = {row["source_id"]: row for row in payload["source_summary_rows"]}
    assert source_rows["akshare"]["state"] == "failed"
    assert source_rows["akshare"]["timeout_count"] == 1
    assert source_rows["cninfo_announcement"]["raw_object_count"] == 2
    assert payload["batch_task_rows"][0]["task_id"] == task_id
    assert payload["batch_task_rows"][0]["state"] == "failed"
    assert payload["batch_task_rows"][0]["progress_percent"] == 100
    assert payload["batch_task_rows"][0]["symbol_preview"] == "SZ000001"
    assert payload["crawl_task_rows"][0]["task_id"] == crawl_task_id
    assert payload["crawl_task_rows"][0]["state"] == "running"
    assert payload["crawl_task_rows"][0]["progress_percent"] == 50


def test_qdc_console_builds_restricted_daily_pipeline_command(tmp_path: Path) -> None:
    settings = QdcSettings.from_yaml(_write_config(tmp_path))

    command = _daily_pipeline_command(
        settings,
        {
            "date": "2026-05-13",
            "symbols": " sh600000, sz000001 ",
            "batch_size": 2,
            "control_only": True,
            "refresh_stock_basic": True,
            "crawl_documents": False,
        },
    )

    assert command[:3] == [sys.executable, "-m", "quant_data_center.cli"]
    assert command[command.index("--config") + 1] == str(settings.config_path)
    assert "daily-pipeline" in command
    assert "--watch" in command
    assert command[command.index("--symbols") + 1] == "SH600000,SZ000001"
    assert command[command.index("--batch-size") + 1] == "2"
    assert "--control-only" in command
    assert "--no-skip-stock-basic-refresh" in command
    assert "--no-crawl-documents" in command
