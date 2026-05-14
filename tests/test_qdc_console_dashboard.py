from __future__ import annotations

import json
import sys
from collections import deque
from pathlib import Path

from quant_data_center.cli import (
    DEFAULT_CRAWL_SOURCE_PARALLELISM,
    _daily_pipeline_document_instrument_filter,
    _qlib_provider_stock_instruments,
)
from quant_data_center.console import (
    DailyPipelineProcessManager,
    QdcConsoleData,
    _daily_pipeline_command,
    _progress_state,
)
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
    manifest_path = (
        settings.raw_root
        / "documents"
        / "2026-05-11"
        / "sina_finance_news"
        / "unit-test"
        / "manifest.json"
    )
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        json.dumps(
            {
                "dataset": "news",
                "source_id": "sina_finance_news",
                "partition_value": "2026-05-11",
                "provider_record_count": 10,
                "empty_result_count": 0,
                "duplicate_record_count": 2,
                "parse_failed_count": 1,
                "parsed_unique_record_count": 7,
                "mapped_source_record_count": 3,
                "mapping_failed_count": 4,
            }
        ),
        encoding="utf-8",
    )
    database.insert_source_object(
        dataset="news",
        source_id="sina_finance_news",
        layer="raw_manifest",
        uri=str(manifest_path),
        content_hash="unit-test",
        size_bytes=manifest_path.stat().st_size,
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
    assert source_rows["sina_finance_news"]["provider_record_count"] == 10
    assert source_rows["sina_finance_news"]["duplicate_rate"] == 0.2
    assert source_rows["sina_finance_news"]["parse_failed_rate"] == 0.1
    assert source_rows["sina_finance_news"]["mapping_rate"] == round(3 / 7, 6)
    assert source_rows["nbd_company_news"]["state"] == "manual"
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


def test_qdc_console_builds_crawl_daily_command(tmp_path: Path) -> None:
    settings = QdcSettings.from_yaml(_write_config(tmp_path))

    command = _daily_pipeline_command(
        settings,
        {
            "workflow": "crawl_daily",
            "date": "2026-05-13",
            "source_id": "cninfo_announcement",
            "symbols": " sh600000, sz000001 ",
            "page_size": 20,
            "max_pages": 2,
            "download_pdfs": True,
            "control_only": True,
        },
    )

    assert command[:3] == [sys.executable, "-m", "quant_data_center.cli"]
    assert command[command.index("--config") + 1] == str(settings.config_path)
    assert "crawl-daily" in command
    assert "daily-pipeline" not in command
    assert command[command.index("--source-id") + 1] == "cninfo_announcement"
    assert command[command.index("--symbols") + 1] == "SH600000,SZ000001"
    assert command[command.index("--page-size") + 1] == "20"
    assert command[command.index("--max-pages") + 1] == "2"
    assert "--download-pdfs" in command
    assert "--control-only" in command


def test_qdc_crawl_defaults_to_serial_provider_stock_universe(tmp_path: Path) -> None:
    settings = QdcSettings.from_yaml(_write_config(tmp_path))
    instruments_path = settings.qlib_root / "cn_data" / "instruments" / "all.txt"
    instruments_path.parent.mkdir(parents=True)
    instruments_path.write_text(
        "\n".join(
            [
                "SH000300\t2005-01-04\t2026-05-13",
                "SH600000\t2000-01-04\t2026-05-13",
                "SZ000001\t2000-01-04\t2026-05-13",
                "SZ399300\t2005-01-04\t2026-05-13",
                "BJ430017\t2023-05-31\t2025-09-30",
                "SH600001\t2000-01-04\t2009-12-15",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    assert DEFAULT_CRAWL_SOURCE_PARALLELISM == 1
    assert _qlib_provider_stock_instruments(settings, trade_date="2026-05-13") == [
        "SH600000",
        "SZ000001",
    ]
    instrument_filter, mode = _daily_pipeline_document_instrument_filter(
        settings=settings,
        universe="all_a",
        symbols_arg=None,
        symbols=["SH600000", "SZ000001", "SH600001"],
        all_market=True,
        crawl_date="2026-05-13",
    )

    assert mode == "qlib_provider"
    assert instrument_filter == ["SH600000", "SZ000001"]


def test_qdc_console_can_stop_running_daily_pipeline_process(tmp_path: Path) -> None:
    settings = QdcSettings.from_yaml(_write_config(tmp_path))
    manager = DailyPipelineProcessManager(settings)

    class FakeProcess:
        def __init__(self) -> None:
            self.return_code: int | None = None
            self.terminated = False
            self.killed = False

        def poll(self) -> int | None:
            return self.return_code

        def terminate(self) -> None:
            self.terminated = True
            self.return_code = -15

        def kill(self) -> None:
            self.killed = True
            self.return_code = -9

        def wait(self, timeout: int | None = None) -> int | None:
            return self.return_code

    process = FakeProcess()
    manager._run = {
        "run_id": "unit-test",
        "status": "running",
        "command": ["qdc", "daily-pipeline", "--watch"],
        "start_at": "2026-05-13T21:28:05",
        "end_at": None,
        "return_code": None,
        "stop_requested_at": None,
        "logs": deque(maxlen=10),
        "stdout": deque(maxlen=10),
        "stderr": deque(maxlen=10),
        "process": process,
    }

    payload = manager.stop()

    assert payload["accepted"] is True
    assert process.terminated is True
    assert process.killed is False
    assert payload["run"]["status"] == "stopped"
    assert payload["run"]["return_code"] == -15
    assert payload["run"]["stop_requested_at"]


def test_qdc_progress_state_keeps_incomplete_running_work_out_of_blocked() -> None:
    assert (
        _progress_state(total=999, success=11, failed=2, running=0, pending=984, stale=0)
        == "partial"
    )
    assert _progress_state(total=999, success=11, failed=2, running=1, pending=984, stale=0) == "running"
    assert _progress_state(total=999, success=11, failed=2, running=0, pending=0, stale=0) == "blocked"
    assert _progress_state(total=999, success=11, failed=0, running=1, pending=984, stale=1) == "blocked"
