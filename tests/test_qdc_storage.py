from __future__ import annotations

import json
import math
import sys
import struct
from datetime import datetime
from types import SimpleNamespace
from pathlib import Path

import pandas as pd

from quant_data_center.cli import _print_json, main
from quant_data_center.console import QdcConsoleData
from quant_data_center.jobs.backfill import parse_date, plan_backfill_tasks
from quant_data_center.settings import QdcSettings
from quant_data_center.storage.database import QdcDatabase
from quant_data_center.storage.schema import CONTROL_TABLES, SILVER_TABLES
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
      - SZ300750
""".strip(),
        encoding="utf-8",
    )
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
    assert preview["rows"][0]["trade_date"] == "2026-05-12"
    assert preview["rows"][0]["close"] == 10.6


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
    assert len(tasks) == 11
    assert {task["status"] for task in tasks} == {"success"}
    assert database.table_counts()["dataset_watermark"] == 7
    job_runs = database.table_counts()["job_run"]
    assert job_runs == 12


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
    assert counts["daily_news_factor"] == 1
    assert counts["daily_announcement_factor"] == 1
    with database.connect() as conn:
        row = conn.execute(
            """
            select n.news_count, a.announcement_count
            from qdc_silver.daily_news_factor n
            join qdc_silver.daily_announcement_factor a
              on n.trade_date = a.trade_date and n.instrument = a.instrument
            """
        ).fetchone()
    assert row == (1.0, 1.0)


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
                "source_id": "unit_test",
            },
            {
                "news_id": "n2",
                "publish_date": "2026-05-11",
                "instrument": "SH600000",
                "title": "公司被立案调查存在退市风险",
                "source_id": "unit_test",
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
                "source_id": "unit_test",
            },
            {
                "announcement_id": "a2",
                "publish_date": "2026-05-11",
                "instrument": "SH600000",
                "title": "年度权益分派公告",
                "source_id": "unit_test",
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
