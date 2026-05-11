"""Command line interface for quant_data_center."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

from quant_data_center.collectors.akshare import AkshareSilverCollector
from quant_data_center.exports.qlib import QlibExporter, QlibProviderVerifier
from quant_data_center.factors import FactorBuilder
from quant_data_center.jobs.backfill import parse_date, parse_symbols, plan_backfill_tasks
from quant_data_center.quality import QualityChecker
from quant_data_center.settings import QdcSettings
from quant_data_center.storage.database import QdcDatabase
from quant_data_center.storage.parquet import QdcParquetSync


REPO_DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "config" / "quant_data_center.yaml"
SUPPORTED_AKSHARE_DATASETS = {
    "stock_basic",
    "trade_calendar",
    "daily_bar",
    "adj_factor",
    "price_limit",
    "trade_status",
    "announcement",
    "news",
}
SYMBOL_BATCH_REQUIRED_DATASETS = {"daily_bar", "adj_factor", "price_limit", "news"}
DAILY_DATASETS = [
    "trade_calendar",
    "daily_bar",
    "adj_factor",
    "price_limit",
    "trade_status",
    "announcement",
    "news",
]


def default_config_path() -> Path:
    env_path = os.environ.get("QDC_CONFIG")
    if env_path:
        return Path(env_path).expanduser()
    if REPO_DEFAULT_CONFIG.exists():
        return REPO_DEFAULT_CONFIG
    return Path("config/quant_data_center.yaml")


def load_settings(config_path: str | Path) -> QdcSettings:
    return QdcSettings.from_yaml(config_path)


def _print_json(payload: dict[str, object]) -> None:
    print(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False))


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


def cmd_init(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    _print_json(
        {
            "status": "ok",
            "message": "quant_data_center initialized",
            "settings": settings.as_dict(),
            "table_counts": database.table_counts(),
        }
    )
    return 0


def cmd_db_info(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    _print_json({"status": "ok", **database.db_info()})
    return 0


def cmd_smoke(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    job_id = database.record_job_run(
        job_type="smoke",
        status="success",
        dataset="system_smoke_test",
        source_id="internal",
        parameters={"purpose": "qdc migration smoke test"},
    )
    _print_json({"status": "ok", "job_id": job_id, **database.db_info()})
    return 0


def cmd_validate_config(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    errors = []
    if settings.database_backend != "duckdb":
        errors.append("runtime.database_backend must be duckdb in current qdc migration")
    if settings.file_format != "parquet":
        errors.append("runtime.file_format must be parquet in current qdc migration")
    for universe, symbols in settings.universes.items():
        if not symbols:
            errors.append(f"universes.{universe}.symbols must not be empty")
    _print_json({"status": "fail" if errors else "ok", "errors": errors})
    return 1 if errors else 0


def cmd_sync_parquet(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    result = QdcParquetSync(settings).sync(layer=args.layer, dataset=args.dataset)
    _print_json({"status": "ok", **result})
    return 0


def cmd_quality(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    result = QualityChecker(settings).run(
        dataset=args.dataset,
        start_date=args.start,
        end_date=args.end,
    )
    _print_json(result)
    return 0 if result["status"] == "ok" else 1


def cmd_export_qlib(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    result = QlibExporter(settings).export(
        provider_uri=args.provider_uri,
        start_date=args.start,
        end_date=args.end,
    )
    _print_json(result)
    return 0


def cmd_verify_qlib(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    instruments = parse_symbols(args.instruments)
    if not instruments:
        instruments = settings.universe_symbols(args.universe)
    if not instruments:
        raise ValueError("verify-qlib requires --instruments or a non-empty --universe")
    result = QlibProviderVerifier(settings).verify(
        provider_uri=args.provider_uri,
        start_date=args.start,
        end_date=args.end,
        instruments=instruments,
        fields=parse_symbols(args.fields),
    )
    _print_json(result)
    return 0 if result["status"] == "ok" else 1


def cmd_build_factors(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    result = FactorBuilder(settings).build(
        factor_set=args.factor_set,
        start_date=args.start,
        end_date=args.end,
    )
    _print_json(result)
    return 0


def cmd_refresh_universe(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    source_id = args.source_id
    if source_id != "akshare" and not source_id.startswith("akshare"):
        raise ValueError(f"unsupported qdc source_id for refresh-universe: {source_id}")
    index_symbol = args.index_symbol or _default_index_symbol(args.universe)
    snapshot_date = args.snapshot_date or date.today().isoformat()
    row_count = AkshareSilverCollector(settings).collect_universe_constituents(
        source_id=source_id,
        universe=args.universe,
        index_symbol=index_symbol,
        snapshot_date=snapshot_date,
    )
    job_id = database.record_job_run(
        job_type="refresh_universe",
        status="success",
        dataset="universe_constituent",
        source_id=source_id,
        universe=args.universe,
        start_date=snapshot_date,
        end_date=snapshot_date,
        parameters={
            "index_symbol": index_symbol,
            "snapshot_date": snapshot_date,
            "row_count": row_count,
        },
    )
    _print_json(
        {
            "status": "ok",
            "job_id": job_id,
            "universe": args.universe,
            "index_symbol": index_symbol,
            "snapshot_date": snapshot_date,
            "row_count": row_count,
        }
    )
    return 0


def cmd_list_universe(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    symbols = database.latest_universe_symbols(args.universe)
    if not symbols:
        symbols = settings.universe_symbols(args.universe)
    _print_json(
        {
            "status": "ok",
            "universe": args.universe,
            "symbol_count": len(symbols),
            "symbols": symbols,
        }
    )
    return 0


def cmd_plan_backfill(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    _validate_backfill_plan_support(
        dataset=args.dataset,
        source_id=args.source_id,
    )
    symbols = _resolve_plan_symbols(
        settings=settings,
        database=database,
        dataset=args.dataset,
        universe=args.universe or "",
        symbols_arg=args.symbols,
    )
    _validate_backfill_plan_symbols(
        dataset=args.dataset,
        symbols=symbols,
    )
    specs = plan_backfill_tasks(
        dataset=args.dataset,
        source_id=args.source_id,
        universe=args.universe or "",
        start_date=parse_date(args.start),
        end_date=parse_date(args.end),
        symbols=symbols,
        batch_size=args.batch_size,
        chunk_days=args.chunk_days,
    )
    planned = []
    inserted_count = 0
    duplicate_count = 0
    for spec in specs:
        task_id, inserted = database.insert_backfill_task(
            dataset=spec.dataset,
            source_id=spec.source_id,
            universe=spec.universe,
            start_date=spec.start_date.isoformat(),
            end_date=spec.end_date.isoformat(),
            symbols=spec.symbols,
        )
        inserted_count += int(inserted)
        duplicate_count += int(not inserted)
        planned.append(
            {
                "task_id": task_id,
                "inserted": inserted,
                "dataset": spec.dataset,
                "source_id": spec.source_id,
                "universe": spec.universe,
                "start_date": spec.start_date.isoformat(),
                "end_date": spec.end_date.isoformat(),
                "symbols": spec.symbols,
            }
        )
    _print_json(
        {
            "status": "ok",
            "planned_count": len(planned),
            "inserted_count": inserted_count,
            "duplicate_count": duplicate_count,
            "tasks": planned,
        }
    )
    return 0


def cmd_list_backfill(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    tasks = database.list_backfill_tasks(
        status=args.status,
        dataset=args.dataset,
        limit=args.limit,
    )
    _print_json({"status": "ok", "task_count": len(tasks), "tasks": tasks})
    return 0


def cmd_run_backfill(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    task_status = "failed" if args.retry_failed else "pending"
    tasks = database.list_backfill_tasks(
        status=task_status,
        dataset=args.dataset,
        limit=args.limit_tasks,
    )
    if not tasks:
        _print_json(
            {
                "status": "ok",
                "message": f"no {task_status} backfill tasks",
                "task_status": task_status,
                "results": [],
            }
        )
        return 0
    results, has_failures = _run_backfill_tasks(
        settings=settings,
        database=database,
        tasks=tasks,
        control_only=bool(args.control_only),
    )
    _print_json(
        {
            "status": "partial" if has_failures else "ok",
            "task_status": task_status,
            "ran_count": len(results),
            "results": results,
        }
    )
    return 1 if has_failures else 0


def cmd_daily(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    run_date = parse_date(args.date).isoformat()
    symbols = _resolve_plan_symbols(
        settings=settings,
        database=database,
        dataset="daily_bar",
        universe=args.universe,
        symbols_arg=args.symbols,
    )
    _validate_backfill_plan_symbols(dataset="daily_bar", symbols=symbols)
    planned = []
    for dataset in DAILY_DATASETS:
        _validate_backfill_plan_support(dataset=dataset, source_id=args.source_id)
        task_symbols = symbols if dataset in SYMBOL_BATCH_REQUIRED_DATASETS else []
        specs = plan_backfill_tasks(
            dataset=dataset,
            source_id=args.source_id,
            universe=args.universe if dataset in SYMBOL_BATCH_REQUIRED_DATASETS else "",
            start_date=parse_date(run_date),
            end_date=parse_date(run_date),
            symbols=task_symbols,
            batch_size=args.batch_size,
            chunk_days=1,
        )
        for spec in specs:
            task_id, inserted = database.insert_backfill_task(
                dataset=spec.dataset,
                source_id=spec.source_id,
                universe=spec.universe,
                start_date=spec.start_date.isoformat(),
                end_date=spec.end_date.isoformat(),
                symbols=spec.symbols,
            )
            planned.append(
                {
                    "task_id": task_id,
                    "inserted": inserted,
                    "dataset": spec.dataset,
                    "symbols": spec.symbols,
                }
            )
    if args.plan_only:
        _print_json({"status": "ok", "date": run_date, "planned": planned, "results": []})
        return 0

    tasks = [
        task
        for task in database.fetch_backfill_tasks_by_ids([str(item["task_id"]) for item in planned])
        if task["status"] in {"pending", "failed"}
    ]
    results, has_failures = _run_backfill_tasks(
        settings=settings,
        database=database,
        tasks=tasks[: args.limit_tasks] if args.limit_tasks else tasks,
        control_only=bool(args.control_only),
    )
    job_id = database.record_job_run(
        job_type="daily",
        status="failed" if has_failures else "success",
        dataset="daily",
        source_id=args.source_id,
        universe=args.universe,
        start_date=run_date,
        end_date=run_date,
        parameters={
            "planned_count": len(planned),
            "ran_count": len(results),
            "control_only": bool(args.control_only),
        },
    )
    _print_json(
        {
            "status": "partial" if has_failures else "ok",
            "job_id": job_id,
            "date": run_date,
            "planned_count": len(planned),
            "ran_count": len(results),
            "planned": planned,
            "results": results,
        }
    )
    return 1 if has_failures else 0


def _run_backfill_tasks(
    *,
    settings: QdcSettings,
    database: QdcDatabase,
    tasks: list[dict[str, object]],
    control_only: bool,
) -> tuple[list[dict[str, object]], bool]:
    results = []
    has_failures = False
    for task in tasks:
        task_id = str(task["task_id"])
        database.mark_backfill_task_running(task_id)
        try:
            if control_only:
                row_count = 0
                job_type = "backfill_control_only"
            else:
                row_count = _run_real_backfill_task(settings=settings, task=task)
                job_type = "backfill"
            job_id = database.record_job_run(
                job_type=job_type,
                status="success",
                dataset=str(task["dataset"]),
                source_id=str(task["source_id"]),
                universe=str(task.get("universe") or ""),
                start_date=str(task["start_date"]),
                end_date=str(task["end_date"]),
                parameters={
                    "task_id": task_id,
                    "symbols": task.get("symbol_batch_json") or [],
                    "control_only": control_only,
                    "row_count": row_count,
                },
            )
            database.finish_backfill_task(task_id=task_id, status="success")
            database.upsert_dataset_watermark(
                dataset=str(task["dataset"]),
                source_id=str(task["source_id"]),
                universe=str(task.get("universe") or ""),
                start_date=str(task["start_date"]),
                end_date=str(task["end_date"]),
                job_id=job_id,
            )
            results.append(
                {"task_id": task_id, "job_id": job_id, "status": "success", "row_count": row_count}
            )
        except Exception as exc:
            has_failures = True
            error_message = str(exc)[:1000]
            job_id = database.record_job_run(
                job_type="backfill",
                status="failed",
                dataset=str(task["dataset"]),
                source_id=str(task["source_id"]),
                universe=str(task.get("universe") or ""),
                start_date=str(task["start_date"]),
                end_date=str(task["end_date"]),
                parameters={"task_id": task_id, "symbols": task.get("symbol_batch_json") or []},
                error_message=error_message,
            )
            database.finish_backfill_task(
                task_id=task_id,
                status="failed",
                last_error=error_message,
            )
            results.append(
                {
                    "task_id": task_id,
                    "job_id": job_id,
                    "status": "failed",
                    "error_message": error_message,
                }
            )
    return results, has_failures


def _run_real_backfill_task(*, settings: QdcSettings, task: dict[str, object]) -> int:
    dataset = str(task["dataset"])
    source_id = str(task["source_id"])
    if not source_id.startswith("akshare") and source_id != "akshare":
        raise ValueError(f"unsupported qdc source_id for real backfill: {source_id}")
    collector = AkshareSilverCollector(settings)
    if dataset == "stock_basic":
        return collector.collect_stock_basic(source_id=source_id)
    if dataset == "trade_calendar":
        return collector.collect_trade_calendar(
            source_id=source_id,
            start_date=str(task["start_date"]),
            end_date=str(task["end_date"]),
        )
    if dataset == "daily_bar":
        return collector.collect_daily_bar(
            source_id=source_id,
            start_date=str(task["start_date"]),
            end_date=str(task["end_date"]),
            instruments=list(task.get("symbol_batch_json") or []),
        )
    if dataset == "adj_factor":
        return collector.collect_adj_factor(
            source_id=source_id,
            start_date=str(task["start_date"]),
            end_date=str(task["end_date"]),
            instruments=list(task.get("symbol_batch_json") or []),
        )
    if dataset == "price_limit":
        return collector.collect_price_limit(
            source_id=source_id,
            start_date=str(task["start_date"]),
            end_date=str(task["end_date"]),
            instruments=list(task.get("symbol_batch_json") or []),
        )
    if dataset == "trade_status":
        return collector.collect_trade_status(
            source_id=source_id,
            start_date=str(task["start_date"]),
            end_date=str(task["end_date"]),
        )
    if dataset == "announcement":
        return collector.collect_announcements(
            source_id=source_id,
            start_date=str(task["start_date"]),
            end_date=str(task["end_date"]),
            instruments=list(task.get("symbol_batch_json") or []),
        )
    if dataset == "news":
        return collector.collect_news(
            source_id=source_id,
            start_date=str(task["start_date"]),
            end_date=str(task["end_date"]),
            instruments=list(task.get("symbol_batch_json") or []),
        )
    raise ValueError(f"unsupported qdc dataset for real backfill: {dataset}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qdc", allow_abbrev=False)
    parser.add_argument(
        "--config",
        default=str(default_config_path()),
        help="Path to qdc yaml config; defaults to QDC_CONFIG or repo config/quant_data_center.yaml",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Create qdc directories and DuckDB schema")
    init_parser.set_defaults(func=cmd_init)

    info_parser = subparsers.add_parser("db-info", help="Show qdc database table counts")
    info_parser.set_defaults(func=cmd_db_info)

    smoke_parser = subparsers.add_parser("smoke", help="Run a no-network qdc smoke check")
    smoke_parser.set_defaults(func=cmd_smoke)

    validate_parser = subparsers.add_parser("validate-config", help="Validate qdc config")
    validate_parser.set_defaults(func=cmd_validate_config)

    sync_parser = subparsers.add_parser(
        "sync-parquet",
        help="Synchronize DuckDB silver tables into derived Parquet layers",
    )
    sync_parser.add_argument("--layer", choices=["all", "silver", "gold"], default="all")
    sync_parser.add_argument("--dataset")
    sync_parser.set_defaults(func=cmd_sync_parquet)

    quality_parser = subparsers.add_parser("quality", help="Run local qdc_silver quality checks")
    quality_parser.add_argument("--dataset")
    quality_parser.add_argument("--start", help="YYYY-MM-DD")
    quality_parser.add_argument("--end", help="YYYY-MM-DD")
    quality_parser.set_defaults(func=cmd_quality)

    export_parser = subparsers.add_parser(
        "export-qlib",
        help="Export daily QDC data into a Qlib-compatible provider directory",
    )
    export_parser.add_argument("--provider-uri")
    export_parser.add_argument("--start", help="YYYY-MM-DD")
    export_parser.add_argument("--end", help="YYYY-MM-DD")
    export_parser.set_defaults(func=cmd_export_qlib)

    verify_qlib_parser = subparsers.add_parser(
        "verify-qlib",
        help="Verify that Qlib can read a QDC exported provider",
    )
    verify_qlib_parser.add_argument("--provider-uri")
    verify_qlib_parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    verify_qlib_parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    verify_qlib_parser.add_argument("--instruments", help="Comma-separated instruments")
    verify_qlib_parser.add_argument("--universe", default="csi300")
    verify_qlib_parser.add_argument(
        "--fields",
        default="$close,$volume,$announcement_count,$news_count",
        help="Comma-separated Qlib fields",
    )
    verify_qlib_parser.set_defaults(func=cmd_verify_qlib)

    factor_parser = subparsers.add_parser(
        "build-factors",
        help="Build deterministic daily factors from QDC silver tables",
    )
    factor_parser.add_argument("--factor-set", required=True)
    factor_parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    factor_parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    factor_parser.set_defaults(func=cmd_build_factors)

    daily_parser = subparsers.add_parser(
        "daily",
        help="Plan and run daily QDC collection tasks for one date",
    )
    daily_parser.add_argument("--date", required=True, help="YYYY-MM-DD or YYYYMMDD")
    daily_parser.add_argument("--universe", default="csi300")
    daily_parser.add_argument("--source-id", default="akshare")
    daily_parser.add_argument("--symbols", help="Comma-separated symbols overriding universe")
    daily_parser.add_argument("--batch-size", type=int, default=50)
    daily_parser.add_argument("--limit-tasks", type=int)
    daily_parser.add_argument("--plan-only", action="store_true")
    daily_parser.add_argument("--control-only", action="store_true")
    daily_parser.set_defaults(func=cmd_daily)

    refresh_universe_parser = subparsers.add_parser(
        "refresh-universe",
        help="Fetch an index constituent snapshot into qdc_silver.universe_constituent",
    )
    refresh_universe_parser.add_argument("--universe", required=True)
    refresh_universe_parser.add_argument("--source-id", default="akshare")
    refresh_universe_parser.add_argument("--index-symbol")
    refresh_universe_parser.add_argument("--snapshot-date", help="YYYY-MM-DD")
    refresh_universe_parser.set_defaults(func=cmd_refresh_universe)

    list_universe_parser = subparsers.add_parser(
        "list-universe",
        help="List latest QDC universe symbols, falling back to config symbols",
    )
    list_universe_parser.add_argument("--universe", required=True)
    list_universe_parser.set_defaults(func=cmd_list_universe)

    plan_parser = subparsers.add_parser("plan-backfill", help="Create resumable backfill tasks")
    plan_parser.add_argument("--dataset", required=True)
    plan_parser.add_argument("--source-id", required=True)
    plan_parser.add_argument("--universe", default="")
    plan_parser.add_argument("--start", required=True, help="YYYY-MM-DD or YYYYMMDD")
    plan_parser.add_argument("--end", required=True, help="YYYY-MM-DD or YYYYMMDD")
    plan_parser.add_argument("--symbols", help="Comma-separated symbols for symbol batching")
    plan_parser.add_argument("--batch-size", type=int, default=0)
    plan_parser.add_argument("--chunk-days", type=int, default=0)
    plan_parser.set_defaults(func=cmd_plan_backfill)

    list_parser = subparsers.add_parser("list-backfill", help="List planned backfill tasks")
    list_parser.add_argument("--dataset")
    list_parser.add_argument("--status")
    list_parser.add_argument("--limit", type=int)
    list_parser.set_defaults(func=cmd_list_backfill)

    run_parser = subparsers.add_parser("run-backfill", help="Run pending backfill tasks")
    run_parser.add_argument("--dataset")
    run_parser.add_argument("--limit-tasks", type=int)
    run_parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Run failed backfill tasks instead of pending tasks",
    )
    run_parser.add_argument(
        "--control-only",
        action="store_true",
        help="Validate task state flow without collecting real data",
    )
    run_parser.set_defaults(func=cmd_run_backfill)

    return parser


def _validate_backfill_plan_support(*, dataset: str, source_id: str) -> None:
    if source_id != "akshare" and not source_id.startswith("akshare"):
        raise ValueError(f"unsupported qdc source_id for plan-backfill: {source_id}")
    if dataset not in SUPPORTED_AKSHARE_DATASETS:
        supported = ", ".join(sorted(SUPPORTED_AKSHARE_DATASETS))
        raise ValueError(f"unsupported qdc dataset for plan-backfill: {dataset}; supported: {supported}")


def _resolve_plan_symbols(
    *,
    settings: QdcSettings,
    database: QdcDatabase,
    dataset: str,
    universe: str,
    symbols_arg: str | None,
) -> list[str]:
    symbols = parse_symbols(symbols_arg)
    if symbols or dataset not in SYMBOL_BATCH_REQUIRED_DATASETS:
        return symbols
    if universe:
        snapshot_symbols = database.latest_universe_symbols(universe)
        if snapshot_symbols:
            return snapshot_symbols
        return settings.universe_symbols(universe)
    return []


def _validate_backfill_plan_symbols(
    *,
    dataset: str,
    symbols: list[str],
) -> None:
    if dataset in SYMBOL_BATCH_REQUIRED_DATASETS and not symbols:
        raise ValueError(
            f"{dataset} backfill requires --symbols or --universe with configured symbols"
        )


def _default_index_symbol(universe: str) -> str:
    defaults = {
        "csi300": "000300",
        "csi500": "000905",
        "csi1000": "000852",
    }
    return defaults.get(universe.lower(), universe)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except Exception as exc:  # pragma: no cover - CLI guard
        print(f"qdc error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
