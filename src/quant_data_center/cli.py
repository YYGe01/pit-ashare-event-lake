"""Command line interface for quant_data_center."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from quant_data_center.collectors.akshare import (
    AkshareSilverCollector,
    EastmoneySilverCollector,
    SinaRealtimeSilverCollector,
)
from quant_data_center.console import run_console
from quant_data_center.crawlers.registry import crawler_source_spec, enabled_daily_source_specs
from quant_data_center.crawlers.sources.cninfo import CninfoAnnouncementCrawler
from quant_data_center.crawlers.sources.eastmoney import EastmoneyRollNewsCrawler
from quant_data_center.crawlers.sources.investor_interaction import (
    CninfoInvestorInteractionCrawler,
)
from quant_data_center.crawlers.sources.nbd import NbdCompanyNewsCrawler
from quant_data_center.crawlers.sources.public_sentiment import EastmoneyPublicSentimentCrawler
from quant_data_center.crawlers.sources.research_report import EastmoneyResearchReportCrawler
from quant_data_center.crawlers.sources.sina import SinaFinanceNewsCrawler
from quant_data_center.crawlers.sources.sse import SseAnnouncementCrawler
from quant_data_center.crawlers.sources.vendor_news import (
    VENDOR_NEWS_SOURCE_IDS,
    VendorNewsCrawler,
)
from quant_data_center.exports.qlib import (
    QlibExporter,
    QlibProviderVerifier,
)
from quant_data_center.factor_engine import build_text_event_classifier
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
FULL_MARKET_UNIVERSES = {"all", "all_a", "ashare", "cn_ashare"}
DAILY_DATASETS = [
    "trade_calendar",
    "daily_bar",
    "adj_factor",
    "price_limit",
    "trade_status",
]
EASTMONEY_DAILY_DATASETS = [
    "daily_bar",
    "adj_factor",
    "price_limit",
    "trade_status",
]
SINA_DAILY_DATASETS = ["trade_calendar", "daily_bar", "adj_factor", "price_limit"]
DEFAULT_CRAWL_SOURCE_PARALLELISM = 1
DEFAULT_CRAWL_REQUEST_TIMEOUT_SECONDS = 30.0
DEFAULT_CRAWL_SOURCE_TIMEOUT_SECONDS = 180.0
DEFAULT_DAILY_TASK_PARALLELISM = 1


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


def _watch_print(enabled: bool, message: str) -> None:
    if enabled:
        print(message, file=sys.stderr)


def _short_symbol_preview(symbols: list[str], *, max_items: int = 3) -> str:
    if not symbols:
        return "-"
    shown = symbols[:max_items]
    suffix = ""
    if len(symbols) > max_items:
        suffix = f"...(+{len(symbols) - max_items})"
    return ",".join(shown) + suffix


def _watch_task_prefix(*, phase: str, index: int, total: int) -> str:
    return f"[{phase}] {index}/{total}"


def _summarize_export_result(payload: dict[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    object_ids = result.pop("object_ids", None)
    if isinstance(object_ids, list):
        result["object_id_count"] = len(object_ids)
        result["object_id_sample"] = object_ids[:5]
    return result


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
    if settings.text_event_classifier.provider not in {"rule", "llm"}:
        errors.append("llm.text_event.provider must be one of: rule, llm")
    if not settings.text_event_classifier.model:
        errors.append("llm.text_event.model must not be empty")
    if settings.text_event_classifier.max_tokens <= 0:
        errors.append("llm.text_event.max_tokens must be greater than 0")
    _print_json({"status": "fail" if errors else "ok", "errors": errors})
    return 1 if errors else 0


def cmd_console(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    run_console(settings, host=args.host, port=args.port)
    return 0


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
        market_name=args.market_name,
    )
    _print_json(_summarize_export_result(result))
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
        expected_latest_date=args.expected_latest_date,
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


def cmd_classify_text_event(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    classifier = build_text_event_classifier(
        args.provider,
        settings=settings.text_event_classifier,
    )
    result = classifier.classify(
        title=args.title,
        body=args.body,
        document_type=args.document_type,
    )
    _print_json({"status": "ok", "classification": result.to_dict()})
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
        watch=False,
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


def cmd_recover_running(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    tasks = database.recover_running_backfill_tasks(
        dataset=args.dataset,
        older_than_minutes=args.older_than_minutes,
        limit=args.limit_tasks,
        reason=args.reason,
    )
    _print_json({"status": "ok", "recovered_count": len(tasks), "tasks": tasks})
    return 0


def cmd_split_backfill(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    result = database.split_backfill_task(task_id=args.task_id, batch_size=args.batch_size)
    _print_json({"status": "ok", **result})
    return 0


def cmd_crawl_plan(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    crawl_date = parse_date(args.date).isoformat()
    spec = crawler_source_spec(args.source_id)
    planned = _plan_crawl_tasks(database=database, source_spec=spec.to_record(), crawl_date=crawl_date)
    run_id = database.record_crawl_run(
        status="success",
        source_id=spec.source_id,
        dataset=spec.dataset,
        crawl_date=crawl_date,
        planned_count=len(planned),
        parameters={"control_only": bool(args.control_only), "command": "crawl-plan"},
    )
    _print_json(
        {
            "status": "ok",
            "run_id": run_id,
            "source_id": spec.source_id,
            "dataset": spec.dataset,
            "date": crawl_date,
            "planned_count": len(planned),
            "inserted_count": sum(1 for item in planned if item["inserted"]),
            "duplicate_count": sum(1 for item in planned if not item["inserted"]),
            "tasks": planned,
        }
    )
    return 0


def cmd_crawl_list(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    tasks = database.list_crawl_tasks(
        status=args.status,
        source_id=args.source_id,
        dataset=args.dataset,
        limit=args.limit,
    )
    _print_json({"status": "ok", "task_count": len(tasks), "tasks": tasks})
    return 0


def cmd_crawl_run(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    if args.source_id:
        database.upsert_crawler_source(crawler_source_spec(args.source_id).to_record())
    task_status = "failed" if args.retry_failed else "pending"
    tasks = database.list_crawl_tasks(
        status=task_status,
        source_id=args.source_id,
        dataset=args.dataset,
        limit=args.limit_tasks,
    )
    if not tasks:
        _print_json(
            {
                "status": "ok",
                "message": f"no {task_status} crawl tasks",
                "task_status": task_status,
                "results": [],
            }
        )
        return 0
    task_dates = {str(task["crawl_date"]) for task in tasks}
    instrument_filter, instrument_filter_mode = _resolve_crawl_document_instrument_filter(
        settings=settings,
        symbols_arg=args.symbols,
        crawl_date=next(iter(task_dates)) if len(task_dates) == 1 else None,
    )
    stock_basic_mapping = _ensure_stock_basic_for_document_mapping(
        settings=settings,
        database=database,
        selected_tasks=tasks,
        instrument_filter_mode=instrument_filter_mode,
        control_only=bool(args.control_only),
    )
    results, has_failures = _run_crawl_tasks(
        settings=settings,
        database=database,
        tasks=tasks,
        control_only=bool(args.control_only),
        page_size=args.page_size,
        max_pages=args.max_pages,
        download_pdfs=not bool(args.skip_pdf_download),
        pdf_limit=args.pdf_limit,
        instrument_filter=instrument_filter,
        parallelism=args.parallel_sources,
        request_timeout_seconds=args.request_timeout_seconds,
        source_timeout_seconds=args.source_timeout_seconds,
        instrument_parallelism=args.instrument_parallelism,
        instrument_limit=args.instrument_limit,
        watch=False,
    )
    run_id = database.record_crawl_run(
        status="failed" if has_failures else "success",
        source_id=args.source_id,
        dataset=args.dataset,
        planned_count=len(tasks),
        success_count=sum(1 for item in results if item["status"] == "success"),
        failed_count=sum(1 for item in results if item["status"] == "failed"),
        document_count=sum(int(item.get("document_count", 0)) for item in results),
        raw_object_count=sum(int(item.get("raw_object_count", 0)) for item in results),
        parameters={
            "control_only": bool(args.control_only),
            "retry_failed": bool(args.retry_failed),
            "task_status": task_status,
            "page_size": args.page_size,
            "max_pages": args.max_pages,
            "download_pdfs": not bool(args.skip_pdf_download),
            "pdf_limit": args.pdf_limit,
            "instrument_filter_mode": instrument_filter_mode,
            "instrument_filter_count": len(instrument_filter or []),
            "instrument_filter_preview": (instrument_filter or [])[:10],
            "stock_basic_mapping": stock_basic_mapping,
            "parallel_sources": args.parallel_sources,
            "instrument_parallelism": args.instrument_parallelism,
            "instrument_limit": args.instrument_limit,
            "request_timeout_seconds": args.request_timeout_seconds,
            "source_timeout_seconds": args.source_timeout_seconds,
        },
    )
    _print_json(
        {
            "status": "partial" if has_failures else "ok",
            "run_id": run_id,
            "task_status": task_status,
            "ran_count": len(results),
            "results": results,
        }
    )
    return 1 if has_failures else 0


def cmd_crawl_daily(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    crawl_date = parse_date(args.date).isoformat() if args.date else _today(settings)
    planned = []
    for spec in enabled_daily_source_specs(args.source_id):
        planned.extend(
            _plan_crawl_tasks(
                database=database,
                source_spec=spec.to_record(),
                crawl_date=crawl_date,
            )
        )
    if args.plan_only:
        _print_json(
            {
                "status": "ok",
                "date": crawl_date,
                "planned_count": len(planned),
                "planned": planned,
                "results": [],
            }
        )
        return 0
    retryable_statuses = {"pending", "failed"} | ({"success"} if args.force else set())
    tasks = [
        task
        for task in database.list_crawl_tasks(source_id=args.source_id)
        if task["crawl_date"] == crawl_date and task["status"] in retryable_statuses
    ]
    selected_tasks = tasks[: args.limit_tasks] if args.limit_tasks else tasks
    instrument_filter, instrument_filter_mode = _resolve_crawl_document_instrument_filter(
        settings=settings,
        symbols_arg=args.symbols,
        crawl_date=crawl_date,
    )
    stock_basic_mapping = _ensure_stock_basic_for_document_mapping(
        settings=settings,
        database=database,
        selected_tasks=selected_tasks,
        instrument_filter_mode=instrument_filter_mode,
        control_only=bool(args.control_only),
    )
    results, has_failures = _run_crawl_tasks(
        settings=settings,
        database=database,
        tasks=selected_tasks,
        control_only=bool(args.control_only),
        page_size=args.page_size,
        max_pages=args.max_pages,
        download_pdfs=not bool(args.skip_pdf_download),
        pdf_limit=args.pdf_limit,
        instrument_filter=instrument_filter,
        parallelism=args.parallel_sources,
        request_timeout_seconds=args.request_timeout_seconds,
        source_timeout_seconds=args.source_timeout_seconds,
        instrument_parallelism=args.instrument_parallelism,
        instrument_limit=args.instrument_limit,
        watch=False,
    )
    status = "partial" if has_failures or len(selected_tasks) < len(tasks) else "ok"
    run_id = database.record_crawl_run(
        status="success" if status == "ok" else "failed",
        source_id=args.source_id,
        crawl_date=crawl_date,
        planned_count=len(planned),
        success_count=sum(1 for item in results if item["status"] == "success"),
        failed_count=sum(1 for item in results if item["status"] == "failed"),
        document_count=sum(int(item.get("document_count", 0)) for item in results),
        raw_object_count=sum(int(item.get("raw_object_count", 0)) for item in results),
        parameters={
            "control_only": bool(args.control_only),
            "force": bool(args.force),
            "ran_count": len(results),
            "remaining_task_count": len(tasks) - len(selected_tasks),
            "selected_task_statuses": sorted(retryable_statuses),
            "page_size": args.page_size,
            "max_pages": args.max_pages,
            "download_pdfs": not bool(args.skip_pdf_download),
            "pdf_limit": args.pdf_limit,
            "instrument_filter_mode": instrument_filter_mode,
            "instrument_filter_count": len(instrument_filter or []),
            "instrument_filter_preview": (instrument_filter or [])[:10],
            "stock_basic_mapping": stock_basic_mapping,
            "parallel_sources": args.parallel_sources,
            "instrument_parallelism": args.instrument_parallelism,
            "instrument_limit": args.instrument_limit,
            "request_timeout_seconds": args.request_timeout_seconds,
            "source_timeout_seconds": args.source_timeout_seconds,
        },
    )
    _print_json(
        {
            "status": status,
            "run_id": run_id,
            "date": crawl_date,
            "planned_count": len(planned),
            "ran_count": len(results),
            "remaining_task_count": len(tasks) - len(selected_tasks),
            "results": results,
        }
    )
    return 1 if status != "ok" else 0


def cmd_crawl_recover_running(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    tasks = database.recover_running_crawl_tasks(
        source_id=args.source_id,
        older_than_minutes=args.older_than_minutes,
        limit=args.limit_tasks,
        reason=args.reason,
    )
    _print_json({"status": "ok", "recovered_count": len(tasks), "tasks": tasks})
    return 0


def _plan_crawl_tasks(
    *,
    database: QdcDatabase,
    source_spec: dict[str, Any],
    crawl_date: str,
) -> list[dict[str, Any]]:
    database.upsert_crawler_source(source_spec)
    partition_key = f"date={crawl_date}"
    request = {
        "source_id": source_spec["source_id"],
        "dataset": source_spec["dataset"],
        "crawl_date": crawl_date,
        "partition_key": partition_key,
        "parser_version": source_spec["parser_version"],
    }
    task_id, inserted = database.insert_crawl_task(
        source_id=str(source_spec["source_id"]),
        dataset=str(source_spec["dataset"]),
        crawl_date=crawl_date,
        partition_key=partition_key,
        request=request,
    )
    return [
        {
            "task_id": task_id,
            "inserted": inserted,
            "source_id": source_spec["source_id"],
            "dataset": source_spec["dataset"],
            "crawl_date": crawl_date,
            "partition_key": partition_key,
        }
    ]


def _run_crawl_tasks(
    *,
    settings: QdcSettings,
    database: QdcDatabase,
    tasks: list[dict[str, Any]],
    control_only: bool,
    page_size: int,
    max_pages: int | None,
    download_pdfs: bool,
    pdf_limit: int | None,
    instrument_filter: list[str] | None = None,
    parallelism: int = DEFAULT_CRAWL_SOURCE_PARALLELISM,
    request_timeout_seconds: float = DEFAULT_CRAWL_REQUEST_TIMEOUT_SECONDS,
    source_timeout_seconds: float | None = DEFAULT_CRAWL_SOURCE_TIMEOUT_SECONDS,
    instrument_parallelism: int | None = None,
    instrument_limit: int | None = None,
    watch: bool = False,
) -> tuple[list[dict[str, Any]], bool]:
    total_tasks = len(tasks)
    if total_tasks == 0:
        return [], False
    max_workers = max(1, min(int(parallelism or 1), total_tasks))
    task_items = list(enumerate(tasks, start=1))
    if max_workers == 1:
        ordered = [
            _run_one_crawl_task(
                settings=settings,
                database=database,
                task=task,
                index=index,
                total_tasks=total_tasks,
                control_only=control_only,
                page_size=page_size,
                max_pages=max_pages,
                download_pdfs=download_pdfs,
                pdf_limit=pdf_limit,
                instrument_filter=instrument_filter,
                request_timeout_seconds=request_timeout_seconds,
                source_timeout_seconds=source_timeout_seconds,
                instrument_parallelism=instrument_parallelism,
                instrument_limit=instrument_limit,
                watch=watch,
            )
            for index, task in task_items
        ]
    else:
        ordered = []
        _watch_print(
            watch,
            f"{_watch_task_prefix(phase='CRAWL', index=0, total=total_tasks)} PARALLEL sources={max_workers} request_timeout={request_timeout_seconds}s source_timeout={source_timeout_seconds}s",
        )
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    _run_one_crawl_task,
                    settings=settings,
                    database=database,
                    task=task,
                    index=index,
                    total_tasks=total_tasks,
                    control_only=control_only,
                    page_size=page_size,
                    max_pages=max_pages,
                    download_pdfs=download_pdfs,
                    pdf_limit=pdf_limit,
                    instrument_filter=instrument_filter,
                    request_timeout_seconds=request_timeout_seconds,
                    source_timeout_seconds=source_timeout_seconds,
                    instrument_parallelism=instrument_parallelism,
                    instrument_limit=instrument_limit,
                    watch=watch,
                )
                for index, task in task_items
            ]
            ordered = [future.result() for future in as_completed(futures)]
    ordered.sort(key=lambda item: item[0])
    results = [item[1] for item in ordered]
    has_failures = any(item["status"] == "failed" for item in results)
    return results, has_failures


def _run_one_crawl_task(
    *,
    settings: QdcSettings,
    database: QdcDatabase,
    task: dict[str, Any],
    index: int,
    total_tasks: int,
    control_only: bool,
    page_size: int,
    max_pages: int | None,
    download_pdfs: bool,
    pdf_limit: int | None,
    instrument_filter: list[str] | None,
    request_timeout_seconds: float,
    source_timeout_seconds: float | None,
    instrument_parallelism: int | None,
    instrument_limit: int | None,
    watch: bool,
) -> tuple[int, dict[str, Any]]:
    task_id = str(task["task_id"])
    source_id = str(task["source_id"])
    dataset = str(task["dataset"])
    crawl_date = str(task["crawl_date"])
    _watch_print(
        watch,
        f"{_watch_task_prefix(phase='CRAWL', index=index, total=total_tasks)} RUNNING task_id={task_id} source={source_id} dataset={dataset} date={crawl_date}",
    )
    database.mark_crawl_task_running(task_id)
    try:
        if control_only:
            result = {"document_count": 0, "raw_object_count": 0}
        else:
            result = _run_real_crawl_task(
                settings=settings,
                task=task,
                page_size=page_size,
                max_pages=max_pages,
                download_pdfs=download_pdfs,
                pdf_limit=pdf_limit,
                instrument_filter=instrument_filter,
                request_timeout_seconds=request_timeout_seconds,
                source_timeout_seconds=source_timeout_seconds,
                instrument_parallelism=instrument_parallelism,
                instrument_limit=instrument_limit,
            )
        database.finish_crawl_task(task_id=task_id, status="success")
        output = {
            "task_id": task_id,
            "source_id": task["source_id"],
            "dataset": task["dataset"],
            "status": "success",
            "document_count": int(result.get("document_count", 0)),
            "raw_object_count": int(result.get("raw_object_count", 0)),
            "provider_record_count": int(result.get("provider_record_count", 0)),
            "pdf_downloaded_count": int(result.get("pdf_downloaded_count", 0)),
            "pdf_failed_count": int(result.get("pdf_failed_count", 0)),
            "pdf_skipped_count": int(result.get("pdf_skipped_count", 0)),
        }
        for optional_key in (
            "instrument_count",
            "instrument_parallelism",
            "instrument_limit",
            "request_count",
            "org_cache_hit_count",
            "org_cache_update_count",
            "org_failure_count",
            "question_failure_count",
        ):
            if optional_key in result:
                output[optional_key] = result[optional_key]
        _watch_print(
            watch,
            f"{_watch_task_prefix(phase='CRAWL', index=index, total=total_tasks)} OK task_id={task_id} docs={int(result.get('document_count', 0))} raws={int(result.get('raw_object_count', 0))}",
        )
        return index, output
    except Exception as exc:
        error_message = str(exc)[:1000]
        database.finish_crawl_task(
            task_id=task_id,
            status="failed",
            last_error=error_message,
        )
        output = {
            "task_id": task_id,
            "source_id": task["source_id"],
            "dataset": task["dataset"],
            "status": "failed",
            "error_message": error_message,
        }
        _watch_print(
            watch,
            f"{_watch_task_prefix(phase='CRAWL', index=index, total=total_tasks)} FAIL task_id={task_id} error={error_message[:300]}",
        )
        return index, output


def _run_real_crawl_task(
    *,
    settings: QdcSettings,
    task: dict[str, Any],
    page_size: int,
    max_pages: int | None,
    download_pdfs: bool,
    pdf_limit: int | None,
    instrument_filter: list[str] | None = None,
    request_timeout_seconds: float = DEFAULT_CRAWL_REQUEST_TIMEOUT_SECONDS,
    source_timeout_seconds: float | None = DEFAULT_CRAWL_SOURCE_TIMEOUT_SECONDS,
    instrument_parallelism: int | None = None,
    instrument_limit: int | None = None,
) -> dict[str, Any]:
    source_id = str(task["source_id"])
    if source_id == "cninfo_announcement":
        spec = crawler_source_spec(source_id)
        return CninfoAnnouncementCrawler(settings).crawl_date(
            source_id=source_id,
            crawl_date=str(task["crawl_date"]),
            page_size=page_size,
            max_pages=max_pages,
            min_delay_seconds=spec.min_delay_seconds,
            download_pdfs=download_pdfs,
            pdf_limit=pdf_limit,
            instrument_filter=instrument_filter,
            request_timeout_seconds=request_timeout_seconds,
            source_timeout_seconds=source_timeout_seconds,
        )
    if source_id == "sse_announcement":
        spec = crawler_source_spec(source_id)
        return SseAnnouncementCrawler(settings).crawl_date(
            source_id=source_id,
            crawl_date=str(task["crawl_date"]),
            page_size=page_size,
            max_pages=max_pages,
            min_delay_seconds=spec.min_delay_seconds,
            download_pdfs=download_pdfs,
            pdf_limit=pdf_limit,
            instrument_filter=instrument_filter,
            request_timeout_seconds=request_timeout_seconds,
            source_timeout_seconds=source_timeout_seconds,
        )
    if source_id == "sina_finance_news":
        spec = crawler_source_spec(source_id)
        return SinaFinanceNewsCrawler(settings).crawl_date(
            source_id=source_id,
            crawl_date=str(task["crawl_date"]),
            page_size=page_size,
            max_pages=max_pages,
            min_delay_seconds=spec.min_delay_seconds,
            instrument_filter=instrument_filter,
            request_timeout_seconds=request_timeout_seconds,
            source_timeout_seconds=source_timeout_seconds,
        )
    if source_id == "eastmoney_roll_news":
        spec = crawler_source_spec(source_id)
        return EastmoneyRollNewsCrawler(settings).crawl_date(
            source_id=source_id,
            crawl_date=str(task["crawl_date"]),
            page_size=page_size,
            max_pages=max_pages,
            min_delay_seconds=spec.min_delay_seconds,
            instrument_filter=instrument_filter,
            request_timeout_seconds=request_timeout_seconds,
            source_timeout_seconds=source_timeout_seconds,
        )
    if source_id == "eastmoney_research_report":
        spec = crawler_source_spec(source_id)
        return EastmoneyResearchReportCrawler(settings).crawl_date(
            source_id=source_id,
            crawl_date=str(task["crawl_date"]),
            page_size=page_size,
            max_pages=max_pages,
            min_delay_seconds=spec.min_delay_seconds,
            instrument_filter=instrument_filter,
            request_timeout_seconds=request_timeout_seconds,
            source_timeout_seconds=source_timeout_seconds,
        )
    if source_id == "cninfo_investor_interaction":
        spec = crawler_source_spec(source_id)
        return CninfoInvestorInteractionCrawler(settings).crawl_date(
            source_id=source_id,
            crawl_date=str(task["crawl_date"]),
            page_size=page_size,
            max_pages=max_pages,
            min_delay_seconds=spec.min_delay_seconds,
            instrument_filter=instrument_filter,
            request_timeout_seconds=request_timeout_seconds,
            source_timeout_seconds=source_timeout_seconds,
            instrument_parallelism=instrument_parallelism,
            instrument_limit=instrument_limit,
        )
    if source_id == "eastmoney_public_sentiment":
        spec = crawler_source_spec(source_id)
        return EastmoneyPublicSentimentCrawler(settings).crawl_date(
            source_id=source_id,
            crawl_date=str(task["crawl_date"]),
            page_size=page_size,
            max_pages=max_pages,
            min_delay_seconds=spec.min_delay_seconds,
            instrument_filter=instrument_filter,
            request_timeout_seconds=request_timeout_seconds,
            source_timeout_seconds=source_timeout_seconds,
        )
    if source_id == "nbd_company_news":
        spec = crawler_source_spec(source_id)
        return NbdCompanyNewsCrawler(settings).crawl_date(
            source_id=source_id,
            crawl_date=str(task["crawl_date"]),
            page_size=page_size,
            max_pages=max_pages,
            min_delay_seconds=spec.min_delay_seconds,
            instrument_filter=instrument_filter,
            request_timeout_seconds=request_timeout_seconds,
            source_timeout_seconds=source_timeout_seconds,
        )
    if source_id in VENDOR_NEWS_SOURCE_IDS:
        spec = crawler_source_spec(source_id)
        return VendorNewsCrawler(settings).crawl_date(
            source_id=source_id,
            crawl_date=str(task["crawl_date"]),
            page_size=page_size,
            max_pages=max_pages,
            min_delay_seconds=spec.min_delay_seconds,
            instrument_filter=instrument_filter,
            request_timeout_seconds=request_timeout_seconds,
            source_timeout_seconds=source_timeout_seconds,
        )
    raise ValueError(f"unsupported real crawler source_id: {source_id}")


def _plan_daily_tasks(
    *,
    database: QdcDatabase,
    source_ids: list[str],
    universe: str,
    run_date: str,
    symbols: list[str],
    batch_size: int,
) -> list[dict[str, Any]]:
    planned = []
    for source_id in source_ids:
        for dataset in _daily_datasets_for_source(source_id):
            _validate_backfill_plan_support(dataset=dataset, source_id=source_id)
            task_symbols = symbols if dataset in SYMBOL_BATCH_REQUIRED_DATASETS else []
            specs = plan_backfill_tasks(
                dataset=dataset,
                source_id=source_id,
                universe=universe if dataset in SYMBOL_BATCH_REQUIRED_DATASETS else "",
                start_date=parse_date(run_date),
                end_date=parse_date(run_date),
                symbols=task_symbols,
                batch_size=batch_size,
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
                        "source_id": spec.source_id,
                        "symbols": spec.symbols,
                    }
                )
    return planned


def _daily_datasets_for_source(source_id: str) -> list[str]:
    if source_id == "akshare" or source_id.startswith("akshare"):
        return list(DAILY_DATASETS)
    if source_id == "eastmoney":
        return list(EASTMONEY_DAILY_DATASETS)
    if source_id == "sina":
        return list(SINA_DAILY_DATASETS)
    raise ValueError(f"unsupported qdc daily source_id: {source_id}")


def cmd_daily(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    run_date = parse_date(args.date).isoformat()
    symbols = _resolve_daily_symbols(
        settings=settings,
        database=database,
        universe=args.universe,
        symbols_arg=args.symbols,
        all_market=bool(args.all_market),
        source_id=args.source_id,
        refresh_stock_basic=bool(args.refresh_stock_basic),
        plan_only=False,
    )
    _validate_backfill_plan_symbols(dataset="daily_bar", symbols=symbols)
    universe = _daily_task_universe(args.universe, all_market=bool(args.all_market))
    planned = _plan_daily_tasks(
        database=database,
        source_ids=[args.source_id],
        universe=universe,
        run_date=run_date,
        symbols=symbols,
        batch_size=args.batch_size,
    )
    if args.watch:
        _watch_print(
            True,
            f"{_watch_task_prefix(phase='daily', index=1, total=1)} PLAN date={run_date} dataset_count={len(set(task['dataset'] for task in planned)) if planned else 0} task_count={len(planned)}",
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
        watch=bool(args.watch),
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


def _daily_pipeline_option(
    args: argparse.Namespace,
    settings: QdcSettings,
    name: str,
    fallback: Any = None,
) -> Any:
    cli_value = getattr(args, name)
    if cli_value is not None:
        return cli_value
    config_value = getattr(settings.daily_pipeline, name)
    if config_value is not None:
        return config_value
    return fallback


def _daily_pipeline_source_ids(
    *,
    args: argparse.Namespace,
    settings: QdcSettings,
    fallback: str,
) -> list[str]:
    raw_cli = getattr(args, "source_ids", None)
    if raw_cli:
        return parse_symbols(raw_cli)
    if getattr(args, "source_id", None) is not None:
        return [fallback]
    if settings.daily_pipeline.source_ids:
        return list(settings.daily_pipeline.source_ids)
    return [fallback]


def cmd_daily_pipeline(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    database = QdcDatabase(settings)
    database.init_schema()
    run_date = parse_date(args.date).isoformat() if args.date else _today(settings)
    pipeline_universe = _daily_pipeline_option(args, settings, "universe", "all_a")
    source_id = _daily_pipeline_option(args, settings, "source_id", "akshare")
    source_ids = _daily_pipeline_source_ids(args=args, settings=settings, fallback=source_id)
    all_market = bool(
        _daily_pipeline_option(args, settings, "all_market", False)
    ) or _is_full_market_universe(pipeline_universe)
    skip_stock_basic_refresh = bool(
        _daily_pipeline_option(args, settings, "skip_stock_basic_refresh", False)
    )
    batch_size = int(_daily_pipeline_option(args, settings, "batch_size", 50))
    limit_tasks = _daily_pipeline_option(args, settings, "limit_tasks")
    daily_parallelism = int(
        _daily_pipeline_option(
            args,
            settings,
            "daily_parallelism",
            DEFAULT_DAILY_TASK_PARALLELISM,
        )
    )
    provider_uri = _daily_pipeline_option(args, settings, "provider_uri")
    export_start = _daily_pipeline_option(args, settings, "export_start", run_date)
    market_name = _daily_pipeline_option(args, settings, "market_name")
    continue_on_failure = bool(
        _daily_pipeline_option(args, settings, "continue_on_failure", False)
    )
    crawl_documents = bool(_daily_pipeline_option(args, settings, "crawl_documents", False))
    crawl_source_id = _daily_pipeline_option(args, settings, "crawl_source_id")
    crawl_limit_tasks = _daily_pipeline_option(args, settings, "crawl_limit_tasks")
    crawl_page_size = int(_daily_pipeline_option(args, settings, "crawl_page_size", 30))
    crawl_max_pages = _daily_pipeline_option(args, settings, "crawl_max_pages")
    crawl_pdf_limit = _daily_pipeline_option(args, settings, "crawl_pdf_limit")
    crawl_parallelism = int(
        _daily_pipeline_option(
            args,
            settings,
            "crawl_parallelism",
            DEFAULT_CRAWL_SOURCE_PARALLELISM,
        )
    )
    crawl_request_timeout_seconds = float(
        _daily_pipeline_option(
            args,
            settings,
            "crawl_request_timeout_seconds",
            DEFAULT_CRAWL_REQUEST_TIMEOUT_SECONDS,
        )
    )
    crawl_source_timeout_seconds = _daily_pipeline_option(
        args,
        settings,
        "crawl_source_timeout_seconds",
        DEFAULT_CRAWL_SOURCE_TIMEOUT_SECONDS,
    )
    if crawl_source_timeout_seconds is not None:
        crawl_source_timeout_seconds = float(crawl_source_timeout_seconds)
    skip_crawl_pdf_download = bool(
        _daily_pipeline_option(args, settings, "skip_crawl_pdf_download", True)
    )
    skip_factors = bool(_daily_pipeline_option(args, settings, "skip_factors", False))
    skip_sync = bool(_daily_pipeline_option(args, settings, "skip_sync", False))
    skip_quality = bool(_daily_pipeline_option(args, settings, "skip_quality", False))
    skip_export = bool(_daily_pipeline_option(args, settings, "skip_export", False))

    universe = _daily_task_universe(pipeline_universe, all_market=all_market)
    symbols = _resolve_daily_symbols(
        settings=settings,
        database=database,
        universe=pipeline_universe,
        symbols_arg=args.symbols,
        all_market=all_market,
        source_id=source_id,
        refresh_stock_basic=all_market and not skip_stock_basic_refresh,
        plan_only=bool(args.plan_only),
    )
    _validate_backfill_plan_symbols(dataset="daily_bar", symbols=symbols)
    crawl_instrument_filter, crawl_instrument_filter_mode = (
        _daily_pipeline_document_instrument_filter(
            settings=settings,
            universe=pipeline_universe,
            symbols_arg=args.symbols,
            symbols=symbols,
            all_market=all_market,
            crawl_date=run_date,
        )
    )

    steps: list[dict[str, Any]] = []
    status = "ok"
    planned = _plan_daily_tasks(
        database=database,
        source_ids=source_ids,
        universe=universe,
        run_date=run_date,
        symbols=symbols,
        batch_size=batch_size,
    )
    tasks = [
        task
        for task in database.fetch_backfill_tasks_by_ids([str(item["task_id"]) for item in planned])
        if task["status"] in {"pending", "failed"}
    ]
    selected_tasks = tasks[:limit_tasks] if limit_tasks else tasks
    has_incomplete_tasks = len(selected_tasks) < len(tasks)
    if args.watch:
        _watch_print(
            True,
            f"{_watch_task_prefix(phase='pipeline', index=1, total=1)} START date={run_date} universe={universe} planned_tasks={len(planned)} runnable_tasks={len(tasks)} selected_tasks={len(selected_tasks)}",
        )
    if args.plan_only:
        _print_json(
            {
                "status": "ok",
                "date": run_date,
                "universe": universe,
                "symbol_count": len(symbols),
                "planned_count": len(planned),
                "planned": planned,
                "results": [],
            }
        )
        return 0

    daily_results, _has_failures = _run_backfill_tasks(
        settings=settings,
        database=database,
        tasks=selected_tasks,
        control_only=bool(args.control_only),
        parallelism=daily_parallelism,
        watch=bool(args.watch),
    )
    daily_exhausted_units = _backfill_exhausted_units(
        selected_tasks=selected_tasks,
        results=daily_results,
    )
    daily_step_status = "partial" if has_incomplete_tasks or daily_exhausted_units else "ok"
    if args.watch:
        _watch_print(
            True,
            f"{_watch_task_prefix(phase='pipeline', index=1, total=1)} END step=daily status={daily_step_status} ran={len(daily_results)}/{len(selected_tasks)}",
        )
    steps.append(
        {
            "step": "daily",
            "status": daily_step_status,
            "planned_count": len(planned),
            "ran_count": len(daily_results),
            "remaining_task_count": len(tasks) - len(selected_tasks),
            "failed_count": sum(1 for item in daily_results if item["status"] == "failed"),
            "exhausted_units": daily_exhausted_units,
            "results": daily_results,
        }
    )
    if daily_step_status != "ok":
        status = "partial"

    should_continue = daily_step_status == "ok" or continue_on_failure
    crawl_result: dict[str, Any] | None = None
    if should_continue and crawl_documents:
        if args.watch:
            _watch_print(
                True,
                f"{_watch_task_prefix(phase='pipeline', index=2, total=2)} START step=crawl_documents",
            )
        crawl_result = _run_daily_pipeline_crawl_documents(
            settings=settings,
            database=database,
            run_date=run_date,
            source_id=crawl_source_id,
            limit_tasks=crawl_limit_tasks,
            control_only=bool(args.control_only),
            page_size=crawl_page_size,
            max_pages=crawl_max_pages,
            download_pdfs=not skip_crawl_pdf_download,
            pdf_limit=crawl_pdf_limit,
            instrument_filter=crawl_instrument_filter,
            instrument_filter_mode=crawl_instrument_filter_mode,
            parallelism=crawl_parallelism,
            request_timeout_seconds=crawl_request_timeout_seconds,
            source_timeout_seconds=crawl_source_timeout_seconds,
            watch=bool(args.watch),
        )
        steps.append({"step": "crawl_documents", **crawl_result})
        if args.watch:
            _watch_print(
                True,
                f"{_watch_task_prefix(phase='pipeline', index=2, total=2)} END step=crawl_documents status={crawl_result['status']} ran={crawl_result['ran_count']}/{crawl_result['planned_count']}",
            )
        if crawl_result["status"] != "ok":
            status = "partial"
            should_continue = continue_on_failure

    if should_continue and not args.control_only and not skip_factors:
        if args.watch:
            _watch_print(
                True,
                f"{_watch_task_prefix(phase='pipeline', index=3, total=2)} START step=build_factors",
            )
        factor_result = FactorBuilder(settings).build(
            factor_set="all",
            start_date=run_date,
            end_date=run_date,
        )
        steps.append({"step": "build_factors", **factor_result})
        if args.watch:
            _watch_print(
                True,
                f"{_watch_task_prefix(phase='pipeline', index=3, total=2)} END step=build_factors status={factor_result.get('status', 'unknown')}",
            )

    if should_continue and not args.control_only and not skip_sync:
        if args.watch:
            _watch_print(
                True,
                f"{_watch_task_prefix(phase='pipeline', index=4, total=2)} START step=sync_parquet",
            )
        sync_result = QdcParquetSync(settings).sync(layer="all")
        steps.append({"step": "sync_parquet", "status": "ok", **sync_result})
        if args.watch:
            _watch_print(
                True,
                f"{_watch_task_prefix(phase='pipeline', index=4, total=2)} END step=sync_parquet status={sync_result.get('status', 'unknown')}",
            )

    quality_result: dict[str, Any] | None = None
    if should_continue and not args.control_only and not skip_quality:
        if args.watch:
            _watch_print(
                True,
                f"{_watch_task_prefix(phase='pipeline', index=5, total=2)} START step=quality",
            )
        quality_result = QualityChecker(settings).run(start_date=run_date, end_date=run_date)
        steps.append({"step": "quality", **quality_result})
        if args.watch:
            _watch_print(
                True,
                f"{_watch_task_prefix(phase='pipeline', index=5, total=2)} END step=quality status={quality_result.get('status', 'unknown')}",
            )
        if quality_result["status"] != "ok":
            status = "partial"
            should_continue = continue_on_failure

    if should_continue and not args.control_only and not skip_export:
        if args.watch:
            _watch_print(
                True,
                f"{_watch_task_prefix(phase='pipeline', index=6, total=2)} START step=export_qlib",
            )
        export_result = QlibExporter(settings).export(
            provider_uri=provider_uri,
            start_date=export_start,
            end_date=run_date,
            market_name=market_name or universe,
        )
        summarized_export = _summarize_export_result(export_result)
        steps.append({"step": "export_qlib", **summarized_export})
        if args.watch:
            _watch_print(
                True,
                f"{_watch_task_prefix(phase='pipeline', index=6, total=2)} END step=export_qlib status={summarized_export.get('status', 'unknown')} object_id_count={summarized_export.get('object_id_count', 0)}",
            )

    job_id = database.record_job_run(
        job_type="daily_pipeline",
        status="success" if status == "ok" else "failed",
        dataset="daily_pipeline",
        source_id=source_id,
        universe=universe,
        start_date=run_date,
        end_date=run_date,
        parameters={
            "all_market": all_market,
            "source_ids": source_ids,
            "symbol_count": len(symbols),
            "planned_count": len(planned),
            "ran_count": len(daily_results),
            "control_only": bool(args.control_only),
            "daily_parallelism": daily_parallelism,
            "crawl_documents": crawl_documents,
            "crawl_status": crawl_result["status"] if crawl_result else None,
            "crawl_planned_count": crawl_result["planned_count"] if crawl_result else 0,
            "crawl_ran_count": crawl_result["ran_count"] if crawl_result else 0,
            "crawl_failed_count": crawl_result["failed_count"] if crawl_result else 0,
            "crawl_parallel_sources": crawl_parallelism,
            "crawl_request_timeout_seconds": crawl_request_timeout_seconds,
            "crawl_source_timeout_seconds": crawl_source_timeout_seconds,
            "quality_status": quality_result["status"] if quality_result else None,
        },
    )
    _print_json(
        {
            "status": status,
            "job_id": job_id,
            "date": run_date,
            "universe": universe,
            "symbol_count": len(symbols),
            "planned_count": len(planned),
            "ran_count": len(daily_results),
            "steps": steps,
        }
    )
    return 1 if status != "ok" else 0


def _run_daily_pipeline_crawl_documents(
    *,
    settings: QdcSettings,
    database: QdcDatabase,
    run_date: str,
    source_id: str | None,
    limit_tasks: int | None,
    control_only: bool,
    page_size: int,
    max_pages: int | None,
    download_pdfs: bool,
    pdf_limit: int | None,
    instrument_filter: list[str] | None,
    instrument_filter_mode: str,
    parallelism: int,
    request_timeout_seconds: float,
    source_timeout_seconds: float | None,
    watch: bool,
) -> dict[str, Any]:
    _watch_print(
        watch,
        f"{_watch_task_prefix(phase='crawl-documents', index=1, total=1)} START run_date={run_date} source={source_id or 'all'}",
    )
    planned = []
    for spec in enabled_daily_source_specs(source_id):
        planned.extend(
            _plan_crawl_tasks(
                database=database,
                source_spec=spec.to_record(),
                crawl_date=run_date,
            )
        )
    tasks = [
        task
        for task in database.list_crawl_tasks(status="pending", source_id=source_id)
        if task["crawl_date"] == run_date
    ]
    selected_tasks = tasks[:limit_tasks] if limit_tasks else tasks
    results, _has_failures = _run_crawl_tasks(
        settings=settings,
        database=database,
        tasks=selected_tasks,
        control_only=control_only,
        page_size=page_size,
        max_pages=max_pages,
        download_pdfs=download_pdfs,
        pdf_limit=pdf_limit,
        instrument_filter=instrument_filter,
        parallelism=parallelism,
        request_timeout_seconds=request_timeout_seconds,
        source_timeout_seconds=source_timeout_seconds,
        watch=watch,
    )
    remaining_task_count = len(tasks) - len(selected_tasks)
    success_count = sum(1 for item in results if item["status"] == "success")
    failed_count = sum(1 for item in results if item["status"] == "failed")
    exhausted_datasets = _crawl_exhausted_datasets(selected_tasks=selected_tasks, results=results)
    status = "partial" if remaining_task_count or exhausted_datasets else "ok"
    run_id = database.record_crawl_run(
        status="success" if status == "ok" else "failed",
        source_id=source_id,
        crawl_date=run_date,
        planned_count=len(planned),
        success_count=success_count,
        failed_count=failed_count,
        document_count=sum(int(item.get("document_count", 0)) for item in results),
        raw_object_count=sum(int(item.get("raw_object_count", 0)) for item in results),
        parameters={
            "control_only": control_only,
            "command": "daily-pipeline",
            "ran_count": len(results),
            "remaining_task_count": remaining_task_count,
            "source_failure_count": failed_count,
            "exhausted_datasets": exhausted_datasets,
            "page_size": page_size,
            "max_pages": max_pages,
            "download_pdfs": download_pdfs,
            "pdf_limit": pdf_limit,
            "instrument_filter": instrument_filter or [],
            "instrument_filter_mode": instrument_filter_mode,
            "instrument_filter_count": len(instrument_filter or []),
            "instrument_filter_preview": (instrument_filter or [])[:10],
            "parallel_sources": parallelism,
            "request_timeout_seconds": request_timeout_seconds,
            "source_timeout_seconds": source_timeout_seconds,
        },
    )
    return {
        "status": status,
        "run_id": run_id,
        "planned_count": len(planned),
        "ran_count": len(results),
        "remaining_task_count": remaining_task_count,
        "success_count": success_count,
        "failed_count": failed_count,
        "exhausted_datasets": exhausted_datasets,
        "results": results,
    }


def _daily_pipeline_document_instrument_filter(
    *,
    settings: QdcSettings,
    universe: str,
    symbols_arg: str | None,
    symbols: list[str],
    all_market: bool,
    crawl_date: str,
) -> tuple[list[str] | None, str]:
    if parse_symbols(symbols_arg):
        return symbols, "explicit_symbols"
    del settings, crawl_date
    if all_market or _is_full_market_universe(universe):
        return None, "all_market_stock_basic_mapping"
    return symbols, "universe"


def _resolve_crawl_document_instrument_filter(
    *,
    settings: QdcSettings,
    symbols_arg: str | None,
    crawl_date: str | None,
) -> tuple[list[str] | None, str]:
    explicit_symbols = parse_symbols(symbols_arg)
    if explicit_symbols:
        return explicit_symbols, "explicit_symbols"
    del settings, crawl_date
    return None, "stock_basic_mapping"


def _ensure_stock_basic_for_document_mapping(
    *,
    settings: QdcSettings,
    database: QdcDatabase,
    selected_tasks: list[dict[str, Any]],
    instrument_filter_mode: str,
    control_only: bool,
) -> dict[str, Any]:
    if control_only or instrument_filter_mode != "stock_basic_mapping":
        return {"required": False, "refreshed": False}
    if not any(str(task["dataset"]) in {"news", "investor_interaction"} for task in selected_tasks):
        return {"required": False, "refreshed": False}

    active_count = len(database.stock_basic_instruments(active_only=True))
    if active_count:
        return {"required": True, "refreshed": False, "active_count": active_count}

    row_count = AkshareSilverCollector(settings).collect_stock_basic(source_id="akshare")
    active_count = len(database.stock_basic_instruments(active_only=True))
    return {
        "required": True,
        "refreshed": True,
        "row_count": row_count,
        "active_count": active_count,
    }


def _crawl_exhausted_datasets(
    *,
    selected_tasks: list[dict[str, Any]],
    results: list[dict[str, Any]],
) -> list[str]:
    planned_datasets = {str(task["dataset"]) for task in selected_tasks}
    successful_datasets = {
        str(result["dataset"]) for result in results if result.get("status") == "success"
    }
    return sorted(planned_datasets - successful_datasets)


def _backfill_exhausted_units(
    *,
    selected_tasks: list[dict[str, object]],
    results: list[dict[str, object]],
) -> list[dict[str, object]]:
    result_by_task_id = {str(result["task_id"]): result for result in results}
    unit_sources: dict[tuple[str, str, str, tuple[str, ...]], set[str]] = {}
    successful_units: set[tuple[str, str, str, tuple[str, ...]]] = set()
    for task in selected_tasks:
        task_id = str(task["task_id"])
        unit = _backfill_unit_key(task)
        source_id = str(task["source_id"])
        unit_sources.setdefault(unit, set()).add(source_id)
        result = result_by_task_id.get(task_id)
        if result and result.get("status") == "success":
            successful_units.add(unit)
    exhausted = []
    for unit, sources in sorted(unit_sources.items()):
        if unit in successful_units:
            continue
        dataset, start_date, end_date, symbols = unit
        exhausted.append(
            {
                "dataset": dataset,
                "start_date": start_date,
                "end_date": end_date,
                "symbols": list(symbols),
                "source_ids": sorted(sources),
            }
        )
    return exhausted


def _backfill_unit_key(task: dict[str, object]) -> tuple[str, str, str, tuple[str, ...]]:
    symbols = tuple(str(symbol) for symbol in (task.get("symbol_batch_json") or []))
    return (
        str(task["dataset"]),
        str(task["start_date"]),
        str(task["end_date"]),
        symbols,
    )


def _run_backfill_tasks(
    *,
    settings: QdcSettings,
    database: QdcDatabase,
    tasks: list[dict[str, object]],
    control_only: bool,
    parallelism: int = DEFAULT_DAILY_TASK_PARALLELISM,
    watch: bool = False,
) -> tuple[list[dict[str, object]], bool]:
    total_tasks = len(tasks)
    if total_tasks == 0:
        return [], False
    max_workers = max(1, min(int(parallelism or 1), total_tasks))
    task_items = list(enumerate(tasks, start=1))
    if max_workers == 1:
        ordered = [
            _run_one_backfill_task(
                settings=settings,
                database=database,
                task=task,
                index=index,
                total_tasks=total_tasks,
                control_only=control_only,
                watch=watch,
            )
            for index, task in task_items
        ]
    else:
        _watch_print(
            watch,
            f"{_watch_task_prefix(phase='BACKFILL', index=0, total=total_tasks)} PARALLEL tasks={max_workers}",
        )
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    _run_one_backfill_task,
                    settings=settings,
                    database=database,
                    task=task,
                    index=index,
                    total_tasks=total_tasks,
                    control_only=control_only,
                    watch=watch,
                )
                for index, task in task_items
            ]
            ordered = [future.result() for future in as_completed(futures)]
    ordered.sort(key=lambda item: item[0])
    results = [item[1] for item in ordered]
    has_failures = any(item["status"] == "failed" for item in results)
    return results, has_failures


def _run_one_backfill_task(
    *,
    settings: QdcSettings,
    database: QdcDatabase,
    task: dict[str, object],
    index: int,
    total_tasks: int,
    control_only: bool,
    watch: bool,
) -> tuple[int, dict[str, object]]:
    task_id = str(task["task_id"])
    symbols = [str(symbol) for symbol in (task.get("symbol_batch_json") or [])]
    dataset = str(task["dataset"])
    source_id = str(task["source_id"])
    start_date = str(task["start_date"])
    end_date = str(task["end_date"])
    _watch_print(
        watch,
        f"{_watch_task_prefix(phase='BACKFILL', index=index, total=total_tasks)} RUNNING task_id={task_id} "
        f"dataset={dataset} source={source_id} date={start_date}:{end_date} symbols={_short_symbol_preview(symbols)}",
    )
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
        output = {"task_id": task_id, "job_id": job_id, "status": "success", "row_count": row_count}
        _watch_print(
            watch,
            f"{_watch_task_prefix(phase='BACKFILL', index=index, total=total_tasks)} OK task_id={task_id} rows={row_count}",
        )
        return index, output
    except Exception as exc:
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
        output = {
            "task_id": task_id,
            "job_id": job_id,
            "status": "failed",
            "error_message": error_message,
        }
        _watch_print(
            watch,
            f"{_watch_task_prefix(phase='BACKFILL', index=index, total=total_tasks)} FAIL task_id={task_id} error={error_message[:300]}",
        )
        return index, output


def _run_real_backfill_task(*, settings: QdcSettings, task: dict[str, object]) -> int:
    dataset = str(task["dataset"])
    source_id = str(task["source_id"])
    _validate_backfill_plan_support(dataset=dataset, source_id=source_id)
    if source_id == "eastmoney":
        collector = EastmoneySilverCollector(settings)
    elif source_id == "sina":
        if dataset == "trade_calendar":
            collector = AkshareSilverCollector(settings)
        elif dataset in {"daily_bar", "adj_factor", "price_limit"}:
            collector = SinaRealtimeSilverCollector(settings)
        else:
            raise ValueError(f"unsupported qdc dataset for sina real backfill: {dataset}")
    elif source_id == "akshare" or source_id.startswith("akshare"):
        collector = AkshareSilverCollector(settings)
    else:
        raise ValueError(f"unsupported qdc source_id for real backfill: {source_id}")
    if dataset == "stock_basic":
        if not isinstance(collector, AkshareSilverCollector) or source_id in {"eastmoney", "sina"}:
            raise ValueError(f"unsupported qdc dataset for {source_id} real backfill: {dataset}")
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

    console_parser = subparsers.add_parser(
        "console",
        help="Start a read-only local web console for QDC collection state",
    )
    console_parser.add_argument("--host", default="127.0.0.1")
    console_parser.add_argument("--port", type=int, default=8765)
    console_parser.set_defaults(func=cmd_console)

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
    export_parser.add_argument(
        "--market-name",
        help="Optional Qlib instruments market file name, for example csi300 or qdc_smoke",
    )
    export_parser.set_defaults(func=cmd_export_qlib)

    verify_qlib_parser = subparsers.add_parser(
        "verify-qlib",
        help="Verify that Qlib can read the configured base provider",
    )
    verify_qlib_parser.add_argument("--provider-uri")
    verify_qlib_parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    verify_qlib_parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    verify_qlib_parser.add_argument(
        "--expected-latest-date",
        help="Expected latest calendars/day.txt date; defaults to --end",
    )
    verify_qlib_parser.add_argument("--instruments", help="Comma-separated instruments")
    verify_qlib_parser.add_argument("--universe", default="csi300")
    verify_qlib_parser.add_argument(
        "--fields",
        default="$close,$volume,$factor",
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

    classify_parser = subparsers.add_parser(
        "classify-text-event",
        help="Classify one news or announcement title with rule or optional LLM provider",
    )
    classify_parser.add_argument(
        "--provider",
        choices=["rule", "llm"],
        help="Optional provider override; defaults to llm.text_event.provider in config",
    )
    classify_parser.add_argument(
        "--document-type",
        choices=["news", "announcement", "investor_interaction"],
        default="news",
    )
    classify_parser.add_argument("--title", required=True)
    classify_parser.add_argument("--body")
    classify_parser.set_defaults(func=cmd_classify_text_event)

    daily_parser = subparsers.add_parser(
        "daily",
        help="Plan and run daily QDC collection tasks for one date",
    )
    daily_parser.add_argument("--date", required=True, help="YYYY-MM-DD or YYYYMMDD")
    daily_parser.add_argument("--universe", default="csi300")
    daily_parser.add_argument("--source-id", default="akshare")
    daily_parser.add_argument("--symbols", help="Comma-separated symbols overriding universe")
    daily_parser.add_argument(
        "--all-market",
        action="store_true",
        help="Use all active instruments from qdc_silver.stock_basic",
    )
    daily_parser.add_argument(
        "--refresh-stock-basic",
        action="store_true",
        help="Refresh stock_basic before resolving --all-market symbols",
    )
    daily_parser.add_argument("--batch-size", type=int, default=50)
    daily_parser.add_argument("--limit-tasks", type=int)
    daily_parser.add_argument("--plan-only", action="store_true")
    daily_parser.add_argument("--control-only", action="store_true")
    daily_parser.add_argument(
        "--watch",
        action="store_true",
        help="Print per-task execution progress to stderr",
    )
    daily_parser.set_defaults(func=cmd_daily)

    daily_pipeline_parser = subparsers.add_parser(
        "daily-pipeline",
        help="Run the post-close daily QDC collection, factor, quality, and Qlib export pipeline",
    )
    daily_pipeline_parser.add_argument(
        "--date",
        help="YYYY-MM-DD or YYYYMMDD; defaults to today's date in project timezone",
    )
    daily_pipeline_parser.add_argument(
        "--universe",
        help="Universe id; all_a/all/ashare/cn_ashare mean full A-share market",
    )
    daily_pipeline_parser.add_argument("--source-id")
    daily_pipeline_parser.add_argument(
        "--source-ids",
        help="Comma-separated structured daily sources; overrides daily_pipeline.source_ids",
    )
    daily_pipeline_parser.add_argument("--symbols", help="Comma-separated symbols for smoke runs")
    daily_pipeline_parser.add_argument(
        "--all-market",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Force full-market stock_basic symbol resolution",
    )
    daily_pipeline_parser.add_argument(
        "--skip-stock-basic-refresh",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Do not refresh stock_basic before full-market symbol resolution",
    )
    daily_pipeline_parser.add_argument("--batch-size", type=int)
    daily_pipeline_parser.add_argument("--limit-tasks", type=int)
    daily_pipeline_parser.add_argument("--daily-parallelism", type=int)
    daily_pipeline_parser.add_argument("--provider-uri")
    daily_pipeline_parser.add_argument(
        "--export-start",
        help="Optional Qlib export start date; defaults to the daily-pipeline run date",
    )
    daily_pipeline_parser.add_argument(
        "--market-name",
        help="Qlib instruments market file name; defaults to all_a for full market",
    )
    daily_pipeline_parser.add_argument("--plan-only", action="store_true")
    daily_pipeline_parser.add_argument("--control-only", action="store_true")
    daily_pipeline_parser.add_argument(
        "--watch",
        action="store_true",
        help="Print per-stage and per-task execution progress to stderr",
    )
    daily_pipeline_parser.add_argument(
        "--continue-on-failure",
        action=argparse.BooleanOptionalAction,
        default=None,
    )
    daily_pipeline_parser.add_argument(
        "--crawl-documents",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Run enabled daily document crawlers before factor, quality, and Qlib export steps",
    )
    daily_pipeline_parser.add_argument(
        "--crawl-source-id",
        help=(
            "Optional crawler source filter, for example cninfo_announcement, "
            "sse_announcement, eastmoney_roll_news, nbd_company_news, "
            "eastmoney_public_sentiment, sina, wallstreetcn, 10jqka, eastmoney, "
            "yuncaijing, fenghuang, jinrongjie, cls, or yicai"
        ),
    )
    daily_pipeline_parser.add_argument("--crawl-limit-tasks", type=int)
    daily_pipeline_parser.add_argument("--crawl-page-size", type=int)
    daily_pipeline_parser.add_argument("--crawl-max-pages", type=int)
    daily_pipeline_parser.add_argument("--crawl-pdf-limit", type=int)
    daily_pipeline_parser.add_argument("--crawl-parallelism", type=int)
    daily_pipeline_parser.add_argument("--crawl-request-timeout-seconds", type=float)
    daily_pipeline_parser.add_argument("--crawl-source-timeout-seconds", type=float)
    daily_pipeline_parser.add_argument(
        "--skip-crawl-pdf-download",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Only collect announcement metadata during --crawl-documents",
    )
    daily_pipeline_parser.add_argument(
        "--skip-factors", action=argparse.BooleanOptionalAction, default=None
    )
    daily_pipeline_parser.add_argument(
        "--skip-sync", action=argparse.BooleanOptionalAction, default=None
    )
    daily_pipeline_parser.add_argument(
        "--skip-quality", action=argparse.BooleanOptionalAction, default=None
    )
    daily_pipeline_parser.add_argument(
        "--skip-export", action=argparse.BooleanOptionalAction, default=None
    )
    daily_pipeline_parser.set_defaults(func=cmd_daily_pipeline)

    crawl_plan_parser = subparsers.add_parser(
        "crawl-plan",
        help="Create resumable crawler tasks for one source and date",
    )
    crawl_plan_parser.add_argument("--source-id", required=True)
    crawl_plan_parser.add_argument("--date", required=True, help="YYYY-MM-DD or YYYYMMDD")
    crawl_plan_parser.add_argument(
        "--control-only",
        action="store_true",
        help="Keep crawl planning in control-plane mode; real fetchers are not invoked",
    )
    crawl_plan_parser.set_defaults(func=cmd_crawl_plan)

    crawl_list_parser = subparsers.add_parser("crawl-list", help="List crawler tasks")
    crawl_list_parser.add_argument("--source-id")
    crawl_list_parser.add_argument("--dataset")
    crawl_list_parser.add_argument("--status")
    crawl_list_parser.add_argument("--limit", type=int)
    crawl_list_parser.set_defaults(func=cmd_crawl_list)

    crawl_run_parser = subparsers.add_parser("crawl-run", help="Run pending crawler tasks")
    crawl_run_parser.add_argument("--source-id")
    crawl_run_parser.add_argument("--dataset")
    crawl_run_parser.add_argument(
        "--symbols",
        help="Comma-separated instruments; announcement crawlers only persist matching instruments",
    )
    crawl_run_parser.add_argument("--limit-tasks", type=int)
    crawl_run_parser.add_argument("--page-size", type=int, default=30)
    crawl_run_parser.add_argument("--max-pages", type=int)
    crawl_run_parser.add_argument("--pdf-limit", type=int)
    crawl_run_parser.add_argument(
        "--parallel-sources",
        type=int,
        default=DEFAULT_CRAWL_SOURCE_PARALLELISM,
    )
    crawl_run_parser.add_argument(
        "--instrument-parallelism",
        type=int,
        help="Per-source instrument workers for symbol-loop crawlers such as cninfo_investor_interaction",
    )
    crawl_run_parser.add_argument(
        "--instrument-limit",
        type=int,
        help="Limit implicit stock_basic instruments for symbol-loop crawlers; use 0 for all active instruments",
    )
    crawl_run_parser.add_argument(
        "--request-timeout-seconds",
        type=float,
        default=DEFAULT_CRAWL_REQUEST_TIMEOUT_SECONDS,
    )
    crawl_run_parser.add_argument(
        "--source-timeout-seconds",
        type=float,
        default=DEFAULT_CRAWL_SOURCE_TIMEOUT_SECONDS,
    )
    crawl_run_parser.add_argument(
        "--skip-pdf-download",
        dest="skip_pdf_download",
        action="store_true",
        default=True,
        help="Only collect announcement metadata; do not download public PDF files",
    )
    crawl_run_parser.add_argument(
        "--download-pdfs",
        dest="skip_pdf_download",
        action="store_false",
        help="Download public announcement PDF files for this run",
    )
    crawl_run_parser.add_argument("--retry-failed", action="store_true")
    crawl_run_parser.add_argument(
        "--control-only",
        action="store_true",
        help="Validate crawler task state flow without collecting real documents",
    )
    crawl_run_parser.set_defaults(func=cmd_crawl_run)

    crawl_daily_parser = subparsers.add_parser(
        "crawl-daily",
        help="Plan and run daily crawler tasks for enabled document sources",
    )
    crawl_daily_parser.add_argument(
        "--date",
        help="YYYY-MM-DD or YYYYMMDD; defaults to today's date in project timezone",
    )
    crawl_daily_parser.add_argument("--source-id")
    crawl_daily_parser.add_argument(
        "--symbols",
        help="Comma-separated instruments; announcement crawlers only persist matching instruments",
    )
    crawl_daily_parser.add_argument("--limit-tasks", type=int)
    crawl_daily_parser.add_argument("--page-size", type=int, default=30)
    crawl_daily_parser.add_argument("--max-pages", type=int)
    crawl_daily_parser.add_argument("--pdf-limit", type=int)
    crawl_daily_parser.add_argument(
        "--parallel-sources",
        type=int,
        default=DEFAULT_CRAWL_SOURCE_PARALLELISM,
    )
    crawl_daily_parser.add_argument(
        "--instrument-parallelism",
        type=int,
        help="Per-source instrument workers for symbol-loop crawlers such as cninfo_investor_interaction",
    )
    crawl_daily_parser.add_argument(
        "--instrument-limit",
        type=int,
        help="Limit implicit stock_basic instruments for symbol-loop crawlers; use 0 for all active instruments",
    )
    crawl_daily_parser.add_argument(
        "--request-timeout-seconds",
        type=float,
        default=DEFAULT_CRAWL_REQUEST_TIMEOUT_SECONDS,
    )
    crawl_daily_parser.add_argument(
        "--source-timeout-seconds",
        type=float,
        default=DEFAULT_CRAWL_SOURCE_TIMEOUT_SECONDS,
    )
    crawl_daily_parser.add_argument(
        "--skip-pdf-download",
        dest="skip_pdf_download",
        action="store_true",
        default=True,
        help="Only collect announcement metadata; do not download public PDF files",
    )
    crawl_daily_parser.add_argument(
        "--download-pdfs",
        dest="skip_pdf_download",
        action="store_false",
        help="Download public announcement PDF files for this run",
    )
    crawl_daily_parser.add_argument(
        "--force",
        action="store_true",
        help="Rerun existing crawl tasks for this date, including successful tasks",
    )
    crawl_daily_parser.add_argument("--plan-only", action="store_true")
    crawl_daily_parser.add_argument("--control-only", action="store_true")
    crawl_daily_parser.set_defaults(func=cmd_crawl_daily)

    crawl_recover_parser = subparsers.add_parser(
        "crawl-recover-running",
        help="Mark stale running crawler tasks as failed so they can be retried",
    )
    crawl_recover_parser.add_argument("--source-id")
    crawl_recover_parser.add_argument("--older-than-minutes", type=int, default=30)
    crawl_recover_parser.add_argument("--limit-tasks", type=int)
    crawl_recover_parser.add_argument("--reason")
    crawl_recover_parser.set_defaults(func=cmd_crawl_recover_running)

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

    recover_parser = subparsers.add_parser(
        "recover-running",
        help="Mark stale running backfill tasks as failed so they can be retried",
    )
    recover_parser.add_argument("--dataset")
    recover_parser.add_argument("--older-than-minutes", type=int, default=15)
    recover_parser.add_argument("--limit-tasks", type=int)
    recover_parser.add_argument("--reason")
    recover_parser.set_defaults(func=cmd_recover_running)

    split_parser = subparsers.add_parser(
        "split-backfill",
        help="Split one symbol-batched backfill task into smaller pending subtasks",
    )
    split_parser.add_argument("--task-id", required=True)
    split_parser.add_argument("--batch-size", type=int, required=True)
    split_parser.set_defaults(func=cmd_split_backfill)

    return parser


def _validate_backfill_plan_support(*, dataset: str, source_id: str) -> None:
    if source_id == "akshare" or source_id.startswith("akshare"):
        supported = SUPPORTED_AKSHARE_DATASETS
    elif source_id == "eastmoney":
        supported = set(EASTMONEY_DAILY_DATASETS)
    elif source_id == "sina":
        supported = set(SINA_DAILY_DATASETS)
    else:
        raise ValueError(f"unsupported qdc source_id for plan-backfill: {source_id}")
    if dataset not in supported:
        supported_text = ", ".join(sorted(supported))
        raise ValueError(
            f"unsupported qdc dataset for plan-backfill: {dataset}; supported: {supported_text}"
        )


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


def _resolve_daily_symbols(
    *,
    settings: QdcSettings,
    database: QdcDatabase,
    universe: str,
    symbols_arg: str | None,
    all_market: bool,
    source_id: str,
    refresh_stock_basic: bool,
    plan_only: bool,
) -> list[str]:
    symbols = parse_symbols(symbols_arg)
    if symbols:
        return symbols
    if all_market or _is_full_market_universe(universe):
        if refresh_stock_basic and not plan_only:
            AkshareSilverCollector(settings).collect_stock_basic(source_id=source_id)
        symbols = database.stock_basic_instruments(active_only=True)
        if not symbols and (refresh_stock_basic and not plan_only):
            AkshareSilverCollector(settings).collect_stock_basic(source_id=source_id)
            symbols = database.stock_basic_instruments(active_only=True)
        if not symbols:
            if plan_only:
                raise ValueError(
                    "plan-only requires existing stock_basic for full-market universe; "
                    "provide --symbols or run without --plan-only to initialize stock_basic first"
                )
            raise ValueError("all-market daily collection requires non-empty stock_basic")
        return symbols
    return _resolve_plan_symbols(
        settings=settings,
        database=database,
        dataset="daily_bar",
        universe=universe,
        symbols_arg=None,
    )


def _is_full_market_universe(universe: str) -> bool:
    return universe.strip().lower() in FULL_MARKET_UNIVERSES


def _daily_task_universe(universe: str, *, all_market: bool) -> str:
    if all_market or _is_full_market_universe(universe):
        return "all_a"
    return universe


def _today(settings: QdcSettings) -> str:
    try:
        return datetime.now(ZoneInfo(settings.timezone)).date().isoformat()
    except Exception:
        return date.today().isoformat()


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
