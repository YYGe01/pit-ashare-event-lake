"""Command line entrypoint for the V0 collection framework."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pitlake.connectors.runner import ConnectorRunner
from pitlake.control.registry import SourceRegistry, assert_valid_control_plane, validate_control_plane
from pitlake.ops.alerts import dispatch_alert
from pitlake.ops.backup import backup_collection_state
from pitlake.quality.reconciliation import ReconciliationReportStore
from pitlake.settings import ProjectSettings
from pitlake.storage.layout import LakeLayout
from pitlake.storage.manifest_store import ManifestStore
from pitlake.storage.metadata_store import MetadataStore
from pitlake.storage.raw_store import RawStore
from pitlake.quality.checks import QualityRunner
from pitlake.quality.report import QualityReportStore
from pitlake.utils import isoformat, read_json


DEFAULT_CONFIG = Path("config/project.yaml")


def load_settings(config_path: str | Path) -> ProjectSettings:
    return ProjectSettings.from_yaml(config_path)


def cmd_init(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    layout = LakeLayout(settings)
    layout.create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    assert_valid_control_plane(settings.config_dir)
    print(
        json.dumps(
            {
                "status": "ok",
                "message": "pitlake initialized",
                "settings": settings.as_dict(),
                "created_directories": [str(path) for path in layout.required_directories()],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def cmd_validate_config(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    errors = validate_control_plane(settings.config_dir)
    payload = {"status": "fail" if errors else "ok", "errors": errors}
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 1 if errors else 0


def cmd_manifest(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    metadata = MetadataStore(settings)
    metadata.init_schema()
    manifest = ManifestStore(settings).generate_daily_manifest(
        manifest_date=args.date,
        metadata_store=metadata,
        status=args.status,
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


def cmd_quality_report(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    report = QualityReportStore(settings).generate_daily_report(
        report_date=args.date,
        metadata_store=metadata,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] in {"pass", "fail"} else 1


def cmd_reconcile(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    datasets = None
    if args.datasets:
        datasets = [item.strip() for item in args.datasets.split(",") if item.strip()]
    report = ReconciliationReportStore(settings).generate_daily_report(
        report_date=args.date,
        metadata_store=metadata,
        datasets=datasets,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] in {"pass", "warn", "fail"} else 1


def cmd_alert(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    LakeLayout(settings).create()
    payload = _load_alert_payload(args.payload_json)
    status = str(payload.get("status", "unknown")) if payload else "manual"
    message = args.message or f"pitlake alert: status={status}"
    result = dispatch_alert(
        logs_dir=settings.logs_dir,
        message=message,
        payload=payload,
        webhook_url=args.webhook_url,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    webhook_result = result.get("webhook_result") or {}
    if result.get("webhook_attempted") and not webhook_result.get("ok"):
        return 1
    return 0


def cmd_backup(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    LakeLayout(settings).create()
    target_root = Path(args.target_dir).resolve() if args.target_dir else None
    result = backup_collection_state(
        settings,
        target_root=target_root,
        include_raw=args.include_raw,
    )
    payload = {
        "status": "ok",
        "backup_dir": str(result.backup_dir),
        "copied_files": [str(path) for path in result.copied_files],
        "skipped": result.skipped,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def _load_alert_payload(path: str | None) -> dict[str, object]:
    if not path:
        return {}
    payload = read_json(Path(path))
    if not isinstance(payload, dict):
        raise ValueError("alert payload JSON must be an object")
    return payload


def cmd_smoke_run(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()

    source_id = "pitlake_smoke_test"
    provider_id = "internal"
    logical_dataset = "system_smoke_test"
    run_id = metadata.create_run(
        source_id=source_id,
        provider_id=provider_id,
        logical_dataset=logical_dataset,
        connector_name="SmokeConnector",
        connector_version="0.1.0",
        trigger_type="manual",
    )

    raw = RawStore(settings).put_json(
        source_id=source_id,
        provider_id=provider_id,
        logical_dataset=logical_dataset,
        payload={"message": "pitlake smoke run", "created_at": isoformat(), "run_id": run_id},
        run_id=run_id,
        filename_prefix="smoke",
        metadata={"purpose": "local framework smoke test"},
    )
    metadata.insert_raw_object(raw, status="stored")
    quality_results = QualityRunner().check_raw_write(raw)
    metadata.insert_quality_results(quality_results)
    has_failures = QualityRunner.has_critical_failures(quality_results)
    metadata.finish_run(
        run_id,
        status="failed" if has_failures else "success",
        request_count=1,
        success_count=0 if has_failures else 1,
        error_count=1 if has_failures else 0,
        new_item_count=1 if not has_failures else 0,
    )
    manifest = ManifestStore(settings).generate_daily_manifest(
        manifest_date=raw.stored_at[:10],
        metadata_store=metadata,
        status="partial" if has_failures else "complete",
    )
    print(
        json.dumps(
            {
                "status": "failed" if has_failures else "ok",
                "run_id": run_id,
                "raw_uri": raw.raw_uri,
                "manifest_path": manifest["manifest_path"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 1 if has_failures else 0


def cmd_run_source(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    LakeLayout(settings).create()
    MetadataStore(settings).init_schema()
    options = {
        "start_date": args.start_date,
        "end_date": args.end_date,
        "symbols": args.symbols,
        "limit_symbols": args.limit_symbols,
        "manifest_date": args.manifest_date,
    }
    options = {key: value for key, value in options.items() if value not in (None, "")}
    result = ConnectorRunner(settings).run_source(
        source_id=args.source_id,
        trigger_type=args.trigger_type,
        options=options,
        generate_manifest=not args.no_manifest,
    )
    print(
        json.dumps(
            {
                "status": result.status,
                "run_id": result.run_id,
                "source_id": result.source_id,
                "stats": result.stats.__dict__,
                "manifest_path": result.manifest.get("manifest_path") if result.manifest else None,
                "error_message": result.error_message,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if result.status in {"success", "partial"} else 1


def cmd_run_enabled(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    runner = ConnectorRunner(settings)
    sources = SourceRegistry.load(settings.config_dir).enabled_sources()
    options = {
        "start_date": args.start_date,
        "end_date": args.end_date,
        "limit_symbols": args.limit_symbols,
        "manifest_date": args.manifest_date,
    }
    options = {key: value for key, value in options.items() if value not in (None, "")}
    results = []
    for source in sources:
        result = runner.run_source(
            source_id=source["source_id"],
            trigger_type=args.trigger_type,
            options=options,
            generate_manifest=False,
        )
        results.append(
            {
                "status": result.status,
                "run_id": result.run_id,
                "source_id": result.source_id,
                "stats": result.stats.__dict__,
                "error_message": result.error_message,
            }
        )
    manifest = None
    if not args.no_manifest:
        manifest = ManifestStore(settings).generate_daily_manifest(
            manifest_date=args.manifest_date,
            metadata_store=metadata,
            status="complete" if all(item["status"] == "success" for item in results) else "partial",
        )
    payload = {
        "status": "success" if all(item["status"] == "success" for item in results) else "partial",
        "source_count": len(results),
        "results": results,
        "manifest_path": manifest.get("manifest_path") if manifest else None,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload["status"] in {"success", "partial"} else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pitlake")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Path to project.yaml")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Create local data lake directories/schema")
    init_parser.set_defaults(func=cmd_init)

    validate_parser = subparsers.add_parser("validate-config", help="Validate registry/contracts")
    validate_parser.set_defaults(func=cmd_validate_config)

    manifest_parser = subparsers.add_parser("manifest", help="Generate daily manifest")
    manifest_parser.add_argument("--date", required=True, help="Manifest date, e.g. 2026-04-26")
    manifest_parser.add_argument("--status", default="complete")
    manifest_parser.set_defaults(func=cmd_manifest)

    quality_report_parser = subparsers.add_parser(
        "quality-report", help="Generate a daily local quality report"
    )
    quality_report_parser.add_argument("--date", required=True, help="Report date, e.g. 2026-04-26")
    quality_report_parser.set_defaults(func=cmd_quality_report)

    reconcile_parser = subparsers.add_parser(
        "reconcile", help="Generate a daily cross-source reconciliation report"
    )
    reconcile_parser.add_argument("--date", required=True, help="Report date, e.g. 2026-04-26")
    reconcile_parser.add_argument(
        "--datasets",
        help="Comma-separated logical datasets. Defaults to P0 high-risk datasets.",
    )
    reconcile_parser.set_defaults(func=cmd_reconcile)

    alert_parser = subparsers.add_parser(
        "alert", help="Write local alert and optionally send webhook notification"
    )
    alert_parser.add_argument("--message", help="Alert message")
    alert_parser.add_argument("--payload-json", help="Path to report JSON payload")
    alert_parser.add_argument("--webhook-url", help="Webhook URL; otherwise PITLAKE_ALERT_WEBHOOK_URL")
    alert_parser.set_defaults(func=cmd_alert)

    backup_parser = subparsers.add_parser(
        "backup", help="Back up metadata, manifests, reports, and optionally raw data"
    )
    backup_parser.add_argument("--target-dir", help="Backup root; otherwise config/env default")
    backup_parser.add_argument("--include-raw", action="store_true")
    backup_parser.set_defaults(func=cmd_backup)

    smoke_parser = subparsers.add_parser("smoke-run", help="Run a local no-network smoke test")
    smoke_parser.set_defaults(func=cmd_smoke_run)

    run_parser = subparsers.add_parser("run-source", help="Run one configured source connector")
    run_parser.add_argument("--source-id", required=True)
    run_parser.add_argument("--trigger-type", default="manual")
    run_parser.add_argument("--start-date", help="YYYYMMDD or YYYY-MM-DD")
    run_parser.add_argument("--end-date", help="YYYYMMDD or YYYY-MM-DD")
    run_parser.add_argument("--manifest-date", help="YYYY-MM-DD")
    run_parser.add_argument("--symbols", help="Comma-separated stock symbols")
    run_parser.add_argument("--limit-symbols", type=int)
    run_parser.add_argument("--no-manifest", action="store_true")
    run_parser.set_defaults(func=cmd_run_source)

    run_enabled_parser = subparsers.add_parser(
        "run-enabled", help="Run all enabled source connectors"
    )
    run_enabled_parser.add_argument("--trigger-type", default="manual")
    run_enabled_parser.add_argument("--start-date", help="YYYYMMDD or YYYY-MM-DD")
    run_enabled_parser.add_argument("--end-date", help="YYYYMMDD or YYYY-MM-DD")
    run_enabled_parser.add_argument("--manifest-date", required=True, help="YYYY-MM-DD")
    run_enabled_parser.add_argument("--limit-symbols", type=int)
    run_enabled_parser.add_argument("--no-manifest", action="store_true")
    run_enabled_parser.set_defaults(func=cmd_run_enabled)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except Exception as exc:  # pragma: no cover - CLI guard
        print(f"pitlake error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
