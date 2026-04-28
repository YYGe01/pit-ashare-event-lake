"""Read-only data access helpers for the local PitLake console."""

from __future__ import annotations

import json
import sqlite3
from csv import DictWriter
from collections import defaultdict
from io import StringIO
from pathlib import Path
from typing import Any

from pitlake.control.registry import SourceRegistry
from pitlake.control.schedules import SchedulePolicy
from pitlake.settings import ProjectSettings
from pitlake.storage.layout import LakeLayout
from pitlake.storage.metadata_store import MetadataStore

STATUS_RANK = {
    "pass": 0,
    "ok": 0,
    "success": 0,
    "complete": 0,
    "not_expected": 0,
    "warn": 1,
    "partial": 1,
    "missing": 1,
    "fail": 2,
    "failed": 2,
    "error": 2,
}

ISSUE_SEVERITY_RANK = {
    "critical": 0,
    "fail": 0,
    "error": 0,
    "warning": 1,
    "warn": 1,
    "missing": 1,
    "info": 2,
    "pass": 3,
}

DATASET_LABELS = {
    "market_daily_ohlcv": "A股日线 OHLCV",
    "adjustment_factor": "复权因子",
    "trading_calendar": "交易日历",
    "trade_status": "交易状态",
    "price_limit": "涨跌停价格",
    "announcement_index": "公告索引",
    "policy_regulatory_doc": "政策监管文档",
    "commodity_daily": "商品期货日频",
    "global_market_daily": "全球市场日频",
    "financial_indicator": "财务指标",
    "macro_indicator": "宏观指标",
    "capital_flow": "资金行为",
    "fund_holding": "基金持仓",
    "industry_membership": "行业成分",
    "concept_membership": "概念成分",
    "global_event_summary": "全球事件摘要",
    "financial_news": "财经新闻",
    "public_sentiment": "公开热度",
    "weather_daily": "天气日频",
    "market_minute_bar": "A股分钟线样例",
    "research_report_index": "研报索引",
    "social_media_aggregate": "评论聚合",
}

STRUCTURED_DATASETS = {
    "market_daily_ohlcv",
    "market_minute_bar",
    "adjustment_factor",
    "price_limit",
    "commodity_daily",
    "global_market_daily",
    "financial_indicator",
    "macro_indicator",
    "capital_flow",
    "fund_holding",
    "industry_membership",
    "concept_membership",
    "public_sentiment",
    "social_media_aggregate",
    "weather_daily",
}

DOCUMENT_DATASETS = {
    "announcement_index",
    "policy_regulatory_doc",
    "financial_news",
    "global_event_summary",
    "research_report_index",
}

SYMBOL_PAYLOAD_FIELDS = (
    "instrument",
    "symbol",
    "stock_code",
    "security_code",
    "sec_code",
    "code",
)


class PitLakeConsoleData:
    """Build API payloads from local metadata, reports, and registries."""

    def __init__(self, settings: ProjectSettings) -> None:
        self.settings = settings
        self.layout = LakeLayout(settings)
        self.metadata = MetadataStore(settings)

    def overview(self, date: str | None = None) -> dict[str, Any]:
        report_date = date or self.latest_date()
        dates = self.available_dates()
        sources = self._source_configs()
        schedule = self._schedule_by_dataset()
        runs = self._runs_for_day(report_date)
        raw_objects = self._raw_objects_for_day(report_date)
        item_counts = self._item_counts_by_dataset(report_date)
        item_counts_by_source = self._item_counts_by_source(report_date)
        latest_manifest = self._latest_manifest(report_date)
        quality_report = self._latest_quality_report(report_date)
        reconciliation_report = self._latest_reconciliation_report(report_date)
        latest_health = self._latest_source_health()

        source_status = self._source_status_rows(
            report_date=report_date,
            sources=sources,
            runs=runs,
            item_counts_by_source=item_counts_by_source,
            latest_health=latest_health,
        )
        dataset_status = self._dataset_status_rows(
            report_date=report_date,
            sources=sources,
            schedule=schedule,
            runs=runs,
            item_counts=item_counts,
            quality_report=quality_report,
            reconciliation_report=reconciliation_report,
        )
        issues = self._issue_queue(
            report_date=report_date,
            runs=runs,
            source_status=source_status,
            dataset_status=dataset_status,
            quality_report=quality_report,
            reconciliation_report=reconciliation_report,
        )
        overall_status = self._overall_status(
            dataset_status=dataset_status,
            source_status=source_status,
            quality_report=quality_report,
            reconciliation_report=reconciliation_report,
            latest_manifest=latest_manifest,
        )
        return {
            "report_date": report_date,
            "available_dates": dates,
            "status": overall_status,
            "summary": {
                "source_count": len(sources),
                "enabled_source_count": sum(1 for source in sources if source.get("enabled")),
                "run_count": len(runs),
                "failed_run_count": sum(
                    1 for run in runs if self._normalize_status(run["status"]) == "fail"
                ),
                "raw_object_count": len(raw_objects),
                "item_version_count": sum(item_counts.values()),
                "dataset_count": len(dataset_status),
                "dataset_fail_count": sum(
                    1 for row in dataset_status if row["status"] == "fail"
                ),
                "source_fail_count": sum(1 for row in source_status if row["status"] == "fail"),
                "issue_count": len(issues),
                "quality_status": self._report_status(quality_report),
                "reconciliation_status": self._report_status(reconciliation_report),
                "manifest_status": latest_manifest.get("status") if latest_manifest else "missing",
            },
            "latest_manifest": latest_manifest,
            "quality_report": self._report_meta(quality_report),
            "reconciliation_report": self._report_meta(reconciliation_report),
            "issues": issues[:25],
            "datasets": dataset_status,
            "sources": source_status,
            "source_matrix": self.source_matrix(date=report_date, days=7),
            "symbol_universe": self.symbols(date=report_date),
            "recent_runs": self._sort_runs_desc(runs)[:20],
        }

    def datasets(self, date: str | None = None) -> dict[str, Any]:
        report_date = date or self.latest_date()
        overview = self.overview(report_date)
        return {
            "report_date": report_date,
            "datasets": overview["datasets"],
            "available_dates": overview["available_dates"],
        }

    def dataset_detail(
        self,
        logical_dataset: str,
        *,
        date: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        report_date = date or self.latest_date()
        overview = self.overview(report_date)
        dataset_row = next(
            (row for row in overview["datasets"] if row["logical_dataset"] == logical_dataset),
            None,
        )
        sources = [
            source
            for source in self._source_configs()
            if source.get("logical_dataset") == logical_dataset
        ]
        runs = self._runs(logical_dataset=logical_dataset, date=report_date, limit=100)
        raw_objects = self._raw_objects(logical_dataset=logical_dataset, date=report_date, limit=50)
        items = self._items(logical_dataset=logical_dataset, date=report_date, limit=limit)
        quality_rows = self._quality_checks(logical_dataset=logical_dataset, date=report_date)
        quality_findings = self._quality_findings_for_dataset(logical_dataset, report_date)
        reconciliation = self._reconciliation_for_dataset(logical_dataset, report_date)
        return {
            "report_date": report_date,
            "logical_dataset": logical_dataset,
            "label": DATASET_LABELS.get(logical_dataset, logical_dataset),
            "view_type": self._view_type(logical_dataset),
            "summary": dataset_row,
            "sources": sources,
            "runs": runs,
            "items": items,
            "raw_objects": raw_objects,
            "quality_checks": quality_rows,
            "quality_findings": quality_findings,
            "reconciliation": reconciliation,
            "coverage": self.dataset_coverage(logical_dataset, date=report_date, limit=limit),
            "contract": self._contract_payload(logical_dataset),
        }

    def dataset_items(
        self,
        logical_dataset: str,
        *,
        date: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        report_date = date or self.latest_date()
        return {
            "report_date": report_date,
            "logical_dataset": logical_dataset,
            "items": self._items(logical_dataset=logical_dataset, date=report_date, limit=limit),
        }

    def dataset_coverage(
        self,
        logical_dataset: str,
        *,
        date: str | None = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        report_date = date or self.latest_date()
        date_counts = self._fetch_all(
            """
            select substr(stored_at, 1, 10) as date, count(*) as item_count
            from raw_item_version
            where logical_dataset = ?
            group by substr(stored_at, 1, 10)
            order by date desc
            limit 30
            """,
            (logical_dataset,),
        )
        source_counts = self._fetch_all(
            """
            select source_id, count(*) as item_count
            from raw_item_version
            where logical_dataset = ?
              and stored_at like ?
            group by source_id
            order by item_count desc, source_id
            """,
            (logical_dataset, f"{report_date}%"),
        )
        items = self._items(logical_dataset=logical_dataset, date=report_date, limit=limit)
        symbol_counts: dict[str, int] = defaultdict(int)
        for item in items:
            symbol = self._symbol_from_item(item)
            if symbol:
                symbol_counts[symbol] += 1

        expected_symbols = self._dataset_expected_symbols(logical_dataset)
        symbols = sorted(set(symbol_counts) | set(expected_symbols))
        symbol_rows = []
        for symbol in symbols:
            expected_sources = expected_symbols.get(symbol, [])
            item_count = symbol_counts.get(symbol, 0)
            if item_count:
                status = "present"
            elif expected_sources:
                status = "missing"
            else:
                status = "observed"
            symbol_rows.append(
                {
                    "symbol": symbol,
                    "status": status,
                    "item_count": item_count,
                    "expected_source_count": len(expected_sources),
                    "expected_sources": expected_sources,
                }
            )
        return {
            "report_date": report_date,
            "logical_dataset": logical_dataset,
            "coverage_scope": "registry_sample_symbols_only",
            "date_counts": date_counts,
            "source_counts": source_counts,
            "symbol_counts": symbol_rows,
        }

    def dataset_quality(self, logical_dataset: str, *, date: str | None = None) -> dict[str, Any]:
        report_date = date or self.latest_date()
        return {
            "report_date": report_date,
            "logical_dataset": logical_dataset,
            "quality_findings": self._quality_findings_for_dataset(logical_dataset, report_date),
            "quality_checks": self._quality_checks(
                logical_dataset=logical_dataset,
                date=report_date,
            ),
        }

    def dataset_reconciliation(
        self,
        logical_dataset: str,
        *,
        date: str | None = None,
    ) -> dict[str, Any]:
        report_date = date or self.latest_date()
        report = self._latest_reconciliation_report(report_date)
        return {
            "report_date": report_date,
            "logical_dataset": logical_dataset,
            "dataset": self._reconciliation_for_dataset(logical_dataset, report_date),
            "findings": [
                finding
                for finding in (report or {}).get("findings", [])
                if finding.get("logical_dataset") == logical_dataset
            ],
        }

    def sources(self, date: str | None = None) -> dict[str, Any]:
        report_date = date or self.latest_date()
        overview = self.overview(report_date)
        return {
            "report_date": report_date,
            "sources": overview["sources"],
            "available_dates": overview["available_dates"],
        }

    def source_detail(self, source_id: str, *, date: str | None = None) -> dict[str, Any]:
        report_date = date or self.latest_date()
        source_config = self._source_by_id().get(source_id)
        runs = self._runs(source_id=source_id, limit=50)
        day_runs = [run for run in runs if str(run.get("start_at", "")).startswith(report_date)]
        raw_objects = self._raw_objects(source_id=source_id, date=report_date, limit=50)
        items = self._items(source_id=source_id, date=report_date, limit=100)
        quality_rows = self._quality_checks(source_id=source_id, date=report_date)
        health = self._latest_source_health().get(source_id)
        return {
            "report_date": report_date,
            "source_id": source_id,
            "config": source_config,
            "health": health,
            "summary": {
                "run_count_all_time": len(runs),
                "run_count_on_date": len(day_runs),
                "success_run_count_all_time": sum(
                    1 for run in runs if self._normalize_status(run["status"]) == "pass"
                ),
                "failed_run_count_all_time": sum(
                    1 for run in runs if self._normalize_status(run["status"]) == "fail"
                ),
                "new_item_count_on_date": sum(int(run.get("new_item_count") or 0) for run in day_runs),
                "raw_object_count_on_date": len(raw_objects),
                "item_version_count_on_date": len(items),
            },
            "runs": runs,
            "raw_objects": raw_objects,
            "items": items,
            "quality_checks": quality_rows,
        }

    def runs(
        self,
        *,
        date: str | None = None,
        source_id: str | None = None,
        logical_dataset: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        report_date = date or self.latest_date()
        return {
            "report_date": report_date,
            "runs": self._runs(
                date=report_date,
                source_id=source_id,
                logical_dataset=logical_dataset,
                limit=limit,
            ),
        }

    def run_detail(self, run_id: str) -> dict[str, Any]:
        run = self._fetch_one("select * from crawl_run where run_id = ?", (run_id,))
        if not run:
            return {"run_id": run_id, "found": False}
        raw_objects = self._fetch_all(
            "select * from raw_object where run_id = ? order by stored_at",
            (run_id,),
        )
        quality = self._fetch_all(
            "select * from quality_check_result where run_id = ? order by created_at",
            (run_id,),
        )
        item_versions = self._fetch_all(
            """
            select *
            from raw_item_version
            where raw_object_id in (
              select raw_object_id from raw_object where run_id = ?
            )
            order by stored_at
            limit 200
            """,
            (run_id,),
        )
        return {
            "run_id": run_id,
            "found": True,
            "run": run,
            "raw_objects": raw_objects,
            "quality_checks": quality,
            "items": [self._decode_item(item) for item in item_versions],
        }

    def quality_findings(self, date: str | None = None) -> dict[str, Any]:
        report_date = date or self.latest_date()
        report = self._latest_quality_report(report_date)
        checks = self._fetch_all(
            """
            select *
            from quality_check_result
            where created_at like ?
              and status != 'pass'
            order by severity, created_at desc
            limit 300
            """,
            (f"{report_date}%",),
        )
        return {
            "report_date": report_date,
            "report": self._report_meta(report),
            "report_meta": self._report_meta(report),
            "quality_findings": (report or {}).get("quality_findings", []),
            "failed_quality_samples": (report or {}).get("failed_quality_samples", []),
            "warning_quality_samples": (report or {}).get("warning_quality_samples", []),
            "failed_checks": checks,
        }

    def reconciliation(self, date: str | None = None) -> dict[str, Any]:
        report_date = date or self.latest_date()
        report = self._latest_reconciliation_report(report_date)
        return {
            "report_date": report_date,
            "report": report,
            "report_meta": self._report_meta(report),
            "datasets": (report or {}).get("datasets", []),
            "findings": (report or {}).get("findings", []),
        }

    def governance(self, date: str | None = None) -> dict[str, Any]:
        report_date = date or self.latest_date()
        overview = self.overview(report_date)
        quality_report = self._latest_quality_report(report_date)
        reconciliation_report = self._latest_reconciliation_report(report_date)
        source_health = self._source_health_rows(overview["sources"])
        issues = self._governance_issue_rows(
            overview["issues"],
            quality_report=quality_report,
            reconciliation_report=reconciliation_report,
        )
        return {
            "report_date": report_date,
            "available_dates": overview["available_dates"],
            "quality_report": self._report_meta(quality_report),
            "reconciliation_report": self._report_meta(reconciliation_report),
            "phase_status": self.phase_status(),
            "dataset_scores": self._dataset_quality_scores(
                overview["datasets"],
                quality_report=quality_report,
                reconciliation_report=reconciliation_report,
            ),
            "issue_summary": {
                "open_count": len(issues),
                "critical_count": sum(1 for row in issues if row["severity"] in {"critical", "fail"}),
                "warning_count": sum(1 for row in issues if row["severity"] in {"warning", "warn"}),
                "status_flow": "read_only_open_only",
            },
            "issues": issues,
            "volume_baselines": self._volume_baselines(report_date),
            "schema_drift": self._schema_drift_rows(report_date, quality_report),
            "source_health_summary": {
                "source_count": len(source_health),
                "pass_count": sum(1 for row in source_health if row["status"] == "pass"),
                "warn_count": sum(1 for row in source_health if row["status"] == "warn"),
                "fail_count": sum(1 for row in source_health if row["status"] == "fail"),
                "missing_count": sum(1 for row in source_health if row["status"] == "missing"),
            },
            "source_health": source_health,
        }

    def phase_status(self) -> dict[str, Any]:
        """Return the console roadmap status as a machine-readable payload."""

        return {
            "phases": [
                {
                    "phase": 1,
                    "name": "采集观测 MVP",
                    "status": "completed",
                    "completed_capabilities": [
                        "overview",
                        "daily_health",
                        "source_date_matrix",
                        "dataset_health_matrix",
                        "run_detail",
                        "quality_report_view",
                        "reconciliation_report_view",
                    ],
                    "remaining_capabilities": [],
                },
                {
                    "phase": 2,
                    "name": "数据资产和股票 drilldown",
                    "status": "completed",
                    "completed_capabilities": [
                        "dataset_catalog",
                        "dataset_detail",
                        "dataset_coverage",
                        "symbol_detail",
                        "document_feed",
                        "raw_detail",
                        "manifest_view",
                    ],
                    "remaining_capabilities": [],
                },
                {
                    "phase": 3,
                    "name": "质量治理增强",
                    "status": "completed_read_only",
                    "completed_capabilities": [
                        "source_health_display",
                        "volume_baseline",
                        "schema_drift_summary",
                        "dataset_quality_score",
                        "read_only_issue_queue",
                        "alert_artifact_links",
                        "ui_cache_status",
                    ],
                    "remaining_capabilities": [
                        "writable_issue_status_flow_deferred_by_read_only_console_scope",
                    ],
                },
                {
                    "phase": 4,
                    "name": "可选 BI 和全文能力",
                    "status": "completed_local_read_only",
                    "completed_capabilities": [
                        "duckdb_semantic_view_guide",
                        "superset_metabase_connection_guide",
                        "sqlite_like_document_search",
                        "raw_html_text_preview",
                        "raw_pdf_metadata_preview",
                        "json_csv_export_api",
                    ],
                    "remaining_capabilities": [
                        "embedded_pdf_viewer_deferred_until_authorized_fulltext_storage",
                        "external_bi_server_not_started_by_console",
                    ],
                },
            ],
            "scope_note": (
                "PitLake Console remains local and read-only; writable issue transitions, "
                "external BI services, and licensed full-text rendering are intentionally deferred."
            ),
        }

    def tools(self, date: str | None = None) -> dict[str, Any]:
        report_date = date or self.latest_date()
        export_targets = [
            {
                "name": "dataset_items_json",
                "format": "json",
                "endpoint": (
                    "/api/export?kind=dataset_items&format=json"
                    f"&date={report_date}&logical_dataset=market_daily_ohlcv"
                ),
            },
            {
                "name": "dataset_items_csv",
                "format": "csv",
                "endpoint": (
                    "/api/export?kind=dataset_items&format=csv"
                    f"&date={report_date}&logical_dataset=market_daily_ohlcv"
                ),
            },
            {
                "name": "raw_objects_csv",
                "format": "csv",
                "endpoint": f"/api/export?kind=raw_objects&format=csv&date={report_date}",
            },
            {
                "name": "quality_findings_json",
                "format": "json",
                "endpoint": f"/api/export?kind=quality_findings&format=json&date={report_date}",
            },
        ]
        db_path = self.settings.metadata_db.as_posix()
        return {
            "report_date": report_date,
            "phase_status": self.phase_status(),
            "ui_cache": self._ui_cache_status(),
            "alert_artifacts": self._alert_artifacts(),
            "search": {
                "mode": "sqlite_like_document_search",
                "endpoint": "/api/search?q=关键词",
                "searched_fields": [
                    "source registry",
                    "logical_dataset labels",
                    "source_item_key",
                    "title",
                    "source_url",
                    "observed_payload_json",
                    "run_id",
                    "raw_object_id",
                    "content_hash",
                    "manifest_id",
                ],
            },
            "exports": export_targets,
            "bi_guide": {
                "duckdb": [
                    "install sqlite;",
                    "load sqlite;",
                    f"attach '{db_path}' as pitlake (type sqlite);",
                    "select logical_dataset, count(*) as items from pitlake.raw_item_version group by 1;",
                ],
                "superset_or_metabase": (
                    "Use the local SQLite metadata DB as a read-only source for ledger tables. "
                    "Keep raw file previews and PIT evidence links in PitLake Console."
                ),
            },
        }

    def export(
        self,
        *,
        kind: str,
        output_format: str = "json",
        date: str | None = None,
        logical_dataset: str | None = None,
        limit: int = 1000,
    ) -> dict[str, Any]:
        report_date = date or self.latest_date()
        if kind == "dataset_items":
            rows = self._items(
                logical_dataset=logical_dataset,
                date=report_date,
                limit=limit,
            )
            rows = [self._flatten_item_for_export(row) for row in rows]
        elif kind == "raw_objects":
            rows = self._raw_objects(
                date=report_date,
                logical_dataset=logical_dataset,
                limit=limit,
            )
        elif kind == "quality_findings":
            rows = self.quality_findings(report_date).get("quality_findings", [])
        else:
            return {
                "status": "error",
                "message": f"unsupported export kind: {kind}",
                "supported_kinds": ["dataset_items", "raw_objects", "quality_findings"],
            }

        payload: dict[str, Any] = {
            "status": "ok",
            "kind": kind,
            "format": output_format,
            "report_date": report_date,
            "logical_dataset": logical_dataset,
            "row_count": len(rows),
            "rows": rows if output_format == "json" else [],
        }
        if output_format == "csv":
            payload["content_type"] = "text/csv"
            payload["csv"] = self._rows_to_csv(rows)
        elif output_format != "json":
            payload["status"] = "error"
            payload["message"] = f"unsupported export format: {output_format}"
            payload["supported_formats"] = ["json", "csv"]
        return payload

    def raw_objects(
        self,
        *,
        date: str | None = None,
        source_id: str | None = None,
        logical_dataset: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        report_date = date or self.latest_date()
        return {
            "report_date": report_date,
            "raw_objects": self._raw_objects(
                date=report_date,
                source_id=source_id,
                logical_dataset=logical_dataset,
                limit=limit,
            ),
        }

    def raw_detail(self, raw_object_id: str, *, preview_bytes: int = 120_000) -> dict[str, Any]:
        raw = self._fetch_one(
            "select * from raw_object where raw_object_id = ?",
            (raw_object_id,),
        )
        if not raw:
            return {"raw_object_id": raw_object_id, "found": False}
        items = self._fetch_all(
            "select * from raw_item_version where raw_object_id = ? order by stored_at limit 200",
            (raw_object_id,),
        )
        preview = self._raw_preview(raw, preview_bytes=preview_bytes)
        return {
            "raw_object_id": raw_object_id,
            "found": True,
            "raw_object": raw,
            "items": [self._decode_item(item) for item in items],
            "preview": preview,
        }

    def symbols(self, *, date: str | None = None, limit: int = 500) -> dict[str, Any]:
        report_date = date or self.latest_date()
        symbols = self._symbol_universe(report_date, limit=limit)
        return {
            "report_date": report_date,
            "coverage_scope": "registry_sample_symbols_plus_observed_items",
            "symbols": symbols,
        }

    def symbol_detail(
        self,
        symbol: str,
        *,
        date: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        report_date = date or self.latest_date()
        normalized_symbol = self._normalize_symbol(symbol)
        items = self._symbol_items(normalized_symbol, date=report_date, limit=limit)
        raw_objects = self._raw_objects_by_ids(
            [str(item["raw_object_id"]) for item in items if item.get("raw_object_id")]
        )
        quality_checks = self._quality_for_symbol(normalized_symbol, report_date)
        return {
            "report_date": report_date,
            "symbol": symbol,
            "normalized_symbol": normalized_symbol,
            "coverage_scope": "registry_sample_symbols_only",
            "coverage": self._symbol_coverage(normalized_symbol, report_date, items),
            "items": items,
            "raw_objects": raw_objects,
            "quality_checks": quality_checks,
        }

    def source_matrix(self, *, date: str | None = None, days: int = 7) -> dict[str, Any]:
        dates = self.available_dates()
        report_date = date or (dates[0] if dates else "")
        if report_date and report_date not in dates:
            dates.insert(0, report_date)
        selected_dates = [dt for dt in dates if not report_date or dt <= report_date][:days]
        if report_date and report_date not in selected_dates:
            selected_dates = [report_date, *selected_dates][:days]
        selected_dates = sorted(set(selected_dates), reverse=True)

        if not selected_dates:
            return {"dates": [], "rows": []}
        placeholders = ",".join("?" for _ in selected_dates)
        run_rows = self._fetch_all(
            f"""
            select run_id, source_id, logical_dataset, status, start_at, new_item_count, error_count
            from crawl_run
            where substr(start_at, 1, 10) in ({placeholders})
            order by start_at desc
            """,
            tuple(selected_dates),
        )
        runs_by_source_date: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for run in run_rows:
            runs_by_source_date[(run["source_id"], str(run.get("start_at", ""))[:10])].append(run)

        rows = []
        for source in sorted(self._source_configs(), key=lambda item: item["source_id"]):
            cells = []
            for matrix_date in selected_dates:
                day_runs = runs_by_source_date.get((source["source_id"], matrix_date), [])
                if day_runs:
                    status = self._status_from_runs(day_runs)
                elif source.get("enabled"):
                    status = "missing"
                else:
                    status = "not_expected"
                cells.append(
                    {
                        "date": matrix_date,
                        "status": status,
                        "run_count": len(day_runs),
                        "new_item_count": sum(
                            int(run.get("new_item_count") or 0) for run in day_runs
                        ),
                        "error_count": sum(int(run.get("error_count") or 0) for run in day_runs),
                        "run_ids": [run["run_id"] for run in day_runs],
                    }
                )
            rows.append(
                {
                    "source_id": source["source_id"],
                    "logical_dataset": source.get("logical_dataset"),
                    "priority": source.get("priority"),
                    "enabled": bool(source.get("enabled")),
                    "implementation_status": source.get("implementation_status"),
                    "cells": cells,
                }
            )
        return {"dates": selected_dates, "rows": rows}

    def manifests(self, *, limit: int = 100) -> dict[str, Any]:
        rows = self._fetch_all(
            """
            select *
            from collection_manifest
            order by created_at desc
            limit ?
            """,
            (limit,),
        )
        return {"manifests": rows}

    def manifest_detail(self, manifest_id: str) -> dict[str, Any]:
        row = self._fetch_one(
            "select * from collection_manifest where manifest_id = ?",
            (manifest_id,),
        )
        if not row:
            return {"manifest_id": manifest_id, "found": False}
        payload = self._manifest_payload(row)
        return {
            "manifest_id": manifest_id,
            "found": True,
            "manifest": row,
            "payload": payload,
        }

    def search(self, query: str, *, limit: int = 50) -> dict[str, Any]:
        term = query.strip()
        if not term:
            return {"query": query, "results": []}
        like = f"%{term}%"
        results: list[dict[str, Any]] = []
        for source in self._source_configs():
            if term.casefold() in json.dumps(source, ensure_ascii=False).casefold():
                results.append(
                    {
                        "type": "source",
                        "title": source["source_id"],
                        "subtitle": source.get("logical_dataset"),
                        "id": source["source_id"],
                    }
                )
        datasets = sorted({source.get("logical_dataset", "") for source in self._source_configs()})
        for dataset in datasets:
            label = DATASET_LABELS.get(dataset, "")
            if term.casefold() in f"{dataset} {label}".casefold():
                results.append(
                    {
                        "type": "dataset",
                        "title": dataset,
                        "subtitle": label,
                        "id": dataset,
                    }
                )
        item_rows = self._fetch_all(
            """
            select item_version_id, logical_dataset, source_id, source_item_key, title, source_url,
                   source_publish_time, first_seen_at
            from raw_item_version
            where source_item_key like ?
               or coalesce(title, '') like ?
               or coalesce(source_url, '') like ?
               or observed_payload_json like ?
            order by first_seen_at desc
            limit ?
            """,
            (like, like, like, like, limit),
        )
        for row in item_rows:
            results.append(
                {
                    "type": "item",
                    "title": row.get("title") or row["source_item_key"],
                    "subtitle": f"{row['logical_dataset']} / {row['source_id']}",
                    "id": row["item_version_id"],
                    "payload": row,
                }
            )
        run_rows = self._fetch_all(
            """
            select run_id, source_id, logical_dataset, status, start_at
            from crawl_run
            where run_id like ? or source_id like ? or logical_dataset like ?
            order by start_at desc
            limit ?
            """,
            (like, like, like, limit),
        )
        for row in run_rows:
            results.append(
                {
                    "type": "run",
                    "title": row["run_id"],
                    "subtitle": f"{row['source_id']} / {row['status']}",
                    "id": row["run_id"],
                    "payload": row,
                }
            )
        raw_rows = self._fetch_all(
            """
            select raw_object_id, source_id, logical_dataset, content_hash, stored_at
            from raw_object
            where raw_object_id like ?
               or content_hash like ?
               or source_id like ?
               or logical_dataset like ?
            order by stored_at desc
            limit ?
            """,
            (like, like, like, like, limit),
        )
        for row in raw_rows:
            results.append(
                {
                    "type": "raw",
                    "title": row["raw_object_id"],
                    "subtitle": f"{row['logical_dataset']} / {row['source_id']}",
                    "id": row["raw_object_id"],
                    "payload": row,
                }
            )
        manifest_rows = self._fetch_all(
            """
            select manifest_id, manifest_date, status, created_at
            from collection_manifest
            where manifest_id like ? or manifest_date like ? or manifest_hash like ?
            order by created_at desc
            limit ?
            """,
            (like, like, like, limit),
        )
        for row in manifest_rows:
            results.append(
                {
                    "type": "manifest",
                    "title": row["manifest_id"],
                    "subtitle": f"{row['manifest_date']} / {row['status']}",
                    "id": row["manifest_id"],
                    "payload": row,
                }
            )
        symbol_rows = [
            row
            for row in self._symbol_universe(self.latest_date(), limit=limit)
            if term.casefold() in row["symbol"].casefold()
        ]
        for row in symbol_rows:
            results.append(
                {
                    "type": "symbol",
                    "title": row["symbol"],
                    "subtitle": row["scope"],
                    "id": row["symbol"],
                    "payload": row,
                }
            )
        return {"query": query, "results": results[:limit]}

    def _governance_issue_rows(
        self,
        overview_issues: list[dict[str, Any]],
        *,
        quality_report: dict[str, Any] | None,
        reconciliation_report: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        rows = []
        for index, issue in enumerate(overview_issues, start=1):
            rows.append(
                {
                    "issue_id": f"overview-{index}",
                    "status": "open",
                    "status_source": "derived_read_only",
                    "severity": issue.get("severity") or "warning",
                    "kind": issue.get("kind"),
                    "title": issue.get("title") or issue.get("kind"),
                    "detail": issue.get("detail"),
                    "logical_dataset": issue.get("logical_dataset"),
                    "source_id": issue.get("source_id"),
                    "run_id": issue.get("run_id"),
                    "suggested_action": self._suggested_issue_action(issue),
                }
            )
        if quality_report is None:
            rows.append(
                {
                    "issue_id": "quality-report-missing",
                    "status": "open",
                    "status_source": "derived_read_only",
                    "severity": "warning",
                    "kind": "quality_report_missing",
                    "title": "Quality report missing",
                    "detail": "Run pitlake quality-report for this date before relying on quality status.",
                    "logical_dataset": None,
                    "source_id": None,
                    "run_id": None,
                    "suggested_action": "pitlake quality-report --date YYYY-MM-DD",
                }
            )
        if reconciliation_report is None:
            rows.append(
                {
                    "issue_id": "reconciliation-report-missing",
                    "status": "open",
                    "status_source": "derived_read_only",
                    "severity": "warning",
                    "kind": "reconciliation_report_missing",
                    "title": "Reconciliation report missing",
                    "detail": "Run pitlake reconcile for this date to expose counterparty gaps.",
                    "logical_dataset": None,
                    "source_id": None,
                    "run_id": None,
                    "suggested_action": "pitlake reconcile --date YYYY-MM-DD",
                }
            )
        return sorted(
            rows,
            key=lambda row: (
                ISSUE_SEVERITY_RANK.get(str(row.get("severity")).lower(), 2),
                row.get("kind") or "",
            ),
        )

    def _suggested_issue_action(self, issue: dict[str, Any]) -> str:
        kind = str(issue.get("kind") or "")
        if "source" in kind or issue.get("source_id"):
            return "open source detail and inspect latest run/raw evidence"
        if "quality" in kind or "field" in kind or "schema" in kind:
            return "open quality finding and compare observed payload with dataset contract"
        if "reconciliation" in kind or "counterparty" in kind:
            return "open reconciliation report and inspect active/planned counterparty sources"
        if "manifest" in kind:
            return "generate or inspect daily manifest"
        return "inspect linked dataset, run, and raw evidence"

    def _ui_cache_status(self) -> dict[str, Any]:
        cache_path = self.settings.data_lake_root / "collection" / "ui_cache" / "pitlake_ui.sqlite"
        return {
            "status": "present" if cache_path.exists() else "not_configured",
            "path": cache_path.as_posix(),
            "mode": "direct_metadata_reads",
            "note": "Console currently reads SQLite metadata and report JSON directly; cache can be added later.",
        }

    def _alert_artifacts(self) -> dict[str, Any]:
        alert_log = self.settings.logs_dir / "alerts.jsonl"
        return {
            "status": "present" if alert_log.exists() else "missing",
            "path": alert_log.as_posix(),
            "webhook_config": "environment_or_cli_only",
            "note": "No webhook URL is read from git-tracked files or displayed by the console.",
        }

    def _flatten_item_for_export(self, row: dict[str, Any]) -> dict[str, Any]:
        payload = row.get("observed_payload") or {}
        flattened = {
            key: value
            for key, value in row.items()
            if key not in {"observed_payload", "observed_payload_json"}
        }
        for key, value in payload.items():
            export_key = f"payload_{key}"
            if isinstance(value, (dict, list)):
                flattened[export_key] = json.dumps(value, ensure_ascii=False, sort_keys=True)
            else:
                flattened[export_key] = value
        return flattened

    def _rows_to_csv(self, rows: list[dict[str, Any]]) -> str:
        if not rows:
            return ""
        fieldnames = sorted({field for row in rows for field in row})
        handle = StringIO()
        writer = DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    field: json.dumps(value, ensure_ascii=False, sort_keys=True)
                    if isinstance(value, (dict, list))
                    else value
                    for field, value in row.items()
                }
            )
        return handle.getvalue()

    def _symbol_universe(self, date: str, *, limit: int = 500) -> list[dict[str, Any]]:
        registry_symbols: dict[str, set[str]] = defaultdict(set)
        for source in self._source_configs():
            default_options = source.get("default_options") or {}
            for symbol in self._coerce_symbol_list(default_options.get("symbols")):
                normalized = self._normalize_symbol(symbol)
                if normalized:
                    registry_symbols[normalized].add(source.get("logical_dataset", ""))

        observed_symbols: dict[str, set[str]] = defaultdict(set)
        rows = self._fetch_all(
            """
            select logical_dataset, source_item_key, observed_payload_json
            from raw_item_version
            where stored_at like ?
            order by stored_at desc
            limit ?
            """,
            (f"{date}%", max(limit * 20, limit)),
        )
        for row in rows:
            decoded = self._decode_item(row)
            symbol = self._symbol_from_item(decoded)
            if symbol:
                observed_symbols[symbol].add(row.get("logical_dataset", ""))

        symbols = sorted(set(registry_symbols) | set(observed_symbols))
        result = []
        for symbol in symbols[:limit]:
            registry_datasets = sorted(dataset for dataset in registry_symbols.get(symbol, set()) if dataset)
            observed_datasets = sorted(dataset for dataset in observed_symbols.get(symbol, set()) if dataset)
            if registry_datasets and observed_datasets:
                scope = "registry_sample_and_observed"
            elif registry_datasets:
                scope = "registry_sample"
            else:
                scope = "observed_item"
            result.append(
                {
                    "symbol": symbol,
                    "scope": scope,
                    "registry_datasets": registry_datasets,
                    "observed_datasets": observed_datasets,
                }
            )
        return result

    def _symbol_items(self, symbol: str, *, date: str, limit: int) -> list[dict[str, Any]]:
        terms = self._symbol_search_terms(symbol)
        clauses = ["stored_at like ?"]
        params: list[Any] = [f"{date}%"]
        search_clauses = []
        for term in terms:
            like = f"%{term}%"
            search_clauses.append(
                """
                (
                  source_item_key like ?
                  or coalesce(title, '') like ?
                  or coalesce(source_url, '') like ?
                  or observed_payload_json like ?
                )
                """
            )
            params.extend([like, like, like, like])
        clauses.append(f"({' or '.join(search_clauses)})")
        rows = self._fetch_all(
            f"""
            select *
            from raw_item_version
            where {' and '.join(clauses)}
            order by stored_at desc
            limit ?
            """,
            (*params, limit),
        )
        return [self._decode_item(row) for row in rows]

    def _symbol_coverage(
        self,
        symbol: str,
        date: str,
        items: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        expectations = self._symbol_expectations(symbol)
        observed_by_dataset: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in items:
            dataset = item.get("logical_dataset")
            if dataset:
                observed_by_dataset[dataset].append(item)
        datasets = sorted(set(expectations) | set(observed_by_dataset))
        rows = []
        for dataset in datasets:
            expected_sources = expectations.get(dataset, [])
            observed_items = observed_by_dataset.get(dataset, [])
            if observed_items:
                status = "present"
            elif expected_sources:
                status = "missing"
            else:
                status = "observed"
            rows.append(
                {
                    "symbol": symbol,
                    "logical_dataset": dataset,
                    "label": DATASET_LABELS.get(dataset, dataset),
                    "status": status,
                    "item_count": len(observed_items),
                    "expected_source_count": len(expected_sources),
                    "expected_sources": expected_sources,
                    "latest_first_seen_at": max(
                        (str(item.get("first_seen_at") or "") for item in observed_items),
                        default=None,
                    ),
                    "report_date": date,
                }
            )
        return rows

    def _symbol_expectations(self, symbol: str) -> dict[str, list[dict[str, Any]]]:
        expectations: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for source in self._source_configs():
            default_options = source.get("default_options") or {}
            symbols = [self._normalize_symbol(item) for item in self._coerce_symbol_list(default_options.get("symbols"))]
            if symbol not in symbols:
                continue
            dataset = source.get("logical_dataset")
            if not dataset:
                continue
            expectations[dataset].append(
                {
                    "source_id": source.get("source_id"),
                    "enabled": bool(source.get("enabled")),
                    "priority": source.get("priority"),
                    "implementation_status": source.get("implementation_status"),
                }
            )
        return dict(expectations)

    def _dataset_expected_symbols(self, logical_dataset: str) -> dict[str, list[dict[str, Any]]]:
        expected: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for source in self._source_configs():
            if source.get("logical_dataset") != logical_dataset:
                continue
            default_options = source.get("default_options") or {}
            for symbol in self._coerce_symbol_list(default_options.get("symbols")):
                normalized = self._normalize_symbol(symbol)
                if not normalized:
                    continue
                expected[normalized].append(
                    {
                        "source_id": source.get("source_id"),
                        "enabled": bool(source.get("enabled")),
                        "priority": source.get("priority"),
                        "implementation_status": source.get("implementation_status"),
                    }
                )
        return dict(expected)

    def _quality_for_symbol(self, symbol: str, date: str) -> list[dict[str, Any]]:
        terms = self._symbol_search_terms(symbol)
        clauses = ["created_at like ?"]
        params: list[Any] = [f"{date}%"]
        search_clauses = []
        for term in terms:
            like = f"%{term}%"
            search_clauses.append(
                """
                (
                  coalesce(sample_failed_keys, '') like ?
                  or coalesce(observed_value, '') like ?
                )
                """
            )
            params.extend([like, like])
        clauses.append(f"({' or '.join(search_clauses)})")
        return self._fetch_all(
            f"""
            select *
            from quality_check_result
            where {' and '.join(clauses)}
            order by created_at desc
            limit 200
            """,
            tuple(params),
        )

    def _raw_objects_by_ids(self, raw_object_ids: list[str]) -> list[dict[str, Any]]:
        ids = sorted(set(raw_object_ids))
        if not ids:
            return []
        placeholders = ",".join("?" for _ in ids)
        return self._fetch_all(
            f"""
            select *
            from raw_object
            where raw_object_id in ({placeholders})
            order by stored_at desc
            """,
            tuple(ids),
        )

    def _manifest_payload(self, manifest: dict[str, Any]) -> dict[str, Any] | None:
        path = Path(str(manifest.get("manifest_path") or ""))
        if not path.is_absolute():
            path = self.settings.data_lake_root / path
        try:
            resolved = path.resolve()
            data_root = self.settings.data_lake_root.resolve()
            if not resolved.is_relative_to(data_root) or not resolved.exists():
                return None
            return json.loads(resolved.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    def _symbol_from_item(self, item: dict[str, Any]) -> str | None:
        payload = item.get("observed_payload")
        if not isinstance(payload, dict):
            payload = {}
        for field in SYMBOL_PAYLOAD_FIELDS:
            value = payload.get(field)
            symbol = self._normalize_symbol(value)
            if symbol:
                return symbol
        key = str(item.get("source_item_key") or "")
        for part in key.replace("|", ":").split(":"):
            symbol = self._normalize_key_symbol(part)
            if symbol:
                return symbol
        return None

    def _coerce_symbol_list(self, raw_symbols: Any) -> list[str]:
        if raw_symbols is None:
            return []
        if isinstance(raw_symbols, str):
            return [item.strip() for item in raw_symbols.split(",") if item.strip()]
        if isinstance(raw_symbols, list | tuple | set):
            return [str(item).strip() for item in raw_symbols if str(item).strip()]
        return [str(raw_symbols).strip()]

    def _symbol_search_terms(self, symbol: str) -> list[str]:
        terms = {symbol}
        if len(symbol) == 6 and symbol.isdigit():
            terms.update({f"sh{symbol}", f"sz{symbol}", f"bj{symbol}"})
        return sorted(term for term in terms if term)

    def _normalize_symbol(self, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        clean = text.lower().replace(".", "").replace("_", "")
        if clean.startswith(("sh", "sz", "bj")) and clean[2:].isdigit():
            return clean[2:]
        if clean.isdigit() and 5 <= len(clean) <= 6:
            return clean.zfill(6)
        if text.isascii() and any(char.isalpha() for char in text) and len(text) <= 8:
            return text.upper()
        return ""

    def _normalize_key_symbol(self, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        clean = text.lower().replace(".", "").replace("_", "")
        if clean.startswith(("sh", "sz", "bj")) and clean[2:].isdigit():
            return clean[2:]
        if clean.isdigit() and 5 <= len(clean) <= 6:
            return clean.zfill(6)
        if (
            text.isascii()
            and text == text.upper()
            and any(char.isalpha() for char in text)
            and len(text) <= 8
        ):
            return text
        return ""

    def available_dates(self) -> list[str]:
        dates: set[str] = set()
        for row in self._fetch_all("select distinct manifest_date from collection_manifest", ()):
            if row.get("manifest_date"):
                dates.add(str(row["manifest_date"])[:10])
        for table, field in [
            ("crawl_run", "start_at"),
            ("raw_item_version", "stored_at"),
            ("raw_object", "stored_at"),
        ]:
            for row in self._fetch_all(f"select distinct substr({field}, 1, 10) as dt from {table}", ()):
                if row.get("dt"):
                    dates.add(row["dt"])
        for root in [self.layout.quality_root, self.layout.reconciliation_root, self.layout.manifests_root]:
            if not root.exists():
                continue
            for child in root.iterdir():
                if child.is_dir() and child.name.startswith("dt="):
                    dates.add(child.name.removeprefix("dt="))
        return sorted(dates, reverse=True)

    def latest_date(self) -> str:
        dates = self.available_dates()
        if dates:
            return dates[0]
        return ""

    def _source_configs(self) -> list[dict[str, Any]]:
        path = self.settings.config_dir / "source_registry.yaml"
        if not path.exists():
            return []
        return SourceRegistry.load(self.settings.config_dir).sources

    def _source_by_id(self) -> dict[str, dict[str, Any]]:
        return {source["source_id"]: source for source in self._source_configs()}

    def _schedule_by_dataset(self) -> dict[str, dict[str, Any]]:
        path = self.settings.config_dir / "schedule_policy.yaml"
        if not path.exists():
            return {}
        return SchedulePolicy.load(self.settings.config_dir).by_dataset()

    def _runs_for_day(self, date: str) -> list[dict[str, Any]]:
        if not date:
            return []
        return self._fetch_all(
            "select * from crawl_run where start_at like ? order by start_at desc",
            (f"{date}%",),
        )

    def _raw_objects_for_day(self, date: str) -> list[dict[str, Any]]:
        if not date:
            return []
        return self._fetch_all(
            "select * from raw_object where stored_at like ? order by stored_at desc",
            (f"{date}%",),
        )

    def _item_counts_by_dataset(self, date: str) -> dict[str, int]:
        if not date:
            return {}
        rows = self._fetch_all(
            """
            select logical_dataset, count(*) as count
            from raw_item_version
            where stored_at like ?
            group by logical_dataset
            """,
            (f"{date}%",),
        )
        return {row["logical_dataset"]: int(row["count"]) for row in rows}

    def _item_counts_by_source(self, date: str) -> dict[str, int]:
        if not date:
            return {}
        rows = self._fetch_all(
            """
            select source_id, count(*) as count
            from raw_item_version
            where stored_at like ?
            group by source_id
            """,
            (f"{date}%",),
        )
        return {row["source_id"]: int(row["count"]) for row in rows}

    def _latest_manifest(self, date: str) -> dict[str, Any] | None:
        if not date:
            return None
        return self._fetch_one(
            """
            select *
            from collection_manifest
            where manifest_date = ?
            order by created_at desc
            limit 1
            """,
            (date,),
        )

    def _latest_quality_report(self, date: str) -> dict[str, Any] | None:
        return self._read_report(self.layout.quality_root, date, "latest_quality_report.json")

    def _latest_reconciliation_report(self, date: str) -> dict[str, Any] | None:
        return self._read_report(
            self.layout.reconciliation_root,
            date,
            "latest_reconciliation_report.json",
        )

    def _read_report(self, root: Path, date: str, filename: str) -> dict[str, Any] | None:
        if not date:
            return None
        path = root / f"dt={date}" / filename
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {"status": "fail", "error": f"invalid json: {path}"}

    def _latest_source_health(self) -> dict[str, dict[str, Any]]:
        rows = self._fetch_all(
            """
            select sh.*
            from source_health sh
            join (
              select source_id, max(check_time) as check_time
              from source_health
              group by source_id
            ) latest
              on sh.source_id = latest.source_id
             and sh.check_time = latest.check_time
            order by sh.source_id
            """,
            (),
        )
        return {row["source_id"]: row for row in rows}

    def _source_status_rows(
        self,
        *,
        report_date: str,
        sources: list[dict[str, Any]],
        runs: list[dict[str, Any]],
        item_counts_by_source: dict[str, int],
        latest_health: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        runs_by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for run in runs:
            runs_by_source[run["source_id"]].append(run)
        rows = []
        for source in sorted(sources, key=lambda item: item["source_id"]):
            source_id = source["source_id"]
            day_runs = runs_by_source.get(source_id, [])
            health = latest_health.get(source_id)
            run_status = self._status_from_runs(day_runs)
            health_status = self._normalize_status(health["status"]) if health else None
            if not source.get("enabled") and not day_runs:
                status = "missing"
            else:
                status = self._worst_status([run_status, health_status])
            rows.append(
                {
                    "source_id": source_id,
                    "provider_id": source.get("provider_id"),
                    "logical_dataset": source.get("logical_dataset"),
                    "priority": source.get("priority"),
                    "enabled": bool(source.get("enabled")),
                    "implementation_status": source.get("implementation_status"),
                    "status": status,
                    "run_count": len(day_runs),
                    "success_run_count": sum(
                        1 for run in day_runs if self._normalize_status(run["status"]) == "pass"
                    ),
                    "failed_run_count": sum(
                        1 for run in day_runs if self._normalize_status(run["status"]) == "fail"
                    ),
                    "new_item_count": sum(int(run.get("new_item_count") or 0) for run in day_runs),
                    "item_version_count": item_counts_by_source.get(source_id, 0),
                    "last_run_at": day_runs[0]["start_at"] if day_runs else None,
                    "last_error": next(
                        (run.get("error_message") for run in day_runs if run.get("error_message")),
                        None,
                    ),
                    "health": health,
                    "report_date": report_date,
                }
            )
        return rows

    def _dataset_status_rows(
        self,
        *,
        report_date: str,
        sources: list[dict[str, Any]],
        schedule: dict[str, dict[str, Any]],
        runs: list[dict[str, Any]],
        item_counts: dict[str, int],
        quality_report: dict[str, Any] | None,
        reconciliation_report: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        sources_by_dataset: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for source in sources:
            sources_by_dataset[source.get("logical_dataset", "")].append(source)
        runs_by_dataset: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for run in runs:
            runs_by_dataset[run["logical_dataset"]].append(run)
        quality_severity = self._quality_severity_by_dataset(quality_report)
        reconciliation_status = self._reconciliation_status_by_dataset(reconciliation_report)

        all_datasets = sorted(set(sources_by_dataset) | set(item_counts) | set(schedule))
        rows = []
        for dataset in all_datasets:
            dataset_sources = sources_by_dataset.get(dataset, [])
            day_runs = runs_by_dataset.get(dataset, [])
            enabled_count = sum(1 for source in dataset_sources if source.get("enabled"))
            source_count = len(dataset_sources)
            item_count = item_counts.get(dataset, 0)
            statuses = [
                self._status_from_runs(day_runs),
                reconciliation_status.get(dataset),
                quality_severity.get(dataset),
            ]
            policy = schedule.get(dataset) or {}
            if enabled_count and not day_runs and int(policy.get("freshness_slo_minutes") or 0) > 0:
                statuses.append("fail")
            elif enabled_count and item_count == 0 and day_runs:
                statuses.append("warn")
            status = self._worst_status(statuses)
            rows.append(
                {
                    "logical_dataset": dataset,
                    "label": DATASET_LABELS.get(dataset, dataset),
                    "priority": self._dataset_priority(dataset_sources, policy),
                    "view_type": self._view_type(dataset),
                    "status": status,
                    "source_count": source_count,
                    "enabled_source_count": enabled_count,
                    "active_shadow_count": sum(
                        1
                        for source in dataset_sources
                        if str(source.get("implementation_status", "")).startswith("active")
                        and not source.get("enabled")
                    ),
                    "run_count": len(day_runs),
                    "success_run_count": sum(
                        1 for run in day_runs if self._normalize_status(run["status"]) == "pass"
                    ),
                    "failed_run_count": sum(
                        1 for run in day_runs if self._normalize_status(run["status"]) == "fail"
                    ),
                    "new_item_count": sum(int(run.get("new_item_count") or 0) for run in day_runs),
                    "item_version_count": item_count,
                    "quality_status": quality_severity.get(dataset)
                    or ("missing" if quality_report is None else "pass"),
                    "reconciliation_status": reconciliation_status.get(dataset),
                    "freshness_slo_minutes": policy.get("freshness_slo_minutes"),
                    "report_date": report_date,
                }
            )
        return rows

    def _issue_queue(
        self,
        *,
        report_date: str,
        runs: list[dict[str, Any]],
        source_status: list[dict[str, Any]],
        dataset_status: list[dict[str, Any]],
        quality_report: dict[str, Any] | None,
        reconciliation_report: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        issues = []
        if quality_report is None:
            issues.append(
                {
                    "severity": "warning",
                    "kind": "quality_report_missing",
                    "title": f"{report_date} 缺少 quality report",
                    "detail": "运行 pitlake quality-report 生成质量报告。",
                }
            )
        if reconciliation_report is None:
            issues.append(
                {
                    "severity": "warning",
                    "kind": "reconciliation_report_missing",
                    "title": f"{report_date} 缺少 reconciliation report",
                    "detail": "运行 pitlake reconcile 生成对账报告。",
                }
            )
        for run in runs:
            if self._normalize_status(run["status"]) == "fail":
                issues.append(
                    {
                        "severity": "critical",
                        "kind": "run_failed",
                        "title": f"{run['source_id']} 运行失败",
                        "detail": run.get("error_message") or run["run_id"],
                        "logical_dataset": run["logical_dataset"],
                        "source_id": run["source_id"],
                        "run_id": run["run_id"],
                    }
                )
        for row in dataset_status:
            if row["status"] == "fail":
                issues.append(
                    {
                        "severity": "critical",
                        "kind": "dataset_unhealthy",
                        "title": f"{row['logical_dataset']} 数据集异常",
                        "detail": f"item={row['item_version_count']}, run={row['run_count']}",
                        "logical_dataset": row["logical_dataset"],
                    }
                )
        for row in source_status:
            if row["enabled"] and row["status"] == "fail":
                issues.append(
                    {
                        "severity": "critical",
                        "kind": "source_unhealthy",
                        "title": f"{row['source_id']} source 异常",
                        "detail": (row.get("health") or {}).get("notes") or row.get("last_error") or "",
                        "logical_dataset": row["logical_dataset"],
                        "source_id": row["source_id"],
                    }
                )
        for finding in (quality_report or {}).get("quality_findings", [])[:50]:
            issues.append(
                {
                    "severity": self._finding_severity(finding),
                    "kind": finding.get("finding_type", "quality_finding"),
                    "title": f"{finding.get('logical_dataset')} 质量问题",
                    "detail": finding.get("message"),
                    "logical_dataset": finding.get("logical_dataset"),
                    "source_id": finding.get("source_id"),
                }
            )
        for finding in (reconciliation_report or {}).get("findings", [])[:50]:
            issues.append(
                {
                    "severity": self._finding_severity(finding),
                    "kind": finding.get("finding_type", "reconciliation_finding"),
                    "title": f"{finding.get('logical_dataset')} 对账问题",
                    "detail": finding.get("message") or finding.get("identity"),
                    "logical_dataset": finding.get("logical_dataset"),
                }
            )
        return sorted(
            issues,
            key=lambda item: (
                ISSUE_SEVERITY_RANK.get(str(item.get("severity") or "").lower(), 2),
                item["title"],
            ),
        )

    def _overall_status(
        self,
        *,
        dataset_status: list[dict[str, Any]],
        source_status: list[dict[str, Any]],
        quality_report: dict[str, Any] | None,
        reconciliation_report: dict[str, Any] | None,
        latest_manifest: dict[str, Any] | None,
    ) -> str:
        statuses = [
            self._report_status(quality_report),
            self._report_status(reconciliation_report),
            self._normalize_status(latest_manifest.get("status") if latest_manifest else "missing"),
        ]
        statuses.extend(row["status"] for row in dataset_status)
        statuses.extend(row["status"] for row in source_status if row.get("enabled"))
        return self._worst_status(statuses)

    def _quality_severity_by_dataset(self, report: dict[str, Any] | None) -> dict[str, str]:
        result: dict[str, str] = {}
        for finding in (report or {}).get("quality_findings", []):
            dataset = finding.get("logical_dataset")
            if dataset:
                result[dataset] = self._worst_status(
                    [result.get(dataset), self._finding_severity(finding)]
                )
        for check in (report or {}).get("failed_quality_samples", []):
            dataset = check.get("logical_dataset")
            if dataset:
                result[dataset] = self._worst_status([result.get(dataset), "fail"])
        for check in (report or {}).get("warning_quality_samples", []):
            dataset = check.get("logical_dataset")
            if dataset:
                result[dataset] = self._worst_status([result.get(dataset), "warn"])
        return result

    def _reconciliation_status_by_dataset(self, report: dict[str, Any] | None) -> dict[str, str]:
        result = {}
        for dataset in (report or {}).get("datasets", []):
            if dataset.get("logical_dataset"):
                result[dataset["logical_dataset"]] = self._normalize_status(dataset.get("status"))
        return result

    def _quality_findings_for_dataset(self, logical_dataset: str, date: str) -> list[dict[str, Any]]:
        report = self._latest_quality_report(date)
        return [
            finding
            for finding in (report or {}).get("quality_findings", [])
            if finding.get("logical_dataset") == logical_dataset
        ]

    def _reconciliation_for_dataset(self, logical_dataset: str, date: str) -> dict[str, Any] | None:
        report = self._latest_reconciliation_report(date)
        for dataset in (report or {}).get("datasets", []):
            if dataset.get("logical_dataset") == logical_dataset:
                return dataset
        return None

    def _dataset_quality_scores(
        self,
        dataset_rows: list[dict[str, Any]],
        *,
        quality_report: dict[str, Any] | None,
        reconciliation_report: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        quality_missing = quality_report is None
        reconciliation_missing = reconciliation_report is None
        rows = []
        for dataset in dataset_rows:
            score = 100
            factors: list[str] = []
            status_penalty = self._score_penalty(dataset.get("status"), fail=35, warn=18)
            if status_penalty:
                score -= status_penalty
                factors.append(f"dataset_status={dataset.get('status')}")
            quality_status = dataset.get("quality_status")
            if quality_missing:
                score -= 12
                factors.append("quality_report_missing")
            else:
                quality_penalty = self._score_penalty(quality_status, fail=30, warn=15)
                if quality_penalty:
                    score -= quality_penalty
                    factors.append(f"quality_status={quality_status}")
            reconciliation_status = dataset.get("reconciliation_status")
            if reconciliation_missing:
                score -= 8
                factors.append("reconciliation_report_missing")
            else:
                reconciliation_penalty = self._score_penalty(
                    reconciliation_status,
                    fail=20,
                    warn=10,
                )
                if reconciliation_penalty:
                    score -= reconciliation_penalty
                    factors.append(f"reconciliation_status={reconciliation_status}")
            if dataset.get("enabled_source_count") and not dataset.get("run_count"):
                score -= 20
                factors.append("enabled_without_run")
            if dataset.get("run_count") and not dataset.get("item_version_count"):
                score -= 10
                factors.append("run_without_items")

            score = max(0, min(100, score))
            if score >= 80:
                score_status = "pass"
            elif score >= 60:
                score_status = "warn"
            else:
                score_status = "fail"
            rows.append(
                {
                    "logical_dataset": dataset.get("logical_dataset"),
                    "label": dataset.get("label"),
                    "priority": dataset.get("priority"),
                    "status": score_status,
                    "quality_score": score,
                    "dataset_status": dataset.get("status"),
                    "quality_status": quality_status or ("missing" if quality_missing else "pass"),
                    "reconciliation_status": reconciliation_status
                    or ("missing" if reconciliation_missing else "pass"),
                    "item_version_count": dataset.get("item_version_count", 0),
                    "run_count": dataset.get("run_count", 0),
                    "factors": factors or ["clean"],
                }
            )
        return sorted(rows, key=lambda row: (row["quality_score"], row["logical_dataset"] or ""))

    def _score_penalty(self, status: Any, *, fail: int, warn: int) -> int:
        normalized = self._normalize_status(status)
        if normalized == "fail":
            return fail
        if normalized in {"warn", "missing"}:
            return warn
        return 0

    def _volume_baselines(self, report_date: str, *, window_days: int = 30) -> list[dict[str, Any]]:
        rows = self._fetch_all(
            """
            select logical_dataset, substr(stored_at, 1, 10) as dt, count(*) as item_count
            from raw_item_version
            where substr(stored_at, 1, 10) <= ?
            group by logical_dataset, substr(stored_at, 1, 10)
            order by logical_dataset, dt desc
            """,
            (report_date,),
        )
        by_dataset: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_dataset[row["logical_dataset"]].append(
                {"date": row["dt"], "item_count": int(row["item_count"] or 0)}
            )

        datasets = sorted(
            set(by_dataset)
            | {str(source.get("logical_dataset")) for source in self._source_configs()}
        )
        result = []
        for dataset in datasets:
            history = by_dataset.get(dataset, [])[:window_days]
            current_count = next(
                (row["item_count"] for row in history if row["date"] == report_date),
                0,
            )
            baseline_counts = [
                row["item_count"] for row in history if row["date"] < report_date
            ][: window_days - 1]
            baseline_average = (
                round(sum(baseline_counts) / len(baseline_counts), 2)
                if baseline_counts
                else None
            )
            ratio = (
                round(current_count / baseline_average, 4)
                if baseline_average and baseline_average > 0
                else None
            )
            status = "not_enough_history"
            message = "fewer than 3 prior observation days"
            if len(baseline_counts) >= 3 and baseline_average is not None:
                if current_count == 0:
                    status = "fail"
                    message = "current day has no observed items against nonzero baseline"
                elif ratio is not None and (ratio < 0.5 or ratio > 2.0):
                    status = "warn"
                    message = "current item count is outside 0.5x-2.0x baseline range"
                else:
                    status = "pass"
                    message = "current item count is within baseline range"
            result.append(
                {
                    "logical_dataset": dataset,
                    "label": DATASET_LABELS.get(dataset, dataset),
                    "status": status,
                    "current_count": current_count,
                    "baseline_average": baseline_average,
                    "baseline_days": len(baseline_counts),
                    "ratio_to_baseline": ratio,
                    "message": message,
                    "history": history,
                }
            )
        return sorted(
            result,
            key=lambda row: (
                STATUS_RANK.get(self._normalize_status(row["status"]), 1),
                row["logical_dataset"],
            ),
            reverse=True,
        )

    def _schema_drift_rows(
        self,
        report_date: str,
        quality_report: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        rows = []
        for finding in (quality_report or {}).get("quality_findings", []):
            if finding.get("finding_type") != "schema_drift_unknown_fields":
                continue
            observed_value = str(finding.get("observed_value") or "")
            unknown_fields = [
                field.strip()
                for field in observed_value.split(",")
                if field.strip()
            ]
            rows.append(
                {
                    "report_date": report_date,
                    "logical_dataset": finding.get("logical_dataset"),
                    "source_id": finding.get("source_id"),
                    "status": self._finding_severity(finding),
                    "severity": finding.get("severity"),
                    "unknown_fields": unknown_fields,
                    "failed_count": finding.get("failed_count", len(unknown_fields)),
                    "sample_failed_keys": finding.get("sample_failed_keys", []),
                    "message": finding.get("message"),
                }
            )
        return sorted(rows, key=lambda row: (row["logical_dataset"] or "", row["unknown_fields"]))

    def _source_health_rows(self, source_status_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = []
        for source in source_status_rows:
            health = source.get("health") or {}
            if not source.get("enabled") and not health:
                continue
            health_status = self._normalize_status(health.get("status")) if health else "missing"
            rows.append(
                {
                    "source_id": source.get("source_id"),
                    "logical_dataset": source.get("logical_dataset"),
                    "enabled": source.get("enabled"),
                    "status": health_status,
                    "freshness_minutes": health.get("freshness_minutes"),
                    "last_success_time": health.get("last_success_time"),
                    "last_error_time": health.get("last_error_time"),
                    "success_rate_24h": health.get("success_rate_24h"),
                    "new_items_24h": health.get("new_items_24h"),
                    "notes": health.get("notes") or (
                        "source_health ledger missing" if not health else ""
                    ),
                }
            )
        return sorted(
            rows,
            key=lambda row: (
                STATUS_RANK.get(row["status"], 1),
                row["source_id"] or "",
            ),
            reverse=True,
        )

    def _runs(
        self,
        *,
        date: str | None = None,
        source_id: str | None = None,
        logical_dataset: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses = []
        params: list[Any] = []
        if date:
            clauses.append("start_at like ?")
            params.append(f"{date}%")
        if source_id:
            clauses.append("source_id = ?")
            params.append(source_id)
        if logical_dataset:
            clauses.append("logical_dataset = ?")
            params.append(logical_dataset)
        where = f"where {' and '.join(clauses)}" if clauses else ""
        rows = self._fetch_all(
            f"select * from crawl_run {where} order by start_at desc limit ?",
            (*params, limit),
        )
        return self._sort_runs_desc(rows)

    def _raw_objects(
        self,
        *,
        date: str | None = None,
        source_id: str | None = None,
        logical_dataset: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses = []
        params: list[Any] = []
        if date:
            clauses.append("stored_at like ?")
            params.append(f"{date}%")
        if source_id:
            clauses.append("source_id = ?")
            params.append(source_id)
        if logical_dataset:
            clauses.append("logical_dataset = ?")
            params.append(logical_dataset)
        where = f"where {' and '.join(clauses)}" if clauses else ""
        return self._fetch_all(
            f"select * from raw_object {where} order by stored_at desc limit ?",
            (*params, limit),
        )

    def _items(
        self,
        *,
        date: str | None = None,
        source_id: str | None = None,
        logical_dataset: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses = []
        params: list[Any] = []
        if date:
            clauses.append("stored_at like ?")
            params.append(f"{date}%")
        if source_id:
            clauses.append("source_id = ?")
            params.append(source_id)
        if logical_dataset:
            clauses.append("logical_dataset = ?")
            params.append(logical_dataset)
        where = f"where {' and '.join(clauses)}" if clauses else ""
        rows = self._fetch_all(
            f"select * from raw_item_version {where} order by stored_at desc limit ?",
            (*params, limit),
        )
        return [self._decode_item(row) for row in rows]

    def _quality_checks(
        self,
        *,
        date: str | None = None,
        source_id: str | None = None,
        logical_dataset: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses = []
        params: list[Any] = []
        if date:
            clauses.append("created_at like ?")
            params.append(f"{date}%")
        if source_id:
            clauses.append("source_id = ?")
            params.append(source_id)
        if logical_dataset:
            clauses.append("logical_dataset = ?")
            params.append(logical_dataset)
        where = f"where {' and '.join(clauses)}" if clauses else ""
        return self._fetch_all(
            f"select * from quality_check_result {where} order by created_at desc limit 300",
            tuple(params),
        )

    def _contract_payload(self, logical_dataset: str) -> dict[str, Any] | None:
        path = self.settings.config_dir / "dataset_contracts" / f"{logical_dataset}.yaml"
        if not path.exists():
            return None
        try:
            import yaml

            return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}

    def _raw_preview(self, raw: dict[str, Any], *, preview_bytes: int) -> dict[str, Any]:
        path = Path(str(raw.get("storage_path") or ""))
        try:
            resolved = path.resolve()
            data_root = self.settings.data_lake_root.resolve()
            if not resolved.is_relative_to(data_root):
                return {"status": "blocked", "text": "raw path is outside data_lake"}
            if not resolved.exists():
                return {"status": "missing", "text": "raw file is missing"}
            with resolved.open("rb") as handle:
                content = handle.read(preview_bytes + 1)
            truncated = len(content) > preview_bytes
            text = content[:preview_bytes].decode("utf-8", errors="replace")
            parsed: Any = None
            if str(raw.get("mime_type", "")).endswith("json") or resolved.suffix == ".json":
                try:
                    parsed = json.loads(text)
                except json.JSONDecodeError:
                    parsed = None
            return {
                "status": "ok",
                "truncated": truncated,
                "bytes_read": min(len(content), preview_bytes),
                "text": text,
                "json": parsed,
            }
        except OSError as exc:
            return {"status": "error", "text": str(exc)}

    def _decode_item(self, item: dict[str, Any]) -> dict[str, Any]:
        decoded = dict(item)
        payload_text = decoded.get("observed_payload_json") or "{}"
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError:
            payload = {}
        decoded["observed_payload"] = payload if isinstance(payload, dict) else {}
        return decoded

    def _report_meta(self, report: dict[str, Any] | None) -> dict[str, Any]:
        if not report:
            return {"status": "missing"}
        return {
            "report_id": report.get("report_id"),
            "report_type": report.get("report_type"),
            "report_date": report.get("report_date"),
            "created_at": report.get("created_at"),
            "status": report.get("status"),
            "summary": report.get("summary"),
            "report_path": report.get("report_path"),
        }

    def _report_status(self, report: dict[str, Any] | None) -> str:
        if report is None:
            return "missing"
        return self._normalize_status(report.get("status"))

    def _status_from_runs(self, runs: list[dict[str, Any]]) -> str | None:
        if not runs:
            return None
        return self._worst_status([self._normalize_status(run["status"]) for run in runs])

    def _normalize_status(self, status: Any) -> str:
        value = str(status or "missing").lower()
        if value in {"pass", "ok", "success", "complete", "stored", "present", "observed"}:
            return "pass"
        if value in {"not_expected", "not_applicable", "skipped"}:
            return "not_expected"
        if value in {"warn", "warning", "partial", "missing"}:
            return "warn" if value != "missing" else "missing"
        if value in {"fail", "failed", "error"}:
            return "fail"
        return value

    def _worst_status(self, statuses: list[str | None]) -> str:
        selected = "pass"
        selected_rank = -1
        for status in statuses:
            if not status:
                continue
            normalized = self._normalize_status(status)
            rank = STATUS_RANK.get(normalized, 1)
            if rank > selected_rank:
                selected = normalized
                selected_rank = rank
        return selected

    def _finding_severity(self, finding: dict[str, Any]) -> str:
        severity = str(finding.get("severity") or "").lower()
        if severity == "critical":
            return "fail"
        if severity in {"warning", "warn"}:
            return "warn"
        return self._normalize_status(severity)

    def _dataset_priority(
        self,
        dataset_sources: list[dict[str, Any]],
        policy: dict[str, Any],
    ) -> str | None:
        if policy.get("priority"):
            return str(policy["priority"])
        priorities = [source.get("priority") for source in dataset_sources if source.get("priority")]
        if not priorities:
            return None
        return sorted(priorities)[0]

    def _view_type(self, logical_dataset: str) -> str:
        if logical_dataset in DOCUMENT_DATASETS:
            return "document_feed"
        if logical_dataset in STRUCTURED_DATASETS:
            return "structured"
        if logical_dataset == "trading_calendar":
            return "calendar"
        if logical_dataset == "trade_status":
            return "event_timeline"
        return "generic"

    def _sort_runs_desc(self, runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(runs, key=lambda row: str(row.get("start_at") or ""), reverse=True)

    def _fetch_one(self, query: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        rows = self._fetch_all(query, params)
        return rows[0] if rows else None

    def _fetch_all(self, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        try:
            with self.metadata.connect() as conn:
                rows = conn.execute(query, params).fetchall()
        except sqlite3.OperationalError:
            return []
        return [dict(row) for row in rows]
