"""Read-only data access helpers for the local PitLake console."""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any

from pitlake.control.registry import SourceRegistry
from pitlake.control.schedules import SchedulePolicy
from pitlake.settings import ProjectSettings
from pitlake.storage.layout import LakeLayout
from pitlake.storage.metadata_store import MetadataStore

STATUS_RANK = {"pass": 0, "ok": 0, "success": 0, "complete": 0, "warn": 1, "partial": 1, "missing": 1, "fail": 2, "failed": 2, "error": 2}

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
            "contract": self._contract_payload(logical_dataset),
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
        return {"query": query, "results": results[:limit]}

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
            key=lambda item: (0 if item["severity"] == "critical" else 1, item["title"]),
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
        if value in {"pass", "ok", "success", "complete", "stored"}:
            return "pass"
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
