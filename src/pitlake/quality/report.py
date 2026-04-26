"""Daily quality report helpers."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any

from pitlake.settings import ProjectSettings
from pitlake.storage.layout import LakeLayout
from pitlake.storage.metadata_store import MetadataStore
from pitlake.utils import compact_timestamp, isoformat, write_json


def summarize_quality_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    by_status = Counter(result["status"] for result in results)
    by_severity = Counter(result["severity"] for result in results)
    return {
        "check_count": len(results),
        "by_status": dict(by_status),
        "by_severity": dict(by_severity),
    }


@dataclass(frozen=True)
class QualityReportStore:
    """Generate daily local quality reports from the metadata ledger."""

    settings: ProjectSettings

    def generate_daily_report(
        self,
        *,
        report_date: str,
        metadata_store: MetadataStore,
    ) -> dict[str, Any]:
        runs = metadata_store.fetch_runs_for_day(report_date)
        raw_objects = metadata_store.fetch_raw_objects_for_day(report_date)
        quality = metadata_store.fetch_quality_for_day(report_date)
        item_versions = self._fetch_item_versions_for_day(metadata_store, report_date)

        source_summary = self._summarize_sources(runs)
        dataset_summary = self._summarize_datasets(raw_objects, item_versions)
        failed_checks = [item for item in quality if item["status"] != "pass"]

        report = {
            "report_id": f"{report_date}-quality-{compact_timestamp()}",
            "report_type": "daily_quality",
            "report_date": report_date,
            "created_at": isoformat(),
            "status": "fail" if failed_checks or self._has_failed_runs(runs) else "pass",
            "summary": {
                "run_count": len(runs),
                "failed_run_count": sum(
                    1 for run in runs if run["status"] not in {"success", "complete"}
                ),
                "raw_object_count": len(raw_objects),
                "item_version_count": len(item_versions),
                "new_item_count": sum(int(run.get("new_item_count") or 0) for run in runs),
                "duplicate_count": sum(int(run.get("duplicate_count") or 0) for run in runs),
                "quarantine_count": sum(int(run.get("quarantine_count") or 0) for run in runs),
                "quality": summarize_quality_results(quality),
                "failed_quality_check_count": len(failed_checks),
            },
            "sources": source_summary,
            "datasets": dataset_summary,
            "failed_quality_samples": failed_checks[:20],
        }

        report_dir = LakeLayout(self.settings).quality_root / f"dt={report_date}"
        report_path = report_dir / f"quality_report_{compact_timestamp()}.json"
        latest_path = report_dir / "latest_quality_report.json"
        report["report_path"] = report_path.relative_to(self.settings.data_lake_root).as_posix()
        write_json(report_path, report)
        write_json(latest_path, report)
        return report

    def _fetch_item_versions_for_day(
        self,
        metadata_store: MetadataStore,
        report_date: str,
    ) -> list[dict[str, Any]]:
        with metadata_store.connect() as conn:
            rows = conn.execute(
                """
                select *
                from raw_item_version
                where stored_at like ?
                order by stored_at, rowid
                """,
                (f"{report_date}%",),
            ).fetchall()
        return [dict(row) for row in rows]

    def _summarize_sources(self, runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        sources: dict[str, dict[str, Any]] = {}
        for run in runs:
            source_id = run["source_id"]
            current = sources.setdefault(
                source_id,
                {
                    "source_id": source_id,
                    "provider_id": run["provider_id"],
                    "logical_dataset": run["logical_dataset"],
                    "run_count": 0,
                    "success_run_count": 0,
                    "failed_run_count": 0,
                    "request_count": 0,
                    "success_count": 0,
                    "error_count": 0,
                    "new_item_count": 0,
                    "duplicate_count": 0,
                    "quarantine_count": 0,
                    "last_status": None,
                },
            )
            current["run_count"] += 1
            if run["status"] == "success":
                current["success_run_count"] += 1
            else:
                current["failed_run_count"] += 1
            current["request_count"] += int(run.get("request_count") or 0)
            current["success_count"] += int(run.get("success_count") or 0)
            current["error_count"] += int(run.get("error_count") or 0)
            current["new_item_count"] += int(run.get("new_item_count") or 0)
            current["duplicate_count"] += int(run.get("duplicate_count") or 0)
            current["quarantine_count"] += int(run.get("quarantine_count") or 0)
            current["last_status"] = run["status"]
        return sorted(sources.values(), key=lambda item: item["source_id"])

    def _summarize_datasets(
        self,
        raw_objects: list[dict[str, Any]],
        item_versions: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        datasets: dict[str, dict[str, Any]] = {}
        for raw in raw_objects:
            dataset = raw["logical_dataset"]
            current = datasets.setdefault(
                dataset,
                {
                    "logical_dataset": dataset,
                    "providers": set(),
                    "sources": set(),
                    "raw_object_count": 0,
                    "item_version_count": 0,
                },
            )
            current["providers"].add(raw["provider_id"])
            current["sources"].add(raw["source_id"])
            current["raw_object_count"] += 1
        for item in item_versions:
            dataset = item["logical_dataset"]
            current = datasets.setdefault(
                dataset,
                {
                    "logical_dataset": dataset,
                    "providers": set(),
                    "sources": set(),
                    "raw_object_count": 0,
                    "item_version_count": 0,
                },
            )
            current["providers"].add(item["provider_id"])
            current["sources"].add(item["source_id"])
            current["item_version_count"] += 1
        result = []
        for dataset in datasets.values():
            dataset["providers"] = sorted(dataset["providers"])
            dataset["sources"] = sorted(dataset["sources"])
            result.append(dataset)
        return sorted(result, key=lambda item: item["logical_dataset"])

    def _has_failed_runs(self, runs: list[dict[str, Any]]) -> bool:
        return any(run["status"] not in {"success", "complete"} for run in runs)
