"""Daily quality report helpers."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from typing import Any

from pitlake.control.contracts import ContractCatalog, DatasetContract
from pitlake.control.registry import SourceRegistry
from pitlake.control.schedules import SchedulePolicy
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
        strict_coverage: bool = False,
    ) -> dict[str, Any]:
        runs = metadata_store.fetch_runs_for_day(report_date)
        raw_objects = metadata_store.fetch_raw_objects_for_day(report_date)
        quality = metadata_store.fetch_quality_for_day(report_date)
        item_versions = self._fetch_item_versions_for_day(metadata_store, report_date)
        contracts = self._load_contracts()
        sources = self._load_sources()
        schedule = self._load_schedule()

        source_summary = self._summarize_sources(runs)
        dataset_summary = self._summarize_datasets(raw_objects, item_versions)
        quality_findings = self._build_quality_findings(
            report_date=report_date,
            runs=runs,
            item_versions=item_versions,
            contracts=contracts,
            sources=sources,
            schedule=schedule,
            strict_coverage=strict_coverage,
        )
        failed_checks = [
            item
            for item in quality
            if item["status"] != "pass" and item["severity"] == "critical"
        ]
        warning_checks = [
            item
            for item in quality
            if item["status"] != "pass" and item["severity"] != "critical"
        ]
        critical_findings = [
            item for item in quality_findings if item["severity"] == "critical"
        ]
        warning_findings = [item for item in quality_findings if item["severity"] == "warning"]

        report = {
            "report_id": f"{report_date}-quality-{compact_timestamp()}",
            "report_type": "daily_quality",
            "report_date": report_date,
            "created_at": isoformat(),
            "status": self._status(
                runs=runs,
                failed_checks=failed_checks,
                critical_findings=critical_findings,
                warning_checks=warning_checks,
                warning_findings=warning_findings,
            ),
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
                "warning_quality_check_count": len(warning_checks),
                "quality_finding_count": len(quality_findings),
                "critical_quality_finding_count": len(critical_findings),
                "warning_quality_finding_count": len(warning_findings),
                "strict_coverage": strict_coverage,
            },
            "sources": source_summary,
            "datasets": dataset_summary,
            "failed_quality_samples": failed_checks[:20],
            "warning_quality_samples": warning_checks[:20],
            "quality_findings": quality_findings[:100],
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
        items = []
        for row in rows:
            item = dict(row)
            item["observed_payload"] = self._decode_payload(item.get("observed_payload_json"))
            items.append(item)
        return items

    def _load_contracts(self) -> dict[str, DatasetContract]:
        contract_dir = self.settings.config_dir / "dataset_contracts"
        if not contract_dir.exists():
            return {}
        return ContractCatalog.load(contract_dir).by_dataset()

    def _load_sources(self) -> list[dict[str, Any]]:
        path = self.settings.config_dir / "source_registry.yaml"
        if not path.exists():
            return []
        return SourceRegistry.load(self.settings.config_dir).sources

    def _load_schedule(self) -> dict[str, dict[str, Any]]:
        path = self.settings.config_dir / "schedule_policy.yaml"
        if not path.exists():
            return {}
        policy = SchedulePolicy.load(self.settings.config_dir)
        return policy.by_dataset()

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

    def _status(
        self,
        *,
        runs: list[dict[str, Any]],
        failed_checks: list[dict[str, Any]],
        critical_findings: list[dict[str, Any]],
        warning_checks: list[dict[str, Any]],
        warning_findings: list[dict[str, Any]],
    ) -> str:
        if failed_checks or critical_findings or self._has_failed_runs(runs):
            return "fail"
        if warning_checks or warning_findings:
            return "warn"
        return "pass"

    def _build_quality_findings(
        self,
        *,
        report_date: str,
        runs: list[dict[str, Any]],
        item_versions: list[dict[str, Any]],
        contracts: dict[str, DatasetContract],
        sources: list[dict[str, Any]],
        schedule: dict[str, dict[str, Any]],
        strict_coverage: bool,
    ) -> list[dict[str, Any]]:
        findings = []
        findings.extend(self._contract_findings(item_versions=item_versions, contracts=contracts))
        findings.extend(self._anomaly_findings(item_versions=item_versions))
        if strict_coverage:
            findings.extend(
                self._coverage_findings(
                    report_date=report_date,
                    runs=runs,
                    sources=sources,
                    schedule=schedule,
                )
            )
        return findings

    def _contract_findings(
        self,
        *,
        item_versions: list[dict[str, Any]],
        contracts: dict[str, DatasetContract],
    ) -> list[dict[str, Any]]:
        findings: list[dict[str, Any]] = []
        by_dataset = self._items_by_dataset(item_versions)
        internal_fields = {"source_item_key"}
        for logical_dataset, items in by_dataset.items():
            contract = contracts.get(logical_dataset)
            if not contract:
                continue
            allowed_fields = (
                set(contract.required_fields) | set(contract.optional_fields) | internal_fields
            )
            missing_counts: Counter[str] = Counter()
            missing_samples: dict[str, list[str]] = {}
            unknown_fields: set[str] = set()
            for item in items:
                payload = item["observed_payload"]
                item_key = str(item.get("source_item_key") or item.get("item_version_id"))
                for field_name in contract.required_fields:
                    if payload.get(field_name) in (None, ""):
                        missing_counts[field_name] += 1
                        missing_samples.setdefault(field_name, [])
                        if len(missing_samples[field_name]) < 5:
                            missing_samples[field_name].append(item_key)
                unknown_fields.update(set(payload) - allowed_fields)

            if missing_counts:
                findings.append(
                    {
                        "logical_dataset": logical_dataset,
                        "severity": "critical",
                        "finding_type": "required_field_gap",
                        "message": "Observed item payload is missing dataset contract required fields.",
                        "failed_count": sum(missing_counts.values()),
                        "field_counts": dict(sorted(missing_counts.items())),
                        "sample_failed_keys": missing_samples,
                    }
                )
            if unknown_fields:
                findings.append(
                    {
                        "logical_dataset": logical_dataset,
                        "severity": "warning",
                        "finding_type": "schema_drift_unknown_fields",
                        "message": "Observed item payload contains fields not declared in the dataset contract.",
                        "expected_value": "required_fields + optional_fields",
                        "observed_value": ",".join(sorted(unknown_fields)),
                        "failed_count": len(unknown_fields),
                        "sample_failed_keys": sorted(unknown_fields)[:20],
                    }
                )
        return findings

    def _anomaly_findings(self, *, item_versions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        findings: list[dict[str, Any]] = []
        negative_fields_by_dataset = {
            "market_daily_ohlcv": ["open", "high", "low", "close", "volume", "amount"],
            "market_minute_bar": ["open", "high", "low", "close", "volume", "amount"],
            "commodity_daily": [
                "open",
                "high",
                "low",
                "close",
                "settlement",
                "prev_settlement",
                "volume",
                "open_interest",
            ],
            "global_market_daily": ["open", "high", "low", "close", "volume"],
            "price_limit": ["prev_close", "limit_up", "limit_down"],
            "adjustment_factor": ["adj_factor"],
        }
        high_low_datasets = {"market_daily_ohlcv", "market_minute_bar", "commodity_daily"}
        by_dataset = self._items_by_dataset(item_versions)
        for logical_dataset, items in by_dataset.items():
            negative_samples = []
            high_low_samples = []
            price_limit_samples = []
            for item in items:
                payload = item["observed_payload"]
                item_key = str(item.get("source_item_key") or item.get("item_version_id"))
                for field_name in negative_fields_by_dataset.get(logical_dataset, []):
                    value = self._as_float(payload.get(field_name))
                    if value is not None and value < 0:
                        negative_samples.append(f"{item_key}:{field_name}={value}")
                        break
                if logical_dataset in high_low_datasets:
                    high = self._as_float(payload.get("high"))
                    low = self._as_float(payload.get("low"))
                    if high is not None and low is not None and high < low:
                        high_low_samples.append(f"{item_key}:high={high},low={low}")
                if logical_dataset == "price_limit":
                    limit_up = self._as_float(payload.get("limit_up"))
                    limit_down = self._as_float(payload.get("limit_down"))
                    if (
                        limit_up is not None
                        and limit_down is not None
                        and limit_up <= limit_down
                    ):
                        price_limit_samples.append(
                            f"{item_key}:limit_up={limit_up},limit_down={limit_down}"
                        )

            if negative_samples:
                findings.append(
                    {
                        "logical_dataset": logical_dataset,
                        "severity": "warning",
                        "finding_type": "negative_numeric_value",
                        "message": "Observed non-negative market fields contain negative values.",
                        "failed_count": len(negative_samples),
                        "sample_failed_keys": negative_samples[:20],
                    }
                )
            if high_low_samples:
                findings.append(
                    {
                        "logical_dataset": logical_dataset,
                        "severity": "critical",
                        "finding_type": "high_less_than_low",
                        "message": "Observed OHLC payload has high lower than low.",
                        "failed_count": len(high_low_samples),
                        "sample_failed_keys": high_low_samples[:20],
                    }
                )
            if price_limit_samples:
                findings.append(
                    {
                        "logical_dataset": logical_dataset,
                        "severity": "critical",
                        "finding_type": "limit_up_not_greater_than_limit_down",
                        "message": "Observed price-limit payload has limit_up <= limit_down.",
                        "failed_count": len(price_limit_samples),
                        "sample_failed_keys": price_limit_samples[:20],
                    }
                )
        return findings

    def _coverage_findings(
        self,
        *,
        report_date: str,
        runs: list[dict[str, Any]],
        sources: list[dict[str, Any]],
        schedule: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not sources:
            return []
        successful_sources = {
            run["source_id"] for run in runs if run["status"] in {"success", "complete"}
        }
        enabled_sources = [source for source in sources if source.get("enabled")]
        findings = []
        for source in enabled_sources:
            source_id = source["source_id"]
            logical_dataset = source["logical_dataset"]
            policy = schedule.get(logical_dataset, {})
            if int(policy.get("freshness_slo_minutes") or 0) <= 0:
                continue
            if source_id not in successful_sources:
                findings.append(
                    {
                        "logical_dataset": logical_dataset,
                        "source_id": source_id,
                        "severity": "warning",
                        "finding_type": "enabled_source_not_collected",
                        "message": "Enabled source has no successful run on the report date.",
                        "expected_value": f"successful run on {report_date}",
                        "observed_value": "missing",
                        "failed_count": 1,
                        "sample_failed_keys": [source_id],
                    }
                )

        enabled_datasets = sorted({source["logical_dataset"] for source in enabled_sources})
        for logical_dataset in enabled_datasets:
            policy = schedule.get(logical_dataset, {})
            if int(policy.get("freshness_slo_minutes") or 0) <= 0:
                continue
            dataset_success = any(
                run["logical_dataset"] == logical_dataset
                and run["status"] in {"success", "complete"}
                for run in runs
            )
            if not dataset_success:
                findings.append(
                    {
                        "logical_dataset": logical_dataset,
                        "severity": "critical",
                        "finding_type": "dataset_no_successful_run",
                        "message": "Scheduled dataset has no successful source run on the report date.",
                        "expected_value": f"at least one successful run on {report_date}",
                        "observed_value": "missing",
                        "failed_count": 1,
                        "sample_failed_keys": [logical_dataset],
                    }
                )
        return findings

    def _items_by_dataset(
        self,
        item_versions: list[dict[str, Any]],
    ) -> dict[str, list[dict[str, Any]]]:
        by_dataset: dict[str, list[dict[str, Any]]] = {}
        for item in item_versions:
            by_dataset.setdefault(item["logical_dataset"], []).append(item)
        return by_dataset

    def _decode_payload(self, value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        try:
            decoded = json.loads(value or "{}")
        except json.JSONDecodeError:
            return {}
        return decoded if isinstance(decoded, dict) else {}

    def _as_float(self, value: Any) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
