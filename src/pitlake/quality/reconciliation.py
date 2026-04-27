"""Cross-source reconciliation reports for P0 bootstrap datasets."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from pitlake.control.registry import SourceRegistry
from pitlake.settings import ProjectSettings
from pitlake.storage.layout import LakeLayout
from pitlake.storage.metadata_store import MetadataStore
from pitlake.utils import compact_timestamp, isoformat, sha256_json, write_json

DEFAULT_RECONCILIATION_DATASETS = [
    "market_daily_ohlcv",
    "adjustment_factor",
    "price_limit",
    "announcement_index",
    "policy_regulatory_doc",
    "commodity_daily",
    "global_market_daily",
]

NUMERIC_TOLERANCE = 0.0001


@dataclass(frozen=True)
class ReconciliationReportStore:
    settings: ProjectSettings

    def generate_daily_report(
        self,
        *,
        report_date: str,
        metadata_store: MetadataStore,
        datasets: list[str] | None = None,
    ) -> dict[str, Any]:
        target_datasets = datasets or DEFAULT_RECONCILIATION_DATASETS
        registry = SourceRegistry.load(self.settings.config_dir)
        source_configs = registry.sources
        item_versions = self._fetch_item_versions(metadata_store, report_date, target_datasets)
        by_dataset: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in item_versions:
            by_dataset[item["logical_dataset"]].append(item)

        dataset_reports = []
        findings: list[dict[str, Any]] = []
        for dataset in target_datasets:
            configured = [
                source for source in source_configs if source.get("logical_dataset") == dataset
            ]
            actual_items = by_dataset.get(dataset, [])
            dataset_report = self._reconcile_dataset(
                logical_dataset=dataset,
                configured_sources=configured,
                item_versions=actual_items,
            )
            dataset_reports.append(dataset_report)
            findings.extend(dataset_report["findings"])

        report = {
            "report_id": f"{report_date}-reconciliation-{compact_timestamp()}",
            "report_type": "daily_reconciliation",
            "report_date": report_date,
            "created_at": isoformat(),
            "status": self._status(findings),
            "summary": {
                "dataset_count": len(dataset_reports),
                "item_version_count": len(item_versions),
                "finding_count": len(findings),
                "critical_finding_count": sum(
                    1 for finding in findings if finding["severity"] == "critical"
                ),
                "warning_finding_count": sum(
                    1 for finding in findings if finding["severity"] == "warning"
                ),
            },
            "datasets": dataset_reports,
            "findings": findings[:100],
        }

        report_dir = LakeLayout(self.settings).reconciliation_root / f"dt={report_date}"
        report_path = report_dir / f"reconciliation_report_{compact_timestamp()}.json"
        latest_path = report_dir / "latest_reconciliation_report.json"
        report["report_path"] = report_path.relative_to(self.settings.data_lake_root).as_posix()
        write_json(report_path, report)
        write_json(latest_path, report)
        return report

    def _fetch_item_versions(
        self,
        metadata_store: MetadataStore,
        report_date: str,
        datasets: list[str],
    ) -> list[dict[str, Any]]:
        placeholders = ",".join("?" for _ in datasets)
        query = f"""
            select *
            from raw_item_version
            where stored_at like ?
              and logical_dataset in ({placeholders})
            order by logical_dataset, source_id, source_item_key, stored_at
        """
        with metadata_store.connect() as conn:
            rows = conn.execute(query, (f"{report_date}%", *datasets)).fetchall()
        return [self._decode_item(dict(row)) for row in rows]

    def _decode_item(self, item: dict[str, Any]) -> dict[str, Any]:
        try:
            item["observed_payload"] = json.loads(item.get("observed_payload_json") or "{}")
        except json.JSONDecodeError:
            item["observed_payload"] = {}
        return item

    def _reconcile_dataset(
        self,
        *,
        logical_dataset: str,
        configured_sources: list[dict[str, Any]],
        item_versions: list[dict[str, Any]],
    ) -> dict[str, Any]:
        active_sources = sorted({item["source_id"] for item in item_versions})
        enabled_sources = sorted(
            source["source_id"] for source in configured_sources if source.get("enabled")
        )
        counterpart_sources = sorted(
            source["source_id"]
            for source in configured_sources
            if not source.get("enabled") and self._is_counterparty_candidate(source)
        )
        findings: list[dict[str, Any]] = []
        if len(active_sources) < 2:
            findings.append(
                {
                    "logical_dataset": logical_dataset,
                    "severity": "warning",
                    "finding_type": "missing_counterparty_source",
                    "message": "Only one collected source is available for this dataset; cross-source value checks are not yet possible.",
                    "active_sources": active_sources,
                    "planned_counterparty_sources": counterpart_sources,
                }
            )

        by_identity: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in item_versions:
            by_identity[self._identity_key(logical_dataset, item)].append(item)

        compared_group_count = 0
        mismatched_group_count = 0
        for identity, group in by_identity.items():
            sources = {item["source_id"] for item in group}
            if len(sources) < 2:
                continue
            compared_group_count += 1
            mismatches = self._compare_group(logical_dataset, group)
            if mismatches:
                mismatched_group_count += 1
                findings.append(
                    {
                        "logical_dataset": logical_dataset,
                        "severity": "critical",
                        "finding_type": "value_mismatch",
                        "identity": identity,
                        "source_ids": sorted(sources),
                        "mismatches": mismatches,
                    }
                )

        source_counts = [
            {
                "source_id": source_id,
                "item_version_count": sum(1 for item in item_versions if item["source_id"] == source_id),
            }
            for source_id in active_sources
        ]
        return {
            "logical_dataset": logical_dataset,
            "enabled_sources": enabled_sources,
            "active_sources": active_sources,
            "planned_counterparty_sources": counterpart_sources,
            "source_counts": source_counts,
            "identity_group_count": len(by_identity),
            "compared_group_count": compared_group_count,
            "mismatched_group_count": mismatched_group_count,
            "status": "fail" if any(f["severity"] == "critical" for f in findings) else "warn",
            "findings": findings,
        }

    def _identity_key(self, logical_dataset: str, item: dict[str, Any]) -> str:
        payload = item["observed_payload"]
        if logical_dataset in {"market_daily_ohlcv", "adjustment_factor", "price_limit"}:
            return sha256_json(
                {
                    "instrument": payload.get("instrument"),
                    "trading_date": payload.get("trading_date"),
                }
            )
        if logical_dataset == "commodity_daily":
            return sha256_json(
                {
                    "exchange": payload.get("exchange"),
                    "contract": payload.get("contract"),
                    "trading_date": payload.get("trading_date"),
                }
            )
        if logical_dataset == "global_market_daily":
            return sha256_json(
                {
                    "symbol": payload.get("symbol"),
                    "trading_date": payload.get("trading_date"),
                }
            )
        if logical_dataset == "announcement_index":
            return sha256_json(
                {
                    "source_publish_time": payload.get("source_publish_time"),
                    "instrument": payload.get("instrument"),
                    "title": self._normalized_title(payload.get("title")),
                }
            )
        if logical_dataset == "policy_regulatory_doc":
            return sha256_json(
                {
                    "source_publish_time": payload.get("source_publish_time"),
                    "title": self._normalized_title(payload.get("title")),
                }
            )
        return item["dedup_hash"] or item["source_item_key"]

    def _compare_group(self, logical_dataset: str, group: list[dict[str, Any]]) -> list[dict[str, Any]]:
        fields = {
            "market_daily_ohlcv": ["open", "high", "low", "close", "volume", "amount"],
            "adjustment_factor": ["adj_factor", "factor_type"],
            "price_limit": ["prev_close", "limit_up", "limit_down", "limit_rule"],
            "announcement_index": [],
            "policy_regulatory_doc": [],
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
            "global_market_daily": ["open", "high", "low", "close", "currency", "timezone"],
        }.get(logical_dataset, [])
        baseline = group[0]["observed_payload"]
        mismatches = []
        for item in group[1:]:
            payload = item["observed_payload"]
            for field in fields:
                if not self._values_equal(baseline.get(field), payload.get(field)):
                    mismatches.append(
                        {
                            "field": field,
                            "baseline_source_id": group[0]["source_id"],
                            "baseline_value": baseline.get(field),
                            "other_source_id": item["source_id"],
                            "other_value": payload.get(field),
                        }
                    )
        return mismatches

    def _values_equal(self, left: Any, right: Any) -> bool:
        if left == right:
            return True
        try:
            return abs(float(left) - float(right)) <= NUMERIC_TOLERANCE
        except (TypeError, ValueError):
            return False

    def _normalized_title(self, value: Any) -> str:
        return " ".join(str(value or "").split()).casefold()

    def _is_counterparty_candidate(self, source: dict[str, Any]) -> bool:
        status = str(source.get("implementation_status", ""))
        return status.startswith(("active", "planned"))

    def _status(self, findings: list[dict[str, Any]]) -> str:
        if any(finding["severity"] == "critical" for finding in findings):
            return "fail"
        if findings:
            return "warn"
        return "pass"
