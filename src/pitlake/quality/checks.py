"""Minimal V0 quality checks."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pitlake.control.contracts import DatasetContract
from pitlake.storage.raw_store import RawWriteResult


@dataclass(frozen=True)
class CheckResult:
    check_name: str
    check_type: str
    severity: str
    status: str
    expected_value: str | None = None
    observed_value: str | None = None
    failed_count: int = 0
    sample_failed_keys: list[str] = field(default_factory=list)
    run_id: str | None = None
    logical_dataset: str | None = None
    source_id: str | None = None


class QualityRunner:
    """Run hard checks that protect published manifests."""

    def check_raw_write(self, raw: RawWriteResult) -> list[CheckResult]:
        results: list[CheckResult] = []
        results.append(
            CheckResult(
                check_name="raw_file_exists",
                check_type="hard",
                severity="critical",
                status="pass" if raw.storage_path.exists() else "fail",
                expected_value="exists",
                observed_value=str(raw.storage_path),
                failed_count=0 if raw.storage_path.exists() else 1,
                run_id=raw.run_id,
                logical_dataset=raw.logical_dataset,
                source_id=raw.source_id,
            )
        )
        results.append(
            CheckResult(
                check_name="content_hash_not_null",
                check_type="hard",
                severity="critical",
                status="pass" if raw.content_hash.startswith("sha256:") else "fail",
                expected_value="sha256:*",
                observed_value=raw.content_hash,
                failed_count=0 if raw.content_hash.startswith("sha256:") else 1,
                run_id=raw.run_id,
                logical_dataset=raw.logical_dataset,
                source_id=raw.source_id,
            )
        )
        results.append(
            CheckResult(
                check_name="raw_size_positive",
                check_type="hard",
                severity="critical",
                status="pass" if raw.size_bytes > 0 else "fail",
                expected_value="> 0",
                observed_value=str(raw.size_bytes),
                failed_count=0 if raw.size_bytes > 0 else 1,
                run_id=raw.run_id,
                logical_dataset=raw.logical_dataset,
                source_id=raw.source_id,
            )
        )
        return results

    def check_required_fields(
        self,
        *,
        contract: DatasetContract,
        payload: dict[str, Any],
        run_id: str | None = None,
        source_id: str | None = None,
    ) -> list[CheckResult]:
        missing = [
            field_name
            for field_name in contract.required_fields
            if payload.get(field_name) in (None, "")
        ]
        return [
            CheckResult(
                check_name="required_fields_not_null",
                check_type="hard",
                severity="critical",
                status="pass" if not missing else "fail",
                expected_value="all required fields present",
                observed_value=",".join(missing),
                failed_count=len(missing),
                sample_failed_keys=missing[:20],
                run_id=run_id,
                logical_dataset=contract.logical_dataset,
                source_id=source_id,
            )
        ]

    @staticmethod
    def has_critical_failures(results: list[CheckResult]) -> bool:
        return any(result.severity == "critical" and result.status == "fail" for result in results)

