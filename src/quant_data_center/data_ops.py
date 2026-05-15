"""Data operations helpers for quality failure triage."""

from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from quant_data_center.settings import QdcSettings
from quant_data_center.storage.database import QdcDatabase


DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class QualityIssueReporter:
    """Build GitHub-ready reports from qdc_meta.quality_issue rows."""

    def __init__(self, settings: QdcSettings) -> None:
        self.settings = settings
        self.database = QdcDatabase(settings)

    def build_report(
        self,
        *,
        dataset: str | None = None,
        status: str = "open",
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        issues = self.database.list_quality_issues(dataset=dataset, status=status)
        issues = _filter_issues_by_date(issues, start_date=start_date, end_date=end_date)
        issues = sorted(
            issues,
            key=lambda item: str(item.get("created_at") or ""),
            reverse=True,
        )
        if limit > 0:
            issues = issues[:limit]

        issue_count = len(issues)
        report_dataset = dataset or "all"
        date_scope = _date_scope(start_date=start_date, end_date=end_date)
        title = _report_title(issue_count=issue_count, dataset=report_dataset, date_scope=date_scope)
        body = _report_body(
            title=title,
            issues=issues,
            dataset=report_dataset,
            status=status,
            date_scope=date_scope,
            limit=limit,
        )
        return {
            "status": "ok" if issue_count == 0 else "fail",
            "issue_count": issue_count,
            "dataset": report_dataset,
            "status_filter": status,
            "date_scope": date_scope,
            "limit": limit,
            "title": title,
            "body": body,
        }

    def write_report(
        self,
        output_path: str | Path,
        *,
        dataset: str | None = None,
        status: str = "open",
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        report = self.build_report(
            dataset=dataset,
            status=status,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(report["body"]), encoding="utf-8")
        return {**report, "output": str(path)}


def _filter_issues_by_date(
    issues: list[dict[str, Any]],
    *,
    start_date: str | None,
    end_date: str | None,
) -> list[dict[str, Any]]:
    if not start_date and not end_date:
        return issues
    filtered = []
    for issue in issues:
        issue_date = _issue_data_date(issue)
        if not issue_date:
            continue
        if start_date and issue_date < start_date:
            continue
        if end_date and issue_date > end_date:
            continue
        filtered.append(issue)
    return filtered


def _issue_data_date(issue: dict[str, Any]) -> str | None:
    observed = issue.get("observed_value")
    if observed:
        try:
            payload = json.loads(str(observed))
        except json.JSONDecodeError:
            payload = {}
        for key in ("trade_date", "publish_date", "snapshot_date"):
            value = payload.get(key)
            if value:
                return str(value)[:10]

    entity_key = str(issue.get("entity_key") or "")
    first_part = entity_key.split("|", 1)[0]
    if DATE_PATTERN.match(first_part):
        return first_part
    return None


def _date_scope(*, start_date: str | None, end_date: str | None) -> str:
    if start_date and end_date and start_date == end_date:
        return start_date
    if start_date and end_date:
        return f"{start_date}..{end_date}"
    if start_date:
        return f">={start_date}"
    if end_date:
        return f"<={end_date}"
    return "all"


def _report_title(*, issue_count: int, dataset: str, date_scope: str) -> str:
    if issue_count == 0:
        return f"[QDC] Data quality clean: {dataset} {date_scope}"
    return f"[QDC] Data quality failure: {dataset} {date_scope} ({issue_count} issues)"


def _report_body(
    *,
    title: str,
    issues: list[dict[str, Any]],
    dataset: str,
    status: str,
    date_scope: str,
    limit: int,
) -> str:
    generated_at = datetime.now().isoformat(timespec="seconds")
    lines = [
        f"# {title}",
        "",
        "## Summary",
        "",
        f"- Generated at: `{generated_at}`",
        f"- Dataset: `{dataset}`",
        f"- Data date scope: `{date_scope}`",
        f"- Quality issue status filter: `{status}`",
        f"- Included issue rows: `{len(issues)}`",
        f"- Row limit: `{limit}`",
        "",
    ]
    if not issues:
        lines.extend(
            [
                "No matching open quality issues were found.",
                "",
            ]
        )
        return "\n".join(lines)

    lines.extend(
        [
            "## Impact",
            "",
            _summary_table(issues),
            "",
            "## Sample Issues",
            "",
            _sample_table(issues),
            "",
            "## Suggested Codex Task",
            "",
            "Use `.codex/skills/data-ops/SKILL.md` before changing code. Classify the failure as upstream data, configuration/schedule, parser/schema, factor logic, or local environment. Prefer a permanent code or config fix only when the failure is deterministic or recurring; use a retry or source-observation note for one-off upstream outages.",
            "",
            "## Required Checks Before Closing",
            "",
            "- `conda run -n ai-trader qdc validate-config`",
            "- `conda run -n ai-trader qdc quality --start <date> --end <date>`",
            "- `conda run -n ai-trader pytest`",
            "- `conda run -n ai-trader ruff check .`",
            "",
        ]
    )
    return "\n".join(lines)


def _summary_table(issues: list[dict[str, Any]]) -> str:
    counts = Counter(
        (
            str(issue.get("dataset") or ""),
            str(issue.get("issue_type") or ""),
            str(issue.get("severity") or ""),
        )
        for issue in issues
    )
    rows = [
        [dataset, issue_type, severity, str(count)]
        for (dataset, issue_type, severity), count in sorted(counts.items())
    ]
    return _markdown_table(["dataset", "issue_type", "severity", "count"], rows)


def _sample_table(issues: list[dict[str, Any]]) -> str:
    rows = []
    for issue in issues[:20]:
        rows.append(
            [
                str(issue.get("dataset") or ""),
                str(issue.get("issue_type") or ""),
                str(issue.get("entity_key") or ""),
                str(issue.get("source_id") or ""),
                _truncate(str(issue.get("message") or ""), 120),
            ]
        )
    return _markdown_table(["dataset", "issue_type", "entity_key", "source_id", "message"], rows)


def _markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    escaped_headers = [_escape_cell(header) for header in headers]
    lines = [
        "| " + " | ".join(escaped_headers) + " |",
        "| " + " | ".join("---" for _ in escaped_headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(_escape_cell(cell) for cell in row) + " |")
    return "\n".join(lines)


def _escape_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\r", " ").replace("\n", " ")


def _truncate(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."
