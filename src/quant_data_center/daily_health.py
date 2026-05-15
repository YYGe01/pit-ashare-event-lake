"""Daily collection health checks for QDC document pipelines."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path
from statistics import median
from typing import Any

from quant_data_center.crawlers.registry import CRAWL_DAILY_SOURCE_IDS, crawler_source_spec
from quant_data_center.settings import QdcSettings
from quant_data_center.storage.database import QdcDatabase
from quant_data_center.storage.schema import CONTROL_SCHEMA, SILVER_SCHEMA


STATUS_RANK = {"ok": 0, "warning": 1, "error": 2}
DAILY_JOB_STAGES = ("build_factors", "sync_parquet", "quality")
DOCUMENT_DATASETS = (
    "announcement",
    "news",
    "research_report",
    "investor_interaction",
    "public_sentiment",
)
FACTOR_DATASETS = {
    "daily_news_factor": ("news_count",),
    "daily_announcement_factor": ("announcement_count",),
    "daily_research_report_factor": ("research_report_count",),
    "daily_investor_interaction_factor": ("question_count", "reply_count"),
    "daily_public_sentiment_factor": ("public_sentiment_count",),
}
SOURCE_FACTOR_DATASET = {
    "cninfo_announcement": "daily_announcement_factor",
    "sse_announcement": "daily_announcement_factor",
    "sina_finance_news": "daily_news_factor",
    "eastmoney_roll_news": "daily_news_factor",
    "eastmoney_research_report": "daily_research_report_factor",
    "cninfo_investor_interaction": "daily_investor_interaction_factor",
    "eastmoney_public_sentiment": "daily_public_sentiment_factor",
}
SOURCE_POLICIES = {
    "cninfo_announcement": {
        "empty_severity": "warning",
        "min_rows_warning": 1,
        "critical_group": "announcement",
    },
    "sse_announcement": {
        "empty_severity": "warning",
        "min_rows_warning": 1,
        "critical_group": "announcement",
    },
    "sina_finance_news": {
        "empty_severity": "warning",
        "critical_group": "news",
    },
    "eastmoney_roll_news": {
        "empty_severity": "error",
        "min_rows_warning": 50,
        "critical_group": "news",
    },
    "eastmoney_research_report": {
        "empty_severity": "warning",
        "min_rows_warning": 1,
        "critical_group": "research_report",
    },
    "cninfo_investor_interaction": {
        "empty_severity": "ok",
        "critical_group": "investor_interaction",
    },
    "eastmoney_public_sentiment": {
        "empty_severity": "error",
        "min_rows_error": 500,
        "min_rows_warning": 1000,
        "critical_group": "public_sentiment",
    },
}
GROUP_POLICIES = {
    "announcement": {"sources": ("cninfo_announcement", "sse_announcement"), "empty_severity": "error"},
    "news": {"sources": ("eastmoney_roll_news", "sina_finance_news"), "empty_severity": "error"},
    "public_sentiment": {"sources": ("eastmoney_public_sentiment",), "empty_severity": "error"},
}


class DailyHealthReporter:
    """Build a Codex-friendly daily collection health report."""

    def __init__(self, settings: QdcSettings) -> None:
        self.settings = settings
        self.database = QdcDatabase(settings)

    def build_report(
        self,
        *,
        target_date: str,
        source_id: str | None = None,
        lookback_days: int = 20,
    ) -> dict[str, Any]:
        expected_sources = _expected_sources(source_id)
        with self.database.connect() as conn:
            control_tables = _table_names(conn, CONTROL_SCHEMA)
            silver_tables = _table_names(conn, SILVER_SCHEMA)
            market_day = _expected_market_day(conn, silver_tables=silver_tables, target_date=target_date)
            crawl_tasks = _crawl_task_rows(conn, control_tables=control_tables, target_date=target_date)
            crawl_runs = _crawl_run_rows(conn, control_tables=control_tables, target_date=target_date)
            manifest_metrics = _document_manifest_metrics(
                conn,
                control_tables=control_tables,
                target_date=target_date,
            )
            silver_counts = _silver_source_counts(
                conn,
                silver_tables=silver_tables,
                target_date=target_date,
            )
            baselines = _source_baselines(
                conn,
                silver_tables=silver_tables,
                target_date=target_date,
                lookback_days=lookback_days,
            )
            factor_rows = _factor_rows(
                conn,
                silver_tables=silver_tables,
                target_date=target_date,
            )
            job_stages = _job_stage_rows(
                conn,
                control_tables=control_tables,
                target_date=target_date,
            )
            quality_issues = _quality_issue_rows(
                conn,
                control_tables=control_tables,
                target_date=target_date,
            )

        limited_run = _is_limited_run(crawl_runs)
        source_rows = [
            _build_source_row(
                source_id=item,
                target_date=target_date,
                market_day=market_day["is_market_day"],
                limited_run=limited_run,
                crawl_tasks=crawl_tasks.get(item, []),
                manifest=manifest_metrics.get(item, {}),
                silver_count=sum(silver_counts.get((item, dataset), 0) for dataset in DOCUMENT_DATASETS),
                dataset_counts={
                    dataset: silver_counts.get((item, dataset), 0) for dataset in DOCUMENT_DATASETS
                },
                baseline=baselines.get(item),
                factor_rows=factor_rows,
            )
            for item in expected_sources
        ]
        checks = []
        checks.extend(_job_stage_checks(job_stages))
        if source_id is None:
            checks.extend(
                _group_checks(
                    source_rows,
                    market_day=market_day["is_market_day"],
                    limited_run=limited_run,
                )
            )
        checks.extend(_quality_issue_checks(quality_issues))
        for row in source_rows:
            checks.extend(row["checks"])

        status = _max_status([check["severity"] for check in checks])
        report = {
            "status": status,
            "date": target_date,
            "source_filter": source_id,
            "lookback_days": lookback_days,
            "market_day": market_day,
            "limited_run": limited_run,
            "summary": {
                "source_count": len(source_rows),
                "error_count": sum(1 for check in checks if check["severity"] == "error"),
                "warning_count": sum(1 for check in checks if check["severity"] == "warning"),
                "quality_issue_count": len(quality_issues),
            },
            "source_rows": source_rows,
            "factor_rows": factor_rows,
            "job_stages": job_stages,
            "quality_issues": quality_issues,
            "checks": sorted(
                checks,
                key=lambda item: (
                    STATUS_RANK.get(str(item.get("severity")), 0) * -1,
                    str(item.get("source_id") or ""),
                    str(item.get("code") or ""),
                ),
            ),
            "next_actions": _next_actions(checks),
        }
        report["markdown"] = render_daily_health_markdown(report)
        return report


def render_daily_health_markdown(report: dict[str, Any]) -> str:
    lines = [
        f"# QDC Daily Health {report['date']}",
        "",
        f"- Status: `{report['status']}`",
        f"- Market day: `{report['market_day']['is_market_day']}` ({report['market_day']['source']})",
        f"- Limited run: `{report['limited_run']}`",
        f"- Errors: `{report['summary']['error_count']}`",
        f"- Warnings: `{report['summary']['warning_count']}`",
        "",
        "## Source Rows",
        "",
        _markdown_table(
            ["source_id", "status", "provider", "silver", "factor_rows", "parse_fail", "mapping_rate", "reasons"],
            [
                [
                    row["source_id"],
                    row["status"],
                    str(row["provider_record_count"]),
                    str(row["silver_row_count"]),
                    str(row["factor_row_count"]),
                    _rate_text(row["parse_failed_rate"]),
                    _rate_text(row["mapping_rate"]),
                    "; ".join(row["reason_codes"]),
                ]
                for row in report["source_rows"]
            ],
        ),
        "",
        "## Checks",
        "",
    ]
    if report["checks"]:
        lines.append(
            _markdown_table(
                ["severity", "code", "source_id", "message", "next_action"],
                [
                    [
                        check["severity"],
                        check["code"],
                        str(check.get("source_id") or ""),
                        check["message"],
                        check["next_action"],
                    ]
                    for check in report["checks"][:50]
                ],
            )
        )
    else:
        lines.append("No blocking checks.")
    lines.extend(["", "## Next Actions", ""])
    lines.extend(f"- {item}" for item in report["next_actions"])
    return "\n".join(lines) + "\n"


def _expected_sources(source_id: str | None) -> list[str]:
    if source_id:
        crawler_source_spec(source_id)
        return [source_id]
    return list(CRAWL_DAILY_SOURCE_IDS)


def _table_names(conn: Any, schema: str) -> set[str]:
    rows = conn.execute(
        """
        select table_name
        from information_schema.tables
        where table_schema = ?
        """,
        [schema],
    ).fetchall()
    return {str(row[0]) for row in rows}


def _expected_market_day(conn: Any, *, silver_tables: set[str], target_date: str) -> dict[str, Any]:
    if "trade_calendar" in silver_tables:
        row = conn.execute(
            f"""
            select bool_or(is_open) as is_open
            from {SILVER_SCHEMA}.trade_calendar
            where trade_date = ?
            """,
            [target_date],
        ).fetchone()
        if row and row[0] is not None:
            return {"is_market_day": bool(row[0]), "source": "qdc_silver.trade_calendar"}
    parsed = date.fromisoformat(target_date)
    return {"is_market_day": parsed.weekday() < 5, "source": "weekday_fallback"}


def _crawl_task_rows(
    conn: Any,
    *,
    control_tables: set[str],
    target_date: str,
) -> dict[str, list[dict[str, Any]]]:
    if "crawl_task" not in control_tables:
        return {}
    rows = _query_dicts(
        conn,
        f"""
        select source_id, dataset, status, last_error, updated_at
        from {CONTROL_SCHEMA}.crawl_task
        where crawl_date = ?
        """,
        [target_date],
    )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("source_id") or "")].append(row)
    return grouped


def _crawl_run_rows(
    conn: Any,
    *,
    control_tables: set[str],
    target_date: str,
) -> list[dict[str, Any]]:
    if "crawl_run" not in control_tables:
        return []
    return _query_dicts(
        conn,
        f"""
        select *
        from {CONTROL_SCHEMA}.crawl_run
        where crawl_date = ?
        order by created_at desc
        """,
        [target_date],
    )


def _document_manifest_metrics(
    conn: Any,
    *,
    control_tables: set[str],
    target_date: str,
) -> dict[str, dict[str, Any]]:
    if "source_object" not in control_tables:
        return {}
    rows = _query_dicts(
        conn,
        f"""
        select rowid as source_object_rowid, source_id, dataset, uri, created_at
        from {CONTROL_SCHEMA}.source_object
        where layer = 'raw_manifest'
          and (uri like ? or uri like ?)
        order by created_at desc, rowid desc
        limit 5000
        """,
        [f"%documents\\{target_date}\\%", f"%documents/{target_date}/%"],
    )
    manifests_by_source: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        path = Path(str(row.get("uri") or ""))
        if not path.is_file():
            continue
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if str(manifest.get("partition_value") or "") != target_date:
            continue
        source_id = str(manifest.get("source_id") or row.get("source_id") or "")
        manifests_by_source.setdefault(source_id, []).append(manifest)

    metrics: dict[str, dict[str, Any]] = {}
    for source_id, manifests in manifests_by_source.items():
        item = metrics.setdefault(
            source_id,
            {
                "source_id": source_id,
                "manifest_count": 0,
                "provider_record_count": 0,
                "record_count": 0,
                "empty_result_count": 0,
                "duplicate_record_count": 0,
                "parse_failed_count": 0,
                "parsed_unique_record_count": 0,
                "mapped_source_record_count": 0,
                "mapping_failed_count": 0,
                "incomplete_scan_count": 0,
                "scan_stop_reasons": [],
            },
        )
        for manifest in _select_relevant_manifests(manifests):
            item["manifest_count"] += 1
            if manifest.get("date_scan_complete") is False:
                item["incomplete_scan_count"] += 1
                reason = manifest.get("date_scan_stop_reason")
                if reason:
                    item["scan_stop_reasons"].append(str(reason))
            for key in (
                "provider_record_count",
                "record_count",
                "empty_result_count",
                "duplicate_record_count",
                "parse_failed_count",
                "parsed_unique_record_count",
                "mapped_source_record_count",
                "mapping_failed_count",
            ):
                item[key] += int(manifest.get(key) or 0)
    return metrics


def _select_relevant_manifests(manifests_desc: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep latest full manifest plus newer incremental cursor manifests."""

    selected = []
    for manifest in manifests_desc:
        selected.append(manifest)
        if not bool(manifest.get("incremental_cursor_enabled")):
            break
    return selected


def _silver_source_counts(
    conn: Any,
    *,
    silver_tables: set[str],
    target_date: str,
) -> dict[tuple[str, str], int]:
    counts: dict[tuple[str, str], int] = {}
    for dataset in DOCUMENT_DATASETS:
        if dataset not in silver_tables:
            continue
        rows = conn.execute(
            f"""
            select source_id, count(*) as row_count
            from {SILVER_SCHEMA}.{dataset}
            where publish_date = ?
            group by source_id
            """,
            [target_date],
        ).fetchall()
        for source_id, row_count in rows:
            counts[(str(source_id or ""), dataset)] = int(row_count or 0)
    return counts


def _source_baselines(
    conn: Any,
    *,
    silver_tables: set[str],
    target_date: str,
    lookback_days: int,
) -> dict[str, dict[str, Any]]:
    start_date = (date.fromisoformat(target_date) - timedelta(days=max(1, lookback_days))).isoformat()
    rows_by_source: dict[str, list[int]] = defaultdict(list)
    for dataset in DOCUMENT_DATASETS:
        if dataset not in silver_tables:
            continue
        rows = conn.execute(
            f"""
            select publish_date, source_id, count(*) as row_count
            from {SILVER_SCHEMA}.{dataset}
            where publish_date >= ?
              and publish_date < ?
            group by publish_date, source_id
            """,
            [start_date, target_date],
        ).fetchall()
        for _publish_date, source_id, row_count in rows:
            rows_by_source[str(source_id or "")].append(int(row_count or 0))
    return {
        source_id: {
            "history_day_count": len(values),
            "median_silver_rows": float(median(values)) if values else None,
        }
        for source_id, values in rows_by_source.items()
    }


def _factor_rows(
    conn: Any,
    *,
    silver_tables: set[str],
    target_date: str,
) -> dict[str, dict[str, Any]]:
    result = {}
    for dataset, count_fields in FACTOR_DATASETS.items():
        if dataset not in silver_tables:
            result[dataset] = {"row_count": 0, "event_sum": 0.0, "missing_table": True}
            continue
        fields_sql = " + ".join(f"coalesce(sum({field}), 0)" for field in count_fields)
        row = conn.execute(
            f"""
            select count(*) as row_count, {fields_sql} as event_sum
            from {SILVER_SCHEMA}.{dataset}
            where trade_date = ?
            """,
            [target_date],
        ).fetchone()
        result[dataset] = {
            "row_count": int(row[0] or 0) if row else 0,
            "event_sum": float(row[1] or 0) if row else 0.0,
            "missing_table": False,
        }
    return result


def _job_stage_rows(
    conn: Any,
    *,
    control_tables: set[str],
    target_date: str,
) -> dict[str, dict[str, Any]]:
    if "job_run" not in control_tables:
        return {}
    placeholders = ", ".join("?" for _ in DAILY_JOB_STAGES)
    rows = _query_dicts(
        conn,
        f"""
        select *
        from {CONTROL_SCHEMA}.job_run
        where job_type in ({placeholders})
          and (start_date is null or start_date <= ?)
          and (end_date is null or end_date >= ?)
        order by start_at desc, created_at desc
        """,
        [*DAILY_JOB_STAGES, target_date, target_date],
    )
    latest = {}
    for row in rows:
        latest.setdefault(str(row.get("job_type") or ""), row)
    return latest


def _quality_issue_rows(
    conn: Any,
    *,
    control_tables: set[str],
    target_date: str,
) -> list[dict[str, Any]]:
    if "quality_issue" not in control_tables:
        return []
    return _query_dicts(
        conn,
        f"""
        select dataset, source_id, severity, issue_type, entity_key, message, created_at
        from {CONTROL_SCHEMA}.quality_issue
        where status <> 'closed'
          and (entity_key like ? or observed_value like ?)
        order by created_at desc, severity, dataset, issue_type
        limit 100
        """,
        [f"{target_date}|%", f"%{target_date}%"],
    )


def _build_source_row(
    *,
    source_id: str,
    target_date: str,
    market_day: bool,
    limited_run: bool,
    crawl_tasks: list[dict[str, Any]],
    manifest: dict[str, Any],
    silver_count: int,
    dataset_counts: dict[str, int],
    baseline: dict[str, Any] | None,
    factor_rows: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    checks = []
    task_statuses = Counter(str(row.get("status") or "") for row in crawl_tasks)
    provider_records = int(manifest.get("provider_record_count") or 0)
    parsed_unique = int(manifest.get("parsed_unique_record_count") or 0)
    parse_failed = int(manifest.get("parse_failed_count") or 0)
    mapped_count = int(manifest.get("mapped_source_record_count") or 0)
    parse_failed_rate = _ratio(parse_failed, provider_records)
    mapping_rate = _ratio(mapped_count, parsed_unique)
    factor_dataset = SOURCE_FACTOR_DATASET.get(source_id)
    factor_row_count = int((factor_rows.get(factor_dataset or "") or {}).get("row_count") or 0)

    def add(severity: str, code: str, message: str, next_action: str) -> None:
        checks.append(
            {
                "severity": severity,
                "code": code,
                "source_id": source_id,
                "message": message,
                "next_action": next_action,
            }
        )

    if not crawl_tasks and not manifest and not silver_count:
        add("error", "source_not_run", f"{source_id} has no crawl task or data for {target_date}.", "先运行 crawl-daily；若任务未生成，检查默认 source 清单和配置。")
    if task_statuses.get("failed"):
        add("error", "crawl_task_failed", f"{source_id} has failed crawl tasks.", "查看 crawl_task.last_error 和对应 collector；先判断网络波动还是解析/schema 变化。")
    if task_statuses.get("pending") or task_statuses.get("running"):
        add("warning", "crawl_task_unfinished", f"{source_id} still has pending/running tasks.", "等待任务结束或运行 crawl-recover-running 后重跑。")
    if int(manifest.get("incomplete_scan_count") or 0):
        add("warning", "date_scan_incomplete", f"{source_id} date scan was incomplete.", "如果是正式采集，不要使用 --max-pages 限制；重跑该源并确认 date_scan_complete=true。")
    if provider_records > 0 and silver_count == 0:
        add("error", "provider_records_without_silver", f"{source_id} returned provider records but wrote no silver rows.", "优先检查 parser、过滤条件、去重键和 instrument 映射。")
    if parse_failed_rate is not None and provider_records >= 10:
        if parse_failed_rate >= 0.2:
            add("error", "high_parse_failed_rate", f"{source_id} parse_failed_rate={parse_failed_rate:.2%}.", "检查上游字段/HTML/JSON 结构是否变化，并补 collector 测试。")
        elif parse_failed_rate >= 0.05:
            add("warning", "elevated_parse_failed_rate", f"{source_id} parse_failed_rate={parse_failed_rate:.2%}.", "抽样 raw manifest 和 records，确认是否为正常脏数据。")
    if mapping_rate is not None and parsed_unique >= 10:
        if mapping_rate <= 0.3:
            add("error", "low_mapping_rate", f"{source_id} mapping_rate={mapping_rate:.2%}.", "检查 stock_basic、简称歧义和映射规则。")
        elif mapping_rate <= 0.7:
            add("warning", "elevated_mapping_failures", f"{source_id} mapping_rate={mapping_rate:.2%}.", "抽样未映射标题，决定是否补映射规则。")
    policy = SOURCE_POLICIES.get(source_id, {})
    if market_day and not limited_run:
        if provider_records == 0 and silver_count == 0:
            severity = str(policy.get("empty_severity") or "warning")
            if severity != "ok":
                add(severity, "empty_source_result", f"{source_id} produced no records on expected market day.", "先确认是否交易所假日或上游真实空；若非真实空，重跑该源并检查请求参数。")
        min_rows_error = policy.get("min_rows_error")
        min_rows_warning = policy.get("min_rows_warning")
        if min_rows_error is not None and silver_count < int(min_rows_error):
            add("error", "below_hard_min_rows", f"{source_id} silver rows {silver_count} < {min_rows_error}.", "检查源站返回、日期窗口和分页扫描完整性。")
        elif min_rows_warning is not None and silver_count < int(min_rows_warning):
            add("warning", "below_soft_min_rows", f"{source_id} silver rows {silver_count} < {min_rows_warning}.", "和历史基线对比，确认是否正常低量日。")
    if baseline and baseline.get("median_silver_rows") is not None and not limited_run:
        baseline_median = float(baseline["median_silver_rows"])
        if baseline_median >= 10 and silver_count < baseline_median * 0.3:
            add("warning", "below_history_baseline", f"{source_id} silver rows {silver_count} are below 30% of {baseline_median:.1f} median.", "对比最近 raw manifest，判断源站低量、采集截断还是解析变化。")
    if silver_count > 0 and factor_dataset and factor_row_count == 0:
        add("error", "documents_without_factor_rows", f"{source_id} has silver rows but {factor_dataset} has no rows.", "重跑 build-factors；若仍为空，检查 factor builder。")

    return {
        "source_id": source_id,
        "status": _max_status([check["severity"] for check in checks]),
        "dataset_counts": dataset_counts,
        "task_status_counts": dict(task_statuses),
        "manifest_count": int(manifest.get("manifest_count") or 0),
        "provider_record_count": provider_records,
        "silver_row_count": silver_count,
        "factor_dataset": factor_dataset,
        "factor_row_count": factor_row_count,
        "parse_failed_rate": parse_failed_rate,
        "mapping_rate": mapping_rate,
        "baseline": baseline or {},
        "reason_codes": [check["code"] for check in checks],
        "checks": checks,
    }


def _job_stage_checks(job_stages: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    checks = []
    for stage in DAILY_JOB_STAGES:
        row = job_stages.get(stage)
        if not row:
            checks.append(
                {
                    "severity": "warning",
                    "code": "job_stage_missing",
                    "source_id": None,
                    "message": f"{stage} has no job_run covering the target date.",
                    "next_action": f"运行 qdc {stage.replace('_', '-')} 相关步骤；完成后重跑 daily-health。",
                }
            )
            continue
        if str(row.get("status") or "") != "success":
            checks.append(
                {
                    "severity": "error",
                    "code": "job_stage_failed",
                    "source_id": None,
                    "message": f"{stage} latest status is {row.get('status')}.",
                    "next_action": "先修复失败阶段，不要继续使用本日结果。",
                }
            )
    return checks


def _group_checks(
    source_rows: list[dict[str, Any]],
    *,
    market_day: bool,
    limited_run: bool,
) -> list[dict[str, Any]]:
    if not market_day or limited_run:
        return []
    rows_by_source = {row["source_id"]: row for row in source_rows}
    checks = []
    for group, policy in GROUP_POLICIES.items():
        total = sum(int(rows_by_source.get(source, {}).get("silver_row_count") or 0) for source in policy["sources"])
        if total == 0:
            checks.append(
                {
                    "severity": str(policy["empty_severity"]),
                    "code": "empty_critical_source_group",
                    "source_id": ",".join(policy["sources"]),
                    "message": f"{group} group has no silver rows across default sources.",
                    "next_action": "先重跑该组默认源；若 provider 有数据但 silver 为空，检查解析和映射。",
                }
            )
    return checks


def _quality_issue_checks(quality_issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "severity": "error" if str(row.get("severity") or "error") == "error" else "warning",
            "code": f"quality_issue:{row.get('issue_type')}",
            "source_id": row.get("source_id"),
            "message": str(row.get("message") or row.get("issue_type") or "quality issue"),
            "next_action": "先查看 qdc_meta.quality_issue observed_value；确认是数据源、解析、因子还是环境问题。",
        }
        for row in quality_issues
    ]


def _next_actions(checks: list[dict[str, Any]]) -> list[str]:
    if not checks:
        return ["当日采集健康，可以继续下游分析。"]
    actions = []
    if any(check["code"] in {"crawl_task_failed", "source_not_run", "crawl_task_unfinished"} for check in checks):
        actions.append("先补齐或恢复 crawl-daily 任务，再重新生成健康报告。")
    if any(check["code"] in {"provider_records_without_silver", "high_parse_failed_rate"} for check in checks):
        actions.append("优先检查 collector parser/schema 变化；需要永久修复时补测试。")
    if any(check["code"] in {"low_mapping_rate", "elevated_mapping_failures"} for check in checks):
        actions.append("检查 stock_basic 和实体映射规则，避免低置信新闻污染因子。")
    if any(check["code"] == "documents_without_factor_rows" for check in checks):
        actions.append("重跑 build-factors；若仍为空，检查 FactorBuilder。")
    if any(str(check["code"]).startswith("quality_issue:") for check in checks):
        actions.append("运行 qdc quality --start <date> --end <date>，按 quality_issue 逐项定位。")
    actions.append("修复或重跑后再次执行 qdc daily-health --date <date>，直到 status 不再是 error。")
    return list(dict.fromkeys(actions))


def _is_limited_run(crawl_runs: list[dict[str, Any]]) -> bool:
    for row in crawl_runs:
        params = _json_loads(row.get("parameters_json"))
        if params.get("control_only"):
            return True
        if params.get("max_pages") is not None:
            return True
        if int(params.get("instrument_filter_count") or 0) > 0:
            return True
        if int(params.get("remaining_task_count") or 0) > 0:
            return True
    return False


def _query_dicts(conn: Any, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    rows = conn.execute(sql, params or []).fetchall()
    columns = [item[0] for item in conn.description]
    return [dict(zip(columns, row, strict=False)) for row in rows]


def _json_loads(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    try:
        payload = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _ratio(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(float(numerator) / float(denominator), 6)


def _max_status(statuses: list[str]) -> str:
    if not statuses:
        return "ok"
    return max(statuses, key=lambda item: STATUS_RANK.get(item, 0))


def _rate_text(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.2%}"


def _markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(_escape_cell(value) for value in row) + " |")
    return "\n".join(lines)


def _escape_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ").replace("\r", " ")
