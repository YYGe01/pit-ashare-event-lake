"""Local source health and freshness SLO reports."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any

from pitlake.control.registry import SourceRegistry
from pitlake.control.schedules import SchedulePolicy
from pitlake.settings import ProjectSettings
from pitlake.storage.metadata_store import MetadataStore
from pitlake.utils import isoformat, now_cn


@dataclass(frozen=True)
class SourceHealthStatus:
    source_id: str
    logical_dataset: str
    check_time: str
    status: str
    freshness_minutes: float | None = None
    last_success_time: str | None = None
    last_error_time: str | None = None
    success_rate_24h: float | None = None
    new_items_24h: int = 0
    notes: str = ""


@dataclass(frozen=True)
class SourceHealthReportStore:
    settings: ProjectSettings

    def generate_report(
        self,
        *,
        metadata_store: MetadataStore,
        as_of: datetime | None = None,
        include_disabled: bool = False,
        write_ledger: bool = True,
    ) -> dict[str, Any]:
        check_time = as_of or now_cn()
        if check_time.tzinfo is None:
            check_time = check_time.replace(tzinfo=now_cn().tzinfo)
        sources = self._load_sources(include_disabled=include_disabled)
        schedule = self._load_schedule()
        statuses = [
            self._evaluate_source(
                metadata_store=metadata_store,
                source=source,
                schedule=schedule,
                as_of=check_time,
            )
            for source in sources
        ]
        if write_ledger:
            metadata_store.insert_source_health(statuses)

        status = self._status(statuses)
        return {
            "report_type": "source_health",
            "created_at": isoformat(check_time),
            "status": status,
            "summary": {
                "source_count": len(statuses),
                "pass_count": sum(1 for item in statuses if item.status == "pass"),
                "warn_count": sum(1 for item in statuses if item.status == "warn"),
                "fail_count": sum(1 for item in statuses if item.status == "fail"),
            },
            "sources": [asdict(item) for item in statuses],
        }

    def _load_sources(self, *, include_disabled: bool) -> list[dict[str, Any]]:
        registry = SourceRegistry.load(self.settings.config_dir)
        if include_disabled:
            return registry.sources
        return registry.enabled_sources()

    def _load_schedule(self) -> dict[str, dict[str, Any]]:
        path = self.settings.config_dir / "schedule_policy.yaml"
        if not path.exists():
            return {}
        return SchedulePolicy.load(self.settings.config_dir).by_dataset()

    def _evaluate_source(
        self,
        *,
        metadata_store: MetadataStore,
        source: dict[str, Any],
        schedule: dict[str, dict[str, Any]],
        as_of: datetime,
    ) -> SourceHealthStatus:
        source_id = str(source["source_id"])
        logical_dataset = str(source["logical_dataset"])
        runs = self._fetch_runs(metadata_store=metadata_store, source_id=source_id)
        last_success = self._last_run_time(
            [run for run in runs if run["status"] in {"success", "complete"}]
        )
        last_error = self._last_run_time(
            [run for run in runs if run["status"] not in {"success", "complete"}]
        )
        success_rate_24h, new_items_24h = self._window_stats(runs=runs, as_of=as_of)
        freshness_minutes = self._freshness_minutes(
            as_of=as_of,
            last_success_time=last_success,
        )
        slo_minutes = int(schedule.get(logical_dataset, {}).get("freshness_slo_minutes") or 0)
        status, notes = self._evaluate_status(
            freshness_minutes=freshness_minutes,
            slo_minutes=slo_minutes,
            success_rate_24h=success_rate_24h,
            last_success_time=last_success,
        )
        return SourceHealthStatus(
            source_id=source_id,
            logical_dataset=logical_dataset,
            check_time=isoformat(as_of),
            status=status,
            freshness_minutes=freshness_minutes,
            last_success_time=last_success,
            last_error_time=last_error,
            success_rate_24h=success_rate_24h,
            new_items_24h=new_items_24h,
            notes="; ".join(notes),
        )

    def _fetch_runs(
        self,
        *,
        metadata_store: MetadataStore,
        source_id: str,
    ) -> list[dict[str, Any]]:
        with metadata_store.connect() as conn:
            rows = conn.execute(
                """
                select *
                from crawl_run
                where source_id = ?
                order by start_at, rowid
                """,
                (source_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def _last_run_time(self, runs: list[dict[str, Any]]) -> str | None:
        if not runs:
            return None
        last = runs[-1]
        return str(last.get("end_at") or last.get("start_at") or "")

    def _window_stats(
        self,
        *,
        runs: list[dict[str, Any]],
        as_of: datetime,
    ) -> tuple[float | None, int]:
        window_start = as_of - timedelta(hours=24)
        window_runs = [
            run
            for run in runs
            if self._parse_time(str(run.get("start_at") or "")) >= window_start
        ]
        if not window_runs:
            return None, 0
        success_count = sum(
            1 for run in window_runs if run["status"] in {"success", "complete"}
        )
        new_items = sum(int(run.get("new_item_count") or 0) for run in window_runs)
        return success_count / len(window_runs), new_items

    def _freshness_minutes(
        self,
        *,
        as_of: datetime,
        last_success_time: str | None,
    ) -> float | None:
        if not last_success_time:
            return None
        last_success = self._parse_time(last_success_time)
        return round((as_of - last_success).total_seconds() / 60, 2)

    def _evaluate_status(
        self,
        *,
        freshness_minutes: float | None,
        slo_minutes: int,
        success_rate_24h: float | None,
        last_success_time: str | None,
    ) -> tuple[str, list[str]]:
        notes: list[str] = []
        if not last_success_time:
            return "fail", ["no successful run recorded"]
        status = "pass"
        if slo_minutes > 0 and freshness_minutes is not None:
            if freshness_minutes > slo_minutes * 2:
                status = "fail"
                notes.append(f"freshness {freshness_minutes}m exceeds 2x SLO {slo_minutes}m")
            elif freshness_minutes > slo_minutes:
                status = "warn"
                notes.append(f"freshness {freshness_minutes}m exceeds SLO {slo_minutes}m")
        if success_rate_24h is not None and success_rate_24h < 1:
            if status == "pass":
                status = "warn"
            notes.append(f"24h success rate {success_rate_24h:.2f} below 1.00")
        return status, notes

    def _status(self, statuses: list[SourceHealthStatus]) -> str:
        if any(item.status == "fail" for item in statuses):
            return "fail"
        if any(item.status == "warn" for item in statuses):
            return "warn"
        return "pass"

    def _parse_time(self, value: str) -> datetime:
        if not value:
            return datetime.min.replace(tzinfo=now_cn().tzinfo)
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=now_cn().tzinfo)
        return parsed
