"""HTML list connectors for official policy and regulatory document indexes."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from pitlake.connectors.base import BaseConnector, RequestPlan, RunStats
from pitlake.quality.checks import CheckResult, QualityRunner
from pitlake.utils import CN_TZ, sha256_json


@dataclass(frozen=True)
class PolicyListRecord:
    title: str
    source_url: str
    source_publish_time: str | None
    source_department: str
    category: str
    raw_context: str


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "item"):
        return _json_safe(value.item())
    if isinstance(value, float) and value != value:
        return None
    return value


def _normalize_full_date(value: str) -> str | None:
    match = re.search(r"(20\d{2})[-年./](\d{1,2})[-月./](\d{1,2})", value)
    if not match:
        return None
    year, month, day = (int(part) for part in match.groups())
    try:
        return datetime(year, month, day, tzinfo=CN_TZ).isoformat(timespec="seconds")
    except ValueError:
        return None


def _infer_month_day(value: str) -> str | None:
    match = re.search(r"(?<!\d)(\d{1,2})[-/.月](\d{1,2})(?:日)?(?!\d)", value)
    if not match:
        return None
    month, day = (int(part) for part in match.groups())
    now = datetime.now(CN_TZ)
    year = now.year
    try:
        candidate = datetime(year, month, day, tzinfo=CN_TZ)
    except ValueError:
        return None
    if candidate.date() > now.date():
        candidate = candidate.replace(year=year - 1)
    return candidate.isoformat(timespec="seconds")


def _extract_date(value: str) -> str | None:
    return _normalize_full_date(value) or _infer_month_day(value)


class OfficialPolicyHtmlListConnector(BaseConnector):
    """Collect policy/regulatory index rows from an official public HTML list page."""

    connector_version = "0.1.0"
    default_list_url = ""
    default_source_department = ""
    default_category = ""
    default_link_pattern = ""
    default_filename_prefix = "policy_html_list"
    default_check_name = "official_policy_html_list_request"

    def plan_requests(self) -> list[RequestPlan]:
        return []

    def collect(self, *, run_id: str, options: dict[str, Any] | None = None) -> RunStats:
        options = options or {}
        default_options = self.source_config.get("default_options", {})
        list_url = str(
            options.get("list_url") or default_options.get("list_url") or self.default_list_url
        )
        link_pattern = str(
            options.get("link_pattern")
            or default_options.get("link_pattern")
            or self.default_link_pattern
        )
        source_department = str(
            options.get("source_department")
            or default_options.get("source_department")
            or self.default_source_department
        )
        category = str(
            options.get("category") or default_options.get("category") or self.default_category
        )
        limit_items = int(options.get("limit_items") or default_options.get("limit_items") or 20)
        timeout_seconds = int(
            options.get("timeout_seconds") or default_options.get("timeout_seconds") or 20
        )

        stats = RunStats(request_count=1)
        quality = QualityRunner()
        try:
            response = requests.get(
                list_url,
                headers=self._headers(),
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            response.encoding = self._response_encoding(response)
            html = response.text
            records = self._extract_records(
                html=html,
                list_url=list_url,
                link_pattern=link_pattern,
                source_department=source_department,
                category=category,
                limit_items=limit_items,
            )
            raw = self.raw_store.put_bytes(
                source_id=self.source_id,
                provider_id=self.provider_id,
                logical_dataset=self.logical_dataset,
                content=response.content,
                extension="html",
                mime_type=response.headers.get("content-type", "text/html"),
                run_id=run_id,
                filename_prefix=self.default_filename_prefix,
                metadata={
                    "list_url": list_url,
                    "row_count": len(records),
                    "source_department": source_department,
                    "category": category,
                },
            )
            self.metadata_store.insert_raw_object(
                raw,
                request_hash=sha256_json({"url": list_url}),
                request_url=list_url,
                request_params={},
            )
            raw_checks = quality.check_raw_write(raw)
            self.metadata_store.insert_quality_results(raw_checks)
            if quality.has_critical_failures(raw_checks):
                stats.quarantine_count += len(records)
                return stats
            row_count = self._persist_records(
                records=records,
                raw=raw,
                run_id=run_id,
                quality=quality,
            )
            stats.success_count = 1
            stats.new_item_count = row_count["inserted"]
            stats.duplicate_count = row_count["duplicates"]
            stats.quarantine_count += row_count["quarantined"]
        except Exception as exc:
            self._record_source_error(
                run_id=run_id,
                observed_value=str(exc)[:1000],
                sample_key=list_url,
            )
            stats.error_count = 1
        return stats

    def _headers(self) -> dict[str, str]:
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

    def _response_encoding(self, response: Any) -> str:
        encoding = str(getattr(response, "encoding", "") or "").lower()
        apparent = getattr(response, "apparent_encoding", None)
        if not encoding or encoding in {"iso-8859-1", "latin-1"}:
            return str(apparent or "utf-8")
        return str(response.encoding)

    def _extract_records(
        self,
        *,
        html: str,
        list_url: str,
        link_pattern: str,
        source_department: str,
        category: str,
        limit_items: int,
    ) -> list[PolicyListRecord]:
        compiled = re.compile(link_pattern) if link_pattern else None
        soup = BeautifulSoup(html, "html.parser")
        records: list[PolicyListRecord] = []
        seen_urls: set[str] = set()
        for anchor in soup.find_all("a", href=True):
            href = str(anchor.get("href") or "").strip()
            source_url = urljoin(list_url, href)
            if compiled and not compiled.search(source_url):
                continue
            title = _clean_text(anchor.get("title") or anchor.get_text(" ", strip=True))
            if not title or title in {"更多", "更多>>", "加载更多"}:
                continue
            if source_url in seen_urls:
                continue
            context = self._context_text(anchor)
            records.append(
                PolicyListRecord(
                    title=title,
                    source_url=source_url,
                    source_publish_time=_extract_date(context),
                    source_department=source_department,
                    category=category,
                    raw_context=context[:500],
                )
            )
            seen_urls.add(source_url)
            if len(records) >= limit_items:
                break
        return records

    def _context_text(self, anchor: Any) -> str:
        fragments = [_clean_text(anchor.get_text(" ", strip=True))]
        parent = getattr(anchor, "parent", None)
        for _ in range(2):
            if parent is None:
                break
            fragments.append(_clean_text(parent.get_text(" ", strip=True)))
            parent = getattr(parent, "parent", None)
        return _clean_text(" ".join(fragment for fragment in fragments if fragment))

    def _persist_records(
        self,
        *,
        records: list[PolicyListRecord],
        raw: Any,
        run_id: str,
        quality: QualityRunner,
    ) -> dict[str, int]:
        inserted = 0
        duplicates = 0
        quarantined = 0
        for record in records:
            observed = self._normalize_record(record=record, raw=raw)
            checks = quality.check_required_fields(
                contract=self.contract,
                payload=observed,
                run_id=run_id,
                source_id=self.source_id,
            )
            self.metadata_store.insert_quality_results(checks)
            if quality.has_critical_failures(checks):
                quarantined += 1
                continue
            duplicate = self.metadata_store.raw_item_version_exists(
                logical_dataset=self.logical_dataset,
                provider_id=self.provider_id,
                source_item_key=observed["source_item_key"],
                content_hash=raw.content_hash,
            )
            if duplicate:
                duplicates += 1
                continue
            self.metadata_store.insert_raw_item_version(
                logical_dataset=self.logical_dataset,
                provider_id=self.provider_id,
                source_id=self.source_id,
                source_item_key=observed["source_item_key"],
                title=observed["title"],
                source_url=observed["source_url"],
                source_publish_time=observed["source_publish_time"],
                first_seen_at=raw.first_seen_at,
                stored_at=raw.stored_at,
                raw_object_id=raw.raw_object_id,
                content_hash=raw.content_hash,
                dedup_hash=sha256_json(
                    {
                        "provider_id": self.provider_id,
                        "source_url": observed["source_url"],
                    }
                ),
                quality_status="pass",
                observed_payload=observed,
            )
            inserted += 1
        return {"inserted": inserted, "duplicates": duplicates, "quarantined": quarantined}

    def _normalize_record(self, *, record: PolicyListRecord, raw: Any) -> dict[str, Any]:
        source_item_key = (
            f"{self.provider_id}:{sha256_json({'source_url': record.source_url})[7:23]}"
        )
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": source_item_key,
            "title": record.title,
            "source_url": record.source_url,
            "source_department": record.source_department,
            "category": record.category,
            "source_publish_time": record.source_publish_time,
            "metric_payload": {
                "raw_context": _json_safe(record.raw_context),
            },
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }

    def _record_source_error(self, *, run_id: str, observed_value: str, sample_key: str) -> None:
        self.metadata_store.insert_quality_results(
            [
                CheckResult(
                    check_name=self.default_check_name,
                    check_type="source_error",
                    severity="critical",
                    status="fail",
                    expected_value="successful official policy HTML list response",
                    observed_value=observed_value,
                    failed_count=1,
                    sample_failed_keys=[sample_key],
                    run_id=run_id,
                    logical_dataset=self.logical_dataset,
                    source_id=self.source_id,
                )
            ]
        )
