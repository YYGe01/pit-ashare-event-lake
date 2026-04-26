"""AkShare connector for A-share announcement index data."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pitlake.connectors.base import BaseConnector, RequestPlan, RunStats
from pitlake.quality.checks import CheckResult, QualityRunner
from pitlake.utils import sha256_json


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "item"):
        return _json_safe(value.item())
    if isinstance(value, float) and value != value:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _date_to_yyyymmdd(value: Any) -> str:
    compact = str(_json_safe(value)).replace("-", "")[:8]
    datetime.strptime(compact, "%Y%m%d")
    return compact


def _optional_date_to_iso(value: Any) -> str | None:
    value = _json_safe(value)
    if value in (None, "", "NaT"):
        return None
    text = str(value)
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return datetime.strptime(text[:10], "%Y-%m-%d").date().isoformat()
    try:
        return datetime.strptime(text.replace("-", "")[:8], "%Y%m%d").date().isoformat()
    except ValueError:
        return None


def _plain_symbol(symbol: Any) -> str | None:
    value = str(_json_safe(symbol) or "").strip()
    if not value:
        return None
    return value.zfill(6)


def _exchange_from_symbol(symbol: str | None) -> str | None:
    if symbol is None:
        return None
    if symbol.startswith("6"):
        return "SSE"
    if symbol.startswith(("0", "3")):
        return "SZSE"
    if symbol.startswith(("4", "8", "9")):
        return "BSE"
    return "UNKNOWN"


def _get_value(record: dict[str, Any], aliases: tuple[str, ...], index: int) -> Any:
    for alias in aliases:
        if alias in record:
            return record[alias]
    values = list(record.values())
    if index < len(values):
        return values[index]
    return None


class AkshareAnnouncementIndexConnector(BaseConnector):
    """Collect announcement index rows via akshare.stock_notice_report."""

    connector_version = "0.1.0"

    def plan_requests(self) -> list[RequestPlan]:
        return []

    def collect(self, *, run_id: str, options: dict[str, Any] | None = None) -> RunStats:
        options = options or {}
        akshare = __import__("akshare")
        stats = RunStats()
        quality = QualityRunner()
        default_options = self.source_config.get("default_options", {})
        query_date = _date_to_yyyymmdd(options.get("end_date") or default_options["end_date"])
        category = str(options.get("category") or default_options.get("category", "全部"))

        stats.request_count = 1
        try:
            df = akshare.stock_notice_report(symbol=category, date=query_date)
            limit_items = options.get("limit_items") or default_options.get("limit_items")
            if limit_items:
                df = df.head(int(limit_items))
            raw_payload = self._dataframe_payload(df=df, query_date=query_date, category=category)
            raw = self.raw_store.put_json(
                source_id=self.source_id,
                provider_id=self.provider_id,
                logical_dataset=self.logical_dataset,
                payload=raw_payload,
                run_id=run_id,
                filename_prefix=f"announcement_index_{query_date}",
                metadata={
                    "query_date": query_date,
                    "category": category,
                    "akshare_function": "stock_notice_report",
                },
            )
            self.metadata_store.insert_raw_object(
                raw,
                request_hash=sha256_json(
                    {"function": "stock_notice_report", "date": query_date, "category": category}
                ),
                request_url="akshare://stock_notice_report",
                request_params={"date": query_date, "category": category},
            )
            raw_checks = quality.check_raw_write(raw)
            self.metadata_store.insert_quality_results(raw_checks)
            if quality.has_critical_failures(raw_checks):
                stats.quarantine_count += len(df)
                return stats

            row_count = self._persist_rows(df=df, raw=raw, run_id=run_id, quality=quality)
            stats.success_count = 1
            stats.new_item_count = row_count["inserted"]
            stats.duplicate_count = row_count["duplicates"]
            stats.quarantine_count += row_count["quarantined"]
        except Exception as exc:
            self.metadata_store.insert_quality_results(
                [
                    CheckResult(
                        check_name="akshare_stock_notice_report_request",
                        check_type="source_error",
                        severity="critical",
                        status="fail",
                        expected_value="successful AkShare announcement index response",
                        observed_value=str(exc)[:1000],
                        failed_count=1,
                        sample_failed_keys=[query_date],
                        run_id=run_id,
                        logical_dataset=self.logical_dataset,
                        source_id=self.source_id,
                    )
                ]
            )
            stats.error_count = 1
        return stats

    def _dataframe_payload(self, *, df: Any, query_date: str, category: str) -> dict[str, Any]:
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "logical_dataset": self.logical_dataset,
            "function": "stock_notice_report",
            "params": {"date": query_date, "category": category},
            "columns": [str(column) for column in df.columns],
            "row_count": len(df),
            "records": self._records(df),
        }

    def _records(self, df: Any) -> list[dict[str, Any]]:
        records = []
        for record in df.to_dict(orient="records"):
            records.append({str(key): _json_safe(value) for key, value in record.items()})
        return records

    def _persist_rows(
        self,
        *,
        df: Any,
        raw: Any,
        run_id: str,
        quality: QualityRunner,
    ) -> dict[str, int]:
        inserted = 0
        duplicates = 0
        quarantined = 0
        for record in df.to_dict(orient="records"):
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
                        "title": observed["title"],
                    }
                ),
                quality_status="pass",
                observed_payload=observed,
            )
            inserted += 1
        return {"inserted": inserted, "duplicates": duplicates, "quarantined": quarantined}

    def _normalize_record(self, *, record: dict[str, Any], raw: Any) -> dict[str, Any]:
        instrument = _plain_symbol(_get_value(record, ("代码", "stock_code"), 0))
        title = str(_get_value(record, ("公告标题", "title"), 2) or "").strip()
        category = _get_value(record, ("公告类型", "column_name"), 3)
        source_publish_time = _optional_date_to_iso(
            _get_value(record, ("公告日期", "notice_date"), 4)
        )
        source_url = str(_get_value(record, ("网址", "url"), 5) or "").strip()
        source_item_key = sha256_json(
            {
                "source_url": source_url,
                "title": title,
                "source_publish_time": source_publish_time,
            }
        )
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": source_item_key,
            "title": title,
            "source_url": source_url,
            "announcement_id": source_item_key.split(":", 1)[1][:16],
            "instrument": instrument,
            "exchange": _exchange_from_symbol(instrument),
            "security_name": _get_value(record, ("名称", "short_name"), 1),
            "category": _json_safe(category),
            "source_publish_time": source_publish_time,
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }
