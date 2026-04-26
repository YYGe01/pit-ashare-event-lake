"""CZCE official daily commodity futures connector."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import requests

from pitlake.connectors.base import BaseConnector, RequestPlan, RunStats
from pitlake.quality.checks import CheckResult, QualityRunner
from pitlake.utils import sha256_json

CZCE_DAILY_URL = (
    "https://www.czce.com.cn/cn/DFSStaticFiles/Future/{year}/{date}/FutureDataDaily.txt"
)


def _date_to_iso(value: Any) -> str:
    text = str(value).strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return datetime.strptime(text[:10], "%Y-%m-%d").date().isoformat()
    return datetime.strptime(text.replace("-", "")[:8], "%Y%m%d").date().isoformat()


def _date_to_yyyymmdd(value: Any) -> str:
    return _date_to_iso(value).replace("-", "")


def _as_float(value: Any) -> float | None:
    text = str(value or "").replace(",", "").strip()
    if text in {"", "-", "—"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _as_int(value: Any) -> int | None:
    parsed = _as_float(value)
    return int(parsed) if parsed is not None else None


class CzceDailyConnector(BaseConnector):
    """Collect CZCE futures daily rows from the official static text file."""

    connector_version = "0.1.0"

    def plan_requests(self) -> list[RequestPlan]:
        return []

    def collect(self, *, run_id: str, options: dict[str, Any] | None = None) -> RunStats:
        options = options or {}
        default_options = self.source_config.get("default_options", {})
        query_date = _date_to_yyyymmdd(options.get("end_date") or default_options["end_date"])
        limit_rows = options.get("limit_rows") or default_options.get("limit_rows")
        url = CZCE_DAILY_URL.format(year=query_date[:4], date=query_date)
        stats = RunStats(request_count=1)
        quality = QualityRunner()

        try:
            response = requests.get(url, headers=self._headers(), timeout=self._timeout(default_options))
            response.raise_for_status()
            text = response.text
            records = self._parse_records(text)
            if limit_rows:
                records = records[: int(limit_rows)]
            raw = self.raw_store.put_bytes(
                source_id=self.source_id,
                provider_id=self.provider_id,
                logical_dataset=self.logical_dataset,
                content=response.content,
                extension="txt",
                mime_type=response.headers.get("content-type", "text/plain"),
                run_id=run_id,
                filename_prefix=f"czce_daily_{query_date}",
                metadata={"query_date": query_date, "row_count": len(records)},
            )
            self.metadata_store.insert_raw_object(
                raw,
                request_hash=sha256_json({"url": url}),
                request_url=url,
                request_params={"date": query_date},
            )
            raw_checks = quality.check_raw_write(raw)
            self.metadata_store.insert_quality_results(raw_checks)
            if quality.has_critical_failures(raw_checks):
                stats.quarantine_count += len(records)
                return stats
            row_count = self._persist_records(
                records=records,
                trading_date=_date_to_iso(query_date),
                source_url=url,
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
                sample_key=query_date,
            )
            stats.error_count = 1
        return stats

    def _headers(self) -> dict[str, str]:
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
            "Accept": "text/plain,*/*;q=0.8",
        }

    def _timeout(self, default_options: dict[str, Any]) -> int:
        return int(default_options.get("timeout_seconds") or 20)

    def _parse_records(self, text: str) -> list[dict[str, str]]:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        header_index = next(
            (index for index, line in enumerate(lines) if line.startswith("合约代码|")),
            None,
        )
        if header_index is None:
            raise ValueError("CZCE daily file is missing table header")
        headers = [part.strip() for part in lines[header_index].split("|")]
        records: list[dict[str, str]] = []
        for line in lines[header_index + 1 :]:
            columns = [part.strip() for part in line.split("|")]
            if not columns or columns[0] in {"小计", "总计", "合计"}:
                continue
            if len(columns) < len(headers):
                columns += [""] * (len(headers) - len(columns))
            record = dict(zip(headers, columns, strict=False))
            if record.get("合约代码"):
                records.append(record)
        return records

    def _persist_records(
        self,
        *,
        records: list[dict[str, str]],
        trading_date: str,
        source_url: str,
        raw: Any,
        run_id: str,
        quality: QualityRunner,
    ) -> dict[str, int]:
        inserted = 0
        duplicates = 0
        quarantined = 0
        for record in records:
            observed = self._normalize_record(record=record, trading_date=trading_date, raw=raw)
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
                title=f"CZCE {observed['contract']} commodity daily {observed['trading_date']}",
                source_url=source_url,
                first_seen_at=raw.first_seen_at,
                stored_at=raw.stored_at,
                raw_object_id=raw.raw_object_id,
                content_hash=raw.content_hash,
                dedup_hash=sha256_json(
                    {
                        "provider_id": self.provider_id,
                        "exchange": observed["exchange"],
                        "contract": observed["contract"],
                        "trading_date": observed["trading_date"],
                    }
                ),
                quality_status="pass",
                observed_payload=observed,
            )
            inserted += 1
        return {"inserted": inserted, "duplicates": duplicates, "quarantined": quarantined}

    def _normalize_record(self, *, record: dict[str, str], trading_date: str, raw: Any) -> dict[str, Any]:
        contract = str(record.get("合约代码") or "").strip()
        return {
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_item_key": f"{self.provider_id}:CZCE:{contract}:{trading_date}",
            "exchange": "CZCE",
            "contract": contract,
            "trading_date": trading_date,
            "symbol": "".join(char for char in contract if char.isalpha()) or None,
            "open": _as_float(record.get("今开盘")),
            "high": _as_float(record.get("最高价")),
            "low": _as_float(record.get("最低价")),
            "close": _as_float(record.get("今收盘")),
            "settlement": _as_float(record.get("今结算")),
            "prev_settlement": _as_float(record.get("昨结算")),
            "volume": _as_int(record.get("成交量(手)")),
            "open_interest": _as_int(record.get("持仓量")),
            "session": "daily",
            "metric_payload": {
                "open_interest_change": _as_int(record.get("增减量")),
                "turnover_10k_cny": _as_float(record.get("成交额(万元)")),
                "delivery_settlement": _as_float(record.get("交割结算价")),
            },
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
        }

    def _record_source_error(self, *, run_id: str, observed_value: str, sample_key: str) -> None:
        self.metadata_store.insert_quality_results(
            [
                CheckResult(
                    check_name="czce_future_data_daily_request",
                    check_type="source_error",
                    severity="critical",
                    status="fail",
                    expected_value="successful CZCE daily text response",
                    observed_value=observed_value,
                    failed_count=1,
                    sample_failed_keys=[sample_key],
                    run_id=run_id,
                    logical_dataset=self.logical_dataset,
                    source_id=self.source_id,
                )
            ]
        )
