"""Minimal Qlib day-frequency exporter."""

from __future__ import annotations

import hashlib
import logging
import math
import struct
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from quant_data_center.settings import QdcSettings
from quant_data_center.storage.database import QdcDatabase


BASE_PROVIDER_FIELDS = ["$close", "$volume", "$factor"]
QLIB_FIELDS = {
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "vwap": "vwap",
    "volume": "volume",
    "amount": "amount",
    "factor": "adj_factor",
    "limit_up": "limit_up",
    "limit_down": "limit_down",
    "news_count": "news_count",
    "news_sentiment_mean": "news_sentiment_mean",
    "news_positive_count": "news_positive_count",
    "news_negative_count": "news_negative_count",
    "news_growth_count": "news_growth_count",
    "news_risk_count": "news_risk_count",
    "news_financing_count": "news_financing_count",
    "news_weighted_sentiment_sum": "news_weighted_sentiment_sum",
    "news_importance_sum": "news_importance_sum",
    "news_contract_count": "news_contract_count",
    "news_buyback_count": "news_buyback_count",
    "news_shareholder_change_count": "news_shareholder_change_count",
    "news_regulatory_count": "news_regulatory_count",
    "news_litigation_count": "news_litigation_count",
    "news_performance_count": "news_performance_count",
    "announcement_count": "announcement_count",
    "announcement_growth_count": "announcement_growth_count",
    "announcement_risk_count": "announcement_risk_count",
    "announcement_financing_count": "announcement_financing_count",
    "announcement_operation_count": "announcement_operation_count",
    "announcement_sentiment_mean": "announcement_sentiment_mean",
    "announcement_positive_count": "announcement_positive_count",
    "announcement_negative_count": "announcement_negative_count",
    "announcement_weighted_sentiment_sum": "announcement_weighted_sentiment_sum",
    "announcement_importance_sum": "announcement_importance_sum",
    "announcement_contract_count": "announcement_contract_count",
    "announcement_buyback_count": "announcement_buyback_count",
    "announcement_shareholder_change_count": "announcement_shareholder_change_count",
    "announcement_regulatory_count": "announcement_regulatory_count",
    "announcement_litigation_count": "announcement_litigation_count",
    "announcement_performance_count": "announcement_performance_count",
}


class QlibExporter:
    """Export daily QDC silver/gold data into a Qlib-compatible directory."""

    def __init__(self, settings: QdcSettings) -> None:
        self.settings = settings
        self.database = QdcDatabase(settings)

    def export(
        self,
        *,
        provider_uri: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        market_name: str | None = None,
    ) -> dict[str, Any]:
        root = Path(provider_uri).expanduser() if provider_uri else self.settings.qlib_root / "cn_data"
        if not root.is_absolute():
            root = (self.settings.project_root / root).resolve()
        rows = self._load_rows(start_date=start_date, end_date=end_date)
        if not rows:
            raise ValueError("no daily_bar rows available for qdc export-qlib")

        calendars = sorted({str(row["trade_date"]) for row in rows})
        instruments = sorted({str(row["instrument"]).lower() for row in rows})
        calendar_index = {trade_date: index for index, trade_date in enumerate(calendars)}
        rows_by_instrument: dict[str, dict[str, dict[str, Any]]] = {}
        for row in rows:
            instrument = str(row["instrument"]).lower()
            rows_by_instrument.setdefault(instrument, {})[str(row["trade_date"])] = row

        written_files = []
        calendar_path = root / "calendars" / "day.txt"
        calendar_path.parent.mkdir(parents=True, exist_ok=True)
        calendar_path.write_text("\n".join(calendars) + "\n", encoding="utf-8")
        written_files.append(calendar_path)

        instrument_lines = []
        for instrument in instruments:
            dates = sorted(rows_by_instrument[instrument])
            instrument_lines.append(f"{instrument}\t{dates[0]}\t{dates[-1]}")
        instruments_path = root / "instruments" / "all.txt"
        instruments_path.parent.mkdir(parents=True, exist_ok=True)
        instruments_path.write_text("\n".join(instrument_lines) + "\n", encoding="utf-8")
        written_files.append(instruments_path)
        if market_name:
            market_path = root / "instruments" / f"{market_name}.txt"
            market_path.write_text("\n".join(instrument_lines) + "\n", encoding="utf-8")
            written_files.append(market_path)

        for instrument in instruments:
            instrument_rows = rows_by_instrument[instrument]
            dates = sorted(instrument_rows)
            start_index = calendar_index[dates[0]]
            end_index = calendar_index[dates[-1]]
            instrument_dir = root / "features" / instrument
            instrument_dir.mkdir(parents=True, exist_ok=True)
            for feature_name, row_field in QLIB_FIELDS.items():
                values = [float(start_index)]
                for index in range(start_index, end_index + 1):
                    row = instrument_rows.get(calendars[index])
                    values.append(_as_float(row.get(row_field) if row else None))
                path = instrument_dir / f"{feature_name}.day.bin"
                path.write_bytes(b"".join(struct.pack("<f", value) for value in values))
                written_files.append(path)

        object_ids = self._index_files(written_files)
        job_id = self.database.record_job_run(
            job_type="export_qlib",
            status="success",
            dataset="qlib_export",
            source_id="qdc",
            start_date=start_date,
            end_date=end_date,
            parameters={
                "provider_uri": str(root),
                "market_name": market_name,
                "calendar_count": len(calendars),
                "instrument_count": len(instruments),
                "file_count": len(written_files),
            },
        )
        return {
            "status": "ok",
            "provider_uri": str(root),
            "market_name": market_name,
            "job_id": job_id,
            "calendar_count": len(calendars),
            "instrument_count": len(instruments),
            "file_count": len(written_files),
            "object_ids": object_ids,
        }

    def _load_rows(
        self,
        *,
        start_date: str | None,
        end_date: str | None,
    ) -> list[dict[str, Any]]:
        filters = []
        params: list[Any] = []
        if start_date:
            filters.append("b.trade_date >= ?")
            params.append(start_date)
        if end_date:
            filters.append("b.trade_date <= ?")
            params.append(end_date)
        where_clause = f"where {' and '.join(filters)}" if filters else ""
        with self.database.connect() as conn:
            rows = conn.execute(
                f"""
                select
                  b.trade_date,
                  b.instrument,
                  b.open,
                  b.high,
                  b.low,
                  b.close,
                  b.vwap,
                  b.volume,
                  b.amount,
                  a.adj_factor,
                  p.limit_up,
                  p.limit_down,
                  coalesce(n.news_count, 0) as news_count,
                  coalesce(n.news_sentiment_mean, 0) as news_sentiment_mean,
                  coalesce(n.news_positive_count, 0) as news_positive_count,
                  coalesce(n.news_negative_count, 0) as news_negative_count,
                  coalesce(n.news_growth_count, 0) as news_growth_count,
                  coalesce(n.news_risk_count, 0) as news_risk_count,
                  coalesce(n.news_financing_count, 0) as news_financing_count,
                  coalesce(n.news_weighted_sentiment_sum, 0) as news_weighted_sentiment_sum,
                  coalesce(n.news_importance_sum, 0) as news_importance_sum,
                  coalesce(n.news_contract_count, 0) as news_contract_count,
                  coalesce(n.news_buyback_count, 0) as news_buyback_count,
                  coalesce(n.news_shareholder_change_count, 0) as news_shareholder_change_count,
                  coalesce(n.news_regulatory_count, 0) as news_regulatory_count,
                  coalesce(n.news_litigation_count, 0) as news_litigation_count,
                  coalesce(n.news_performance_count, 0) as news_performance_count,
                  coalesce(af.announcement_count, 0) as announcement_count,
                  coalesce(af.announcement_growth_count, 0) as announcement_growth_count,
                  coalesce(af.announcement_risk_count, 0) as announcement_risk_count,
                  coalesce(af.announcement_financing_count, 0) as announcement_financing_count,
                  coalesce(af.announcement_operation_count, 0) as announcement_operation_count,
                  coalesce(af.announcement_sentiment_mean, 0) as announcement_sentiment_mean,
                  coalesce(af.announcement_positive_count, 0) as announcement_positive_count,
                  coalesce(af.announcement_negative_count, 0) as announcement_negative_count,
                  coalesce(af.announcement_weighted_sentiment_sum, 0)
                    as announcement_weighted_sentiment_sum,
                  coalesce(af.announcement_importance_sum, 0) as announcement_importance_sum,
                  coalesce(af.announcement_contract_count, 0) as announcement_contract_count,
                  coalesce(af.announcement_buyback_count, 0) as announcement_buyback_count,
                  coalesce(af.announcement_shareholder_change_count, 0)
                    as announcement_shareholder_change_count,
                  coalesce(af.announcement_regulatory_count, 0) as announcement_regulatory_count,
                  coalesce(af.announcement_litigation_count, 0) as announcement_litigation_count,
                  coalesce(af.announcement_performance_count, 0) as announcement_performance_count
                from qdc_silver.daily_bar b
                left join qdc_silver.adj_factor a
                  on b.trade_date = a.trade_date and b.instrument = a.instrument
                left join qdc_silver.price_limit p
                  on b.trade_date = p.trade_date and b.instrument = p.instrument
                left join qdc_silver.daily_news_factor n
                  on b.trade_date = n.trade_date and b.instrument = n.instrument
                left join qdc_silver.daily_announcement_factor af
                  on b.trade_date = af.trade_date and b.instrument = af.instrument
                {where_clause}
                order by b.trade_date, b.instrument
                """,
                params,
            ).fetchall()
            columns = [item[0] for item in conn.description]
        return [
            {
                column: value.isoformat() if hasattr(value, "isoformat") else value
                for column, value in zip(columns, row, strict=True)
            }
            for row in rows
        ]

    def _index_files(self, paths: list[Path]) -> list[str]:
        objects = []
        for path in paths:
            content = path.read_bytes()
            objects.append(
                {
                    "dataset": "qlib_export",
                    "source_id": "qdc",
                    "layer": "qlib",
                    "uri": str(path),
                    "content_hash": hashlib.sha256(content).hexdigest(),
                    "size_bytes": len(content),
                }
            )
        return self.database.insert_source_objects(objects)


class QlibProviderVerifier:
    """Verify that a Qlib base provider can be read through Qlib."""

    def __init__(self, settings: QdcSettings) -> None:
        self.settings = settings

    def verify(
        self,
        *,
        provider_uri: str | Path | None = None,
        start_date: str,
        end_date: str,
        instruments: list[str],
        fields: list[str] | None = None,
        expected_latest_date: str | None = None,
    ) -> dict[str, Any]:
        qlib_fields = _normalize_qlib_fields(
            fields or self.settings.qlib_provider.required_fields or BASE_PROVIDER_FIELDS
        )
        file_status = inspect_qlib_provider(
            self.settings,
            provider_uri=provider_uri,
            instruments=instruments,
            fields=qlib_fields,
            expected_latest_date=expected_latest_date or end_date,
        )
        root = Path(file_status["provider_uri"])
        result: dict[str, Any] = {
            **file_status,
            "provider_uri": str(root),
            "requested_instruments": instruments,
            "fields": qlib_fields,
        }
        if not root.exists():
            return result
        try:
            qlib = __import__("qlib")
            data_module = __import__("qlib.data", fromlist=["D"])
        except ImportError as exc:
            issues = [
                *result.get("issues", []),
                {
                    "severity": "error",
                    "issue_type": "missing_dependency",
                    "message": (
                        "Qlib is not installed. Install local Qlib with "
                        "`python -m pip install -e E:\\code\\qlib`."
                    ),
                    "error_message": str(exc),
                },
            ]
            return {
                **result,
                "status": _provider_status_from_issues(issues),
                "issues": issues,
                "error_type": "missing_dependency",
                "message": "Qlib is not installed. Install local Qlib with `python -m pip install -e E:\\code\\qlib`.",
                "error_message": str(exc),
            }
        D = data_module.D
        qlib.init(provider_uri=str(root), region="cn", logging_level=logging.WARNING)
        calendar = D.calendar(start_time=start_date, end_time=end_date, freq="day")
        instrument_pool = D.list_instruments(
            D.instruments("all"),
            start_time=start_date,
            end_time=end_date,
            as_list=True,
        )
        features = D.features(
            instruments,
            qlib_fields,
            start_time=start_date,
            end_time=end_date,
            freq="day",
        )
        issues = [
            *result.get("issues", []),
            *_verify_provider_coverage(
                instrument_pool=[str(item) for item in instrument_pool],
                requested_instruments=instruments,
                feature_row_count=len(features),
            ),
            *_verify_sample_fields(features, qlib_fields),
        ]
        return {
            **result,
            "status": _provider_status_from_issues(issues),
            "calendar_count": int(len(calendar)),
            "instrument_count": int(len(instrument_pool)),
            "feature_row_count": int(len(features)),
            "issues": issues,
            "preview": _dataframe_preview(features),
        }


def inspect_qlib_provider(
    settings: QdcSettings,
    *,
    provider_uri: str | Path | None = None,
    instruments: list[str] | None = None,
    fields: list[str] | None = None,
    expected_latest_date: str | None = None,
) -> dict[str, Any]:
    """Inspect provider files without importing Qlib or touching the database."""

    root = _resolve_base_provider_root(settings, provider_uri=provider_uri)
    qlib_fields = _normalize_qlib_fields(
        fields or settings.qlib_provider.required_fields or BASE_PROVIDER_FIELDS
    )
    expected_date = expected_latest_date or latest_complete_trading_date(settings.timezone)
    calendar = _read_day_calendar(root)
    sample_instruments = [
        _qlib_instrument_name(item)
        for item in (instruments or _discover_feature_instruments(root, limit=3))
        if _qlib_instrument_name(item)
    ]
    issues: list[dict[str, Any]] = []
    if not root.exists():
        issues.append(
            {
                "severity": "error",
                "issue_type": "missing_provider",
                "message": f"Qlib provider_uri does not exist: {root}",
            }
        )
    else:
        if calendar["status"] != "ok":
            issues.append(
                {
                    "severity": "error",
                    "issue_type": calendar["status"],
                    "message": calendar["message"],
                    "path": str(calendar["path"]),
                }
            )
        elif calendar["latest_date"] and calendar["latest_date"] < expected_date:
            issues.append(
                {
                    "severity": "warning",
                    "issue_type": "stale_calendar",
                    "message": (
                        f"Qlib calendar latest date {calendar['latest_date']} is older "
                        f"than expected {expected_date}"
                    ),
                    "latest_date": calendar["latest_date"],
                    "expected_latest_date": expected_date,
                }
            )
        if not sample_instruments:
            issues.append(
                {
                    "severity": "error",
                    "issue_type": "missing_sample_instruments",
                    "message": "No sample instruments were provided or discovered under features/",
                }
            )
    feature_files = _feature_file_status(
        root=root,
        instruments=sample_instruments,
        fields=qlib_fields,
    )
    for item in feature_files:
        if not item["exists"]:
            issues.append(
                {
                    "severity": "error",
                    "issue_type": "missing_feature_file",
                    "instrument": item["instrument"],
                    "field": item["field"],
                    "path": item["relative_path"],
                    "message": f"Missing Qlib feature file: {item['relative_path']}",
                }
            )
    return {
        "status": _provider_status_from_issues(issues),
        "provider_uri": str(root),
        "calendar_path": str(calendar["path"]),
        "calendar_latest_date": calendar.get("latest_date"),
        "calendar_count": calendar.get("calendar_count", 0),
        "expected_latest_date": expected_date,
        "sample_instruments": sample_instruments,
        "fields": qlib_fields,
        "feature_files": feature_files,
        "issues": issues,
    }


def qlib_provider_stock_instruments(
    settings: QdcSettings,
    *,
    provider_uri: str | Path | None = None,
    trade_date: str | None = None,
) -> list[str]:
    """Return stock instruments from the configured Qlib provider instruments file."""

    root = _resolve_base_provider_root(settings, provider_uri=provider_uri)
    instruments_path = root / "instruments" / "all.txt"
    if not instruments_path.exists():
        return []
    instruments: list[str] = []
    for line in instruments_path.read_text(encoding="utf-8").splitlines():
        parts = line.split("\t")
        if not parts:
            continue
        instrument = parts[0].strip().upper()
        if not _is_stock_instrument(instrument):
            continue
        if trade_date and len(parts) >= 3:
            start_date = parts[1].strip()
            end_date = parts[2].strip()
            if start_date and trade_date < start_date:
                continue
            if end_date and trade_date > end_date:
                continue
        instruments.append(instrument)
    return sorted(set(instruments))


def latest_complete_trading_date(timezone: str, now: datetime | None = None) -> str:
    """Return a conservative latest complete trading day using weekdays and a post-close cutoff."""

    current = now or datetime.now(ZoneInfo(timezone))
    candidate = current.date()
    if candidate.weekday() >= 5 or current.hour < 18:
        candidate -= timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate.isoformat()


def _resolve_base_provider_root(
    settings: QdcSettings,
    *,
    provider_uri: str | Path | None,
) -> Path:
    raw = provider_uri or settings.qlib_provider.provider_uri or settings.daily_pipeline.provider_uri
    root = Path(raw).expanduser() if raw else settings.qlib_root / "cn_data"
    if not root.is_absolute():
        root = (settings.project_root / root).resolve()
    return root


def _normalize_qlib_fields(fields: list[str] | tuple[str, ...]) -> list[str]:
    normalized = []
    for field in fields:
        text = str(field).strip()
        if not text:
            continue
        normalized.append(text if text.startswith("$") else f"${text}")
    return normalized or list(BASE_PROVIDER_FIELDS)


def _read_day_calendar(root: Path) -> dict[str, Any]:
    path = root / "calendars" / "day.txt"
    if not path.exists():
        return {
            "status": "missing_calendar",
            "path": path,
            "latest_date": None,
            "calendar_count": 0,
            "message": f"Qlib calendar file does not exist: {path}",
        }
    lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]
    dates = [line for line in lines if line]
    if not dates:
        return {
            "status": "empty_calendar",
            "path": path,
            "latest_date": None,
            "calendar_count": 0,
            "message": f"Qlib calendar file is empty: {path}",
        }
    return {
        "status": "ok",
        "path": path,
        "latest_date": dates[-1],
        "calendar_count": len(dates),
        "message": "ok",
    }


def _discover_feature_instruments(root: Path, *, limit: int) -> list[str]:
    feature_root = root / "features"
    if not feature_root.exists():
        return []
    return [item.name for item in sorted(feature_root.iterdir()) if item.is_dir()][:limit]


def _feature_file_status(
    *,
    root: Path,
    instruments: list[str],
    fields: list[str],
) -> list[dict[str, Any]]:
    rows = []
    for instrument in instruments:
        qlib_instrument = _qlib_instrument_name(instrument)
        for field in fields:
            feature_name = _raw_qlib_feature_name(field)
            relative_path = Path("features") / qlib_instrument / f"{feature_name}.day.bin"
            path = root / relative_path
            rows.append(
                {
                    "instrument": qlib_instrument,
                    "field": field,
                    "relative_path": str(relative_path).replace("\\", "/"),
                    "exists": path.exists(),
                    "size_bytes": path.stat().st_size if path.exists() else None,
                }
            )
    return rows


def _qlib_instrument_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if "." in text:
        suffix, symbol = text.split(".", 1)
        if suffix in {"sh", "sz"} and symbol:
            return f"{suffix}{symbol}"
    return text


def _is_stock_instrument(instrument: str) -> bool:
    if len(instrument) != 8:
        return False
    exchange = instrument[:2]
    code = instrument[2:]
    if not code.isdigit():
        return False
    if exchange == "BJ":
        return True
    if exchange == "SH":
        return code.startswith(("60", "68", "90"))
    if exchange == "SZ":
        return code.startswith(("00", "20", "30"))
    return False


def _provider_status_from_issues(issues: list[dict[str, Any]]) -> str:
    if any(item.get("severity") == "error" for item in issues):
        return "fail"
    if issues:
        return "warning"
    return "ok"


def _verify_sample_fields(features: Any, fields: list[str]) -> list[dict[str, Any]]:
    if len(features) == 0:
        return []
    issues = []
    columns = {str(column) for column in getattr(features, "columns", [])}
    for field in fields:
        if field not in columns:
            issues.append(
                {
                    "severity": "error",
                    "issue_type": "missing_sample_field",
                    "field": field,
                    "message": f"Qlib D.features did not return field {field}",
                }
            )
            continue
        try:
            series = features[field]
            has_value = bool(series.notna().any())
        except Exception:
            has_value = False
        if not has_value:
            issues.append(
                {
                    "severity": "error",
                    "issue_type": "empty_sample_field",
                    "field": field,
                    "message": f"Qlib D.features returned no non-null samples for {field}",
                }
            )
    return issues


def _verify_provider_coverage(
    *,
    instrument_pool: list[str],
    requested_instruments: list[str],
    feature_row_count: int,
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    pool = {instrument.lower() for instrument in instrument_pool}
    requested = [_qlib_instrument_name(instrument) for instrument in requested_instruments]
    missing_instruments = [instrument for instrument in requested if instrument not in pool]
    if missing_instruments:
        issues.append(
            {
                "severity": "error",
                "issue_type": "missing_instruments",
                "instruments": missing_instruments,
            }
        )
    if feature_row_count == 0:
        issues.append({"severity": "error", "issue_type": "empty_features"})
    return issues


def _raw_qlib_feature_name(field: str) -> str:
    if not field.startswith("$"):
        return field.strip()
    return field[1:].strip()


def _as_float(value: Any) -> float:
    if value is None:
        return math.nan
    try:
        result = float(value)
    except (TypeError, ValueError):
        return math.nan
    if math.isnan(result):
        return math.nan
    return result


def _dataframe_preview(frame: Any) -> list[dict[str, Any]]:
    reset = frame.reset_index().head(10)
    return [
        {
            str(key): value.isoformat() if hasattr(value, "isoformat") else value
            for key, value in row.items()
        }
        for row in reset.to_dict("records")
    ]
