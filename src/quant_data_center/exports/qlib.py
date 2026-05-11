"""Minimal Qlib day-frequency exporter."""

from __future__ import annotations

import hashlib
import logging
import math
import struct
from pathlib import Path
from typing import Any

from quant_data_center.settings import QdcSettings
from quant_data_center.storage.database import QdcDatabase


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
    "announcement_count": "announcement_count",
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

        object_ids = [self._index_file(path) for path in written_files]
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
                  n.news_count,
                  af.announcement_count
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

    def _index_file(self, path: Path) -> str:
        content = path.read_bytes()
        return self.database.insert_source_object(
            dataset="qlib_export",
            source_id="qdc",
            layer="qlib",
            uri=str(path),
            content_hash=hashlib.sha256(content).hexdigest(),
            size_bytes=len(content),
        )


class QlibProviderVerifier:
    """Verify that a QDC exported provider can be read through Qlib."""

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
    ) -> dict[str, Any]:
        root = Path(provider_uri).expanduser() if provider_uri else self.settings.qlib_root / "cn_data"
        if not root.is_absolute():
            root = (self.settings.project_root / root).resolve()
        qlib_fields = fields or ["$close", "$vwap", "$volume", "$announcement_count", "$news_count"]
        result: dict[str, Any] = {
            "status": "ok",
            "provider_uri": str(root),
            "requested_instruments": instruments,
            "fields": qlib_fields,
        }
        if not root.exists():
            return {
                **result,
                "status": "fail",
                "error_type": "missing_provider",
                "message": f"Qlib provider_uri does not exist: {root}",
            }
        try:
            qlib = __import__("qlib")
            data_module = __import__("qlib.data", fromlist=["D"])
        except ImportError as exc:
            return {
                **result,
                "status": "fail",
                "error_type": "missing_dependency",
                "message": (
                    "Qlib is not installed. Install local Qlib with "
                    "`python -m pip install -e /root/code/qlib "
                    "-i https://pypi.tuna.tsinghua.edu.cn/simple "
                    "--extra-index-url https://mirrors.aliyun.com/pypi/simple`."
                ),
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
        issues = _verify_provider_coverage(
            root=root,
            instrument_pool=[str(item) for item in instrument_pool],
            requested_instruments=instruments,
            fields=qlib_fields,
            feature_row_count=len(features),
        )
        return {
            **result,
            "status": "fail" if issues else "ok",
            "calendar_count": int(len(calendar)),
            "instrument_count": int(len(instrument_pool)),
            "feature_row_count": int(len(features)),
            "issues": issues,
            "preview": _dataframe_preview(features),
        }


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


def _verify_provider_coverage(
    *,
    root: Path,
    instrument_pool: list[str],
    requested_instruments: list[str],
    fields: list[str],
    feature_row_count: int,
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    pool = {instrument.lower() for instrument in instrument_pool}
    requested = [instrument.lower() for instrument in requested_instruments]
    missing_instruments = [instrument for instrument in requested if instrument not in pool]
    if missing_instruments:
        issues.append({"issue_type": "missing_instruments", "instruments": missing_instruments})
    if feature_row_count == 0:
        issues.append({"issue_type": "empty_features"})

    available_requested = [instrument for instrument in requested if instrument in pool]
    missing_feature_files = []
    for field in fields:
        feature_name = _raw_qlib_feature_name(field)
        if not feature_name:
            continue
        for instrument in available_requested:
            path = root / "features" / instrument / f"{feature_name}.day.bin"
            if not path.exists():
                missing_feature_files.append(str(path.relative_to(root)))
    if missing_feature_files:
        issues.append(
            {
                "issue_type": "missing_feature_files",
                "paths": sorted(missing_feature_files),
            }
        )
    return issues


def _raw_qlib_feature_name(field: str) -> str:
    if not field.startswith("$"):
        return ""
    return field[1:].strip()
