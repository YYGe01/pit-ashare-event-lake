"""Deterministic daily factor builders for QDC."""

from __future__ import annotations

from typing import Any

from quant_data_center.factor_engine import (
    build_announcement_factor_rows,
    build_news_factor_rows,
)
from quant_data_center.settings import QdcSettings
from quant_data_center.storage.database import QdcDatabase
from quant_data_center.storage.silver import SilverStore


SUPPORTED_FACTOR_SETS = {"all", "news_v1", "announcement_v1"}


class FactorBuilder:
    """Build simple daily count factors from normalized silver tables."""

    def __init__(self, settings: QdcSettings) -> None:
        self.settings = settings
        self.database = QdcDatabase(settings)
        self.silver = SilverStore(settings)

    def build(self, *, factor_set: str, start_date: str, end_date: str) -> dict[str, Any]:
        if factor_set not in SUPPORTED_FACTOR_SETS:
            supported = ", ".join(sorted(SUPPORTED_FACTOR_SETS))
            raise ValueError(f"unsupported factor_set: {factor_set}; supported: {supported}")
        results = []
        if factor_set in {"all", "news_v1"}:
            news_rows = build_news_factor_rows(
                self.database,
                start_date=start_date,
                end_date=end_date,
                source_id="qdc_news_v1",
            )
            results.append(
                {
                    "factor_set": "news_v1",
                    "row_count": self.silver.upsert_daily_news_factor(news_rows),
                }
            )
        if factor_set in {"all", "announcement_v1"}:
            announcement_rows = build_announcement_factor_rows(
                self.database,
                start_date=start_date,
                end_date=end_date,
                source_id="qdc_announcement_v1",
            )
            results.append(
                {
                    "factor_set": "announcement_v1",
                    "row_count": self.silver.upsert_daily_announcement_factor(announcement_rows),
                }
            )
        total_rows = sum(item["row_count"] for item in results)
        job_id = self.database.record_job_run(
            job_type="build_factors",
            status="success",
            dataset=factor_set,
            source_id="qdc",
            start_date=start_date,
            end_date=end_date,
            parameters={"results": results, "row_count": total_rows},
        )
        return {"status": "ok", "job_id": job_id, "row_count": total_rows, "results": results}
