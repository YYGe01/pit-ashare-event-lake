"""Derived Parquet writers for QDC silver and gold layers."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pandas as pd

from quant_data_center.settings import QdcSettings
from quant_data_center.storage.database import QdcDatabase
from quant_data_center.storage.schema import SILVER_TABLES


SILVER_ORDER_BY = {
    "stock_basic": "instrument",
    "universe_constituent": "universe, snapshot_date, instrument",
    "trade_calendar": "calendar_id, trade_date",
    "daily_bar": "trade_date, instrument",
    "adj_factor": "trade_date, instrument",
    "price_limit": "trade_date, instrument",
    "trade_status": "trade_date, instrument",
    "announcement": "publish_date, instrument, announcement_id",
    "news": "publish_date, instrument, news_id",
    "daily_news_factor": "trade_date, instrument",
    "daily_announcement_factor": "trade_date, instrument",
}


class QdcParquetSync:
    """Synchronize DuckDB research tables into derived Parquet files."""

    def __init__(self, settings: QdcSettings) -> None:
        self.settings = settings
        self.database = QdcDatabase(settings)

    def sync(self, *, layer: str = "all", dataset: str | None = None) -> dict[str, Any]:
        if layer not in {"all", "silver", "gold"}:
            raise ValueError("layer must be one of: all, silver, gold")
        results: dict[str, Any] = {"silver": [], "gold": []}
        if layer in {"all", "silver"}:
            tables = [dataset] if dataset else list(SILVER_TABLES)
            for table in tables:
                if table not in SILVER_TABLES:
                    raise ValueError(f"unknown qdc_silver table: {table}")
                results["silver"].append(self.sync_silver_table(table))
        if layer in {"all", "gold"}:
            if dataset and dataset != "daily_research":
                results["gold"].append(
                    {"dataset": "daily_research", "written": False, "row_count": 0}
                )
            else:
                results["gold"].append(self.sync_gold_daily_research())
        results["written_count"] = sum(
            1
            for layer_results in (results["silver"], results["gold"])
            for item in layer_results
            if item.get("written")
        )
        return results

    def sync_silver_table(self, table: str) -> dict[str, Any]:
        order_by = SILVER_ORDER_BY[table]
        with self.database.connect() as conn:
            df = conn.execute(
                f"select * from qdc_silver.{table} order by {order_by}"
            ).fetchdf()
        path = self.settings.parquet_root / "silver" / table / "part-000.parquet"
        return self._write_dataframe(dataset=table, layer="silver", path=path, df=df)

    def sync_gold_daily_research(self) -> dict[str, Any]:
        with self.database.connect() as conn:
            df = conn.execute(
                """
                select
                  b.trade_date,
                  b.instrument,
                  b.open,
                  b.high,
                  b.low,
                  b.close,
                  b.pre_close,
                  b.volume,
                  b.amount,
                  b.vwap,
                  a.adj_factor,
                  a.factor_type,
                  p.limit_up,
                  p.limit_down,
                  p.prev_close as price_limit_prev_close,
                  p.limit_rule,
                  coalesce(s.trade_status, 'normal') as trade_status,
                  s.halt_reason,
                  coalesce(n.news_count, 0) as news_count,
                  coalesce(n.news_sentiment_mean, 0) as news_sentiment_mean,
                  coalesce(n.news_positive_count, 0) as news_positive_count,
                  coalesce(n.news_negative_count, 0) as news_negative_count,
                  coalesce(n.news_growth_count, 0) as news_growth_count,
                  coalesce(n.news_risk_count, 0) as news_risk_count,
                  coalesce(n.news_financing_count, 0) as news_financing_count,
                  coalesce(af.announcement_count, 0) as announcement_count,
                  coalesce(af.announcement_risk_count, 0) as announcement_risk_count,
                  coalesce(af.announcement_financing_count, 0) as announcement_financing_count,
                  coalesce(af.announcement_operation_count, 0) as announcement_operation_count,
                  b.source_id as daily_bar_source_id
                from qdc_silver.daily_bar b
                left join qdc_silver.adj_factor a
                  on b.trade_date = a.trade_date and b.instrument = a.instrument
                left join qdc_silver.price_limit p
                  on b.trade_date = p.trade_date and b.instrument = p.instrument
                left join qdc_silver.trade_status s
                  on b.trade_date = s.trade_date and b.instrument = s.instrument
                left join qdc_silver.daily_news_factor n
                  on b.trade_date = n.trade_date and b.instrument = n.instrument
                left join qdc_silver.daily_announcement_factor af
                  on b.trade_date = af.trade_date and b.instrument = af.instrument
                order by b.trade_date, b.instrument
                """
            ).fetchdf()
        path = self.settings.parquet_root / "gold" / "daily_research" / "part-000.parquet"
        return self._write_dataframe(dataset="daily_research", layer="gold", path=path, df=df)

    def _write_dataframe(
        self,
        *,
        dataset: str,
        layer: str,
        path: Path,
        df: pd.DataFrame,
    ) -> dict[str, Any]:
        path.parent.mkdir(parents=True, exist_ok=True)
        if df.empty:
            return {"dataset": dataset, "layer": layer, "written": False, "row_count": 0}
        df.to_parquet(path, index=False)
        content = path.read_bytes()
        object_id = self.database.insert_source_object(
            dataset=dataset,
            source_id="qdc",
            layer=layer,
            uri=str(path),
            content_hash=hashlib.sha256(content).hexdigest(),
            size_bytes=len(content),
        )
        return {
            "dataset": dataset,
            "layer": layer,
            "written": True,
            "row_count": int(len(df)),
            "uri": str(path),
            "object_id": object_id,
        }
