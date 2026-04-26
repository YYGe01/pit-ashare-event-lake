import os
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from pitlake.connectors.p2_bootstrap import (
    AkshareAshareMinuteBarConnector,
    AkshareStockCommentAggregateConnector,
    AkshareStockResearchReportIndexConnector,
)
from pitlake.control.contracts import DatasetContract
from pitlake.control.registry import load_yaml_file
from pitlake.settings import ProjectSettings
from pitlake.storage.layout import LakeLayout
from pitlake.storage.metadata_store import MetadataStore
from pitlake.storage.raw_store import RawStore


def make_settings(tmp_path: Path) -> ProjectSettings:
    return ProjectSettings(
        project_root=tmp_path,
        config_dir=Path("config").resolve(),
        data_lake_root=tmp_path / "data_lake",
        metadata_db=tmp_path / "data_lake" / "collection" / "metadata" / "pitlake.sqlite",
        logs_dir=tmp_path / "data_lake" / "collection" / "logs",
        local_backup_dir=tmp_path / "data_lake" / "backups" / "local",
        timezone="Asia/Shanghai",
        metadata_backend="sqlite",
        raw_store="filesystem",
        alert_backend="local_report",
        prefer_free_sources=True,
        paid_providers_enabled=False,
    )


def build_connector(
    *,
    run_root: Path,
    connector_cls,
    source_id: str,
    logical_dataset: str,
    default_options: dict | None = None,
):
    settings = make_settings(run_root)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    contract_path = Path("config/dataset_contracts") / f"{logical_dataset}.yaml"
    connector = connector_cls(
        settings=settings,
        source_config={
            "source_id": source_id,
            "provider_id": "akshare",
            "logical_dataset": logical_dataset,
            "default_options": default_options or {},
        },
        contract=DatasetContract.from_payload(load_yaml_file(contract_path), contract_path),
        raw_store=RawStore(settings),
        metadata_store=metadata,
    )
    run_id = metadata.create_run(
        source_id=source_id,
        provider_id="akshare",
        logical_dataset=logical_dataset,
        connector_name=connector.connector_name,
        connector_version=connector.connector_version,
        trigger_type="manual",
    )
    return connector, metadata, run_id


def count_items(metadata: MetadataStore, logical_dataset: str) -> int:
    with metadata.connect() as conn:
        return conn.execute(
            "select count(*) as count from raw_item_version where logical_dataset = ?",
            (logical_dataset,),
        ).fetchone()["count"]


def short_run_root(prefix: str) -> Path:
    base = Path(os.environ.get("PITLAKE_TEST_ROOT", "data_lake/test_runs"))
    return base / f"{prefix}_{uuid.uuid4().hex[:8]}"


def test_minute_bar_connector_collects_selected_rows(monkeypatch) -> None:
    def fake_minute(symbol: str, period: str, adjust: str) -> pd.DataFrame:
        assert symbol == "sh600000"
        assert period == "1"
        assert adjust == ""
        return pd.DataFrame(
            {
                "day": ["2026-04-24 09:31:00", "2026-04-24 09:32:00"],
                "open": [10.0, 10.1],
                "high": [10.2, 10.2],
                "low": [9.9, 10.0],
                "close": [10.1, 10.2],
                "volume": [1000, 1200],
                "amount": [10100.0, 12240.0],
            }
        )

    monkeypatch.setitem(sys.modules, "akshare", SimpleNamespace(stock_zh_a_minute=fake_minute))
    connector, metadata, run_id = build_connector(
        run_root=short_run_root("p2_minute"),
        connector_cls=AkshareAshareMinuteBarConnector,
        source_id="akshare_ashare_minute_bar",
        logical_dataset="market_minute_bar",
        default_options={"symbols": ["600000"], "limit_symbols": 1, "period": "1", "limit_rows": 1},
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.success_count == 1
    assert stats.new_item_count == 1
    assert count_items(metadata, "market_minute_bar") == 1


def test_research_report_index_connector_collects_metadata(monkeypatch) -> None:
    def fake_report(symbol: str) -> pd.DataFrame:
        assert symbol == "000001"
        return pd.DataFrame(
            {
                "序号": [1],
                "股票代码": ["000001"],
                "股票简称": ["平安银行"],
                "报告名称": ["2025年报点评"],
                "东财评级": ["中性"],
                "机构": ["国信证券"],
                "日期": ["2026-04-24"],
                "报告PDF链接": ["https://example.com/report.pdf"],
            }
        )

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(stock_research_report_em=fake_report),
    )
    connector, metadata, run_id = build_connector(
        run_root=short_run_root("p2_research"),
        connector_cls=AkshareStockResearchReportIndexConnector,
        source_id="akshare_stock_research_report_index",
        logical_dataset="research_report_index",
        default_options={"symbols": ["000001"], "limit_symbols": 1, "limit_items": 1},
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.success_count == 1
    assert stats.new_item_count == 1
    assert count_items(metadata, "research_report_index") == 1


def test_stock_comment_aggregate_connector_limits_public_aggregate_rows(monkeypatch) -> None:
    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            stock_comment_em=lambda: pd.DataFrame(
                {
                    "序号": [1, 2],
                    "代码": ["000001", "000002"],
                    "名称": ["平安银行", "万科A"],
                    "综合得分": [66.6, 50.9],
                    "目前排名": [740, 4572],
                    "关注指数": [85.6, 72.4],
                    "交易日": ["2026-04-24", "2026-04-24"],
                }
            )
        ),
    )
    connector, metadata, run_id = build_connector(
        run_root=short_run_root("p2_social"),
        connector_cls=AkshareStockCommentAggregateConnector,
        source_id="akshare_stock_comment_aggregate",
        logical_dataset="social_media_aggregate",
        default_options={"limit_items": 1},
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.success_count == 1
    assert stats.new_item_count == 1
    assert count_items(metadata, "social_media_aggregate") == 1
