import os
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from pitlake.connectors.p1_bootstrap import (
    AkshareCapitalFlowConnector,
    AkshareConceptMembershipConnector,
    AkshareIndustryMembershipConnector,
    AkshareMacroChinaFinancialCreditConnector,
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


def test_macro_financial_credit_connector_collects_rows(monkeypatch) -> None:
    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            macro_china_new_financial_credit=lambda: pd.DataFrame(
                {"日期": ["2024-03"], "新增人民币贷款": [30900]}
            )
        ),
    )
    connector, metadata, run_id = build_connector(
        run_root=short_run_root("p1_macro"),
        connector_cls=AkshareMacroChinaFinancialCreditConnector,
        source_id="akshare_macro_china_financial_credit",
        logical_dataset="macro_indicator",
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.success_count == 1
    assert stats.new_item_count == 1
    assert count_items(metadata, "macro_indicator") == 1


def test_capital_flow_connector_collects_stock_rows(monkeypatch) -> None:
    def fake_fund_flow(stock: str, market: str) -> pd.DataFrame:
        assert stock == "600000"
        assert market == "sh"
        return pd.DataFrame({"日期": ["2024-04-24"], "主力净流入": [12.3]})

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(stock_individual_fund_flow=fake_fund_flow),
    )
    connector, metadata, run_id = build_connector(
        run_root=short_run_root("p1_flow"),
        connector_cls=AkshareCapitalFlowConnector,
        source_id="akshare_stock_capital_flow",
        logical_dataset="capital_flow",
        default_options={"symbols": ["600000"], "limit_symbols": 1},
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.success_count == 1
    assert stats.new_item_count == 1
    assert count_items(metadata, "capital_flow") == 1


def test_industry_and_concept_membership_connectors_collect_snapshots(
    monkeypatch,
) -> None:
    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            stock_board_industry_cons_em=lambda symbol: pd.DataFrame(
                {"代码": ["600000"], "名称": ["浦发银行"], "权重": [1.0]}
            ),
            stock_board_concept_cons_em=lambda symbol: pd.DataFrame(
                {"代码": ["300750"], "名称": ["宁德时代"], "权重": [2.0]}
            ),
        ),
    )
    industry, metadata1, run_id1 = build_connector(
        run_root=short_run_root("p1_ind"),
        connector_cls=AkshareIndustryMembershipConnector,
        source_id="akshare_industry_membership",
        logical_dataset="industry_membership",
        default_options={"board_names": ["银行"], "limit_boards": 1},
    )
    concept, metadata2, run_id2 = build_connector(
        run_root=short_run_root("p1_con"),
        connector_cls=AkshareConceptMembershipConnector,
        source_id="akshare_concept_membership",
        logical_dataset="concept_membership",
        default_options={"board_names": ["机器人概念"], "limit_boards": 1},
    )

    industry_stats = industry.collect(run_id=run_id1, options={})
    concept_stats = concept.collect(run_id=run_id2, options={})

    assert industry_stats.new_item_count == 1
    assert concept_stats.new_item_count == 1
    assert count_items(metadata1, "industry_membership") == 1
    assert count_items(metadata2, "concept_membership") == 1
