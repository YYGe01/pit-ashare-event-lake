import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from pitlake.connectors.fundamentals.akshare_financial import AkshareFinancialIndicatorConnector
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


def test_akshare_financial_connector_collects_indicator_rows(
    monkeypatch,
) -> None:
    def fake_financial(symbol: str, start_year: str) -> pd.DataFrame:
        assert symbol == "600001"
        assert start_year == "2024"
        return pd.DataFrame(
            {
                "日期": ["2024-03-31", "2024-06-30"],
                "eps": [0.12, 0.25],
                "roe": [1.1, 2.2],
            }
        )

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(stock_financial_analysis_indicator=fake_financial),
    )
    run_root = (Path("data_lake/test_runs") / f"financial_connector_{uuid.uuid4().hex}").resolve()
    settings = make_settings(run_root)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    contract_path = Path("config/dataset_contracts/financial_indicator.yaml")
    connector = AkshareFinancialIndicatorConnector(
        settings=settings,
        source_config={
            "source_id": "akshare_financial_indicator",
            "provider_id": "akshare",
            "logical_dataset": "financial_indicator",
            "default_options": {
                "start_year": "2024",
                "symbols": ["600001"],
                "limit_symbols": 1,
            },
        },
        contract=DatasetContract.from_payload(load_yaml_file(contract_path), contract_path),
        raw_store=RawStore(settings),
        metadata_store=metadata,
    )
    run_id = metadata.create_run(
        source_id="akshare_financial_indicator",
        provider_id="akshare",
        logical_dataset="financial_indicator",
        connector_name="AkshareFinancialIndicatorConnector",
        connector_version="0.1.0",
        trigger_type="manual",
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.request_count == 1
    assert stats.success_count == 1
    assert stats.error_count == 0
    assert stats.new_item_count == 2
    with metadata.connect() as conn:
        rows = conn.execute(
            """
            select source_item_key, observed_payload_json
            from raw_item_version
            where logical_dataset = 'financial_indicator'
            order by source_item_key
            """
        ).fetchall()
        raw_count = conn.execute(
            "select count(*) as count from raw_object where source_id = 'akshare_financial_indicator'"
        ).fetchone()["count"]

    assert raw_count == 1
    assert len(rows) == 2
    assert rows[0]["source_item_key"] == "akshare:600001:2024-03-31"
    assert '"period_type":"Q1"' in rows[0]["observed_payload_json"]
    assert '"metric_payload":{"eps":0.12,"roe":1.1}' in rows[0]["observed_payload_json"]
