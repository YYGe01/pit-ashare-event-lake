import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from pitlake.connectors.market.akshare_adjustment import AkshareAdjustmentFactorConnector
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


def test_akshare_adjustment_connector_infers_qfq_factor(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_daily(symbol: str, start_date: str, end_date: str, adjust: str) -> pd.DataFrame:
        assert symbol == "sh600001"
        assert start_date == "20260424"
        assert end_date == "20260424"
        if adjust == "":
            return pd.DataFrame({"date": ["2026-04-24"], "close": [10.0]})
        if adjust == "qfq":
            return pd.DataFrame({"date": ["2026-04-24"], "close": [8.0]})
        raise AssertionError(f"unexpected adjust: {adjust}")

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(stock_zh_a_daily=fake_daily),
    )
    settings = make_settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    contract_path = Path("config/dataset_contracts/adjustment_factor.yaml")
    contract = DatasetContract.from_payload(load_yaml_file(contract_path), contract_path)
    connector = AkshareAdjustmentFactorConnector(
        settings=settings,
        source_config={
            "source_id": "adj_factor",
            "provider_id": "akshare",
            "logical_dataset": "adjustment_factor",
            "default_options": {
                "start_date": "20260424",
                "end_date": "20260424",
                "symbols": ["600001"],
                "limit_symbols": 1,
            },
        },
        contract=contract,
        raw_store=RawStore(settings),
        metadata_store=metadata,
    )
    run_id = metadata.create_run(
        source_id="adj_factor",
        provider_id="akshare",
        logical_dataset="adjustment_factor",
        connector_name="AkshareAdjustmentFactorConnector",
        connector_version="0.1.0",
        trigger_type="manual",
    )

    stats = connector.collect(run_id=run_id, options={})
    with metadata.connect() as conn:
        quality_rows = [
            dict(row)
            for row in conn.execute(
                "select check_name, observed_value from quality_check_result"
            ).fetchall()
        ]

    assert stats.request_count == 2
    assert stats.success_count == 2, quality_rows
    assert stats.error_count == 0
    assert stats.new_item_count == 1
    with metadata.connect() as conn:
        rows = conn.execute(
            """
            select source_item_key, observed_payload_json
            from raw_item_version
            where logical_dataset = 'adjustment_factor'
            """
        ).fetchall()
        raw_count = conn.execute(
            "select count(*) as count from raw_object where source_id = 'adj_factor'"
        ).fetchone()["count"]

    assert raw_count == 1
    assert len(rows) == 1
    assert rows[0]["source_item_key"] == "akshare:600001:2026-04-24"
    assert '"adj_factor":0.8' in rows[0]["observed_payload_json"]
    assert '"factor_type":"qfq_close_ratio_v0_inferred"' in rows[0]["observed_payload_json"]
