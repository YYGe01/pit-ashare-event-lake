import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from pitlake.connectors.commodities.akshare_futures import AkshareCommodityDailyConnector
from pitlake.connectors.global_markets.akshare_us import AkshareGlobalMarketDailyConnector
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


def test_akshare_commodity_connector_collects_target_date(tmp_path: Path, monkeypatch) -> None:
    def fake_futures(symbol: str) -> pd.DataFrame:
        assert symbol == "RB0"
        return pd.DataFrame(
            {
                "date": ["2026-04-24"],
                "open": [3000.0],
                "high": [3050.0],
                "low": [2990.0],
                "close": [3020.0],
                "volume": [100],
                "hold": [200],
                "settle": [3010.0],
            }
        )

    monkeypatch.setitem(sys.modules, "akshare", SimpleNamespace(futures_zh_daily_sina=fake_futures))
    settings = make_settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    contract_path = Path("config/dataset_contracts/commodity_daily.yaml")
    connector = AkshareCommodityDailyConnector(
        settings=settings,
        source_config={
            "source_id": "akshare_commodity_daily",
            "provider_id": "akshare",
            "logical_dataset": "commodity_daily",
            "default_options": {"end_date": "20260424", "symbols": ["RB0"], "limit_symbols": 1},
        },
        contract=DatasetContract.from_payload(load_yaml_file(contract_path), contract_path),
        raw_store=RawStore(settings),
        metadata_store=metadata,
    )
    run_id = metadata.create_run(
        source_id="akshare_commodity_daily",
        provider_id="akshare",
        logical_dataset="commodity_daily",
        connector_name="AkshareCommodityDailyConnector",
        connector_version="0.1.0",
        trigger_type="manual",
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.success_count == 1
    assert stats.new_item_count == 1


def test_akshare_global_connector_collects_target_date(tmp_path: Path, monkeypatch) -> None:
    def fake_us(symbol: str, adjust: str) -> pd.DataFrame:
        assert symbol == "AAPL"
        assert adjust == ""
        return pd.DataFrame(
            {
                "date": ["2026-04-24"],
                "open": [170.0],
                "high": [172.0],
                "low": [169.0],
                "close": [171.0],
            }
        )

    monkeypatch.setitem(sys.modules, "akshare", SimpleNamespace(stock_us_daily=fake_us))
    settings = make_settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    contract_path = Path("config/dataset_contracts/global_market_daily.yaml")
    connector = AkshareGlobalMarketDailyConnector(
        settings=settings,
        source_config={
            "source_id": "akshare_global_market_daily",
            "provider_id": "akshare",
            "logical_dataset": "global_market_daily",
            "default_options": {"end_date": "20260424", "symbols": ["AAPL"], "limit_symbols": 1},
        },
        contract=DatasetContract.from_payload(load_yaml_file(contract_path), contract_path),
        raw_store=RawStore(settings),
        metadata_store=metadata,
    )
    run_id = metadata.create_run(
        source_id="akshare_global_market_daily",
        provider_id="akshare",
        logical_dataset="global_market_daily",
        connector_name="AkshareGlobalMarketDailyConnector",
        connector_version="0.1.0",
        trigger_type="manual",
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.success_count == 1
    assert stats.new_item_count == 1
