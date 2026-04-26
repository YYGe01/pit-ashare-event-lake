import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from pitlake.connectors.market.akshare_calendar import AkshareTradingCalendarConnector
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


def test_akshare_trading_calendar_connector_collects_window(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_trade_dates() -> pd.DataFrame:
        return pd.DataFrame(
            {
                "trade_date": [
                    "2026-04-23",
                    "2026-04-24",
                    "2026-04-27",
                ]
            }
        )

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(tool_trade_date_hist_sina=fake_trade_dates),
    )
    settings = make_settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    contract_path = Path("config/dataset_contracts/trading_calendar.yaml")
    contract = DatasetContract.from_payload(load_yaml_file(contract_path), contract_path)
    connector = AkshareTradingCalendarConnector(
        settings=settings,
        source_config={
            "source_id": "ashare_trading_calendar",
            "provider_id": "akshare",
            "logical_dataset": "trading_calendar",
            "default_options": {
                "start_date": "20260424",
                "end_date": "20260424",
                "calendar_id": "cn_ashare",
            },
        },
        contract=contract,
        raw_store=RawStore(settings),
        metadata_store=metadata,
    )
    run_id = metadata.create_run(
        source_id="ashare_trading_calendar",
        provider_id="akshare",
        logical_dataset="trading_calendar",
        connector_name="AkshareTradingCalendarConnector",
        connector_version="0.1.0",
        trigger_type="manual",
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.request_count == 1
    assert stats.success_count == 1
    assert stats.error_count == 0
    assert stats.new_item_count == 1
    with metadata.connect() as conn:
        rows = conn.execute(
            """
            select source_item_key, observed_payload_json
            from raw_item_version
            where logical_dataset = 'trading_calendar'
            """
        ).fetchall()
        raw_count = conn.execute(
            "select count(*) as count from raw_object where source_id = 'ashare_trading_calendar'"
        ).fetchone()["count"]

    assert raw_count == 1
    assert len(rows) == 1
    assert rows[0]["source_item_key"] == "akshare:cn_ashare:2026-04-24"
    assert '"is_trading_day":true' in rows[0]["observed_payload_json"]
