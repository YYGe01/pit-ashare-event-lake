import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from pitlake.connectors.market.akshare_trade_status import AkshareTradeStatusConnector
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


def test_akshare_trade_status_connector_collects_halt_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_trade_status(date: str) -> pd.DataFrame:
        assert date == "20260424"
        return pd.DataFrame(
            {
                "序号": [1],
                "代码": ["600001"],
                "名称": ["样例股份"],
                "停牌时间": ["2026-04-24"],
                "停牌截止时间": ["2026-04-24"],
                "停牌期限": ["1天"],
                "停牌原因": ["重大事项"],
                "所属市场": ["沪市"],
                "预计复牌时间": [pd.NaT],
            }
        )

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(stock_tfp_em=fake_trade_status),
    )
    settings = make_settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    contract_path = Path("config/dataset_contracts/trade_status.yaml")
    contract = DatasetContract.from_payload(load_yaml_file(contract_path), contract_path)
    connector = AkshareTradeStatusConnector(
        settings=settings,
        source_config={
            "source_id": "ashare_trade_status",
            "provider_id": "akshare",
            "logical_dataset": "trade_status",
            "default_options": {
                "start_date": "20260424",
                "end_date": "20260424",
            },
        },
        contract=contract,
        raw_store=RawStore(settings),
        metadata_store=metadata,
    )
    run_id = metadata.create_run(
        source_id="ashare_trade_status",
        provider_id="akshare",
        logical_dataset="trade_status",
        connector_name="AkshareTradeStatusConnector",
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
            where logical_dataset = 'trade_status'
            """
        ).fetchall()
        raw_count = conn.execute(
            "select count(*) as count from raw_object where source_id = 'ashare_trade_status'"
        ).fetchone()["count"]

    assert raw_count == 1
    assert len(rows) == 1
    assert rows[0]["source_item_key"] == "akshare:600001:2026-04-24"
    assert '"trade_status":"halted"' in rows[0]["observed_payload_json"]
    assert '"exchange":"SSE"' in rows[0]["observed_payload_json"]
