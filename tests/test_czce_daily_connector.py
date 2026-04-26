from pathlib import Path

from pitlake.connectors.commodities.czce import CzceDailyConnector
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


class FakeResponse:
    status_code = 200
    headers = {"content-type": "text/plain; charset=utf-8"}

    def __init__(self) -> None:
        self.text = """
郑州商品交易所期货每日行情表(2026-04-24)
合约代码|昨结算|今开盘|最高价|最低价|今收盘|今结算|涨跌1|涨跌2|成交量(手)|持仓量|增减量|成交额(万元)|交割结算价
AP605 |9,219.00|9,219.00|9,538.00|9,177.00|9,485.00|9,384.00|266.00|165.00|4,354|6,133|-890|40,859.91|
小计 | | | | | | | | |4,354|6,133|-890|40,859.91|
"""
        self.content = self.text.encode("utf-8")

    def raise_for_status(self) -> None:
        return None


def test_czce_daily_connector_collects_official_contract_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_get(url: str, headers: dict, timeout: int) -> FakeResponse:
        calls["url"] = url
        calls["headers"] = headers
        calls["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr("requests.get", fake_get)
    settings = make_settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    contract_path = Path("config/dataset_contracts/commodity_daily.yaml")
    contract = DatasetContract.from_payload(load_yaml_file(contract_path), contract_path)
    connector = CzceDailyConnector(
        settings=settings,
        source_config={
            "source_id": "czce_daily_commodity",
            "provider_id": "czce",
            "logical_dataset": "commodity_daily",
            "default_options": {"end_date": "20260424", "timeout_seconds": 5},
        },
        contract=contract,
        raw_store=RawStore(settings),
        metadata_store=metadata,
    )
    run_id = metadata.create_run(
        source_id="czce_daily_commodity",
        provider_id="czce",
        logical_dataset="commodity_daily",
        connector_name=connector.connector_name,
        connector_version=connector.connector_version,
        trigger_type="manual",
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.request_count == 1
    assert stats.success_count == 1
    assert stats.error_count == 0
    assert stats.new_item_count == 1
    assert str(calls["url"]).endswith("/Future/2026/20260424/FutureDataDaily.txt")
    with metadata.connect() as conn:
        rows = conn.execute(
            """
            select source_item_key, title, source_url, observed_payload_json
            from raw_item_version
            where logical_dataset = 'commodity_daily'
            """
        ).fetchall()

    assert len(rows) == 1
    assert rows[0]["source_item_key"] == "czce:CZCE:AP605:2026-04-24"
    assert rows[0]["title"] == "CZCE AP605 commodity daily 2026-04-24"
    assert rows[0]["source_url"].endswith("/FutureDataDaily.txt")
    assert '"exchange":"CZCE"' in rows[0]["observed_payload_json"]
    assert '"contract":"AP605"' in rows[0]["observed_payload_json"]
    assert '"settlement":9384.0' in rows[0]["observed_payload_json"]
