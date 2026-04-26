from pathlib import Path

from pitlake.connectors.commodities.shfe import ShfeDailyConnector
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
    headers = {"content-type": "application/json"}

    def json(self) -> dict:
        return {
            "o_day": "24",
            "o_curinstrument": [
                {
                    "PRODUCTGROUPID": "cu",
                    "PRODUCTID": "cu_f",
                    "PRODUCTNAME": "铜",
                    "DELIVERYMONTH": "2605",
                    "OPENPRICE": 102800,
                    "HIGHESTPRICE": 103460,
                    "LOWESTPRICE": 102180,
                    "CLOSEPRICE": 102340,
                    "SETTLEMENTPRICE": 102570,
                    "PRESETTLEMENTPRICE": 102910,
                    "VOLUME": 59846,
                    "OPENINTEREST": 86223,
                    "OPENINTERESTCHG": -5768,
                }
            ],
        }

    def raise_for_status(self) -> None:
        return None


def test_shfe_daily_connector_collects_official_contract_rows(
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
    connector = ShfeDailyConnector(
        settings=settings,
        source_config={
            "source_id": "shfe_daily_commodity",
            "provider_id": "shfe",
            "logical_dataset": "commodity_daily",
            "default_options": {"end_date": "20260424", "timeout_seconds": 5},
        },
        contract=contract,
        raw_store=RawStore(settings),
        metadata_store=metadata,
    )
    run_id = metadata.create_run(
        source_id="shfe_daily_commodity",
        provider_id="shfe",
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
    assert str(calls["url"]).endswith("/kx20260424.dat")
    with metadata.connect() as conn:
        rows = conn.execute(
            """
            select source_item_key, title, source_url, observed_payload_json
            from raw_item_version
            where logical_dataset = 'commodity_daily'
            """
        ).fetchall()

    assert len(rows) == 1
    assert rows[0]["source_item_key"] == "shfe:SHFE:cu2605:2026-04-24"
    assert rows[0]["title"] == "SHFE cu2605 commodity daily 2026-04-24"
    assert rows[0]["source_url"].endswith("/kx20260424.dat")
    assert '"exchange":"SHFE"' in rows[0]["observed_payload_json"]
    assert '"contract":"cu2605"' in rows[0]["observed_payload_json"]
    assert '"settlement":102570.0' in rows[0]["observed_payload_json"]
