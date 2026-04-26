from pathlib import Path

from pitlake.connectors.commodities.gfex import GfexDailyConnector
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

    def json(self) -> dict:
        return {
            "code": "0",
            "msg": "接口访问成功！",
            "data": [
                {
                    "variety": "多晶硅",
                    "varietyEn": "Poly Silicon",
                    "varietyOrder": "ps",
                    "delivMonth": "2605",
                    "open": 37970,
                    "high": 37970,
                    "low": 35330,
                    "close": 35330,
                    "clearPrice": 36200,
                    "lastClear": 38820,
                    "volumn": 5685,
                    "openInterest": 8486,
                    "diffI": -1179,
                    "turnover": 61742.244,
                }
            ],
        }

    def raise_for_status(self) -> None:
        return None


def test_gfex_daily_connector_collects_official_contract_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_post(url: str, headers: dict, data: dict, timeout: int) -> FakeResponse:
        calls["url"] = url
        calls["headers"] = headers
        calls["data"] = data
        calls["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr("requests.post", fake_post)
    settings = make_settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    contract_path = Path("config/dataset_contracts/commodity_daily.yaml")
    contract = DatasetContract.from_payload(load_yaml_file(contract_path), contract_path)
    connector = GfexDailyConnector(
        settings=settings,
        source_config={
            "source_id": "gfex_daily_commodity",
            "provider_id": "gfex",
            "logical_dataset": "commodity_daily",
            "default_options": {"end_date": "20260424", "timeout_seconds": 5},
        },
        contract=contract,
        raw_store=RawStore(settings),
        metadata_store=metadata,
    )
    run_id = metadata.create_run(
        source_id="gfex_daily_commodity",
        provider_id="gfex",
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
    assert calls["data"] == {"trade_date": "20260424", "trade_type": "0", "variety": ""}
    with metadata.connect() as conn:
        rows = conn.execute(
            """
            select source_item_key, title, source_url, observed_payload_json
            from raw_item_version
            where logical_dataset = 'commodity_daily'
            """
        ).fetchall()

    assert len(rows) == 1
    assert rows[0]["source_item_key"] == "gfex:GFEX:ps2605:2026-04-24"
    assert rows[0]["title"] == "GFEX ps2605 commodity daily 2026-04-24"
    assert rows[0]["source_url"].endswith("/u/interfacesWebTiDayQuotes/loadList")
    assert '"exchange":"GFEX"' in rows[0]["observed_payload_json"]
    assert '"contract":"ps2605"' in rows[0]["observed_payload_json"]
    assert '"settlement":36200.0' in rows[0]["observed_payload_json"]
