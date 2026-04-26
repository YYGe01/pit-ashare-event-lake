from pathlib import Path

from pitlake.connectors.announcements.szse import SzseAnnouncementConnector
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
            "announceCount": 1,
            "data": [
                {
                    "id": "3a64b194-2356-46a5-a522-246270da7ed0",
                    "annId": 1219804172,
                    "title": "纳川股份：简式权益变动报告书(更正后)",
                    "publishTime": "2024-04-24 22:18:16",
                    "attachPath": "/disc/disk03/finalpage/2024-04-24/example.PDF",
                    "attachFormat": "PDF",
                    "attachSize": 278,
                    "secCode": ["300198"],
                    "secName": ["纳川股份"],
                    "bigCategoryId": "010105",
                }
            ],
        }

    def raise_for_status(self) -> None:
        return None


def test_szse_connector_collects_announcement_index_rows(tmp_path: Path, monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_post(url: str, params: dict, headers: dict, json: dict, timeout: int) -> FakeResponse:
        calls["url"] = url
        calls["params"] = params
        calls["headers"] = headers
        calls["json"] = json
        calls["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr("requests.post", fake_post)
    settings = make_settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    contract_path = Path("config/dataset_contracts/announcement_index.yaml")
    contract = DatasetContract.from_payload(load_yaml_file(contract_path), contract_path)
    connector = SzseAnnouncementConnector(
        settings=settings,
        source_config={
            "source_id": "szse_announcement_list",
            "provider_id": "szse",
            "logical_dataset": "announcement_index",
            "default_options": {
                "start_date": "20240424",
                "end_date": "20240424",
                "channel_code": "listedNotice_disc",
                "page_size": 5,
                "max_pages": 1,
                "timeout_seconds": 5,
            },
        },
        contract=contract,
        raw_store=RawStore(settings),
        metadata_store=metadata,
    )
    run_id = metadata.create_run(
        source_id="szse_announcement_list",
        provider_id="szse",
        logical_dataset="announcement_index",
        connector_name=connector.connector_name,
        connector_version=connector.connector_version,
        trigger_type="manual",
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.request_count == 1
    assert stats.success_count == 1
    assert stats.error_count == 0
    assert stats.new_item_count == 1
    assert calls["json"]["seDate"] == ["2024-04-24", "2024-04-24"]
    assert calls["json"]["channelCode"] == ["listedNotice_disc"]
    assert calls["json"]["pageSize"] == 5
    with metadata.connect() as conn:
        rows = conn.execute(
            """
            select source_item_key, title, source_url, source_publish_time, observed_payload_json
            from raw_item_version
            where logical_dataset = 'announcement_index'
            """
        ).fetchall()

    assert len(rows) == 1
    assert rows[0]["source_item_key"] == "szse:1219804172"
    assert rows[0]["title"] == "纳川股份：简式权益变动报告书(更正后)"
    assert rows[0]["source_url"].startswith("https://disc.static.szse.cn/download/")
    assert rows[0]["source_publish_time"] == "2024-04-24T22:18:16+08:00"
    assert '"instrument":"300198"' in rows[0]["observed_payload_json"]
    assert '"exchange":"SZSE"' in rows[0]["observed_payload_json"]
