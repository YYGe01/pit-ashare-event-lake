from pathlib import Path

from pitlake.connectors.announcements.cninfo import CninfoAnnouncementConnector
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

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {
            "totalAnnouncement": 1,
            "announcements": [
                {
                    "secCode": "300198",
                    "secName": "纳川股份",
                    "announcementId": "1219804173",
                    "announcementTitle": "关于简式权益变动报告书的更正公告",
                    "announcementTime": 1713968296000,
                    "adjunctUrl": "finalpage/2024-04-24/1219804173.PDF",
                    "adjunctSize": 208,
                    "adjunctType": "PDF",
                    "pageColumn": "SZCY",
                    "columnId": "09020202||160203",
                }
            ],
        }


def test_cninfo_connector_collects_announcement_index_rows(tmp_path: Path, monkeypatch) -> None:
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
    contract_path = Path("config/dataset_contracts/announcement_index.yaml")
    contract = DatasetContract.from_payload(load_yaml_file(contract_path), contract_path)
    connector = CninfoAnnouncementConnector(
        settings=settings,
        source_config={
            "source_id": "cninfo_announcement_list",
            "provider_id": "cninfo",
            "logical_dataset": "announcement_index",
            "default_options": {
                "start_date": "20240424",
                "end_date": "20240424",
                "column": "szse",
                "page_size": 10,
                "max_pages": 1,
                "timeout_seconds": 5,
            },
        },
        contract=contract,
        raw_store=RawStore(settings),
        metadata_store=metadata,
    )
    run_id = metadata.create_run(
        source_id="cninfo_announcement_list",
        provider_id="cninfo",
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
    assert calls["data"]["seDate"] == "2024-04-24~2024-04-24"
    assert calls["data"]["pageSize"] == "10"
    with metadata.connect() as conn:
        rows = conn.execute(
            """
            select source_item_key, title, source_url, source_publish_time, observed_payload_json
            from raw_item_version
            where logical_dataset = 'announcement_index'
            """
        ).fetchall()

    assert len(rows) == 1
    assert rows[0]["source_item_key"] == "cninfo:1219804173"
    assert rows[0]["title"] == "关于简式权益变动报告书的更正公告"
    assert rows[0]["source_url"].endswith("/finalpage/2024-04-24/1219804173.PDF")
    assert rows[0]["source_publish_time"] == "2024-04-24T22:18:16+08:00"
    assert '"instrument":"300198"' in rows[0]["observed_payload_json"]
    assert '"pdf_url":"https://static.cninfo.com.cn/finalpage/2024-04-24/1219804173.PDF"' in rows[
        0
    ]["observed_payload_json"]
