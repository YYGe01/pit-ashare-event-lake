import json
from pathlib import Path

from pitlake.connectors.announcements.bse import BseAnnouncementConnector
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

    @property
    def text(self) -> str:
        payload = [
            {
                "listInfo": {
                    "content": [
                        {
                            "companyCd": "920185",
                            "companyName": "贝特瑞",
                            "destFilePath": (
                                "/disclosure/2026/2026-04-24/"
                                "08736e1a85be4a189b5a2aef9521e281.pdf"
                            ),
                            "disclosurePostTitle": "",
                            "disclosureTitle": "[定期报告]贝特瑞:2025年年度报告摘要",
                            "fileExt": "pdf",
                            "publishDate": "2026-04-24",
                            "xxfcbj": "2",
                            "xxzrlx": "B",
                        }
                    ],
                    "number": 0,
                    "numberOfElements": 1,
                    "totalElements": 1,
                }
            }
        ]
        return f"jQuery123({json.dumps(payload, ensure_ascii=False)})"

    def raise_for_status(self) -> None:
        return None


def test_bse_connector_collects_announcement_index_rows(tmp_path: Path, monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_get(url: str, headers: dict, params: dict, timeout: int) -> FakeResponse:
        calls["url"] = url
        calls["headers"] = headers
        calls["params"] = params
        calls["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr("requests.get", fake_get)
    settings = make_settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    contract_path = Path("config/dataset_contracts/announcement_index.yaml")
    contract = DatasetContract.from_payload(load_yaml_file(contract_path), contract_path)
    connector = BseAnnouncementConnector(
        settings=settings,
        source_config={
            "source_id": "bse_announcement_list",
            "provider_id": "bse",
            "logical_dataset": "announcement_index",
            "default_options": {
                "start_date": "20260424",
                "end_date": "20260424",
                "disclosure_type": "5",
                "market_layer": "2",
                "max_pages": 1,
                "timeout_seconds": 5,
            },
        },
        contract=contract,
        raw_store=RawStore(settings),
        metadata_store=metadata,
    )
    run_id = metadata.create_run(
        source_id="bse_announcement_list",
        provider_id="bse",
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
    assert calls["params"]["startTime"] == "2026-04-24"
    assert calls["params"]["endTime"] == "2026-04-24"
    assert calls["params"]["disclosureType[]"] == ["5"]
    assert calls["params"]["xxfcbj[]"] == ["2"]
    assert "needFields[]" in calls["params"]
    with metadata.connect() as conn:
        rows = conn.execute(
            """
            select source_item_key, title, source_url, source_publish_time, observed_payload_json
            from raw_item_version
            where logical_dataset = 'announcement_index'
            """
        ).fetchall()

    assert len(rows) == 1
    assert rows[0]["source_item_key"] == "bse:08736e1a85be4a189b5a2aef9521e281"
    assert rows[0]["title"] == "[定期报告]贝特瑞:2025年年度报告摘要"
    assert rows[0]["source_url"].startswith("https://www.bse.cn/disclosure/")
    assert rows[0]["source_publish_time"] == "2026-04-24T00:00:00+08:00"
    assert '"instrument":"920185"' in rows[0]["observed_payload_json"]
    assert '"exchange":"BSE"' in rows[0]["observed_payload_json"]
    assert '"category":"定期报告"' in rows[0]["observed_payload_json"]
