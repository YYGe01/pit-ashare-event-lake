import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from pitlake.connectors.announcements.akshare_notice import AkshareAnnouncementIndexConnector
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


def test_akshare_announcement_connector_collects_index_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_notice(symbol: str, date: str) -> pd.DataFrame:
        assert symbol == "全部"
        assert date == "20260424"
        return pd.DataFrame(
            {
                "代码": ["600001"],
                "名称": ["样例股份"],
                "公告标题": ["2026 年第一季度报告"],
                "公告类型": ["财务报告"],
                "公告日期": ["2026-04-24"],
                "网址": ["https://data.eastmoney.com/notices/detail/600001/abc.html"],
            }
        )

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(stock_notice_report=fake_notice),
    )
    settings = make_settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    contract_path = Path("config/dataset_contracts/announcement_index.yaml")
    contract = DatasetContract.from_payload(load_yaml_file(contract_path), contract_path)
    connector = AkshareAnnouncementIndexConnector(
        settings=settings,
        source_config={
            "source_id": "akshare_announcement_index",
            "provider_id": "akshare",
            "logical_dataset": "announcement_index",
            "default_options": {
                "end_date": "20260424",
                "category": "全部",
                "limit_items": 10,
            },
        },
        contract=contract,
        raw_store=RawStore(settings),
        metadata_store=metadata,
    )
    run_id = metadata.create_run(
        source_id="akshare_announcement_index",
        provider_id="akshare",
        logical_dataset="announcement_index",
        connector_name="AkshareAnnouncementIndexConnector",
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
            select title, source_url, observed_payload_json
            from raw_item_version
            where logical_dataset = 'announcement_index'
            """
        ).fetchall()

    assert len(rows) == 1
    assert rows[0]["title"] == "2026 年第一季度报告"
    assert rows[0]["source_url"].endswith("/abc.html")
    assert '"instrument":"600001"' in rows[0]["observed_payload_json"]
