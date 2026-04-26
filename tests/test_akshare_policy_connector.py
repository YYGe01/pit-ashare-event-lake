import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from pitlake.connectors.policy.akshare_cctv import AkshareCctvPolicyConnector
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


def test_akshare_policy_connector_collects_cctv_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_news(date: str) -> pd.DataFrame:
        assert date == "20260424"
        return pd.DataFrame(
            {
                "date": ["20260424"],
                "title": ["国务院常务会议部署有关工作"],
                "content": ["会议部署稳增长相关工作。"],
            }
        )

    monkeypatch.setitem(sys.modules, "akshare", SimpleNamespace(news_cctv=fake_news))
    settings = make_settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    contract_path = Path("config/dataset_contracts/policy_regulatory_doc.yaml")
    contract = DatasetContract.from_payload(load_yaml_file(contract_path), contract_path)
    connector = AkshareCctvPolicyConnector(
        settings=settings,
        source_config={
            "source_id": "akshare_cctv_policy_news",
            "provider_id": "akshare",
            "logical_dataset": "policy_regulatory_doc",
            "default_options": {"end_date": "20260424", "limit_items": 5},
        },
        contract=contract,
        raw_store=RawStore(settings),
        metadata_store=metadata,
    )
    run_id = metadata.create_run(
        source_id="akshare_cctv_policy_news",
        provider_id="akshare",
        logical_dataset="policy_regulatory_doc",
        connector_name="AkshareCctvPolicyConnector",
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
            where logical_dataset = 'policy_regulatory_doc'
            """
        ).fetchall()

    assert len(rows) == 1
    assert rows[0]["title"] == "国务院常务会议部署有关工作"
    assert "tv.cctv.com" in rows[0]["source_url"]
    assert '"category":"policy_macro_news"' in rows[0]["observed_payload_json"]
