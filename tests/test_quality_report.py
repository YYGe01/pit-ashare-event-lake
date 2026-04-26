from pathlib import Path

from pitlake.quality.report import QualityReportStore
from pitlake.settings import ProjectSettings
from pitlake.storage.layout import LakeLayout
from pitlake.storage.metadata_store import MetadataStore
from pitlake.storage.raw_store import RawStore


def make_settings(tmp_path: Path) -> ProjectSettings:
    return ProjectSettings(
        project_root=tmp_path,
        config_dir=tmp_path / "config",
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


def test_quality_report_store_writes_daily_report(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    run_id = metadata.create_run(
        source_id="sample_source",
        provider_id="sample_provider",
        logical_dataset="sample_dataset",
        connector_name="SampleConnector",
        connector_version="0.1.0",
        trigger_type="manual",
    )
    raw = RawStore(settings).put_json(
        source_id="sample_source",
        provider_id="sample_provider",
        logical_dataset="sample_dataset",
        payload={"message": "ok"},
        run_id=run_id,
        filename_prefix="sample",
    )
    metadata.insert_raw_object(raw)
    metadata.insert_raw_item_version(
        logical_dataset="sample_dataset",
        provider_id="sample_provider",
        source_id="sample_source",
        source_item_key="sample:1",
        first_seen_at=raw.first_seen_at,
        stored_at=raw.stored_at,
        raw_object_id=raw.raw_object_id,
        content_hash=raw.content_hash,
        quality_status="pass",
        observed_payload={"id": 1},
    )
    metadata.finish_run(
        run_id,
        status="success",
        request_count=1,
        success_count=1,
        new_item_count=1,
    )

    report = QualityReportStore(settings).generate_daily_report(
        report_date=raw.stored_at[:10],
        metadata_store=metadata,
    )

    assert report["status"] == "pass"
    assert report["summary"]["run_count"] == 1
    assert report["summary"]["raw_object_count"] == 1
    assert report["summary"]["item_version_count"] == 1
    assert report["sources"][0]["source_id"] == "sample_source"
    assert (settings.data_lake_root / report["report_path"]).exists()
