from pathlib import Path

from pitlake.settings import ProjectSettings
from pitlake.storage.layout import LakeLayout
from pitlake.storage.manifest_store import ManifestStore
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


def test_raw_metadata_manifest_roundtrip(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    run_id = metadata.create_run(
        source_id="pitlake_smoke_test",
        provider_id="internal",
        logical_dataset="system_smoke_test",
        connector_name="SmokeConnector",
        connector_version="0.1.0",
        trigger_type="manual",
    )
    raw = RawStore(settings).put_json(
        source_id="pitlake_smoke_test",
        provider_id="internal",
        logical_dataset="system_smoke_test",
        payload={"message": "ok"},
        run_id=run_id,
        filename_prefix="smoke",
    )
    metadata.insert_raw_object(raw)
    metadata.finish_run(run_id, status="success", request_count=1, success_count=1)
    manifest = ManifestStore(settings).generate_daily_manifest(
        manifest_date=raw.stored_at[:10],
        metadata_store=metadata,
    )

    assert raw.storage_path.exists()
    assert raw.content_hash.startswith("sha256:")
    assert manifest["summary"]["run_count"] == 1
    assert manifest["summary"]["raw_object_count"] == 1

