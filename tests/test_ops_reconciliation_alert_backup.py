import json
from pathlib import Path

from pitlake.ops.alerts import dispatch_alert
from pitlake.ops.backup import backup_collection_state
from pitlake.quality.reconciliation import ReconciliationReportStore
from pitlake.settings import ProjectSettings
from pitlake.storage.layout import LakeLayout
from pitlake.storage.manifest_store import ManifestStore
from pitlake.storage.metadata_store import MetadataStore
from pitlake.storage.raw_store import RawStore


def make_settings(tmp_path: Path) -> ProjectSettings:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "source_registry.yaml").write_text(
        """
sources:
  - source_id: primary_price_limit
    provider_id: primary
    logical_dataset: price_limit
    source_type: python_api
    access_method: test
    auth_type: none
    priority: P0
    enabled: true
    implementation_status: active_v0
  - source_id: shadow_price_limit
    provider_id: shadow
    logical_dataset: price_limit
    source_type: python_api
    access_method: test
    auth_type: none
    priority: P0
    enabled: false
    implementation_status: planned_shadow
""",
        encoding="utf-8",
    )
    return ProjectSettings(
        project_root=tmp_path,
        config_dir=config_dir,
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


def test_reconciliation_reports_missing_counterparty_and_value_mismatch(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    first_raw = _insert_price_limit_item(
        settings,
        metadata,
        source_id="primary_price_limit",
        provider_id="primary",
        limit_up=11.0,
        limit_down=9.0,
    )
    _insert_price_limit_item(
        settings,
        metadata,
        source_id="shadow_price_limit",
        provider_id="shadow",
        limit_up=11.1,
        limit_down=9.0,
    )

    report = ReconciliationReportStore(settings).generate_daily_report(
        report_date=first_raw.stored_at[:10],
        metadata_store=metadata,
        datasets=["price_limit"],
    )

    assert report["status"] == "fail"
    assert report["summary"]["critical_finding_count"] == 1
    assert report["datasets"][0]["compared_group_count"] == 1
    assert report["datasets"][0]["mismatched_group_count"] == 1
    assert (settings.data_lake_root / report["report_path"]).exists()


def test_alert_and_backup_write_operational_artifacts(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    raw = _insert_price_limit_item(
        settings,
        metadata,
        source_id="primary_price_limit",
        provider_id="primary",
        limit_up=11.0,
        limit_down=9.0,
    )
    ManifestStore(settings).generate_daily_manifest(
        manifest_date=raw.stored_at[:10],
        metadata_store=metadata,
    )

    alert_result = dispatch_alert(
        logs_dir=settings.logs_dir,
        message="test alert",
        payload={"status": "fail"},
    )
    alert_path = Path(alert_result["local_alert_path"])
    assert alert_path.exists()
    assert json.loads(alert_path.read_text(encoding="utf-8").splitlines()[-1])["message"] == (
        "test alert"
    )

    backup = backup_collection_state(settings)
    assert backup.backup_dir.exists()
    assert any(path.name == "pitlake.sqlite" for path in backup.copied_files)
    assert any(path.name == "published_manifests" for path in backup.copied_files)


def _insert_price_limit_item(
    settings: ProjectSettings,
    metadata: MetadataStore,
    *,
    source_id: str,
    provider_id: str,
    limit_up: float,
    limit_down: float,
):
    run_id = metadata.create_run(
        source_id=source_id,
        provider_id=provider_id,
        logical_dataset="price_limit",
        connector_name="TestConnector",
        connector_version="0.1.0",
        trigger_type="manual",
    )
    raw = RawStore(settings).put_json(
        source_id=source_id,
        provider_id=provider_id,
        logical_dataset="price_limit",
        payload={"source": source_id},
        run_id=run_id,
        filename_prefix=source_id,
    )
    metadata.insert_raw_object(raw)
    metadata.insert_raw_item_version(
        logical_dataset="price_limit",
        provider_id=provider_id,
        source_id=source_id,
        source_item_key=f"{provider_id}:000001:2026-04-24",
        first_seen_at=raw.first_seen_at,
        stored_at=raw.stored_at,
        raw_object_id=raw.raw_object_id,
        content_hash=raw.content_hash,
        dedup_hash="000001:2026-04-24",
        quality_status="pass",
        observed_payload={
            "instrument": "000001",
            "trading_date": "2026-04-24",
            "prev_close": 10.0,
            "limit_up": limit_up,
            "limit_down": limit_down,
            "limit_rule": "main_board_normal_10pct_v0_inferred",
        },
    )
    metadata.finish_run(run_id, status="success", request_count=1, success_count=1)
    return raw
