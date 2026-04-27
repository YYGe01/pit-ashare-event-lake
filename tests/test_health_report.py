from datetime import timedelta
from pathlib import Path

from pitlake.ops.health import SourceHealthReportStore
from pitlake.settings import ProjectSettings
from pitlake.storage.layout import LakeLayout
from pitlake.storage.metadata_store import MetadataStore
from pitlake.utils import now_cn


def make_settings(tmp_path: Path) -> ProjectSettings:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "source_registry.yaml").write_text(
        """
sources:
  - source_id: fresh_source
    provider_id: sample_provider
    logical_dataset: sample_dataset
    source_type: python_api
    access_method: test
    auth_type: none
    priority: P0
    enabled: true
    adapter_class: pitlake.connectors.fake.FreshConnector
  - source_id: never_success_source
    provider_id: sample_provider
    logical_dataset: sample_dataset
    source_type: python_api
    access_method: test
    auth_type: none
    priority: P0
    enabled: true
    adapter_class: pitlake.connectors.fake.NeverSuccessConnector
""",
        encoding="utf-8",
    )
    (config_dir / "schedule_policy.yaml").write_text(
        """
timezone: Asia/Shanghai
policies:
  - logical_dataset: sample_dataset
    priority: P0
    cadence: daily
    windows:
      - "20:00"
    freshness_slo_minutes: 60
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


def test_health_report_records_slo_status(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    run_id = metadata.create_run(
        source_id="fresh_source",
        provider_id="sample_provider",
        logical_dataset="sample_dataset",
        connector_name="FreshConnector",
        connector_version="0.1.0",
        trigger_type="manual",
    )
    metadata.finish_run(run_id, status="success", request_count=1, success_count=1)

    report = SourceHealthReportStore(settings).generate_report(
        metadata_store=metadata,
        as_of=now_cn() + timedelta(minutes=30),
    )

    by_source = {item["source_id"]: item for item in report["sources"]}
    assert report["status"] == "fail"
    assert by_source["fresh_source"]["status"] == "pass"
    assert by_source["never_success_source"]["status"] == "fail"
    with metadata.connect() as conn:
        rows = conn.execute("select source_id, status from source_health").fetchall()
    assert {row["source_id"] for row in rows} == {"fresh_source", "never_success_source"}
