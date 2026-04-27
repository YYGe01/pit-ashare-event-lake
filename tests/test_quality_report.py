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


def test_quality_report_flags_contract_drift_and_anomalies(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    contract_dir = settings.config_dir / "dataset_contracts"
    contract_dir.mkdir(parents=True)
    (contract_dir / "market_daily_ohlcv.yaml").write_text(
        """
logical_dataset: market_daily_ohlcv
contract_version: 1
primary_key_fields:
  - provider_id
  - instrument
  - trading_date
required_fields:
  - provider_id
  - source_id
  - instrument
  - trading_date
  - open
  - high
  - low
  - close
  - first_seen_at
  - raw_uri
  - content_hash
optional_fields: []
quality_rules: {}
""",
        encoding="utf-8",
    )
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    run_id = metadata.create_run(
        source_id="sample_source",
        provider_id="sample_provider",
        logical_dataset="market_daily_ohlcv",
        connector_name="SampleConnector",
        connector_version="0.1.0",
        trigger_type="manual",
    )
    raw = RawStore(settings).put_json(
        source_id="sample_source",
        provider_id="sample_provider",
        logical_dataset="market_daily_ohlcv",
        payload={"message": "bad high/low"},
        run_id=run_id,
        filename_prefix="sample",
    )
    metadata.insert_raw_object(raw)
    metadata.insert_raw_item_version(
        logical_dataset="market_daily_ohlcv",
        provider_id="sample_provider",
        source_id="sample_source",
        source_item_key="sample:000001:2026-04-24",
        first_seen_at=raw.first_seen_at,
        stored_at=raw.stored_at,
        raw_object_id=raw.raw_object_id,
        content_hash=raw.content_hash,
        quality_status="pass",
        observed_payload={
            "provider_id": "sample_provider",
            "source_id": "sample_source",
            "source_item_key": "sample:000001:2026-04-24",
            "instrument": "000001",
            "trading_date": "2026-04-24",
            "open": 10.0,
            "high": 9.5,
            "low": 10.1,
            "close": 10.0,
            "first_seen_at": raw.first_seen_at,
            "raw_uri": raw.raw_uri,
            "content_hash": raw.content_hash,
            "unexpected_provider_field": "x",
        },
    )
    metadata.finish_run(run_id, status="success", request_count=1, success_count=1)

    report = QualityReportStore(settings).generate_daily_report(
        report_date=raw.stored_at[:10],
        metadata_store=metadata,
    )

    finding_types = {finding["finding_type"] for finding in report["quality_findings"]}
    assert report["status"] == "fail"
    assert "schema_drift_unknown_fields" in finding_types
    assert "high_less_than_low" in finding_types


def test_quality_report_strict_coverage_warns_on_missing_enabled_source(
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path)
    settings.config_dir.mkdir(parents=True, exist_ok=True)
    (settings.config_dir / "source_registry.yaml").write_text(
        """
sources:
  - source_id: collected_source
    provider_id: sample_provider
    logical_dataset: sample_dataset
    source_type: python_api
    access_method: test
    auth_type: none
    priority: P0
    enabled: true
    adapter_class: pitlake.connectors.fake.CollectedConnector
  - source_id: missing_source
    provider_id: sample_provider
    logical_dataset: sample_dataset
    source_type: python_api
    access_method: test
    auth_type: none
    priority: P0
    enabled: true
    adapter_class: pitlake.connectors.fake.MissingConnector
""",
        encoding="utf-8",
    )
    (settings.config_dir / "schedule_policy.yaml").write_text(
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
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    run_id = metadata.create_run(
        source_id="collected_source",
        provider_id="sample_provider",
        logical_dataset="sample_dataset",
        connector_name="SampleConnector",
        connector_version="0.1.0",
        trigger_type="manual",
    )
    metadata.finish_run(run_id, status="success", request_count=1, success_count=1)
    with metadata.connect() as conn:
        report_date = conn.execute(
            "select start_at from crawl_run where run_id = ?",
            (run_id,),
        ).fetchone()["start_at"][:10]

    report = QualityReportStore(settings).generate_daily_report(
        report_date=report_date,
        metadata_store=metadata,
        strict_coverage=True,
    )

    assert report["status"] == "warn"
    assert report["summary"]["strict_coverage"] is True
    assert any(
        finding["finding_type"] == "enabled_source_not_collected"
        and finding["source_id"] == "missing_source"
        for finding in report["quality_findings"]
    )
