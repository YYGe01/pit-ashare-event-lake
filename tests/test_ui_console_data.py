from __future__ import annotations

from pathlib import Path

from pitlake.quality.report import QualityReportStore
from pitlake.settings import ProjectSettings
from pitlake.storage.layout import LakeLayout
from pitlake.storage.metadata_store import MetadataStore
from pitlake.storage.raw_store import RawWriteResult
from pitlake.ui.console_data import PitLakeConsoleData
from pitlake.utils import isoformat, sha256_bytes, stable_json_dumps


def _settings(tmp_path: Path) -> ProjectSettings:
    config_dir = tmp_path / "config"
    (config_dir / "dataset_contracts").mkdir(parents=True)
    (config_dir / "source_registry.yaml").write_text(
        """
sources:
  - source_id: demo_market_daily
    provider_id: demo
    logical_dataset: market_daily_ohlcv
    source_type: python_api
    access_method: local
    auth_type: none
    priority: P0
    enabled: true
    implementation_status: active_v0
    adapter_class: demo.Adapter
""",
        encoding="utf-8",
    )
    (config_dir / "schedule_policy.yaml").write_text(
        """
policies:
  - logical_dataset: market_daily_ohlcv
    priority: P0
    cadence: daily
    windows: []
    freshness_slo_minutes: 1440
""",
        encoding="utf-8",
    )
    (config_dir / "dataset_contracts" / "market_daily_ohlcv.yaml").write_text(
        """
logical_dataset: market_daily_ohlcv
contract_version: 1
description: Demo market data.
primary_key_fields:
  - instrument
  - trading_date
required_fields:
  - instrument
  - trading_date
  - open
  - high
  - low
  - close
optional_fields:
  - volume
  - amount
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


def _raw(
    settings: ProjectSettings,
    payload: dict[str, object],
    run_id: str | None = None,
) -> RawWriteResult:
    stored_at = isoformat()
    content = (stable_json_dumps(payload) + "\n").encode("utf-8")
    storage_path = settings.data_lake_root / f"raw_{len(content)}.json"
    storage_path.parent.mkdir(parents=True, exist_ok=True)
    storage_path.write_bytes(content)
    metadata_path = storage_path.with_suffix(".json.meta.json")
    metadata_path.write_text("{}", encoding="utf-8")
    return RawWriteResult(
        raw_object_id=f"raw-{len(content)}-{run_id or 'none'}",
        source_id="demo_market_daily",
        provider_id="demo",
        logical_dataset="market_daily_ohlcv",
        raw_uri=storage_path.relative_to(settings.data_lake_root).as_posix(),
        storage_path=storage_path,
        metadata_path=metadata_path,
        mime_type="application/json",
        size_bytes=len(content),
        content_hash=sha256_bytes(content),
        stored_at=stored_at,
        first_seen_at=stored_at,
        run_id=run_id,
    )


def test_console_overview_and_drilldown(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()

    run_id = metadata.create_run(
        source_id="demo_market_daily",
        provider_id="demo",
        logical_dataset="market_daily_ohlcv",
        connector_name="DemoConnector",
        connector_version="0.1",
        trigger_type="manual",
    )
    raw = _raw(settings, {"rows": 1}, run_id=run_id)
    metadata.insert_raw_object(raw)
    metadata.insert_raw_item_version(
        logical_dataset="market_daily_ohlcv",
        provider_id="demo",
        source_id="demo_market_daily",
        source_item_key="000001|2026-04-24",
        first_seen_at=raw.first_seen_at,
        stored_at=raw.stored_at,
        raw_object_id=raw.raw_object_id,
        content_hash=raw.content_hash,
        quality_status="pass",
        observed_payload={
            "instrument": "000001",
            "trading_date": "2026-04-24",
            "open": 10,
            "high": 11,
            "low": 9,
            "close": 10.5,
        },
    )
    metadata.finish_run(run_id, status="success", request_count=1, success_count=1, new_item_count=1)
    date = raw.stored_at[:10]
    QualityReportStore(settings).generate_daily_report(
        report_date=date,
        metadata_store=metadata,
        strict_coverage=True,
    )

    console = PitLakeConsoleData(settings)
    overview = console.overview(date)
    assert overview["summary"]["run_count"] == 1
    assert overview["summary"]["item_version_count"] == 1
    assert overview["datasets"][0]["logical_dataset"] == "market_daily_ohlcv"

    dataset = console.dataset_detail("market_daily_ohlcv", date=date)
    assert dataset["items"][0]["observed_payload"]["instrument"] == "000001"

    raw_detail = console.raw_detail(raw.raw_object_id)
    assert raw_detail["found"] is True
    assert raw_detail["preview"]["status"] == "ok"
    assert raw_detail["preview"]["json"]["rows"] == 1


def test_console_search_finds_source_and_items(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    raw = _raw(settings, {})
    metadata.insert_raw_object(raw)
    metadata.insert_raw_item_version(
        logical_dataset="market_daily_ohlcv",
        provider_id="demo",
        source_id="demo_market_daily",
        source_item_key="000001|2026-04-24",
        title="Ping An Bank",
        first_seen_at=raw.first_seen_at,
        stored_at=raw.stored_at,
        raw_object_id=raw.raw_object_id,
        content_hash=raw.content_hash,
        quality_status="pass",
        observed_payload={"instrument": "000001", "trading_date": "2026-04-24"},
    )

    console = PitLakeConsoleData(settings)
    payload = console.search("000001")
    assert any(result["type"] == "item" for result in payload["results"])
    payload = console.search("demo_market_daily")
    assert any(result["type"] == "source" for result in payload["results"])
