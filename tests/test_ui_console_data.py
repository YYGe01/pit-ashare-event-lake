from __future__ import annotations

from pathlib import Path

from pitlake.quality.report import QualityReportStore
from pitlake.settings import ProjectSettings
from pitlake.storage.layout import LakeLayout
from pitlake.storage.manifest_store import ManifestStore
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
    default_options:
      symbols:
        - "000001"
        - "600000"
      limit_symbols: 2
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
    manifest = ManifestStore(settings).generate_daily_manifest(
        manifest_date=date,
        metadata_store=metadata,
    )

    console = PitLakeConsoleData(settings)
    overview = console.overview(date)
    assert overview["summary"]["run_count"] == 1
    assert overview["summary"]["item_version_count"] == 1
    assert overview["datasets"][0]["logical_dataset"] == "market_daily_ohlcv"
    assert overview["source_matrix"]["dates"] == [date]
    assert any(row["symbol"] == "000001" for row in overview["symbol_universe"]["symbols"])

    dataset = console.dataset_detail("market_daily_ohlcv", date=date)
    assert dataset["items"][0]["observed_payload"]["instrument"] == "000001"
    symbol_status = {row["symbol"]: row["status"] for row in dataset["coverage"]["symbol_counts"]}
    assert symbol_status["000001"] == "present"
    assert symbol_status["600000"] == "missing"

    raw_detail = console.raw_detail(raw.raw_object_id)
    assert raw_detail["found"] is True
    assert raw_detail["preview"]["status"] == "ok"
    assert raw_detail["preview"]["json"]["rows"] == 1

    symbol = console.symbol_detail("000001", date=date)
    coverage = {row["logical_dataset"]: row["status"] for row in symbol["coverage"]}
    assert coverage["market_daily_ohlcv"] == "present"

    missing_symbol = console.symbol_detail("600000", date=date)
    missing_coverage = {
        row["logical_dataset"]: row["status"] for row in missing_symbol["coverage"]
    }
    assert missing_coverage["market_daily_ohlcv"] == "missing"

    manifests = console.manifests()
    assert manifests["manifests"][0]["manifest_id"] == manifest["manifest_id"]
    manifest_detail = console.manifest_detail(manifest["manifest_id"])
    assert manifest_detail["found"] is True
    assert manifest_detail["payload"]["manifest_id"] == manifest["manifest_id"]

    governance = console.governance(date)
    assert governance["dataset_scores"][0]["logical_dataset"] == "market_daily_ohlcv"
    assert governance["volume_baselines"]
    assert governance["source_health_summary"]["missing_count"] == 1
    assert governance["phase_status"]["phases"][0]["status"] == "completed"
    assert governance["issue_summary"]["status_flow"] == "read_only_open_only"

    tools = console.tools(date)
    assert tools["ui_cache"]["mode"] == "direct_metadata_reads"
    assert any(target["format"] == "csv" for target in tools["exports"])

    exported_json = console.export(
        kind="dataset_items",
        output_format="json",
        date=date,
        logical_dataset="market_daily_ohlcv",
    )
    assert exported_json["row_count"] == 1
    assert exported_json["rows"][0]["payload_instrument"] == "000001"

    exported_csv = console.export(
        kind="dataset_items",
        output_format="csv",
        date=date,
        logical_dataset="market_daily_ohlcv",
    )
    assert "payload_instrument" in exported_csv["csv"]


def test_console_governance_flags_volume_and_schema_drift(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    report_date = "2026-04-24"

    run_id = metadata.create_run(
        source_id="demo_market_daily",
        provider_id="demo",
        logical_dataset="market_daily_ohlcv",
        connector_name="DemoConnector",
        connector_version="0.1",
        trigger_type="manual",
    )
    metadata.finish_run(run_id, status="success", request_count=1, success_count=1, new_item_count=1)
    with metadata.connect() as conn:
        conn.execute(
            """
            update crawl_run
            set start_at = ?, end_at = ?, created_at = ?
            where run_id = ?
            """,
            (
                f"{report_date}T20:00:00+08:00",
                f"{report_date}T20:01:00+08:00",
                f"{report_date}T20:00:00+08:00",
                run_id,
            ),
        )

    for day in ["2026-04-21", "2026-04-22", "2026-04-23"]:
        for index in range(5):
            metadata.insert_raw_item_version(
                logical_dataset="market_daily_ohlcv",
                provider_id="demo",
                source_id="demo_market_daily",
                source_item_key=f"000001|{day}|{index}",
                first_seen_at=f"{day}T20:00:00+08:00",
                stored_at=f"{day}T20:00:00+08:00",
                raw_object_id=f"raw-{day}-{index}",
                content_hash=f"hash-{day}-{index}",
                quality_status="pass",
                observed_payload={
                    "instrument": "000001",
                    "trading_date": day,
                    "open": 10,
                    "high": 11,
                    "low": 9,
                    "close": 10.5,
                },
            )
    metadata.insert_raw_item_version(
        logical_dataset="market_daily_ohlcv",
        provider_id="demo",
        source_id="demo_market_daily",
        source_item_key=f"000001|{report_date}",
        first_seen_at=f"{report_date}T20:00:00+08:00",
        stored_at=f"{report_date}T20:00:00+08:00",
        raw_object_id="raw-current",
        content_hash="hash-current",
        quality_status="pass",
        observed_payload={
            "instrument": "000001",
            "trading_date": report_date,
            "open": 10,
            "high": 11,
            "low": 9,
            "close": 10.5,
            "unexpected_vendor_field": "new",
        },
    )
    QualityReportStore(settings).generate_daily_report(
        report_date=report_date,
        metadata_store=metadata,
    )

    governance = PitLakeConsoleData(settings).governance(report_date)
    market_volume = next(
        row
        for row in governance["volume_baselines"]
        if row["logical_dataset"] == "market_daily_ohlcv"
    )
    assert market_volume["status"] == "warn"
    assert market_volume["current_count"] == 1
    assert market_volume["baseline_average"] == 5
    assert market_volume["ratio_to_baseline"] == 0.2

    drift = governance["schema_drift"][0]
    assert drift["logical_dataset"] == "market_daily_ohlcv"
    assert drift["unknown_fields"] == ["unexpected_vendor_field"]
    score = governance["dataset_scores"][0]
    assert score["logical_dataset"] == "market_daily_ohlcv"
    assert score["quality_score"] < 100


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
    payload = console.search(raw.raw_object_id)
    assert any(result["type"] == "raw" for result in payload["results"])
