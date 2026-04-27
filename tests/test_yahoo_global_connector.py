from datetime import datetime, timezone
from pathlib import Path

from pitlake.connectors.global_markets.yahoo import YahooFinanceGlobalDailyConnector
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

    def json(self) -> dict:
        timestamp = int(datetime(2026, 4, 24, tzinfo=timezone.utc).timestamp())
        return {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "currency": "USD",
                            "symbol": "AAPL",
                            "exchangeName": "NMS",
                            "shortName": "Apple Inc.",
                            "exchangeTimezoneName": "America/New_York",
                            "instrumentType": "EQUITY",
                            "dataGranularity": "1d",
                        },
                        "timestamp": [timestamp],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [170.0],
                                    "high": [172.0],
                                    "low": [169.0],
                                    "close": [171.0],
                                    "volume": [1000],
                                }
                            ],
                            "adjclose": [{"adjclose": [171.0]}],
                        },
                    }
                ],
                "error": None,
            }
        }

    def raise_for_status(self) -> None:
        return None


def test_yahoo_global_connector_collects_target_date(tmp_path: Path, monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_get(url: str, params: dict, headers: dict, timeout: int) -> FakeResponse:
        calls["url"] = url
        calls["params"] = params
        calls["headers"] = headers
        calls["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr("requests.get", fake_get)
    settings = make_settings(tmp_path)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    contract_path = Path("config/dataset_contracts/global_market_daily.yaml")
    connector = YahooFinanceGlobalDailyConnector(
        settings=settings,
        source_config={
            "source_id": "yahoo_finance_global_daily",
            "provider_id": "yahoo_finance",
            "logical_dataset": "global_market_daily",
            "default_options": {
                "end_date": "20260424",
                "symbols": ["AAPL"],
                "limit_symbols": 1,
                "timeout_seconds": 5,
            },
        },
        contract=DatasetContract.from_payload(load_yaml_file(contract_path), contract_path),
        raw_store=RawStore(settings),
        metadata_store=metadata,
    )
    run_id = metadata.create_run(
        source_id="yahoo_finance_global_daily",
        provider_id="yahoo_finance",
        logical_dataset="global_market_daily",
        connector_name="YahooFinanceGlobalDailyConnector",
        connector_version="0.1.0",
        trigger_type="manual",
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.request_count == 1
    assert stats.success_count == 1
    assert stats.error_count == 0
    assert stats.new_item_count == 1
    assert calls["url"].endswith("/v8/finance/chart/AAPL")
    assert calls["params"]["interval"] == "1d"
    with metadata.connect() as conn:
        rows = conn.execute(
            """
            select source_item_key, title, observed_payload_json
            from raw_item_version
            where logical_dataset = 'global_market_daily'
            """
        ).fetchall()

    assert len(rows) == 1
    assert rows[0]["source_item_key"] == "yahoo_finance:AAPL:2026-04-24"
    assert rows[0]["title"] == "AAPL Yahoo daily 2026-04-24"
    assert '"currency":"USD"' in rows[0]["observed_payload_json"]
    assert '"timezone":"America/New_York"' in rows[0]["observed_payload_json"]
