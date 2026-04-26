import os
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

from pitlake.connectors.market.baostock_daily import BaoStockMarketDailyConnector
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


def short_run_root(prefix: str) -> Path:
    base = Path(os.environ.get("PITLAKE_TEST_ROOT", "data_lake/test_runs"))
    return base / f"{prefix}_{uuid.uuid4().hex[:8]}"


class FakeBaoStockResult:
    error_code = "0"
    error_msg = ""
    fields = [
        "date",
        "code",
        "open",
        "high",
        "low",
        "close",
        "preclose",
        "volume",
        "amount",
        "adjustflag",
        "turn",
        "tradestatus",
        "pctChg",
        "isST",
    ]

    def __init__(self) -> None:
        self.rows = [
            [
                "2026-04-24",
                "sh.600001",
                "10.00",
                "10.50",
                "9.80",
                "10.20",
                "10.10",
                "123400",
                "1258680.00",
                "3",
                "0.42",
                "1",
                "0.9901",
                "0",
            ]
        ]
        self.index = -1

    def next(self) -> bool:
        self.index += 1
        return self.index < len(self.rows)

    def get_row_data(self) -> list[str]:
        return self.rows[self.index]


def test_baostock_daily_connector_collects_shadow_daily_bar(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: dict[str, object] = {"logged_out": False}

    def fake_login() -> SimpleNamespace:
        return SimpleNamespace(error_code="0", error_msg="")

    def fake_logout() -> None:
        calls["logged_out"] = True

    def fake_query(
        code: str,
        fields: str,
        start_date: str,
        end_date: str,
        frequency: str,
        adjustflag: str,
    ) -> FakeBaoStockResult:
        calls["query"] = {
            "code": code,
            "fields": fields,
            "start_date": start_date,
            "end_date": end_date,
            "frequency": frequency,
            "adjustflag": adjustflag,
        }
        return FakeBaoStockResult()

    monkeypatch.setitem(
        sys.modules,
        "baostock",
        SimpleNamespace(
            login=fake_login,
            logout=fake_logout,
            query_history_k_data_plus=fake_query,
        ),
    )
    settings = make_settings(short_run_root("baostock_daily"))
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    contract_path = Path("config/dataset_contracts/market_daily_ohlcv.yaml")
    contract = DatasetContract.from_payload(load_yaml_file(contract_path), contract_path)
    connector = BaoStockMarketDailyConnector(
        settings=settings,
        source_config={
            "source_id": "baostock_market_daily_shadow",
            "provider_id": "baostock",
            "logical_dataset": "market_daily_ohlcv",
            "default_options": {
                "start_date": "20260424",
                "end_date": "20260424",
                "symbols": ["600001"],
                "limit_symbols": 1,
                "adjustflag": "3",
            },
        },
        contract=contract,
        raw_store=RawStore(settings),
        metadata_store=metadata,
    )
    run_id = metadata.create_run(
        source_id="baostock_market_daily_shadow",
        provider_id="baostock",
        logical_dataset="market_daily_ohlcv",
        connector_name=connector.connector_name,
        connector_version=connector.connector_version,
        trigger_type="manual",
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.request_count == 1
    assert stats.success_count == 1
    assert stats.error_count == 0
    assert stats.new_item_count == 1
    assert calls["logged_out"] is True
    assert calls["query"] == {
        "code": "sh.600001",
        "fields": BaoStockMarketDailyConnector.fields,
        "start_date": "2026-04-24",
        "end_date": "2026-04-24",
        "frequency": "d",
        "adjustflag": "3",
    }
    with metadata.connect() as conn:
        rows = conn.execute(
            """
            select source_item_key, observed_payload_json
            from raw_item_version
            where logical_dataset = 'market_daily_ohlcv'
            """
        ).fetchall()
        raw_count = conn.execute(
            "select count(*) as count from raw_object where source_id = ?",
            ("baostock_market_daily_shadow",),
        ).fetchone()["count"]

    assert raw_count == 1
    assert len(rows) == 1
    assert rows[0]["source_item_key"] == "baostock:600001:2026-04-24"
    assert '"provider_id":"baostock"' in rows[0]["observed_payload_json"]
    assert '"prev_close":10.1' in rows[0]["observed_payload_json"]
