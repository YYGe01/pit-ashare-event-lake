from pathlib import Path

from pitlake.connectors.commodities.dce import DceDailyConnector
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
    text = """
    <html>
      <body>
        <table>
          <tr>
            <th>商品名称</th><th>合约名称</th><th>开盘价</th><th>最高价</th>
            <th>最低价</th><th>收盘价</th><th>前结算价</th><th>结算价</th>
            <th>成交量</th><th>持仓量</th>
          </tr>
          <tr>
            <td>豆一</td><td>a2605</td><td>4030</td><td>4050</td>
            <td>4010</td><td>4040</td><td>4020</td><td>4035</td>
            <td>1,200</td><td>8,000</td>
          </tr>
          <tr>
            <td>豆一</td><td>小计</td><td></td><td></td>
            <td></td><td></td><td></td><td></td><td>1,200</td><td></td>
          </tr>
        </table>
      </body>
    </html>
    """

    def raise_for_status(self) -> None:
        return None


def test_dce_daily_connector_collects_contract_rows(tmp_path: Path, monkeypatch) -> None:
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
    contract_path = Path("config/dataset_contracts/commodity_daily.yaml")
    connector = DceDailyConnector(
        settings=settings,
        source_config={
            "source_id": "dce_daily_commodity",
            "provider_id": "dce",
            "logical_dataset": "commodity_daily",
            "default_options": {"end_date": "20260424", "timeout_seconds": 5},
        },
        contract=DatasetContract.from_payload(load_yaml_file(contract_path), contract_path),
        raw_store=RawStore(settings),
        metadata_store=metadata,
    )
    run_id = metadata.create_run(
        source_id="dce_daily_commodity",
        provider_id="dce",
        logical_dataset="commodity_daily",
        connector_name="DceDailyConnector",
        connector_version="0.1.0",
        trigger_type="manual",
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.request_count == 1
    assert stats.success_count == 1
    assert stats.error_count == 0
    assert stats.new_item_count == 1
    assert calls["params"] == {
        "dayQuotes.variety": "all",
        "dayQuotes.trade_type": "0",
        "year": "2026",
        "month": "3",
        "day": "24",
    }
    with metadata.connect() as conn:
        rows = conn.execute(
            """
            select source_item_key, title, observed_payload_json
            from raw_item_version
            where logical_dataset = 'commodity_daily'
            """
        ).fetchall()

    assert len(rows) == 1
    assert rows[0]["source_item_key"] == "dce:DCE:a2605:2026-04-24"
    assert rows[0]["title"] == "DCE a2605 commodity daily 2026-04-24"
    assert '"exchange":"DCE"' in rows[0]["observed_payload_json"]
    assert '"contract":"a2605"' in rows[0]["observed_payload_json"]
    assert '"settlement":4035.0' in rows[0]["observed_payload_json"]
