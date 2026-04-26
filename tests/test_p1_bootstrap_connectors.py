import os
import json
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from pitlake.connectors.p1_bootstrap import (
    AkshareCapitalFlowConnector,
    AkshareConceptMembershipConnector,
    AkshareBaiduEconomicNewsConnector,
    AkshareFundPortfolioHoldConnector,
    AkshareHsgtNorthboundConnector,
    AkshareIndustryMembershipConnector,
    AkshareLhbDetailConnector,
    AkshareMacroChinaFinancialCreditConnector,
    AkshareMarginTradingDetailConnector,
    AkshareStockHotRankConnector,
    AkshareStockNewsConnector,
    AkshareStockNewsMainCxConnector,
    GdeltDocArtListConnector,
    OpenMeteoWeatherDailyConnector,
)
from pitlake.connectors.base import ResponsePayload
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


def build_connector(
    *,
    run_root: Path,
    connector_cls,
    source_id: str,
    logical_dataset: str,
    provider_id: str = "akshare",
    default_options: dict | None = None,
):
    settings = make_settings(run_root)
    LakeLayout(settings).create()
    metadata = MetadataStore(settings)
    metadata.init_schema()
    contract_path = Path("config/dataset_contracts") / f"{logical_dataset}.yaml"
    connector = connector_cls(
        settings=settings,
        source_config={
            "source_id": source_id,
            "provider_id": provider_id,
            "logical_dataset": logical_dataset,
            "default_options": default_options or {},
        },
        contract=DatasetContract.from_payload(load_yaml_file(contract_path), contract_path),
        raw_store=RawStore(settings),
        metadata_store=metadata,
    )
    run_id = metadata.create_run(
        source_id=source_id,
        provider_id=provider_id,
        logical_dataset=logical_dataset,
        connector_name=connector.connector_name,
        connector_version=connector.connector_version,
        trigger_type="manual",
    )
    return connector, metadata, run_id


def count_items(metadata: MetadataStore, logical_dataset: str) -> int:
    with metadata.connect() as conn:
        return conn.execute(
            "select count(*) as count from raw_item_version where logical_dataset = ?",
            (logical_dataset,),
        ).fetchone()["count"]


def short_run_root(prefix: str) -> Path:
    base = Path(os.environ.get("PITLAKE_TEST_ROOT", "data_lake/test_runs"))
    return base / f"{prefix}_{uuid.uuid4().hex[:8]}"


def test_macro_financial_credit_connector_collects_rows(monkeypatch) -> None:
    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            macro_china_new_financial_credit=lambda: pd.DataFrame(
                {"日期": ["2024-03"], "新增人民币贷款": [30900]}
            )
        ),
    )
    connector, metadata, run_id = build_connector(
        run_root=short_run_root("p1_macro"),
        connector_cls=AkshareMacroChinaFinancialCreditConnector,
        source_id="akshare_macro_china_financial_credit",
        logical_dataset="macro_indicator",
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.success_count == 1
    assert stats.new_item_count == 1
    assert count_items(metadata, "macro_indicator") == 1


def test_capital_flow_connector_collects_stock_rows(monkeypatch) -> None:
    def fake_fund_flow(stock: str, market: str) -> pd.DataFrame:
        assert stock == "600000"
        assert market == "sh"
        return pd.DataFrame({"日期": ["2024-04-24"], "主力净流入": [12.3]})

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(stock_individual_fund_flow=fake_fund_flow),
    )
    connector, metadata, run_id = build_connector(
        run_root=short_run_root("p1_flow"),
        connector_cls=AkshareCapitalFlowConnector,
        source_id="akshare_stock_capital_flow",
        logical_dataset="capital_flow",
        default_options={"symbols": ["600000"], "limit_symbols": 1},
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.success_count == 1
    assert stats.new_item_count == 1
    assert count_items(metadata, "capital_flow") == 1


def test_margin_trading_connector_collects_sse_and_szse_rows(monkeypatch) -> None:
    def fake_sse(date: str) -> pd.DataFrame:
        assert date == "20260424"
        return pd.DataFrame(
            {
                "信用交易日期": ["20260424"],
                "标的证券代码": ["600000"],
                "融资余额": [100.0],
            }
        )

    def fake_szse(date: str) -> pd.DataFrame:
        assert date == "20260424"
        return pd.DataFrame(
            {
                "信用交易日期": ["20260424"],
                "标的证券代码": ["000001"],
                "融资余额": [200.0],
            }
        )

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            stock_margin_detail_sse=fake_sse,
            stock_margin_detail_szse=fake_szse,
        ),
    )
    connector, metadata, run_id = build_connector(
        run_root=short_run_root("p1_margin"),
        connector_cls=AkshareMarginTradingDetailConnector,
        source_id="akshare_margin_trading_detail",
        logical_dataset="capital_flow",
        default_options={"end_date": "20260424", "markets": ["SSE", "SZSE"]},
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.success_count == 2
    assert stats.new_item_count == 2
    assert count_items(metadata, "capital_flow") == 2


def test_lhb_detail_connector_collects_rows(monkeypatch) -> None:
    def fake_lhb(start_date: str, end_date: str) -> pd.DataFrame:
        assert start_date == "20260424"
        assert end_date == "20260424"
        return pd.DataFrame(
            {
                "代码": ["000062"],
                "名称": ["深圳华强"],
                "上榜日": ["2026-04-24"],
                "龙虎榜净买额": [94022391.19],
            }
        )

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(stock_lhb_detail_em=fake_lhb),
    )
    connector, metadata, run_id = build_connector(
        run_root=short_run_root("p1_lhb"),
        connector_cls=AkshareLhbDetailConnector,
        source_id="akshare_lhb_detail",
        logical_dataset="capital_flow",
        default_options={"start_date": "20260424", "end_date": "20260424"},
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.success_count == 1
    assert stats.new_item_count == 1
    assert count_items(metadata, "capital_flow") == 1


def test_hsgt_northbound_connector_collects_rows(monkeypatch) -> None:
    def fake_hsgt(symbol: str) -> pd.DataFrame:
        assert symbol == "北向资金"
        return pd.DataFrame(
            {
                "日期": ["2026-04-24"],
                "当日成交净买额": [12.34],
                "买入成交额": [100.0],
                "卖出成交额": [87.66],
            }
        )

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(stock_hsgt_hist_em=fake_hsgt),
    )
    connector, metadata, run_id = build_connector(
        run_root=short_run_root("p1_hsgt"),
        connector_cls=AkshareHsgtNorthboundConnector,
        source_id="akshare_hsgt_northbound_flow",
        logical_dataset="capital_flow",
        default_options={"symbol": "北向资金"},
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.success_count == 1
    assert stats.new_item_count == 1
    assert count_items(metadata, "capital_flow") == 1


def test_fund_portfolio_hold_connector_collects_rows(monkeypatch) -> None:
    def fake_fund_hold(symbol: str, date: str) -> pd.DataFrame:
        assert symbol == "基金持仓"
        assert date == "20260331"
        return pd.DataFrame(
            {
                "股票代码": ["600000"],
                "基金代码": ["008286"],
                "持股数": [10000],
            }
        )

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(stock_report_fund_hold=fake_fund_hold),
    )
    connector, metadata, run_id = build_connector(
        run_root=short_run_root("p1_fund_hold"),
        connector_cls=AkshareFundPortfolioHoldConnector,
        source_id="akshare_fund_portfolio_hold",
        logical_dataset="fund_holding",
        default_options={"symbol": "基金持仓", "end_date": "20260331"},
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.success_count == 1
    assert stats.new_item_count == 1
    assert count_items(metadata, "fund_holding") == 1


def test_stock_news_connector_collects_metadata(monkeypatch) -> None:
    def fake_stock_news(symbol: str) -> pd.DataFrame:
        assert symbol == "600000"
        return pd.DataFrame(
            {
                "新闻标题": ["浦发银行发布公告"],
                "新闻链接": ["https://example.com/news"],
                "发布时间": ["2026-04-24 20:00:00"],
            }
        )

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(stock_news_em=fake_stock_news),
    )
    connector, metadata, run_id = build_connector(
        run_root=short_run_root("p1_stock_news"),
        connector_cls=AkshareStockNewsConnector,
        source_id="akshare_stock_news_em",
        logical_dataset="financial_news",
        default_options={"symbols": ["600000"], "limit_symbols": 1},
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.success_count == 1
    assert stats.new_item_count == 1
    assert count_items(metadata, "financial_news") == 1


def test_stock_news_main_cx_connector_collects_metadata(monkeypatch) -> None:
    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            stock_news_main_cx=lambda: pd.DataFrame(
                {
                    "标题": ["财新财经新闻"],
                    "链接": ["https://example.com/cx"],
                    "发布时间": ["2026-04-24 21:00:00"],
                }
            )
        ),
    )
    connector, metadata, run_id = build_connector(
        run_root=short_run_root("p1_cx_news"),
        connector_cls=AkshareStockNewsMainCxConnector,
        source_id="akshare_stock_news_main_cx",
        logical_dataset="financial_news",
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.success_count == 1
    assert stats.new_item_count == 1
    assert count_items(metadata, "financial_news") == 1


def test_baidu_economic_news_connector_collects_metadata(monkeypatch) -> None:
    def fake_economic_news(date: str) -> pd.DataFrame:
        assert date == "20260424"
        return pd.DataFrame(
            {
                "标题": ["宏观政策新闻"],
                "链接": ["https://example.com/economic"],
                "发布时间": ["2026-04-24 09:00:00"],
            }
        )

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(news_economic_baidu=fake_economic_news),
    )
    connector, metadata, run_id = build_connector(
        run_root=short_run_root("p1_baidu_news"),
        connector_cls=AkshareBaiduEconomicNewsConnector,
        source_id="akshare_baidu_economic_news",
        logical_dataset="financial_news",
        default_options={"end_date": "20260424"},
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.success_count == 1
    assert stats.new_item_count == 1
    assert count_items(metadata, "financial_news") == 1


def test_stock_hot_rank_connector_collects_rows(monkeypatch) -> None:
    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            stock_hot_rank_em=lambda: pd.DataFrame(
                {"排名": [1], "代码": ["300750"], "股票名称": ["宁德时代"], "热度": [9999]}
            )
        ),
    )
    connector, metadata, run_id = build_connector(
        run_root=short_run_root("p1_hot_rank"),
        connector_cls=AkshareStockHotRankConnector,
        source_id="akshare_stock_hot_rank",
        logical_dataset="public_sentiment",
    )

    stats = connector.collect(run_id=run_id, options={})

    assert stats.success_count == 1
    assert stats.new_item_count == 1
    assert count_items(metadata, "public_sentiment") == 1


def test_industry_and_concept_membership_connectors_collect_snapshots(
    monkeypatch,
) -> None:
    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            stock_board_industry_cons_em=lambda symbol: pd.DataFrame(
                {"代码": ["600000"], "名称": ["浦发银行"], "权重": [1.0]}
            ),
            stock_board_concept_cons_em=lambda symbol: pd.DataFrame(
                {"代码": ["300750"], "名称": ["宁德时代"], "权重": [2.0]}
            ),
        ),
    )
    industry, metadata1, run_id1 = build_connector(
        run_root=short_run_root("p1_ind"),
        connector_cls=AkshareIndustryMembershipConnector,
        source_id="akshare_industry_membership",
        logical_dataset="industry_membership",
        default_options={"board_names": ["银行"], "limit_boards": 1},
    )
    concept, metadata2, run_id2 = build_connector(
        run_root=short_run_root("p1_con"),
        connector_cls=AkshareConceptMembershipConnector,
        source_id="akshare_concept_membership",
        logical_dataset="concept_membership",
        default_options={"board_names": ["机器人概念"], "limit_boards": 1},
    )

    industry_stats = industry.collect(run_id=run_id1, options={})
    concept_stats = concept.collect(run_id=run_id2, options={})

    assert industry_stats.new_item_count == 1
    assert concept_stats.new_item_count == 1
    assert count_items(metadata1, "industry_membership") == 1
    assert count_items(metadata2, "concept_membership") == 1


def test_gdelt_doc_connector_collects_article_metadata(monkeypatch) -> None:
    connector, metadata, run_id = build_connector(
        run_root=short_run_root("p1_gdelt"),
        connector_cls=GdeltDocArtListConnector,
        source_id="gdelt_doc_global_event_summary",
        provider_id="gdelt",
        logical_dataset="global_event_summary",
        default_options={"query": "china market", "timespan": "24h", "maxrecords": 2},
    )

    def fake_execute(request):
        payload = {
            "articles": [
                {
                    "url": "https://example.com/a",
                    "title": "China market headline",
                    "seendate": "20260426083000",
                    "sourceCountry": "US",
                    "language": "English",
                }
            ]
        }
        return ResponsePayload(
            request=request,
            status_code=200,
            content=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"content-type": "application/json"},
            final_url="https://api.gdeltproject.org/api/v2/doc/doc",
            mime_type="application/json",
        )

    monkeypatch.setattr(connector, "execute_request", fake_execute)

    stats = connector.collect(run_id=run_id, options={})

    assert stats.success_count == 1
    assert stats.new_item_count == 1
    assert count_items(metadata, "global_event_summary") == 1


def test_open_meteo_weather_connector_collects_daily_rows(monkeypatch) -> None:
    connector, metadata, run_id = build_connector(
        run_root=short_run_root("p1_weather"),
        connector_cls=OpenMeteoWeatherDailyConnector,
        source_id="open_meteo_weather_daily",
        provider_id="open_meteo",
        logical_dataset="weather_daily",
        default_options={
            "start_date": "2026-04-24",
            "end_date": "2026-04-24",
            "locations": [{"location_id": "shanghai", "latitude": 31.2304, "longitude": 121.4737}],
        },
    )

    def fake_execute(request):
        payload = {
            "daily": {
                "time": ["2026-04-24"],
                "temperature_2m_max": [25.1],
                "precipitation_sum": [0.0],
            }
        }
        return ResponsePayload(
            request=request,
            status_code=200,
            content=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"content-type": "application/json"},
            final_url="https://archive-api.open-meteo.com/v1/archive",
            mime_type="application/json",
        )

    monkeypatch.setattr(connector, "execute_request", fake_execute)

    stats = connector.collect(run_id=run_id, options={})

    assert stats.success_count == 1
    assert stats.new_item_count == 1
    assert count_items(metadata, "weather_daily") == 1
