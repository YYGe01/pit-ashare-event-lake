# A 股 PIT 事件数据湖

这是一个面向中国 A 股日频/周频量化研究的 point-in-time 数据采集项目。

当前默认研究节奏是每日或每周生成候选股票和预测分，用于辅助调仓；分钟级行情、Level-2、tick 和逐笔委托不属于第一阶段默认采集范围。

当前仓库只聚焦“数据采集层”：数据源注册、采集运行账本、原始数据追加保存、每日清单、监控、备份和审计。事件抽取、特征工程、模型训练和回测属于后续研究层。

## 设计文档

- `docs/pre_collection_requirements_zh.md`：正式建立采集框架前需要由使用者提供或确认的输入清单，包括账号/API Key、授权、数据源下载包、部署环境、存储、备份、预算和告警。
- `docs/v0_runtime_decisions_zh.md`：V0 已确认的运行决策，包括只做 A 股日频 P0、免费源优先、本地电脑先验证、服务器迁移建议、C 盘数据湖路径、备份和告警策略。
- `docs/project_data_flow_zh.md`：小白版当前项目数据流说明，用真实 A 股日线样例解释每个阶段的输入、处理和输出。
- `docs/realtime_pit_data_collection_plan_zh.md`：PIT 数据采集实施手册，说明如何保存原始数据、时间账本、核心表、目录和首月落地任务。
- `docs/pit_data_collection_architecture_zh.md`：长期采集架构总纲，说明数据源/供应商抽象、质量门禁、治理、运维和供应商切换机制。

## Agent 开发入口

- `AGENTS.md`：Codex、Cursor 和其他 coding agent 的通用项目指令。
- `.cursor/rules/`：Cursor Project Rules，按 Cursor 官方推荐的 `.mdc` 格式保存。
- `docs/agent_journal/`：每次 agent 工作的简短日志，用于跨天续接。

## 环境

```powershell
conda env create -f environment.yml
conda activate pit-ashare-event-lake
pip install -e .
```

## V0 本地验证

```powershell
pitlake validate-config
pitlake init
pitlake smoke-run
pitlake quality-report --date 2026-04-26
```

`smoke-run` 不访问外网，只验证本地 raw 写入、SQLite 元数据账本、质量检查和每日 manifest 生成。

## V0 真实采集

当前已启用九个 P0 bootstrap source：

- `akshare_market_daily_ohlcv`：A 股日线 OHLCV，使用 `akshare.stock_zh_a_daily`。
- `akshare_adjustment_factor`：A 股复权因子，使用不复权 close 与前复权 close 比值做 V0 推算。
- `akshare_announcement_index`：A 股公告索引，使用 `akshare.stock_notice_report`。
- `akshare_cctv_policy_news`：政策/宏观新闻，使用 `akshare.news_cctv`。
- `ashare_trading_calendar`：A 股交易日历，使用 `akshare.tool_trade_date_hist_sina`。
- `ashare_trade_status`：A 股停复牌/交易状态，使用 `akshare.stock_tfp_em`。
- `ashare_price_limit`：A 股涨跌停价格，使用 `akshare.stock_zh_a_daily` 的前收盘价和板块规则做 V0 推算。
- `akshare_commodity_daily`：商品期货日频，使用 `akshare.futures_zh_daily_sina`。
- `akshare_global_market_daily`：全球市场日频样例，使用 `akshare.stock_us_daily`。

第一次本地测试建议限制 symbol 数量：

```powershell
pitlake run-source --source-id akshare_market_daily_ohlcv --start-date 20260424 --end-date 20260424 --limit-symbols 3 --manifest-date 2026-04-26
pitlake run-enabled --start-date 20260424 --end-date 20260424 --limit-symbols 3 --manifest-date 2026-04-26
pitlake quality-report --date 2026-04-26
pitlake reconcile --date 2026-04-26
pitlake alert --message "pitlake daily check failed" --payload-json data_lake/collection/reconciliation_reports/dt=2026-04-26/latest_reconciliation_report.json
pitlake backup
```

日线和涨跌停连接器默认采样 `000001`、`600000`、`300750`。交易日历连接器默认采集 `20260424` 的 `cn_ashare` 交易日记录。交易状态连接器默认查询 `20260424` 的停复牌记录。同一天重复运行时，框架会保留 raw 采集事实，并在 `raw_item_version` 层识别已存在的 item version。

## 范围

- 原始采集数据只追加保存，不覆盖历史版本。
- 每条数据都记录 `first_seen_at`，即系统第一次看到它的时间。
- 保存数据源元信息、原始响应、原始文件、内容哈希和每日采集清单。
- 下游解析、事件抽取、特征、模型和回测不写入采集层。
## V0 对账、告警和备份

`pitlake reconcile --date YYYY-MM-DD` 生成每日对账报告，默认覆盖 `adjustment_factor`、`price_limit`、`announcement_index` 和 `policy_regulatory_doc`。当前只有一个已采集 source 时，报告会标记 `missing_counterparty_source`；后续启用 shadow/official source 后，同一命令会比较同一观察项的关键字段差异。

```powershell
pitlake reconcile --date 2026-04-26
pitlake reconcile --date 2026-04-26 --datasets adjustment_factor,price_limit
```

`pitlake alert` 默认写入 `data_lake/collection/logs/alerts.jsonl`。如需外部 webhook，不要把 URL 写入 git，用环境变量或命令参数：

```powershell
$env:PITLAKE_ALERT_WEBHOOK_URL="https://example.invalid/webhook"
pitlake alert --message "pitlake daily check failed" --payload-json data_lake/collection/quality_reports/dt=2026-04-26/latest_quality_report.json
```

`pitlake backup` 默认备份 SQLite metadata、manifest、quality report 和 reconciliation report 到 `data_lake/backups/local/`。外部备份目录优先用 `PITLAKE_EXTERNAL_BACKUP_DIR`，也可以用 `--target-dir` 指定；raw 数据需要显式加 `--include-raw`。

```powershell
pitlake backup
$env:PITLAKE_EXTERNAL_BACKUP_DIR="E:\pitlake_backup"
pitlake backup --include-raw
pitlake backup --target-dir E:\pitlake_backup
```

## P1 bootstrap

P0 目前已经达到 bootstrap 闭环：9 个 P0 logical_dataset 都有至少一个 enabled source。严格意义上的 P0 长期稳定完成，还需要继续观察连续运行、shadow/official source 对账、外部告警和外部备份。

当前已开始 P1 采集层 bootstrap：

- `akshare_financial_indicator`：A 股财务指标，使用 `akshare.stock_financial_analysis_indicator`，将 provider 返回的指标列原样保存到 `metric_payload`。
- `akshare_macro_china_financial_credit`：中国新增金融信贷宏观指标，使用 `akshare.macro_china_new_financial_credit`，将 provider 返回指标保存到 `metric_payload`。
- `akshare_stock_capital_flow`：个股资金流样例，使用 `akshare.stock_individual_fund_flow`，默认采样 `600000`。
- `akshare_margin_trading_detail`：沪深融资融券明细，使用 `akshare.stock_margin_detail_sse` / `stock_margin_detail_szse`。
- `akshare_lhb_detail`：龙虎榜明细，使用 `akshare.stock_lhb_detail_em`。
- `akshare_hsgt_northbound_flow`：北向资金汇总，使用 `akshare.stock_hsgt_hist_em`。
- `akshare_industry_membership`：行业板块成分股快照，使用 `akshare.stock_board_industry_cons_em`，默认采样 `银行`。
- `akshare_concept_membership`：概念板块成分股快照，使用 `akshare.stock_board_concept_cons_em`，默认采样 `机器人概念`。
- `gdelt_doc_global_event_summary`：全球新闻/事件元数据摘要，使用 GDELT DOC 2.0 `ArtList`，只保存文章元数据，不做事件抽取。
- `open_meteo_weather_daily`：天气日频观测，使用 Open-Meteo Historical Weather API，默认采样上海和深圳。
- `akshare_fund_portfolio_hold`：基金持仓公开快照，使用 `akshare.stock_report_fund_hold`。
- `akshare_stock_news_main_cx`：财经新闻标题元数据，使用 `akshare.stock_news_main_cx`。
- `akshare_baidu_economic_news`：百度经济日历/财经事件，使用 `akshare.news_economic_baidu`。
- `akshare_stock_hot_rank`：股票公开热度排行代理，使用 `akshare.stock_hot_rank_em`。

当前 P1 bootstrap 已可交付：低成本公开源覆盖了财务、宏观、资金行为、行业/概念、新闻/事件、天气、基金持仓和公开热度代理。严格生产级仍需要连续运行观察、官方/付费源对账和采样范围扩大。

`akshare_stock_news_em` 当前在本地 AkShare 1.18.57 会触发上游正则错误，保留为 disabled planned source，不进入 P1 交付闭环。

单独运行示例：

```powershell
pitlake run-source --source-id akshare_financial_indicator --start-date 20240101 --limit-symbols 1 --manifest-date 2026-04-26
pitlake run-source --source-id akshare_macro_china_financial_credit --manifest-date 2026-04-26
pitlake run-source --source-id akshare_stock_capital_flow --limit-symbols 1 --manifest-date 2026-04-26
pitlake run-source --source-id akshare_margin_trading_detail --end-date 20260424 --manifest-date 2026-04-26
pitlake run-source --source-id akshare_lhb_detail --start-date 20260424 --end-date 20260424 --manifest-date 2026-04-26
pitlake run-source --source-id akshare_hsgt_northbound_flow --manifest-date 2026-04-26
pitlake run-source --source-id akshare_industry_membership --manifest-date 2026-04-26
pitlake run-source --source-id akshare_concept_membership --manifest-date 2026-04-26
pitlake run-source --source-id gdelt_doc_global_event_summary --manifest-date 2026-04-26
pitlake run-source --source-id open_meteo_weather_daily --manifest-date 2026-04-26
pitlake run-source --source-id akshare_fund_portfolio_hold --manifest-date 2026-04-26
pitlake run-source --source-id akshare_stock_news_main_cx --manifest-date 2026-04-26
pitlake run-source --source-id akshare_baidu_economic_news --manifest-date 2026-04-26
pitlake run-source --source-id akshare_stock_hot_rank --manifest-date 2026-04-26
```

Windows 深路径下运行测试时，如果 raw 文件路径触发 260 字符限制，可把测试根目录指向较短路径：

```powershell
$env:PITLAKE_TEST_ROOT="C:\Users\73498\.codex\memories\pitlake_tests"
pytest
```
