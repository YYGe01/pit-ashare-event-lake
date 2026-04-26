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
```

日线和涨跌停连接器默认采样 `000001`、`600000`、`300750`。交易日历连接器默认采集 `20260424` 的 `cn_ashare` 交易日记录。交易状态连接器默认查询 `20260424` 的停复牌记录。同一天重复运行时，框架会保留 raw 采集事实，并在 `raw_item_version` 层识别已存在的 item version。

## 范围

- 原始采集数据只追加保存，不覆盖历史版本。
- 每条数据都记录 `first_seen_at`，即系统第一次看到它的时间。
- 保存数据源元信息、原始响应、原始文件、内容哈希和每日采集清单。
- 下游解析、事件抽取、特征、模型和回测不写入采集层。
