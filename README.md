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

## 数据源覆盖汇总

口径截至 2026-04-26，并与 `config/source_registry.yaml`、`config/provider_registry.yaml` 和 `docs/v0_runtime_decisions_zh.md` 对齐。

- 已登记 source 共 47 个：26 个 `enabled: true`，21 个 disabled/planned。
- 当前 enabled source 全部来自免费、公开或开源数据源，不需要账号或付费凭据。
- `run-enabled` 只运行 enabled source；disabled/planned source 是后续对账、官方补源或付费授权占位，不代表当前已有可运行爬取方法。
- 下表“支持源数”格式为“当前可运行 / 已登记总数”。

### 厂商和用途速览

| 厂商/源 | 费用和授权 | 当前用途 | 当前状态 |
| --- | --- | --- | --- |
| AkShare | 免费开源库，无凭据 | P0/P1/P2 bootstrap 主力源，覆盖 A 股日线、复权推算、公告索引、政策新闻、商品、全球市场、财务、资金、板块、基金、新闻、热度、分钟线、研报索引和评论聚合 | 已启用，适合作 bootstrap；严肃研究前需要官方源或付费源对账 |
| GDELT | 免费公开 API，无凭据 | 全球新闻/事件文章元数据摘要 | 已启用，metadata only，不做事件抽取 |
| Open-Meteo | 免费公开 API，无凭据 | 地点级日频天气观测 | 已启用 |
| BaoStock | 免费开源库，无凭据 | A 股日线 shadow/fallback | 已实现 connector，但默认不启用；可手动 `run-source` 做 AkShare 日线对账 |
| CNINFO / SSE / SZSE / BSE | 免费公开网站/交易所 | 公告官方/补充源 | CNINFO、SSE、SZSE、BSE 公告索引 connector 已实现但默认不启用；PDF/detail 下载仍未开发 |
| CSRC / gov.cn / PBC | 免费公开监管/政府网站 | 政策监管官方源 | 已登记但未启用，当前没有可运行 connector |
| SHFE / DCE / CZCE / GFEX | 免费公开交易所网站 | 国内商品期货官方结算/日频源 | 已登记但未启用，当前没有可运行 connector |
| Stooq / Yahoo Finance | 免费公开网站/API | 全球市场日频 shadow/supplemental 候选 | 已登记但未启用，当前没有可运行 connector |
| NASA FIRMS | 免费或配额/API key | 遥感/另类观测候选 | 已登记但未启用，需先确认 API key、地理映射和 alpha 假设 |
| Tushare Pro | 付费或积分/token | 后续 A 股官方化/稳定化补源候选 | 只在 provider registry 预留，当前没有 source |
| Wind | 付费 vendor | Level-2、tick、后续对账和 fallback | 已登记 P2 planned source，但未启用，需授权和容量评估 |
| Choice | 付费 vendor | 授权全文、研报/新闻/电话会、供应链关系 | 已登记 P2 planned source，但未启用，需合同确认 |
| RavenPack | 付费 vendor | 专业结构化事件 feed | 已登记 P2 planned source，但未启用，需授权 |

### “已登记但未启用”的含义

`source_registry.yaml` 里的 source 分三种状态：

- 已启用：`enabled: true`，有 connector 代码和基础测试，`pitlake run-enabled` 会实际运行。
- 已登记但未启用：已经写入 `source_registry.yaml` 作为后续补源、对账或授权占位，但 `enabled: false`，`run-enabled` 不会运行；多数这类 source 当前还没有可运行 connector。
- 只预留 provider：例如 Tushare Pro，目前只在 `provider_registry.yaml` 里预留厂商和凭据引用，还没有具体 source 配置。

免费源没有直接启用，主要是因为“免费”不等于“已经可稳定采集”。每个 source 启用前都需要明确字段映射、限频、异常处理、raw 保存口径、contract 校验和最小测试；网页类 source 还要处理分页、附件、HTML 结构变化和条款风险。

| 源类别 | 暂不启用原因 |
| --- | --- |
| BaoStock | 已实现 A 股日线 shadow connector，但默认不随 `run-enabled` 运行；原因是先保持 AkShare bootstrap 主流程稳定，再用 `run-source` 单独观察 BaoStock 的可用性、字段口径和重复采集表现。 |
| CNINFO / SSE / SZSE / BSE | CNINFO、SSE、SZSE、BSE 公告索引 connector 已实现，可手动采列表元数据和 PDF URL；默认不随 `run-enabled` 运行，原因是还需要连续观察限频、分页、字段变化和重复采集表现。PDF 下载、附件下载和详情页解析仍未开发。 |
| CSRC / gov.cn / PBC | 这些是政策监管权威源，应后续补。当前 AkShare CCTV 只是 bootstrap 代理源；官方网站需要单独解析栏目、HTML、附件和发布时间。 |
| SHFE / DCE / CZCE / GFEX | 商品交易所官方结算源值得接，但各交易所下载路径、文件格式、字段口径和交易日发布时间不同；当前 AkShare 先覆盖商品日频样例。 |
| Stooq / Yahoo Finance | 适合作全球市场 shadow/supplemental 候选，但还没写 connector；Yahoo Finance 的 raw 存储和使用条款需要谨慎确认。 |
| NASA FIRMS | 可能免费或配额受限，但属于另类/遥感数据；没有 API key/配额、地理映射、采集范围和 alpha 假设前，启用后会产生暂时无法消费的数据。 |
| Tushare Pro | 目前没有 token，也没有 source 级配置；后续适合作 A 股稳定化、补全或对账源。 |
| Wind / Choice / RavenPack | 付费授权源，没有合同、凭据、存储权限和容量评估前不能启用，也不能保存未授权全文或高频 raw。 |

AkShare 现在的定位是 bootstrap 主源：先把采集框架、raw append-only 存储、SQLite metadata、manifest、质量检查和 `run-enabled` 闭环跑通。它不应被视为长期唯一来源；生产级阶段仍需要逐步补 BaoStock、官方公告/监管/交易所源、全球市场 fallback，以及必要时的付费源对账。

“当前 AkShare 先覆盖某类数据”表示已经有一个最小可运行采集入口，不表示该类数据已经完整、权威或生产级。例如：

- 公告索引：`akshare_announcement_index` 已能通过 `akshare.stock_notice_report` 保存公告列表/索引元数据，包括标题、股票、公告日期、类别和链接等字段；CNINFO/SSE/SZSE/BSE 官方公告索引源也已可手动采列表元数据和 PDF URL，但还没有下载 PDF、附件或解析完整详情页。
- 商品日频：`akshare_commodity_daily` 已能通过 `akshare.futures_zh_daily_sina` 保存商品期货日频样例，默认样本是 `RB0`；但还没有接 SHFE/DCE/CZCE/GFEX 官方结算文件，也没有覆盖全品种全合约、夜盘、结算价、持仓量等完整交易所口径。

### P0 数据集覆盖

| logical_dataset | 数据内容 | 当前可运行源 | 支持源数 | 已登记但未启用源 | 当前判断 |
| --- | --- | --- | --- | --- | --- |
| `market_daily_ohlcv` | A 股日线 OHLCV | `akshare_market_daily_ohlcv` 默认启用；`baostock_market_daily_shadow` 可手动运行 / 免费 | 2 / 2，默认启用 1 个 | BaoStock shadow 默认不随 `run-enabled` 运行 | Bootstrap 已完成；已开始 shadow/fallback 对账能力 |
| `adjustment_factor` | 复权因子 | `akshare_adjustment_factor` / AkShare / 免费 | 1 / 1 | 无 | Bootstrap 已完成；当前是未复权 close 与前复权 close 比值推算 |
| `trading_calendar` | A 股交易日历 | `ashare_trading_calendar` / AkShare / 免费 | 1 / 1 | 无 | Bootstrap 已完成；当前主要记录 AkShare 返回的交易日 |
| `trade_status` | 停复牌/交易状态 | `ashare_trade_status` / AkShare / 免费 | 1 / 1 | 无 | Bootstrap 已完成；当前记录停复牌行，不是全市场正常交易快照 |
| `price_limit` | 涨跌停价格 | `ashare_price_limit` / AkShare / 免费 | 1 / 1 | 无 | Bootstrap 已完成；当前用前收盘价和板块规则推算，特殊规则需后续补 |
| `announcement_index` | 公告索引 | `akshare_announcement_index` 默认启用；`cninfo_announcement_list`、`sse_announcement_list`、`szse_announcement_list`、`bse_announcement_list` 可手动运行 / 免费 | 5 / 5，默认启用 1 个 | CNINFO/SSE/SZSE/BSE 默认不随 `run-enabled` 运行 | Bootstrap 已完成；官方索引源已可手动对账，PDF/detail 下载仍未开发 |
| `policy_regulatory_doc` | 政策/监管文档或新闻 | `akshare_cctv_policy_news` / AkShare / 免费 | 1 / 4 | CSRC、gov.cn、PBC | Bootstrap 已完成；权威监管网站源仍未开发 |
| `commodity_daily` | 商品期货日频 | `akshare_commodity_daily` / AkShare / 免费 | 1 / 5 | SHFE、DCE、CZCE、GFEX | Bootstrap 已完成；交易所官方结算源仍未开发 |
| `global_market_daily` | 全球市场日频 | `akshare_global_market_daily` / AkShare / 免费 | 1 / 3 | Stooq、Yahoo Finance | Bootstrap 已完成；shadow/supplemental 源仍未开发 |

### P1 数据集覆盖

| logical_dataset | 数据内容 | 当前可运行源 | 支持源数 | 已登记但未启用源 | 当前判断 |
| --- | --- | --- | --- | --- | --- |
| `financial_indicator` | A 股财务指标 | `akshare_financial_indicator` / AkShare / 免费 | 1 / 1 | 无 | Bootstrap 已完成；披露时间和修订版本需后续对账 |
| `macro_indicator` | 宏观指标 | `akshare_macro_china_financial_credit` / AkShare / 免费 | 1 / 1 | 无 | Bootstrap 已完成；provider 指标保留在 `metric_payload` |
| `capital_flow` | 个股资金流、融资融券、龙虎榜、北向资金 | 4 个 AkShare source / 免费 | 4 / 4 | 无 | Bootstrap 已完成；交易所/港交所/付费源对账仍待后续 |
| `fund_holding` | 公募基金持仓公开快照 | `akshare_fund_portfolio_hold` / AkShare / 免费 | 1 / 1 | 无 | Bootstrap 已完成；真实披露时间和完整持仓明细需后续对账 |
| `industry_membership` | 行业板块成分股快照 | `akshare_industry_membership` / AkShare / 免费 | 1 / 1 | 无 | Bootstrap 已完成 |
| `concept_membership` | 概念板块成分股快照 | `akshare_concept_membership` / AkShare / 免费 | 1 / 1 | 无 | Bootstrap 已完成 |
| `global_event_summary` | 全球新闻/事件元数据摘要 | `gdelt_doc_global_event_summary` / GDELT / 免费 | 1 / 1 | 无 | Bootstrap 已完成；只保存文章元数据 |
| `weather_daily` | 地点级天气日频 | `open_meteo_weather_daily` / Open-Meteo / 免费 | 1 / 1 | 无 | Bootstrap 已完成 |
| `financial_news` | 财经新闻标题/经济事件元数据 | `akshare_stock_news_main_cx`、`akshare_baidu_economic_news` / AkShare / 免费 | 2 / 3 | `akshare_stock_news_em`，本地 AkShare 1.18.57 上游正则错误 | Bootstrap 已完成；不做事件抽取或情绪打分 |
| `public_sentiment` | 公开热度/关注度代理 | `akshare_stock_hot_rank` / AkShare / 免费 | 1 / 1 | 无 | Bootstrap 已完成；不解释为真实情绪标签 |

### P2 数据集覆盖

| logical_dataset | 数据内容 | 当前可运行源 | 支持源数 | 已登记但未启用源 | 当前判断 |
| --- | --- | --- | --- | --- | --- |
| `market_minute_bar` | A 股分钟线样例 | `akshare_ashare_minute_bar` / AkShare / 免费 | 1 / 1 | 无 | 低成本 bootstrap 已完成；不是完整分钟级历史或回放级行情 |
| `research_report_index` | 研报元数据索引 | `akshare_stock_research_report_index` / AkShare / 免费 | 1 / 1 | 无 | 低成本 bootstrap 已完成；只保存 metadata/link，不下载 PDF 或全文 |
| `social_media_aggregate` | 公开评论/关注度聚合指标 | `akshare_stock_comment_aggregate` / AkShare / 免费 | 1 / 1 | 无 | 低成本 bootstrap 已完成；不保存个人帖子或评论正文 |
| `market_level2_snapshot` | Level-2 盘口快照 | 无 | 0 / 1 | Wind paid planned | 未开发；需要付费授权、高容量存储和独立回放设计 |
| `market_tick_trade` | tick / 逐笔成交 | 无 | 0 / 1 | Wind paid planned | 未开发；需要付费授权和高容量存储设计 |
| `licensed_text_document` | 授权新闻/研报/电话会全文 | 无 | 0 / 1 | Choice paid planned | 未开发；没有明确合同授权前不得保存全文 raw |
| `supply_chain_relationship` | 供应链/客户供应商关系 | 无 | 0 / 1 | Choice paid planned | 未开发；需要付费授权和版本化口径 |
| `alternative_data_observation` | 遥感/另类观测 | 无 | 0 / 1 | NASA FIRMS planned | 未开发；需先确认 API key/配额、地理映射和 alpha 假设 |
| `professional_event_feed` | 专业结构化事件库 | 无 | 0 / 1 | RavenPack paid planned | 未开发；需付费授权，并用于与自建事件抽取对账 |

### 当前完成度结论

- P0：bootstrap 已完成，9 个 P0 logical_dataset 均有至少 1 个 enabled source；`market_daily_ohlcv` 已有 BaoStock shadow connector 可手动运行，生产级仍缺更多 official source 对账。
- P1：bootstrap 已完成，10 个 P1 logical_dataset 均有 enabled source；生产级未完成，主要缺官方披露时间、交易所/港交所/付费源对账和更稳定 schema 映射。
- P2：低成本 bootstrap 已完成，已启用分钟线样例、研报索引和公开聚合指标；高成本 P2 数据未完成，Level-2、tick、授权全文、供应链、遥感和专业事件库仍是 disabled planned。
- 当前真正“没有可运行爬取方法”的 logical_dataset 是：`market_level2_snapshot`、`market_tick_trade`、`licensed_text_document`、`supply_chain_relationship`、`alternative_data_observation`、`professional_event_feed`。

### 采集架构和功能完成度

当前仓库的“采集层框架”已经基本开发完毕，可以支持从 source 配置到 raw 落盘、metadata 记账、质量检查和 manifest 发布的一条本地闭环。

| 模块 | 当前状态 | 说明 |
| --- | --- | --- |
| 数据源注册表 | 已完成 bootstrap | `provider_registry.yaml`、`source_registry.yaml` 已覆盖 P0/P1/P2 bootstrap 和 planned source。 |
| dataset contract | 已完成 bootstrap | P0/P1/P2 已登记 logical_dataset 都有 contract；高成本 P2 只有 contract 和 planned source。 |
| connector 动态加载 | 已完成 | enabled 或 active source 会校验 `adapter_class` 可导入；disabled planned source 不强制有代码。 |
| raw append-only 存储 | 已完成 | raw JSON 和 sidecar metadata 追加保存，不覆盖历史 raw。 |
| SQLite metadata 账本 | 已完成 | 记录 run、raw object、raw item version、quality result 和重复采集事实。 |
| manifest | 已完成 | 支持按日期生成每日 manifest。 |
| quality report | 已完成 bootstrap | 支持每日质量报告；规则仍是基础层，后续要扩展字段漂移、覆盖率和跨源差异。 |
| reconciliation | 已完成框架 | 已有 P0 对账框架；但多数数据集目前只有单源，会报告缺少 counterparty。 |
| alert | 已完成入口 | 支持本地 JSONL 和 webhook 参数/环境变量；外部通知渠道尚未实际配置。 |
| backup | 已完成入口 | 支持备份 metadata、manifest、quality/reconciliation report；外部备份目录和 raw 备份策略需生产运行时确认。 |
| CLI | 已完成 bootstrap | `validate-config`、`init`、`smoke-run`、`run-source`、`run-enabled`、`quality-report`、`reconcile`、`alert`、`backup` 可用。 |
| 测试 | 已完成 bootstrap | 覆盖核心存储、metadata、manifest、quality、reconciliation、ops 和主要 connector normalization。 |

未完成事项主要集中在“生产级数据可靠性”和“高成本 P2 数据”，不是采集框架骨架本身：

- 官方/影子源：BaoStock 日线 shadow connector、CNINFO/SSE/SZSE/BSE 公告索引 connector 已实现但默认不启用；CSRC/gov.cn/PBC、SHFE/DCE/CZCE/GFEX、Stooq/Yahoo 仍未开发 connector。
- 跨源对账：P0 高风险数据集还缺真实 counterparty source，当前只能报告 `missing_counterparty_source`，不能完成字段级差异对账。
- 覆盖范围：许多 bootstrap connector 默认只采样少量 symbol、少量 board 或少量 item，尚未扩大到全市场稳定采集。
- PIT 口径：财务、基金持仓、公告、研报和新闻的真实披露时间、修订版本和回溯修正还需要官方源或付费源验证。
- 质量规则：还需要增强字段漂移检测、缺口检测、覆盖率监控、异常值检查和连续运行 SLO。
- 运维生产化：还需要外部告警渠道、非本机备份、定时调度、失败重试、运行看板和 7-30 天连续观察。
- P2 高成本数据：Level-2、tick、授权全文、供应链、遥感、专业事件库都未启用；必须先有授权、预算、容量设计和明确 alpha 假设。

因此当前结论是：采集框架和 P0/P1/P2 bootstrap 已经可交付；生产级数据湖尚未完成。下一步如果继续开发，优先级应是先补免费 shadow/official source 并做对账，而不是直接扩展研究层逻辑。

第一次本地测试建议限制 symbol 数量：

```powershell
pitlake run-source --source-id akshare_market_daily_ohlcv --start-date 20260424 --end-date 20260424 --limit-symbols 3 --manifest-date 2026-04-26
pitlake run-source --source-id baostock_market_daily_shadow --start-date 20260424 --end-date 20260424 --limit-symbols 3 --manifest-date 2026-04-26
pitlake run-source --source-id cninfo_announcement_list --start-date 20260424 --end-date 20260424 --manifest-date 2026-04-26
pitlake run-source --source-id sse_announcement_list --start-date 20260424 --end-date 20260424 --manifest-date 2026-04-26
pitlake run-source --source-id szse_announcement_list --start-date 20260424 --end-date 20260424 --manifest-date 2026-04-26
pitlake run-source --source-id bse_announcement_list --start-date 20260424 --end-date 20260424 --manifest-date 2026-04-26
pitlake run-enabled --start-date 20260424 --end-date 20260424 --limit-symbols 3 --manifest-date 2026-04-26
pitlake quality-report --date 2026-04-26
pitlake reconcile --date 2026-04-26
pitlake alert --message "pitlake daily check failed" --payload-json data_lake/collection/reconciliation_reports/dt=2026-04-26/latest_reconciliation_report.json
pitlake backup
```

`run-enabled` 会运行当前所有 `enabled: true` 的 P0/P1/P2 source；只想验证单个 source 时使用 `run-source`。

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

## P2 bootstrap

P2 按高成本/高难数据处理：当前只启用低成本公开样例源，Level-2、tick、全文研报/新闻、电话会纪要、供应链数据库、遥感和专业事件库均保留为 disabled planned source，必须等授权、预算、容量和 alpha 假设确认后再启用。

当前已启用三个 P2 bootstrap source：

- `akshare_ashare_minute_bar`：A 股分钟线样例，使用 `akshare.stock_zh_a_minute`，默认只采样 `600000` 最近 240 条 1 分钟 bar。
- `akshare_stock_research_report_index`：个股研报元数据索引，使用 `akshare.stock_research_report_em`，只保存元数据和链接，不下载 PDF 或全文。
- `akshare_stock_comment_aggregate`：公开评论/关注度聚合指标，使用 `akshare.stock_comment_em`，只保存聚合指标，不保存个人帖子或评论正文。

单独运行示例：

```powershell
pitlake run-source --source-id akshare_ashare_minute_bar --manifest-date 2026-04-26
pitlake run-source --source-id akshare_stock_research_report_index --limit-symbols 1 --manifest-date 2026-04-26
pitlake run-source --source-id akshare_stock_comment_aggregate --manifest-date 2026-04-26
```

Windows 深路径下运行测试时，如果 raw 文件路径触发 260 字符限制，可把测试根目录指向较短路径：

```powershell
$env:PITLAKE_TEST_ROOT="C:\Users\73498\.codex\memories\pitlake_tests"
pytest
```
