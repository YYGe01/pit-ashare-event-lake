# V0/P1/P2 运行决策记录

> 更新时间：2026-04-26  
> 目的：只记录会影响后续维护和运行的长期决策、边界和口径。具体代码改动、验证流水和下一步事项写入 `docs/agent_journal/`；直接运行命令写入 `README.md`。

## 文档分工

```text
README.md：当前可运行命令、enabled source 清单、使用入口。
docs/agent_journal/YYYY-MM-DD.md：每次 agent 工作摘要、验证结果、遗留问题。
docs/v0_runtime_decisions_zh.md：范围边界、数据口径、运行策略、迁移/备份/告警/付费源决策。
```

普通 connector、测试或 CLI 增量不再要求更新本文；只有范围、口径、数据源取舍、运维策略或 P0/P1 交付边界变化时才更新。

## 采集层范围

当前仓库只做 point-in-time 数据采集层：

```text
数据源注册表
dataset contract
raw append-only 存储
SQLite metadata 账本
质量检查
manifest
对账、告警、备份入口
```

不在本仓库实现事件抽取、情绪打分、特征工程、模型训练、回测或交易逻辑。

当前默认频率是日频/低频；分钟级、Level-2、tick、逐笔委托和盘口数据不属于 V0/P1 默认范围。

## P0 交付边界

P0 bootstrap 已达到“每个 P0 logical_dataset 至少一个 enabled source”的状态：

```text
market_daily_ohlcv
adjustment_factor
trading_calendar
trade_status
price_limit
announcement_index
policy_regulatory_doc
commodity_daily
global_market_daily
```

P0 的长期生产级完成条件仍是：

```text
连续运行 7-30 天；
关键数据集有 shadow/official source 对账；
外部告警可用；
至少一份不在 C 盘/本机单点上的备份；
失败、空数据、字段漂移和重复采集都能在报告中被发现。
```

部分 P0 source 仍是 bootstrap 或推算口径，例如 `adjustment_factor`、`price_limit`、`announcement_index`、`policy_regulatory_doc`。这些数据可用于采集框架验证，但进入严肃研究前必须补官方源或付费源对账。

“AkShare 已覆盖某类数据”只表示当前有最小可运行采集入口，不表示完整、权威或生产级覆盖。例如 `akshare_announcement_index` 只保存公告列表/索引元数据，不等同于 CNINFO/交易所公告全文、PDF 和附件归档；`akshare_commodity_daily` 只保存商品期货日频样例，不等同于 SHFE/DCE/CZCE/GFEX 官方结算文件和全品种全合约覆盖。

`baostock_market_daily_shadow` 已实现为 `market_daily_ohlcv` 的免费 shadow/fallback connector，但默认仍保持 `enabled: false`。使用者可通过 `pitlake run-source --source-id baostock_market_daily_shadow ...` 手动观察稳定性和字段口径；在连续运行和对账结果稳定前，不纳入默认 `run-enabled` 主流程。

## P1 交付边界

P1 bootstrap 已可交付，当前覆盖：

```text
financial_indicator
macro_indicator
capital_flow
fund_holding
industry_membership
concept_membership
global_event_summary
financial_news
public_sentiment
weather_daily
```

P1 仍然只做采集层 bootstrap：

```text
财务指标保留 provider 原始指标到 metric_payload；
宏观、资金流、融资融券、龙虎榜、北向资金保留 provider 原始指标到 metric_payload；
基金持仓作为公开快照保存，真实披露时间和修订版本后续对账；
行业/概念成分以 snapshot_date 表示观察快照，不推断历史真实生效区间；
GDELT、财经新闻和经济事件只保存元数据，不做事件分类、情绪打分或股票映射；
公开热度排行只作为 attention proxy，不解释为真实情绪；
天气只保存地点日频观测，不推断公司或行业影响。
```

`akshare_stock_news_em` 在本地 AkShare 1.18.57 会触发上游正则错误，因此保留为 disabled planned source；P1 交付使用 `akshare_stock_news_main_cx`、`akshare_baidu_economic_news` 和 `gdelt_doc_global_event_summary` 覆盖新闻/事件元数据。

P1 生产级继续需要：

```text
扩大默认采样范围；
连续运行观察；
官方披露、交易所、港交所或付费源对账；
明确每类 provider 字段的长期 schema 映射。
```

## P2 交付边界

P2 bootstrap 已启用的低成本公开样例源：

```text
market_minute_bar：akshare_ashare_minute_bar，默认 600000 的最近 240 条 1 分钟 bar；
research_report_index：akshare_stock_research_report_index，只保存研报元数据和链接；
social_media_aggregate：akshare_stock_comment_aggregate，只保存公开评论/关注度聚合指标。
```

P2 不等于生产级高频或全文数据湖。以下类别当前只保留 dataset contract 和 disabled planned source：

```text
Level-2 order book；
tick / 逐笔成交；
新闻全文、研报全文和电话会纪要；
供应链/客户供应商数据库；
卫星、遥感和其他另类数据；
专业新闻事件库。
```

这些 planned source 只有在授权、预算、容量、频率限制、备份策略、alpha 假设和研究层消费方式都明确后才能启用。尤其是全文、研报、电话会纪要、社媒正文和付费事件库，不得在没有合同或明确许可时保存 raw 正文。

## 免费源和付费源策略

当前没有 Tushare、券商 API、Wind、Choice、iFinD、RavenPack、NASA FIRMS 等账号或 Token。

```text
优先免费/公开数据源；
不启用需要账号的 provider；
不绕过登录、验证码、付费墙或反爬机制；
为 Tushare/Wind/Choice/RavenPack/NASA FIRMS 等预留 provider 和 credential_ref；
免费源跑稳后，再决定是否开通付费源用于对账、补全或稳定性提升。
```

AkShare 的长期定位详见 `docs/akshare_data_risk_assessment_zh.md`：它适合作为 bootstrap、低成本历史 backfill 和 shadow/fallback 来源，但不能单独视为严格 PIT vendor；历史补采必须保留真实 `first_seen_at`，并显式区分 backfill 与每日 live observation。

真实密钥不写入 git、Markdown、测试 fixture 或聊天记录。后续如开通账号，只在配置里使用 `credential_ref`，例如 `TUSHARE_TOKEN`。

## 本地和长期运行

当前建议分两阶段：

```text
阶段一：本地电脑开发和验证；
阶段二：P0/P1/P2 bootstrap 连续稳定后迁移到低功耗小主机、NAS、家用服务器或低成本云服务器。
```

本地运行期间需要保证采集窗口内电脑不休眠、网络可访问目标源，并定期检查 C 盘空间。长期不建议只依赖经常关机或休眠的个人电脑。

## 数据湖和备份

当前默认路径：

```text
data_lake 根目录：仓库内 data_lake/
metadata 数据库：data_lake/collection/metadata/pitlake.sqlite
raw 文件：data_lake/collection/raw_immutable/
manifest：data_lake/collection/published_manifests/
质量报告：data_lake/collection/quality_reports/
对账报告：data_lake/collection/reconciliation_reports/
本地备份：data_lake/backups/local/
日志：data_lake/collection/logs/
```

`data_lake/` 已被 `.gitignore` 忽略，不进入 git。

备份策略：

```text
V0/P1/P2 bootstrap 当前：本地 metadata、manifest、quality report、reconciliation report 备份；
真实连续运行后：metadata/manifest 每日备份，raw 每周备份；
长期目标：至少一份不在 C 盘/本机单点上的备份。
```

raw 数据只追加不覆盖；不得修改历史 raw 文件或伪造更早的 `first_seen_at`。

## 告警和对账

当前告警默认写本地 JSONL；如需外部通知，使用环境变量或命令参数传入 webhook，不写入 git。

对账当前先覆盖高风险 P0 数据集。只有单一 bootstrap source 时，报告会标记缺少 counterparty；启用 shadow/official source 后，再按同一 observation identity 比较关键字段。

## 当前未完成事项

采集层框架当前已经具备本地 bootstrap 闭环：source registry、dataset contract、raw append-only 存储、SQLite metadata、quality result、manifest、quality report、reconciliation、alert 和 backup 入口均已实现。

生产级数据湖仍未完成，主要缺口是：

```text
免费 shadow/official source connector：BaoStock 日线 shadow 已实现但默认不启用；CNINFO/SSE/SZSE/BSE、CSRC/gov.cn/PBC、SHFE/DCE/CZCE/GFEX、Stooq/Yahoo 仍未开发；
高风险 P0 数据集的真实跨源对账；
默认采样范围从少量 symbol/board/item 扩大到稳定全市场或明确覆盖范围；
财务、基金、公告、新闻、研报等数据的真实披露时间和修订版本验证；
字段漂移、覆盖率、缺口、异常值和连续运行 SLO 的质量规则增强；
外部告警、非本机备份、定时调度、失败重试和运行看板；
Level-2、tick、授权全文、供应链、遥感、专业事件库等 P2 高成本数据的授权、预算、容量和 alpha 假设确认。
```
