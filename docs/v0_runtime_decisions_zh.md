# V0 运行决策记录

> 更新时间：2026-04-26  
> 目的：记录正式建立采集框架时已经确认的范围、默认路径、运行方式、备份、告警和付费源策略。后续如果迁移服务器、购买数据源或升级 P1/P2，应更新本文。

## 1. 已确认范围

当前 V0 只做 A 股日频采集框架，不做分钟级、Level-2、tick、逐笔委托或盘口数据。

第一阶段只做 P0 数据集：

```text
日线/复权
交易日历
停复牌/涨跌停
公告
政策监管
商品日频
全球市场日频
```

## 14. 2026-04-26 P1 bootstrap 开始：财务指标

P0 当前状态：已达到“每个 P0 logical_dataset 至少一个 enabled bootstrap source”；但长期稳定完成仍需要 7 天以上连续运行、shadow/official source 对账、外部告警和外部备份。在不扩大到研究层的前提下，已开始 P1 采集层 bootstrap。

首个 P1 source：

```text
source_id: akshare_financial_indicator
logical_dataset: financial_indicator
provider_id: akshare
connector: pitlake.connectors.fundamentals.akshare_financial.AkshareFinancialIndicatorConnector
akshare function: stock_financial_analysis_indicator
default sample symbols: 600000
default start_year: 2024
```

说明：

```text
P1 财务指标先作为 bootstrap source 接入；
connector 只标准化 instrument、exchange、report_date、period_type 等 PIT 基础字段；
AkShare/Sina 返回的指标列先完整保存到 metric_payload，不在 V0 阶段强行绑定具体财务指标字段口径；
真正 PIT 研究使用前，还需要结合公告/财报披露时间、修订版本和官方文件对账。
```

P0 稳定运行后，再升级 P1 和 P2。

## 2. 账号和付费源策略

当前没有 Tushare、券商 API、Wind、Choice、iFinD 等账号或 Token。

V0 策略：

```text
优先免费/公开数据源；
不启用需要账号的 provider；
不绕过登录、验证码、付费墙或反爬机制；
为 Tushare/Wind/Choice 等预留 provider 和 credential_ref；
等 P0 免费源跑稳后，再决定是否开通付费源用于对账、补全或稳定性提升。
```

真实密钥不写入 git、Markdown 或聊天记录。后续如开通账号，只在配置里使用 `credential_ref`，例如 `TUSHARE_TOKEN`。

## 3. 本地电脑还是服务器

当前建议分两阶段：

### 阶段一：本地电脑开发和验证

现在先用本地电脑即可，不需要立刻组服务器。原因：

```text
当前只做日频 P0，不需要 7x24 高频采集；
框架、注册表、数据契约、manifest、质量检查需要先验证；
前期预算有限，先把免费源跑通更重要；
本地调试连接器和排查字段变化更方便。
```

本地电脑运行条件：

```text
采集窗口内不要关机；
关闭自动休眠，至少保证 16:30、20:00、23:00、次日 08:30 这些窗口可运行；
网络要能访问目标数据源；
Windows Task Scheduler 或 APScheduler 后续负责定时运行；
C 盘空间要定期检查。
```

### 阶段二：P0 稳定后迁移到长期运行环境

当 P0 连续 7-30 天稳定后，建议迁移到更可靠的长期运行环境：

```text
优先方案：低功耗小主机 / NAS / 家用服务器 + 外接硬盘备份；
可选方案：低成本云服务器；
不建议长期只依赖经常关机或休眠的个人电脑。
```

选择建议：

| 方案 | 适合场景 | 风险 |
| --- | --- | --- |
| 本地电脑 | 开发、调试、前 2-4 周验证 | 休眠、关机、断网导致采集断流 |
| 小主机/NAS | 长期低成本运行、可接移动硬盘 | 需要自己维护电源、网络和备份 |
| 云服务器 | 7x24 稳定、远程方便 | 月费、磁盘费、部分国内源访问质量需要验证 |

当前决策：先本地运行，框架预留迁移能力；P0 连续稳定后再迁移。

## 4. 数据湖和备份路径

当前只有 C 盘，因此 V0 默认：

```text
data_lake 根目录：仓库内 data_lake/
metadata 数据库：data_lake/collection/metadata/pitlake.sqlite
raw 文件：data_lake/collection/raw_immutable/
manifest：data_lake/collection/published_manifests/
本地备份：data_lake/backups/local/
日志：data_lake/collection/logs/
```

`data_lake/` 已被 `.gitignore` 忽略，不进入 git。

备份策略：

```text
V0 当前：先做本地 metadata/manifest 备份；
P0 连接器开始真实运行后：metadata/manifest 每日备份，raw 每周备份；
P0 连续稳定后：增加外部硬盘、NAS、云盘或对象存储备份；
长期目标：至少一份不在 C 盘上的备份。
```

## 5. 告警策略

当前没有指定邮件、飞书、企业微信或 Telegram，因此 V0 默认：

```text
先写本地 JSONL 日志；
生成本地每日采集报告；
P0 真实采集开始后，source 连续失败、manifest 未生成、raw 为空、hash 缺失等情况进入报告；
等你提供通知通道后，再接邮件或 webhook。
```

本地告警不能替代长期运维告警。真实 P0 跑起来后，建议至少接一个外部通知通道。

## 6. 当前已落地的框架命令

在项目根目录运行：

```powershell
conda env create -f environment.yml
conda activate pit-ashare-event-lake
pip install -e .
pitlake validate-config
pitlake init
pitlake smoke-run
```

如果还没有安装包，也可以临时使用：

```powershell
$env:PYTHONPATH="src"
python -m pitlake.cli validate-config
python -m pitlake.cli init
python -m pitlake.cli smoke-run
```

`smoke-run` 不访问外网，只验证本地 raw 写入、metadata 账本、质量检查和 manifest 生成。

## 7. 下一步实现顺序

优先级如下：

```text
1. 完成连接器运行模板和任务调度封装；
2. 实现第一个真实 P0 连接器：A 股日线/复权或公告；
3. 为第一个连接器跑通 raw 保存、raw_item_version、quality gate、manifest；
4. 增加交易日历、停复牌/涨跌停；
5. 增加公告和政策监管；
6. 增加商品和全球市场日频；
7. 连续运行 7 天后，再考虑 shadow source、外部备份和外部告警。
```

## 8. 2026-04-26 首个真实采集闭环

已落地首个真实 P0 source：

```text
source_id: akshare_market_daily_ohlcv
logical_dataset: market_daily_ohlcv
provider_id: akshare
connector: pitlake.connectors.market.akshare_daily.AkshareMarketDailyConnector
akshare function: stock_zh_a_daily
default sample symbols: 000001, 600000, 300750
```

本地验证命令：

```powershell
pip install -e .
pitlake run-enabled --start-date 20260424 --end-date 20260424 --limit-symbols 3 --manifest-date 2026-04-26
```

验证结果：

```text
status: success
source_count: 1
request_count: 3
success_count: 3
error_count: 0
manifest generated: yes
```

说明：

```text
AkShare 的 stock_zh_a_hist / Eastmoney 历史接口在当前本地网络下会出现远端断开或代理连接失败；
因此 V0 首个连接器改用当前可访问的 stock_zh_a_daily；
后续应把 Eastmoney hist 接口作为单独 shadow source，不直接覆盖当前已跑通的 bootstrap source。
```

当前这一步已经跑通：

```text
config source -> connector runner -> AkShare request -> raw append-only store -> SQLite metadata -> quality checks -> raw_item_version -> daily manifest
```

## 9. 2026-04-26 交易日历连接器

已落地第二个真实 P0 source：

```text
source_id: ashare_trading_calendar
logical_dataset: trading_calendar
provider_id: akshare
connector: pitlake.connectors.market.akshare_calendar.AkshareTradingCalendarConnector
akshare function: tool_trade_date_hist_sina
default calendar_id: cn_ashare
default date window: 20260424 - 20260424
```

说明：

```text
AkShare 的 tool_trade_date_hist_sina 当前返回交易日列表；
V0 先把返回日期标准化为 trading_calendar item，并将 is_trading_day 固定为 true；
非交易日补全和交易所官方日历对账后续再做，不在当前 bootstrap source 中硬推断。
```

当前 enabled P0 source 已变为：

```text
akshare_market_daily_ohlcv
ashare_trading_calendar
```

本地验证结果：

```text
command: pitlake run-enabled --start-date 20260424 --end-date 20260424 --limit-symbols 3 --manifest-date 2026-04-26
status: success
source_count: 2
akshare_market_daily_ohlcv: request_count=3, success_count=3, error_count=0
ashare_trading_calendar: request_count=1, success_count=1, error_count=0, new_item_count=1
manifest generated: yes
```

后续默认顺序更新为：

```text
1. 实现 trade_status；
2. 实现 price_limit；
3. 让 market_daily_ohlcv、trading_calendar、trade_status、price_limit 通过 run-enabled 一起稳定运行；
4. 生成每日质量报告；
5. 简单市场约束数据稳定后，再接公告采集。
```

## 10. 2026-04-26 交易状态连接器

已落地第三个真实 P0 source：

```text
source_id: ashare_trade_status
logical_dataset: trade_status
provider_id: akshare
connector: pitlake.connectors.market.akshare_trade_status.AkshareTradeStatusConnector
akshare function: stock_tfp_em
default date window: 20260424 - 20260424
```

说明：

```text
AkShare 的 stock_tfp_em 返回指定日期的停复牌信息；
V0 先把返回行标准化为 trade_status item，并将 trade_status 写为 halted；
全市场正常交易状态、盘中临停细分状态和交易所官方对账后续再补。
```

当前 enabled P0 source 已变为：

```text
akshare_market_daily_ohlcv
ashare_trading_calendar
ashare_trade_status
```

本地验证结果：

```text
command: pitlake run-enabled --start-date 20260424 --end-date 20260424 --limit-symbols 3 --manifest-date 2026-04-26
status: success
source_count: 3
akshare_market_daily_ohlcv: request_count=3, success_count=3, error_count=0, duplicate_count=3
ashare_trading_calendar: request_count=1, success_count=1, error_count=0, duplicate_count=1
ashare_trade_status: request_count=1, success_count=1, error_count=0, new_item_count=13, duplicate_count=14
manifest: collection/published_manifests/dt=2026-04-26/collection_manifest_6b0a89e558f4defb.json
```

后续默认顺序更新为：

```text
1. 实现 price_limit；
2. 让 market_daily_ohlcv、trading_calendar、trade_status、price_limit 通过 run-enabled 一起稳定运行；
3. 生成每日质量报告；
4. 简单市场约束数据稳定后，再接公告采集。
```

## 11. 2026-04-26 涨跌停价格连接器

已落地第四个真实 P0 source：

```text
source_id: ashare_price_limit
logical_dataset: price_limit
provider_id: akshare
connector: pitlake.connectors.market.akshare_price_limit.AksharePriceLimitConnector
akshare function: stock_zh_a_daily
default sample symbols: 000001, 600000, 300750
default date window: 20260424 - 20260424
```

说明：

```text
V0 先使用 AkShare 日线的目标日前一条 close 作为 prev_close；
再按板块规则推算 limit_up / limit_down：
主板普通股票 10%，创业板/科创板普通股票 20%，北交所普通股票 30%；
ST、退市整理、上市初期无涨跌幅限制等特殊规则暂未覆盖，后续需接官方或更完整 source 对账。
```

当前 enabled P0 source 已变为：

```text
akshare_market_daily_ohlcv
ashare_trading_calendar
ashare_trade_status
ashare_price_limit
```

本地验证结果：

```text
command: pitlake run-enabled --start-date 20260424 --end-date 20260424 --limit-symbols 3 --manifest-date 2026-04-26
status: success
source_count: 4
akshare_market_daily_ohlcv: request_count=3, success_count=3, error_count=0, duplicate_count=3
ashare_trading_calendar: request_count=1, success_count=1, error_count=0, duplicate_count=1
ashare_trade_status: request_count=1, success_count=1, error_count=0, duplicate_count=27
ashare_price_limit: request_count=3, success_count=3, error_count=0, new_item_count=3
manifest: collection/published_manifests/dt=2026-04-26/collection_manifest_11b3639d1628ad57.json
```

后续默认顺序更新为：

```text
1. 生成每日质量报告；
2. 连续运行观察四个 enabled P0 source 的稳定性；
3. 简单市场约束数据稳定后，再接公告采集。
```

## 12. 2026-04-26 P0 bootstrap 闭环

已补齐 V0/P0 bootstrap source：

```text
akshare_market_daily_ohlcv -> market_daily_ohlcv
akshare_adjustment_factor -> adjustment_factor
akshare_announcement_index -> announcement_index
akshare_cctv_policy_news -> policy_regulatory_doc
ashare_trading_calendar -> trading_calendar
ashare_trade_status -> trade_status
ashare_price_limit -> price_limit
akshare_commodity_daily -> commodity_daily
akshare_global_market_daily -> global_market_daily
```

同时新增每日质量报告命令：

```powershell
pitlake quality-report --date 2026-04-26
```

最终本地验证结果：

```text
validate-config: ok
pytest: 12 passed
ruff check .: passed
run-enabled: success
source_count: 9
manifest: collection/published_manifests/dt=2026-04-26/collection_manifest_4954ce04b2ecf768.json
quality_report: collection/quality_reports/dt=2026-04-26/quality_report_20260426T160001+0800.json
```

说明：

```text
当前 P0 已达到“每个 P0 logical_dataset 至少一个 enabled bootstrap source”的状态；
部分 source 是 V0 推算或非官方 bootstrap 口径，例如 adjustment_factor、price_limit、announcement_index、policy_regulatory_doc；
交易所、监管机构、Stooq/Yahoo 等官方或 shadow source 仍保留为后续稳定性和对账增强。
2026-04-26 的质量报告状态为 fail，是因为当天早期调试失败 run 也被如实纳入报告；最新一次 9 source run-enabled 全部成功。
```

后续默认顺序更新为：

```text
1. 连续运行 7 天观察 P0 bootstrap source 稳定性；
2. 增加 shadow/official source 对账，优先复权因子、涨跌停、公告和政策监管；
3. 接外部告警和备份；
4. P0 稳定后再进入 P1/P2 或研究层。
```
## 13. 2026-04-26 对账、告警和备份入口

已新增最小可运行的 P0 对账、告警和备份能力：

```powershell
pitlake reconcile --date 2026-04-26
pitlake alert --message "pitlake daily check failed" --payload-json data_lake/collection/reconciliation_reports/dt=2026-04-26/latest_reconciliation_report.json
pitlake backup
```

说明：

```text
reconcile 默认覆盖 adjustment_factor、price_limit、announcement_index、policy_regulatory_doc；
当前只有单一 bootstrap source 时，会把缺少 shadow/official counterparty 作为 warning 写入报告；
后续启用 shadow/official source 后，同一命令会自动按观察项 identity 比较关键字段；
alert 默认写本地 alerts.jsonl，可通过 PITLAKE_ALERT_WEBHOOK_URL 或 --webhook-url 发送外部 webhook；
backup 默认备份 SQLite metadata、manifest、quality report 和 reconciliation report，可通过 PITLAKE_EXTERNAL_BACKUP_DIR 或 --target-dir 指向外部盘/NAS/同步目录；
raw 数据体量更大，需显式使用 --include-raw。
```
