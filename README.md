# quant_data_center

本仓库当前主线是个人 A 股事件与文本因子中心 `quant_data_center`，服务 Qlib 研究。

新的职责边界是：

```text
Qlib / 社区 cn_data / Qlib 增量脚本
  -> 负责基础结构化行情、复权因子、交易日历和 instruments

quant_data_center
  -> 负责公告、新闻、研报、舆情、互动问答、监管处罚、招投标等非结构化和另类数据
  -> 标准化为日频 external factors
  -> 对齐并导出给 Qlib 使用
```

本仓库不做模型训练、组合回测、实盘下单或交易终端适配。历史回补和 AkShare 结构化采集能力保留为诊断、smoke 和应急补数能力，但不再是默认每日主线。

当前实施计划见：

```text
docs/迁移实施计划.md
docs/每日自动采集实施计划.md
docs/训练数据源完整性评估与改进事项.md
docs/数据流阅读指南.md
docs/控制台产品设计方案.md
```

## 当前入口

项目统一使用已存在的 `ai-trader` conda 环境；不要新建 `quant-data-center` conda 环境。需要同步依赖时，在 `ai-trader` 中执行 editable install。

```powershell
conda activate ai-trader
python -m pip install -e ".[market,dev]"
qdc validate-config
qdc init
qdc db-info
qdc verify-qlib --provider-uri ~/.qlib/qlib_data/cn_data --start 2026-05-13 --end 2026-05-13 --instruments "SH600000,SZ000001" --fields '$close,$volume,$factor'
```

当前默认工作流是非结构化采集和外部因子加工：

```powershell
qdc crawl-daily --date 2026-05-13 --source-id cninfo_announcement --page-size 100 --skip-pdf-download
qdc crawl-daily --date 2026-05-13 --source-id sse_announcement --page-size 100 --skip-pdf-download
qdc crawl-daily --date 2026-05-13 --source-id eastmoney_roll_news --page-size 100
qdc build-factors --factor-set all --start 2026-05-13 --end 2026-05-13
qdc sync-parquet --layer all
qdc quality
qdc console --host 127.0.0.1 --port 8765
```

`crawl-run` / `crawl-daily` 默认只采公告 metadata，不下载 PDF；需要留存公开 PDF 时显式加 `--download-pdfs`，可再配合 `--pdf-limit` 控制 smoke 下载量。
滚动新闻源会按目标日期窗口向后翻页，跳过目标日之后的新闻，直到完整覆盖目标日；完整采集不建议传 `--max-pages`，只做接口 smoke 时再用它限制页数。
新闻采集依赖 `qdc_silver.stock_basic` 做标题到 instrument 的映射；当本地 `stock_basic` 为空时，`crawl-run` / `crawl-daily` 会先用 AkShare 初始化映射基准。`crawl-daily` 会自动重跑同日 failed 任务；若修复映射或解析逻辑后需要重跑已 success 的同日任务，显式加 `--force`。

```powershell
qdc crawl-daily --date 2026-05-13 --source-id eastmoney_roll_news --page-size 100 --force
```

单条文本事件分类可用于规则或 LLM 冒烟验证；全量因子默认仍走规则引擎：

```powershell
qdc classify-text-event --document-type announcement --title "公司收到交易所监管问询函"
```

## Qlib 基础行情底座

QDC 不负责每天采集或下载社区版 `cn_data`。基础行情维护放在上层 `E:\code\qlib`：

```text
E:\code\qlib
  -> Qlib 框架源码
  -> 社区 cn_data 下载、替换或 Yahoo 增量更新脚本
```

本机已确认存在：

```text
C:\Users\Yuangen.yu\.qlib\qlib_data\cn_data
```

该目录包含 Qlib 标准结构：

```text
calendars/
features/
instruments/
```

并可读取 `$open`、`$high`、`$low`、`$close`、`$volume`、`$factor`。但当前本机这份数据日历只到 `2020-09-25`，需要由 Qlib 仓库侧更新到最近交易日后，再作为 QDC external factor 对齐底座。

Qlib 社区数据源目前使用：

```text
https://github.com/chenditc/investment_data/releases
```

最近检查时，latest release 为 `2026-05-13`，资产 `qlib_bin.tar.gz` 约 550MB，近期 release 呈每日发布节奏。下载、解压、替换和健康检查属于 Qlib 仓库职责，不在 QDC 每日采集中执行。

QDC 对 Qlib provider 只做轻量校验：

```text
provider_uri 是否存在
calendars/day.txt 最新日期是否到最近完整交易日
features 中 OHLCV 和 factor 是否存在
抽样 D.features 能否读取 $close / $volume / $factor
QDC external factors 是否能和该日历、instrument 对齐
```

## 非结构化数据源

当前已接入的每日文档源：

| 类型 | source_id | 默认定位 |
| --- | --- | --- |
| 公告 | `cninfo_announcement` | 主源，优先保留 metadata，可按需下载 PDF |
| 公告 | `sse_announcement` | 上交所补源，默认可跳过 PDF 下载 |
| 新闻 | `eastmoney_roll_news` | 当日滚动新闻补位 |
| 新闻 | `sina_finance_news` | 近实时补位，历史日期可靠性有限 |
| 新闻 | `nbd_company_news` | 手动 smoke 源；已退出默认每日源 |

额外 opt-in 新闻源：

```text
sina
wallstreetcn
10jqka
eastmoney
yuncaijing
fenghuang
jinrongjie
cls
yicai
```

这些源适合做覆盖率和事件映射观察，不直接作为训练级历史新闻结论。正式新闻训练需要授权历史新闻源。

后续优先扩展的非结构化类别：

```text
研报全文 / 研报评论
互动问答 / 投资者关系
股吧 / 雪球 / 公开舆情
交易所监管问询和处罚
法律诉讼和执行信息
招投标 / 政府采购
招聘 JD
专利 / 商标 / 软著
```

## 历史和诊断能力

以下命令保留为历史能力、smoke 或应急诊断，不再作为默认每日主线：

```powershell
qdc plan-backfill --dataset daily_bar --source-id akshare --universe csi300 --start 2026-05-01 --end 2026-05-03 --batch-size 1 --chunk-days 2
qdc list-backfill --dataset daily_bar
qdc run-backfill --dataset daily_bar --limit-tasks 4 --control-only
qdc run-backfill --dataset daily_bar --retry-failed --limit-tasks 4
qdc recover-running --dataset daily_bar --older-than-minutes 15
qdc split-backfill --task-id <task_id> --batch-size 10
qdc daily --date 2026-05-11 --universe csi300 --control-only
qdc daily-pipeline --date 2026-05-11 --symbols "SH600000,SZ000001" --batch-size 1 --control-only
```

既有 AkShare 结构化能力包括：

```text
stock_basic
trade_calendar
daily_bar
adj_factor
price_limit
trade_status
announcement
news
```

这些能力不删除，但日常不再用它们全 A 重采 OHLCV 和复权因子。

## Qlib 联调

如需验证 Qlib provider，可安装上层 Qlib 源码：

```powershell
conda run -n ai-trader python -m pip install -e E:\code\qlib
```

QDC 后续导出的重点是外部因子字段，例如：

```text
$news_count
$news_sentiment_mean
$news_risk_count
$announcement_count
$announcement_financing_count
$announcement_regulatory_count
```

当前仓库保留 Qlib handler 示例：

```text
src/quant_data_center/qlib_ext/handlers.py
config/qlib/workflow_config_lightgbm_alpha158_qdc_external.yaml
```

目标是在 Qlib 基础 `cn_data` 上追加 QDC external factors，而不是用 QDC 重建完整基础行情 provider。

## 项目结构

```text
config/quant_data_center.yaml                 QDC 运行配置
config/quant_data_center_daily_only.yaml      历史 daily-only 隔离配置，后续会降级为 smoke/诊断配置
src/quant_data_center/                        QDC 源码
src/quant_data_center/console_static/         本地控制台静态页面
tests/test_qdc_storage.py                     当前 QDC 聚焦测试
docs/每日自动采集实施计划.md                  当前非结构化每日采集计划
docs/迁移实施计划.md                          长期迁移总纲和边界
docs/训练数据源完整性评估与改进事项.md         训练数据职责和缺口评估
docs/数据流阅读指南.md                        数据流阅读指南
docs/控制台产品设计方案.md                    控制台信息架构
docs/工作日志/                                智能体工作记录
data/quant_data_center/                       本地运行数据，已 gitignored
data/quant_data_center_daily_only/             历史 daily-only 运行数据，已 gitignored
```

## 验证

常规验证：

```powershell
qdc validate-config
pytest
ruff check .
```

非结构化链路 smoke：

```powershell
qdc crawl-daily --date 2026-05-13 --source-id cninfo_announcement --page-size 10 --max-pages 1 --skip-pdf-download
qdc verify-qlib --provider-uri ~/.qlib/qlib_data/cn_data --start 2026-05-13 --end 2026-05-13 --instruments "SH600000,SZ000001" --fields '$close,$volume,$factor'
qdc build-factors --factor-set all --start 2026-05-13 --end 2026-05-13
qdc sync-parquet --layer all
qdc quality
```

自动化环境未激活 shell 时：

```powershell
conda run -n ai-trader qdc validate-config
conda run -n ai-trader pytest
conda run -n ai-trader ruff check .
```
