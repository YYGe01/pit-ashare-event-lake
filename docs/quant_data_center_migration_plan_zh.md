# quant_data_center 改造实施文档

> 创建日期：2026-05-11
> 当前状态：迁移启动，先落实施文档和新项目骨架。
> 目标：把当前仓库改造成个人量化统一数据中心 `quant_data_center`，服务 Qlib 研究。

## 1. 改造结论

当前项目主线是个人量化统一数据中心：

```text
quant_data_center
  -> 数据采集
  -> 历史回补
  -> 每日增量
  -> 标准化清洗
  -> 稳定日频因子加工
  -> 导出 Qlib 可读数据
```

不在本项目里做：

```text
模型训练
Qlib qrun 实验配置管理
组合回测
实盘下单
东方财富/掘金终端适配
```

后续边界：

```text
当前仓库                          -> quant_data_center
/root/code/qlib                   -> Qlib 框架源码，尽量不改
/root/code/quant_research          -> 后续再建，放实验、模型、回测、target_weight
/root/code/quant_live_trading      -> 更后面再建，放终端执行和对账
```

第一阶段先不新建 `quant_research`，避免在采集、因子、研究都未稳定时拆太多项目。

## 2. 为什么要大改

当前项目已有价值：

```text
已有 connector 和 normalize 经验
已有 raw 落盘和质量检查基础
已验证 AkShare 数据源入口和基础 normalize 经验
```

但当前核心存储偏采集审计账本：

```text
crawl_run
raw_object
raw_item_version
quality_check_result
collection_manifest
source_health
lineage_event
```

这些表对严肃数据湖有用，但对个人 Qlib 研究不够直接。Qlib 研究优先需要：

```text
stock_basic
trade_calendar
daily_bar
adj_factor
price_limit
trade_status
announcement
news
daily_news_factor
daily_announcement_factor
qlib export
```

因此本次迁移不追求兼容此前的 CLI、manifest 和 metadata 账本。当前主线按 QDC 模块重新实现。

## 3. 目标分层

数据分四层：

```text
raw
  保存上游原始响应、下载文件和关键请求 metadata，只作证据与重算来源。

bronze
  provider 原始结构化结果，字段尽量贴近上游。

silver
  统一口径研究表，统一股票代码、日期、字段名和去重规则。

gold
  Qlib/模型可直接消费的日频因子表。
```

推荐目录：

```text
data/
└── quant_data_center/
    ├── qdc.duckdb
    ├── raw/
    ├── parquet/
    │   ├── bronze/
    │   ├── silver/
    │   └── gold/
    ├── qlib/
    └── logs/
```

代码目录目标：

```text
src/quant_data_center/
├── cli.py
├── collectors/
├── normalizers/
├── storage/
├── jobs/
├── factor_engine/
├── qlib_export/
└── quality/
```

## 4. 存储选择

第一版使用：

```text
DuckDB + Parquet
```

暂不上 PostgreSQL。原因：

```text
个人维护成本低
适合本地批量分析
适合和 Pandas/Polars/Qlib 导出衔接
不需要维护数据库服务
```

DuckDB 存控制表、小维表和视图；Parquet 存行情、公告、新闻、因子等大表。

## 5. 控制表设计

先建立最小控制表：

```text
job_run
  每次 backfill/daily/build_factors/export_qlib/smoke 的运行记录。

backfill_task
  可恢复的历史回补任务分片。

dataset_watermark
  每个 dataset/source/universe 的完成水位。

source_object
  raw 文件索引，替代旧 raw_object。

quality_issue
  缺失、重复、异常值、字段漂移等质量问题。
```

这些表用于调度和可恢复运行，不再复刻旧的复杂 manifest/lineage 体系。

## 6. 研究表设计

第一版核心 silver 表：

```text
stock_basic
trade_calendar
daily_bar
adj_factor
price_limit
trade_status
announcement
news
news_security_map
```

第一版核心 gold 表：

```text
daily_price_factor
daily_news_factor
daily_announcement_factor
```

统一股票代码优先使用 Qlib 友好格式：

```text
SH600000
SZ000001
BJ430047
```

同时可以保留展示/外部接口格式：

```text
600000.SH
000001.SZ
430047.BJ
```

## 7. 时间口径

文本类数据必须保留多个时间字段：

```text
publish_time       来源显示发布时间
arrive_time        理论上你能看到的时间
arrive_time_quality observed / estimated / unknown
collect_time       本系统采集入库时间
effective_time     因子允许使用的时间
```

历史回补新闻通常无法证明真实 `arrive_time`，不能伪装成当年已经看到。规则：

```text
is_backfill = true
collect_time = 实际采集时间
arrive_time_quality = estimated 或 unknown
effective_time = 保守规则估算
```

公告、新闻、财务披露进入因子前，都必须经过交易日历对齐。时间对齐逻辑集中放在：

```text
src/quant_data_center/factor_engine/calendar_align.py
```

不要散落在 connector 或 Qlib YAML 里。

## 8. 历史回补设计

历史回补不能只是 `start_date/end_date` 循环。目标流程：

```text
qdc plan-backfill
  -> 生成 backfill_task：dataset + source + date_range + symbol_batch

qdc run-backfill
  -> 逐个 task 拉数据
  -> 写 raw
  -> 写 bronze
  -> merge 到 silver
  -> 更新 task 状态和 dataset_watermark

qdc daily
  -> 本质上是 date_range=单日的 backfill
  -> 走同一套 normalizer 和 writer
```

回补和每日增量必须走同一套 writer：

```text
collect -> raw -> bronze -> normalize -> silver -> factor_engine -> gold
```

区别只在：

```text
job_type = backfill / daily
is_backfill = true / false
```

## 9. factor_engine 放置原则

第一阶段 `factor_engine` 放在当前项目，也就是：

```text
src/quant_data_center/factor_engine/
```

原因：

```text
因子加工依赖原始表、清洗表、交易日历、股票映射
稳定因子应该可重算、可版本化、可导出
它是数据中心的一部分，不是 Qlib 框架源码的一部分
```

Qlib 项目只负责模型训练、回测、预测、分析。`/root/code/qlib` 里可以保留少量自定义 handler 示例，但不要承载采集、清洗、新闻去重、LLM 打标和历史回补。

## 10. Qlib 接入

Qlib 不直接读新闻正文，也不直接读 DuckDB 控制表。目标流程：

```text
silver/gold Parquet
  -> qdc export-qlib
  -> Qlib provider_uri
  -> qrun / DatasetH / Alpha158 / 自定义 handler
```

导出字段示例：

```text
open
high
low
close
volume
amount
factor
news_count_1d
sentiment_mean_3d
announcement_penalty_60d
```

Qlib 侧职责：

```text
基础行情字段 -> Alpha158/Alpha360
外部稳定因子字段 -> 自定义 handler 追加
模型训练/回测 -> Qlib
```

## 11. MVP 范围

第一版不要追求全功能。

MVP 数据范围：

```text
股票池：CSI300
时间：2015-01-01 到当前日期
行情：日线、复权、交易日历、涨跌停、停复牌
文本：公告标题、新闻标题
因子：量价因子、公告计数/事件因子、新闻计数/简单情绪因子
输出：Qlib provider_uri
```

暂不做：

```text
全 A 十年新闻正文
LLM 全量打标
分钟线
Level-2
实盘下单
东方财富/掘金适配
```

## 12. 分阶段实施计划

### 阶段 0：文档和新骨架

目标：

```text
新增本实施文档
新增 src/quant_data_center 包
新增 qdc CLI
新增 DuckDB 控制表 schema
提供 qdc init / qdc db-info / qdc smoke
```

当前已完成：

```text
qdc validate-config
qdc init
qdc db-info
qdc smoke
qdc plan-backfill
qdc list-backfill
qdc run-backfill --control-only
qdc run-backfill --retry-failed
qdc run-backfill 真实 AkShare 分支：stock_basic / trade_calendar / daily_bar / adj_factor / price_limit / trade_status
qdc run-backfill 真实 AkShare 分支：announcement / news
qdc refresh-universe / list-universe
qdc daily
qdc_silver.stock_basic / universe_constituent / trade_calendar / daily_bar / adj_factor / price_limit / trade_status / announcement / news / daily_news_factor / daily_announcement_factor schema
SilverStore upsert writer
AkShare 阶段 2 回补写 raw JSON、bronze Parquet，并登记 qdc_meta.source_object
qdc build-factors 生成 news_v1 / announcement_v1 count 因子
qdc sync-parquet 同步 silver/gold Parquet
qdc quality 基础质量检查
qdc export-qlib 基础 day provider 导出
qdc verify-qlib 使用本地 Qlib 读取导出的 provider
```

`run-backfill --control-only` 只用于验证任务状态流和水位表，不采集真实数据。默认 `run-backfill` 只运行 `pending` 任务；需要人工恢复失败任务时，使用 `run-backfill --retry-failed` 显式运行 `failed` 任务。当前 `run-backfill` 已支持 AkShare 的 `stock_basic`、`trade_calendar`、`daily_bar`、`adj_factor`、`price_limit`、`trade_status`、`announcement`、`news`；其他 dataset/source 会在 `plan-backfill` 或 `run-backfill` 阶段明确拒绝。

`daily_bar`、`adj_factor`、`price_limit`、`news` 可从 `qdc_silver.universe_constituent` 最新快照或配置里的静态 `--universe` 展开 symbol，也可以显式传入 `--symbols` 覆盖。当前已经支持 AkShare 指数成分快照刷新；历史成分变更追溯可作为后续增强。

DuckDB 当前按单写进程使用；不要并行执行多个会写同一个 `qdc.duckdb` 的命令。

当前 `qdc_silver` 仍先落 DuckDB 表，作为真实 collector 迁移时的标准写入目标；AkShare 阶段 2 collector 已写入 raw JSON 和 bronze Parquet，`sync-parquet` 可从 DuckDB 派生 silver/gold Parquet 文件。

验收：

```bash
qdc init
qdc db-info
qdc smoke
qdc refresh-universe --universe csi300 --snapshot-date 2026-05-11
qdc list-universe --universe csi300
qdc plan-backfill --dataset daily_bar --source-id akshare --universe csi300 --start 2026-05-01 --end 2026-05-03 --batch-size 1 --chunk-days 2
qdc list-backfill --dataset daily_bar
qdc run-backfill --dataset daily_bar --limit-tasks 4 --control-only
qdc run-backfill --dataset daily_bar --retry-failed --limit-tasks 4
qdc plan-backfill --dataset trade_calendar --source-id akshare --start 2026-05-01 --end 2026-05-03
qdc run-backfill --dataset trade_calendar --limit-tasks 1
qdc daily --date 2026-05-11 --universe csi300 --control-only
qdc build-factors --factor-set all --start 2026-05-01 --end 2026-05-03
qdc sync-parquet --layer all
qdc quality --dataset daily_bar --start 2026-05-01 --end 2026-05-03
qdc export-qlib --start 2026-05-01 --end 2026-05-03 --provider-uri data/quant_data_center/qlib/cn_data
qdc verify-qlib --start 2026-05-01 --end 2026-05-03 --instruments SH600000,SZ000001 --provider-uri data/quant_data_center/qlib/cn_data
pytest tests/test_qdc_storage.py
```

### 阶段 1：项目入口迁移

目标：

```text
pyproject 项目名改为 quant-data-center
README 切换到 quant_data_center 口径
明确 qdc 为新入口
```

验收：

```bash
pip install -e .
qdc init
qdc smoke
```

### 阶段 2：行情和日历回补

目标：

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

当前已接通 AkShare 真实回补分支；`daily_bar`、`adj_factor`、`price_limit`、`news` 已支持 `--universe` 展开。`refresh-universe` 会写入最新成分快照，`plan-backfill` 优先使用快照里的 symbol。

当前可执行命令示例：

```bash
qdc plan-backfill --dataset daily_bar --source-id akshare --universe csi300 --start 2015-01-01 --end 2026-05-11 --batch-size 50
qdc run-backfill --dataset daily_bar --limit-tasks 20
qdc plan-backfill --dataset adj_factor --source-id akshare --universe csi300 --start 2015-01-01 --end 2026-05-11 --batch-size 50
qdc plan-backfill --dataset price_limit --source-id akshare --universe csi300 --start 2015-01-01 --end 2026-05-11 --batch-size 50
qdc plan-backfill --dataset trade_status --source-id akshare --start 2026-05-11 --end 2026-05-11
qdc plan-backfill --dataset announcement --source-id akshare --start 2026-05-11 --end 2026-05-11
qdc plan-backfill --dataset news --source-id akshare --universe csi300 --start 2026-05-11 --end 2026-05-11
qdc daily --date 2026-05-11 --universe csi300
```

已实现的质量命令：

```bash
qdc quality --dataset daily_bar --start 2015-01-01 --end 2026-05-11
```

### 阶段 3：Qlib 基础导出

目标：

```text
从 silver/daily_bar、adj_factor 导出 Qlib provider_uri
在 /root/code/qlib 中跑通 LightGBM Alpha158 基线
```

基础导出和 Qlib data-layer verify 命令已实现；导出字段已包含 Alpha158 默认需要的 `$vwap`。Alpha158 baseline 需要更长历史区间和更完整 universe，不使用 1 个交易日、2 个标的的 smoke provider：

```bash
qdc export-qlib --start 2015-01-01 --end 2026-05-11 --provider-uri data/quant_data_center/qlib/cn_data
qdc verify-qlib --start 2015-01-01 --end 2026-05-11 --instruments SH600000,SZ000001 --provider-uri data/quant_data_center/qlib/cn_data
cd /root/code/qlib/examples
qrun benchmarks/LightGBM/workflow_config_lightgbm_Alpha158_2026.yaml
```

本地 Qlib 源码联调安装命令：

```bash
conda run -n ai-trader python -m pip install -e /root/code/qlib -i https://pypi.tuna.tsinghua.edu.cn/simple --extra-index-url https://mirrors.aliyun.com/pypi/simple
```

### 阶段 4：公告和新闻入库

目标：

```text
announcement
news
news_security_map
去重
标题级规则标签
时间对齐
```

已实现的回补命令：

```bash
qdc plan-backfill --dataset announcement --source-id akshare --start 2023-01-01 --end 2026-05-11
qdc plan-backfill --dataset news --source-id akshare --universe csi300 --start 2023-01-01 --end 2026-05-11
qdc run-backfill --dataset announcement
qdc run-backfill --dataset news
```

### 阶段 5：稳定因子加工

目标：

```text
daily_news_factor
daily_announcement_factor
factor_version
可重算
可增量更新
```

已实现的基础命令：

```bash
qdc build-factors --factor-set all --start 2023-01-01 --end 2026-05-11
qdc build-factors --factor-set news_v1 --start 2023-01-01 --end 2026-05-11
qdc build-factors --factor-set announcement_v1 --start 2023-01-01 --end 2026-05-11
```

### 阶段 6：外部因子进入 Qlib

目标：

```text
导出 news/announcement 因子为 Qlib 字段
Qlib 自定义 handler 拼接 Alpha158 + 外部因子
做消融对比
```

对照组：

```text
LightGBM Alpha158
LightGBM Alpha158 + news_count
LightGBM Alpha158 + announcement_factor
LightGBM Alpha158 + news + announcement
```

## 13. 当前迁移状态

截至 2026-05-11：

```text
已确定：项目目标改为 quant_data_center
已确定：factor_engine 放在当前项目，不放 Qlib 源码仓库
已确定：第一版使用 DuckDB + Parquet
已确定：Qlib 只消费导出后的日频字段，不直接消费 raw 新闻/公告
已实现：qdc CLI、DuckDB 控制表、配置校验、init/db-info/smoke
已实现：plan-backfill / list-backfill / run-backfill 控制面、任务水位、显式 failed task retry 和单写进程约束
已实现：qdc_silver 基础行情、universe、公告、新闻和日频因子表
已实现：AkShare 真实回补分支 stock_basic / trade_calendar / daily_bar / adj_factor / price_limit / trade_status / announcement / news
已实现：refresh-universe 最新成分快照；plan-backfill 优先使用快照 symbol
已实现：qdc daily 每日增量编排入口
已实现：AkShare 回补 raw JSON、bronze Parquet 和 source_object 账本登记
已实现：qdc build-factors 基础新闻/公告 count 因子
已实现：silver/gold Parquet 派生同步
已实现：qdc quality 基础质量命令
已实现：qdc export-qlib 基础 day provider 导出，包含行情、vwap、复权、涨跌停和新闻/公告 count 因子
已实现：qdc verify-qlib 通过本地 Qlib 校验 provider，可发现缺失 instrument / 空 feature，并输出严格 JSON
已验证：Qlib Alpha158 handler 可读取 QDC provider 并生成特征/label
待实现：Qlib LightGBM Alpha158 baseline qrun 训练联调
已验证：真实 AkShare 小样本 smoke，覆盖 csi300 成分快照、stock_basic、daily、公告、基础因子、Parquet、quality 和 Qlib 导出
已验证：本地 /root/code/qlib 以 editable 方式安装到 ai-trader，并读取 QDC 导出的 Qlib provider
待增强：历史成分变更追溯、更细质量规则、完整 universe Qlib 导出和 Alpha158 baseline 配置
```

## 14. 真实数据 smoke 记录

2026-05-11 使用真实 AkShare 小样本跑通：

```bash
qdc refresh-universe --universe csi300 --snapshot-date 2026-05-11
qdc daily --date 2024-01-02 --universe csi300 --symbols SH600000,SZ000001 --batch-size 2 --limit-tasks 20
qdc build-factors --factor-set all --start 2024-01-02 --end 2024-01-02
qdc sync-parquet --layer all
qdc quality --start 2024-01-02 --end 2024-01-02
qdc export-qlib --start 2024-01-02 --end 2024-01-02 --provider-uri data/quant_data_center/qlib/cn_data
qdc verify-qlib --start 2024-01-02 --end 2024-01-02 --instruments SH600000,SZ000001 --provider-uri data/quant_data_center/qlib/cn_data
qdc plan-backfill --dataset stock_basic --source-id akshare --start 2024-01-02 --end 2024-01-02
qdc run-backfill --dataset stock_basic --limit-tasks 1
qdc sync-parquet --layer silver --dataset stock_basic
qdc quality --dataset stock_basic
```

结果：

```text
csi300 universe_constituent: 300 rows
stock_basic: 5515 rows
daily_bar / adj_factor / price_limit: each 2 rows
trade_status: 850 rows
announcement: 852 rows
daily_announcement_factor: 454 rows
quality: 0 issue
Qlib export: 2 instruments, 1 calendar date, 26 files
Qlib verify: D.calendar / D.list_instruments / D.features 可读取 2 条 feature 行，包含 $vwap，issues=[]
```

真实源修复：

```text
stock_news_em 上游正则异常：记录 raw error，任务返回 success 且 row_count=0
stock_tfp_em 非 A 股代码：跳过
stock_tfp_em 重复状态行：按 trade_date/instrument first-wins 去重
announcement 重复公告行：按 announcement_id 去重
daily 同一批 failed task：再次运行会重试 pending/failed task
```

## 15. Qlib Alpha158 data-layer smoke

2026-05-11 使用真实 AkShare 扩展两标的日线历史样本：

```bash
qdc plan-backfill --dataset daily_bar --source-id akshare --start 2023-01-01 --end 2024-12-31 --symbols SH600000,SZ000001 --batch-size 2 --chunk-days 800
qdc run-backfill --dataset daily_bar --limit-tasks 3
qdc quality --dataset daily_bar --start 2023-01-01 --end 2024-12-31
qdc export-qlib --start 2023-01-01 --end 2024-12-31 --provider-uri data/quant_data_center/qlib/cn_data
qdc verify-qlib --start 2023-01-01 --end 2024-12-31 --instruments SH600000,SZ000001 --provider-uri data/quant_data_center/qlib/cn_data
```

结果：

```text
daily_bar: 968 rows
quality daily_bar: 0 issue
Qlib export: 2 instruments, 484 calendar dates, 26 files
Qlib verify: 968 feature rows, issues=[]
Qlib Alpha158 handler: shape=(968, 159), non_null_label=964
```

说明：当前只验证 Alpha158 data handler 能读取 QDC provider 并生成特征/label；正式 LightGBM baseline 仍需要完整 universe、训练/验证/测试切分和 qrun 配置。
