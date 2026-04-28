# Data Lake 前端观测与浏览控制台设计

> 更新日期：2026-04-27  
> 目标：基于现有 `data_lake/`、SQLite metadata、quality report、reconciliation report、manifest 和 raw append-only 文件，设计一个让使用者可以掌握采集状态、发现异常、浏览数据和追溯 raw 证据的前端页面。  
> 建议产品名：`PitLake Console`

## 0. 当前落地状态

截至 2026-04-28，仓库内已实现本地只读 `PitLake Console`：

```text
阶段 1 MVP：已完成。
阶段 2：已完成数据资产目录、dataset 详情、dataset coverage、股票覆盖 drilldown、raw detail、对账中心和 manifest 页面；K 线/复杂图表、watchlist 持久化和全文/PDF viewer 仍未实现。
阶段 3：已完成只读治理视图，包括 dataset quality score、近 30 天 volume baseline/schema drift 汇总、source_health 最新状态、只读派生 issue 队列、告警产物位置和 UI cache 状态。可写 issue 状态流转仍因控制台只读边界延后。
阶段 4：已完成本地只读版本，包括 DuckDB 语义查询提示、Superset/Metabase 连接建议、SQLite LIKE 文档搜索、raw HTML/PDF 元数据预览和 JSON/CSV 导出 API。不在当前仓库启动外部 BI 服务，也不展示未授权全文/PDF 正文。
```

当前股票缺失检查只基于 `source_registry.yaml` 的 registry sample symbols 和当天已观测 item。由于还没有统一 security master / stock universe，控制台不能声明全市场股票缺失。

## 1. 设计结论

不要把第一版做成一个“文件浏览器”或“所有 JSON/CSV 都塞进表格”的页面。成熟数据平台的共同做法是：先让用户知道数据是否可信，再允许用户按资产、日期、source、股票和 raw 证据逐层钻取。

推荐形态是一个只读本地 Web 控制台：

```text
第一屏：今天采集是否正常，哪些 source / logical_dataset / 股票有问题
第二层：按日期、logical_dataset、source、股票、运行批次 drill down
第三层：按数据类型展示合适视图，结构化数据不只用表格，文档数据也不只用纯文本
第四层：回到 raw_object、manifest、quality_check_result、reconciliation finding 和原始文件路径
```

最佳实现路线不是直接引入一个通用 BI 工具替代项目 UI。Metabase、Superset 适合做临时分析和图表，但它们不知道 `first_seen_at`、raw hash、manifest、source registry、PIT 口径、采集失败 run、对账缺口这些项目核心语义。第一版应该自研一个轻量 `PitLake Console`，把通用 BI 作为后续可选外挂。

## 2. 业界方案调研

### 2.1 可借鉴模式

| 参考方案 | 成熟做法 | 对本项目的借鉴 |
| --- | --- | --- |
| Apache Airflow UI | 用 DAG / task 的时间矩阵、Grid View、Graph View、Runs、Logs 让用户快速定位 pipeline 哪一步失败。官方 UI 文档强调 Grid View 用行列状态矩阵跨时间查看任务状态，并点击 cell 进入日志和元数据。 | `PitLake Console` 应该有“source x 日期”的采集状态矩阵，点击任意 cell 展开 run、error、raw objects、quality checks。 |
| OpenMetadata | 把 discovery、lineage、quality、observability、incident、owner 放在同一数据资产页面；数据质量支持 table/column tests、health dashboard、alert、resolution workflow。 | 每个 `logical_dataset` 页面应该像“数据资产详情页”：概览、contract、质量、freshness、来源、对账、样本、raw 证据集中展示。 |
| Great Expectations Data Docs | 把 expectation、validation result、profiling result 渲染成可读 HTML，作为持续更新的数据质量报告。 | 现有 `quality_reports` 不应该只是 JSON 文件，前端要把质量结果转成可读、可筛选、可定位的检查结果页面。 |
| dbt Sources / Freshness | source 可声明、测试、文档化，并计算 freshness；`dbt source freshness` 输出 pass/warn/error 和 artifact。 | 本项目已有 `source_registry.yaml`、`schedule_policy.yaml`、`source_health`，应把 freshness SLO 做成核心状态，不要只展示最近 raw 文件。 |
| Apache Superset | 适合 SQL 数据探索、chart、dashboard、virtual dataset 和交互过滤。 | 后续可以把 `raw_item_version` 派生视图注册到 Superset，用于研究者自由画图；但它不适合作为采集运维主入口。 |
| Metabase | 用模型、问题、仪表盘和搜索降低普通用户理解数据表的门槛。模型可以把复杂表封装成用户容易提问的 starting point。 | 前端需要给每类 dataset 做“语义化视图”，例如股票时间线、公告流、交易日历、质量问题列表，而不是暴露 SQLite 原始表名。 |

### 2.2 调研得到的产品原则

1. 先回答“今天能不能信”，再回答“数据长什么样”。
2. 状态必须可钻取：红黄绿状态不是结论，用户必须能点到具体 source、run、股票、字段、raw 文件。
3. 数据资产页要合并 discovery、quality、freshness、coverage、lineage 和样本，不要分散在多个文件夹。
4. 文档类数据用 feed、时间线、全文/HTML/PDF viewer、实体过滤和来源卡片，不要只用大表格。
5. 行情和指标数据用时间序列、K 线、柱状、热力图、透视表、异常点标注，表格只作为精确行查看。
6. 通用 BI 工具可作为补充，但项目核心控制台必须理解 `logical_dataset`、`source_id`、`manifest_date`、`first_seen_at`、append-only raw 和 PIT 语义。

### 2.3 参考资料

- Apache Airflow UI Overview: <https://airflow.apache.org/docs/apache-airflow/stable/ui.html>
- OpenMetadata Data Quality: <https://docs.open-metadata.org/latest/how-to-guides/data-quality-observability/quality>
- OpenMetadata Getting Started / Discovery / Lineage / Observability: <https://docs.open-metadata.org/latest/quick-start/getting-started>
- OpenMetadata Lineage View: <https://docs.open-metadata.org/latest/how-to-guides/data-lineage/explore>
- Great Expectations Data Docs: <https://docs.greatexpectations.io/docs/0.18/reference/learn/terms/data_docs/>
- dbt Sources and Source Freshness: <https://docs.getdbt.com/docs/build/sources>
- Apache Superset User Docs: <https://superset.apache.org/user-docs/>
- Metabase Models: <https://www.metabase.com/docs/latest/data-modeling/models>

## 3. 当前项目事实基础

截至 2026-04-27 本地检查，当前仓库已经具备做前端控制台的关键基础：

```text
config/source_registry.yaml：47 个 source，26 个 enabled source，28 个 logical_dataset
config/dataset_contracts/：29 个 dataset contract
data_lake/collection/metadata/pitlake.sqlite：采集 metadata 账本
data_lake/collection/raw_immutable/：按 source 组织的 append-only raw 文件
data_lake/collection/published_manifests/：每日 manifest 快照
data_lake/collection/quality_reports/：每日质量报告 JSON
data_lake/collection/reconciliation_reports/：每日对账报告 JSON
data_lake/collection/logs/：告警和运行日志
```

SQLite 当前核心表：

| 表 | 当前行数 | 前端用途 |
| --- | ---: | --- |
| `crawl_run` | 62 | 运行批次、状态、耗时、新增数、错误数 |
| `raw_object` | 90 | raw 文件证据、URI、hash、大小、request 参数 |
| `raw_item_version` | 10183 | 标准化观测项、`first_seen_at`、payload、数据样本和股票 drilldown |
| `quality_check_result` | 10585 | 字段级/数据集级质量检查结果 |
| `collection_manifest` | 44 | 发布快照、manifest 状态和统计 |
| `source_health` | 0 | freshness/SLO 结果，当前表已建但本地尚未写入结果 |

当前数据湖已经收集到的 `logical_dataset` 包括：

```text
adjustment_factor
announcement_index
capital_flow
commodity_daily
financial_news
fund_holding
global_event_summary
global_market_daily
market_daily_ohlcv
market_minute_bar
policy_regulatory_doc
price_limit
public_sentiment
research_report_index
social_media_aggregate
trade_status
trading_calendar
weather_daily
```

设计上应优先消费这些已有产物，避免第一版就重写采集框架。

## 4. 用户核心问题

这个前端必须优先解决以下问题：

1. 今天所有应该采集的数据是否都采集了？
2. 哪些 `logical_dataset` 缺数据、延迟、失败或质量异常？
3. 哪些 `source_id` 失败、返回空数据、字段漂移、行数异常或重复异常？
4. 哪些股票在某个 dataset 里缺失或异常？
5. 某天的数据质量是 pass、warn 还是 fail，原因是什么？
6. 哪个异常是采集失败，哪个是数据源本身为空，哪个是缺少对账源？
7. 我想看某个 dataset、source、股票、日期时，能不能直接点进去看样本、图表、文档和 raw 文件？
8. 某条数据第一次什么时候被系统看到，来自哪个 raw object，在哪个 manifest 发布？
9. 发生异常时，我应该优先修哪个 source 或 connector？
10. 当前数据湖覆盖范围到底有多大，哪些是 enabled，哪些只是 shadow/planned？

## 5. 信息架构

推荐路由和页面：

```text
/                         今日总览
/days/:date               每日采集健康页
/datasets                 数据资产目录
/datasets/:logical_dataset 数据资产详情页
/sources                  Source 目录
/sources/:source_id        Source 详情页
/symbols/:symbol           股票视角时间线
/runs                      运行批次列表
/runs/:run_id              单次运行详情
/quality                   质量问题中心
/reconciliation            对账中心
/raw                       Raw 文件浏览
/raw/:raw_object_id        Raw 证据详情
/manifests                 Manifest 快照
/search                    全局搜索结果
```

全局导航只保留高频入口：

```text
今日总览
数据资产
股票
质量
运行
Raw
搜索
```

## 6. 第一屏：首页总览

首页不是营销页，也不是文件夹列表。它应该是一个操作台，默认日期为最近一个 manifest date 或用户选择的日期。

### 6.1 顶部状态条

顶部固定展示 8 个高信号指标：

| 指标 | 数据来源 | 状态含义 |
| --- | --- | --- |
| 今日总状态 | 聚合 `crawl_run`、`quality_report`、`reconciliation_report`、freshness | `ok` / `warn` / `fail` |
| 应采 source 覆盖 | `source_registry.yaml` + `schedule_policy.yaml` + `crawl_run` | enabled source 是否按计划运行 |
| 失败 run 数 | `crawl_run.status != success` | 直接进入故障队列 |
| 新增 item 数 | `crawl_run.new_item_count`、`raw_item_version` | 观察采集量 |
| raw object 数 | `raw_object` | 是否实际落盘 |
| 质量失败数 | `quality_check_result`、`quality_reports` | contract / anomaly / drift |
| 对账警告数 | `reconciliation_reports` | 缺 counterparty 或跨源差异 |
| 最新 manifest | `collection_manifest` | 当前可发布快照 |

状态颜色：

```text
绿色：按计划采集，质量检查通过，对账无 critical
黄色：缺少 shadow/counterparty、freshness 临界、行数异常、schema drift、部分 source 没有预期覆盖
红色：enabled source 失败、应采 dataset 无数据、critical quality fail、raw 未落盘、manifest 缺失
灰色：disabled/planned source，没有当天采集预期
```

### 6.2 异常优先队列

首页中间区域显示“最需要处理的 10 个问题”，按严重度和影响范围排序：

```text
1. P0 enabled source 失败
2. P0 logical_dataset 当天无任何 item
3. 已运行但 raw_object=0 或 new_item_count=0 且不符合空数据预期
4. critical quality fail
5. price / OHLC / limit 等基础数值异常
6. schema drift 或 required field 缺失
7. freshness 超过 error SLO
8. reconciliation critical diff
9. reconciliation missing_counterparty_source
10. duplicate / quarantine 异常升高
```

每条问题都要能点开：

```text
问题 -> logical_dataset -> source -> run -> quality check -> affected item keys -> raw object -> original file
```

### 6.3 Source x 日期状态矩阵

借鉴 Airflow Grid View：

```text
行：source_id
列：最近 7 / 14 / 30 个采集日期
cell：success / warn / fail / skipped / no expectation
hover：run_id、耗时、新增数、重复数、错误数
click：打开对应 source + date 的运行详情抽屉
```

默认只展示 enabled source，用户可以切换：

```text
enabled only
enabled + active_shadow
all registered
P0 / P1 / P2
```

### 6.4 Logical Dataset 健康矩阵

这是普通用户最容易理解的视角：

```text
行：logical_dataset
列：采集状态、质量状态、freshness、对账状态、今日新增、近 7 日趋势、主要 source
```

点击 `market_daily_ohlcv`、`announcement_index` 等进入数据资产详情页。

## 7. 每日采集健康页

`/days/:date` 解决“一眼看出这一天是否异常”的需求。

页面布局：

```text
左侧：日期选择器、交易日/非交易日标识、manifest 版本
顶部：日级总状态和统计
中部：logical_dataset 覆盖矩阵
下部：失败 run、质量问题、对账问题、缺失股票、raw 文件列表
```

### 7.1 日级判定模型

每日状态由以下子状态取最大严重度：

| 子状态 | 规则 |
| --- | --- |
| `collection_status` | enabled source 应运行未运行或运行失败为红；shadow/planned 不影响红色，只作为黄色提示 |
| `raw_status` | run 成功但 `raw_object` 未写入为红；raw size 为 0 或异常小为黄/红 |
| `item_status` | 应有 `raw_item_version` 但没有为红；低于历史基线为黄 |
| `quality_status` | critical fail 为红；warning finding 为黄 |
| `freshness_status` | 超过 `freshness_slo_minutes` 为黄/红 |
| `reconciliation_status` | 关键字段跨源差异为红；缺 counterparty 为黄 |
| `manifest_status` | 当天无 manifest 或 manifest error_count > 0 为黄/红 |

### 7.2 缺失股票判定

“哪些股票没有收集好”必须谨慎定义，避免误报。

第一版只在“有明确预期 universe”的情况下判定股票缺失：

```text
1. source_registry.default_options.symbols 中列出的样本股票
2. 用户在 UI 里选择的 watchlist
3. 未来 security_master / stock_universe 数据集给出的全市场股票池
```

如果没有全市场 universe，不应该声称“全市场股票缺失”。页面要显示：

```text
当前缺失检查口径：registry sample symbols / watchlist / full universe
```

股票缺失矩阵：

```text
行：symbol
列：logical_dataset
cell：present / missing / not applicable / source returned empty / disabled
click：进入 symbol + dataset + date drilldown
```

## 8. 数据资产目录

`/datasets` 是面向用户的“我想看哪个就点哪个”的主入口。

分组方式：

```text
P0 核心市场数据
P1 扩展研究输入
P2 高成本/样例数据
文档/新闻/公告
宏观/商品/全球市场/天气
```

每个 asset card 或列表行展示：

```text
logical_dataset
中文名称
优先级 P0/P1/P2
已注册 source 数 / enabled source 数
最新 first_seen_at
今日新增 item 数
近 7 日新增趋势
质量状态
对账状态
contract 字段数
主要展示方式
```

目录默认使用列表 + 筛选，不使用大面积装饰卡片。核心操作是搜索、过滤和点击。

## 9. 数据资产详情页

`/datasets/:logical_dataset` 是最重要的页面。所有 dataset 统一骨架，但中间数据展示组件按类型变化。

### 9.1 通用 tab

```text
Overview      状态、覆盖、source、最新 manifest、关键指标
Explore       按该 dataset 类型定制的数据浏览
Quality       quality checks、schema drift、required fields、异常值
Coverage      日期、source、symbol、item count 覆盖情况
Reconcile     跨 source 对账和缺 counterparty
Runs          该 dataset 的 crawl_run 列表和运行趋势
Raw Evidence  raw_object 和原始文件
Contract      dataset_contract YAML 可读视图
Lineage       source -> raw -> item_version -> manifest，未来接下游研究层
```

### 9.2 Overview

展示：

```text
当前状态：ok/warn/fail
最新采集时间：max(first_seen_at)
最新 source：source_id
今日 item 数、近 7 日 item 数
source 覆盖：enabled / shadow / planned
质量摘要：pass/fail/warn
对账摘要：ok/warn/fail
raw 证据数和总大小
```

### 9.3 Explore 展示组件选择

不要强制所有数据都用表格。推荐映射：

| logical_dataset 类型 | 推荐主视图 | 辅助视图 |
| --- | --- | --- |
| `market_daily_ohlcv` | K 线 + 成交量 + symbol/date 选择器 | 数据表、raw JSON、异常点标注 |
| `market_minute_bar` | 分钟线折线/蜡烛图 + 时间窗口缩放 | 数据表、缺口检测 |
| `adjustment_factor` | 复权因子阶梯线/折线 | 除权除息事件占位、数据表 |
| `price_limit` | 涨跌停上下限区间图 + symbol 列表 | limit 规则说明、异常值 |
| `trading_calendar` | 月历/交易日热力图 | 日期表 |
| `trade_status` | 停复牌事件时间线 + symbol 过滤 | 事件表 |
| `announcement_index` | 公告 feed + 公司/类别/日期过滤 | PDF URL、元数据表、raw link |
| `policy_regulatory_doc` | 政策文档 feed + 部门/日期/关键词过滤 | HTML 文本预览、source card |
| `financial_news` / `global_event_summary` | 新闻流、时间线、关键词/来源过滤 | 元数据表、原始 payload |
| `research_report_index` | 研报卡片列表 + 股票/机构/日期过滤 | 链接、metadata |
| `financial_indicator` / `macro_indicator` | 指标选择器 + 时间序列图 | 透视表、metric_payload JSON |
| `capital_flow` | 资金流柱状/折线 + scope 切换 | 明细表 |
| `fund_holding` | 持仓快照表 + top holdings / sector 分布 | 基金/股票 drilldown |
| `industry_membership` / `concept_membership` | 成分股列表 + membership diff | 网络/分组视图 |
| `commodity_daily` | 合约时间序列 + 品种/交易所过滤 | 结算价/持仓量表 |
| `global_market_daily` | 全球资产价格走势 | 数据表 |
| `weather_daily` | 地点日历/时间序列/地图点 | 数据表 |
| `public_sentiment` / `social_media_aggregate` | 排名榜 + 趋势图 | 明细表 |
| `licensed_text_document` 等未来全文类 | 文档阅读器 + metadata side panel | 只在授权允许时显示正文 |

### 9.4 Quality tab

质量页要从“报告 JSON”变成用户可读的检查结果：

```text
检查名称
检查类型
severity
status
expected_value
observed_value
failed_count
sample_failed_keys
created_at
关联 run_id
关联 affected rows
```

关键交互：

```text
按 severity/status/source/check_type 过滤
点击 sample_failed_keys 跳到具体 item
点击 run_id 跳到运行详情
点击字段名跳到 contract 字段说明
```

### 9.5 Coverage tab

覆盖页要展示：

```text
按日期的 item_count 趋势
按 source 的 item_count 分布
按 symbol 的 present/missing 矩阵
与近 7/30 日基线相比的 volume anomaly
enabled source 是否按 SLO 刷新
```

### 9.6 Raw Evidence tab

每条标准化数据必须能回到 raw 证据：

```text
raw_object_id
source_id
run_id
storage_path
metadata_path
mime_type
size_bytes
content_hash
first_seen_at
request_url
request_params_json
```

raw 展示方式：

```text
JSON：JSON tree viewer + raw text
CSV/TXT：分页表格 + raw text
HTML：安全 sandbox iframe 或纯文本预览
PDF：只显示链接/内嵌 viewer，取决于授权和本地文件是否存在
二进制/未知：只展示 metadata、hash、路径，不直接渲染
```

## 10. 股票详情页

`/symbols/:symbol` 解决“我想看某只股票所有收集情况”的需求。

页面目标不是做交易终端，而是做“该股票在数据湖里的证据时间线”。

推荐模块：

```text
顶部：symbol、市场、watchlist 状态、最近交易日
采集覆盖：该 symbol 在各 logical_dataset 的 present/missing 状态
行情：日线 K 线、分钟样本、复权因子、涨跌停
状态：停复牌、交易日历关联
公告：公告 feed
资金：资金流、龙虎榜、北向/融资融券相关记录
持仓：基金持仓快照
新闻/研报：新闻、研报索引、公开热度
质量：与该 symbol 有关的 quality findings
raw：该 symbol 相关 raw/item version 列表
```

股票页的核心价值是发现“某只股票缺了哪个 dataset”。因此顶部必须有一张 coverage strip：

```text
market_daily_ohlcv      present
adjustment_factor       present
price_limit             present
trade_status            no event expected
announcement_index      present
financial_indicator     missing in configured watchlist
fund_holding            not applicable for date
```

## 11. Source 详情页

`/sources/:source_id` 面向维护 connector 的用户。

展示：

```text
source registry 信息
enabled / implementation_status / priority
provider_id
logical_dataset
adapter_class
auth_type / credential_ref 只显示 key 名，不显示真实凭据
allowed_frequency
default_options
最近 30 次 run
成功率、失败率、平均耗时、新增 item 趋势
最近错误 message
quality check 结果
raw object 列表
相关 manifest
```

source 页应该能直接回答：

```text
这个 source 是否应该运行？
最近有没有变慢？
是否经常返回空？
是否发生字段漂移？
它采集的数据在哪里？
它有没有 shadow/counterparty？
```

## 12. 运行详情页

`/runs/:run_id` 是定位问题的证据页。

展示：

```text
run metadata：source、dataset、start/end、status、trigger_type
统计：request_count、success_count、error_count、new_item_count、duplicate_count、quarantine_count
错误：error_message、stack trace/log link 如果存在
raw_object：本次写入文件
raw_item_version：本次新增/重复 item
quality_check_result：本次检查
manifest：本次发布快照
```

如果未来支持重跑，按钮必须谨慎，第一版建议只读，不在 UI 中直接触发采集。

## 13. 质量问题中心

`/quality` 聚合所有质量问题，而不是让用户到每个 dataset 里找。

默认视图：

```text
当前未解决问题
最近 7 天新问题
按 severity 排序
按 logical_dataset/source/check_name 聚合
```

问题类型：

```text
required_field_missing
schema_drift_unknown_field
value_anomaly
coverage_gap
freshness_slo_breach
run_failure
raw_write_missing
manifest_missing
reconciliation_diff
missing_counterparty_source
```

质量评分建议：

```text
dataset_quality_score = 100
  - critical_fail * 25
  - warning_fail * 5
  - freshness_warn * 10
  - missing_expected_source * 20
  - reconciliation_critical * 25
下限为 0，仅用于排序，不作为研究可信度的唯一依据。
```

## 14. 对账中心

`/reconciliation` 专门回答“哪些数据没有对好”。

当前项目对账的特殊点是：很多 dataset 还只有一个 bootstrap source。因此 UI 要区分两类问题：

```text
缺少 counterparty：黄色，表示无法跨源验证，不一定是数据错误
跨源字段差异：红色或黄色，取决于字段和差异程度
```

对账页展示：

```text
report_date
logical_dataset
status
finding_type
severity
active_sources
planned_counterparty_sources
diff fields
affected observation identity
sample values by source
```

## 15. Raw 文件浏览

`/raw` 不作为首页，但必须可用。

筛选：

```text
source_id
logical_dataset
date
mime_type
run_id
content_hash
size range
status
```

列表列：

```text
raw_object_id
source_id
logical_dataset
stored_at
size_bytes
mime_type
content_hash short
storage_path
```

点击后进入 raw detail：

```text
metadata
preview
linked item versions
linked quality checks
linked manifest
file path
```

raw 页面要保持只读，避免误删或覆盖 append-only 文件。

## 16. 全局搜索

搜索是用户“我想看哪个就点哪个”的关键入口。

搜索范围：

```text
logical_dataset
source_id
provider_id
symbol
title
source_url
source_item_key
run_id
raw_object_id
content_hash
quality check name
announcement/news/research title
```

搜索结果分组：

```text
数据资产
Source
股票
文档/公告/新闻/研报
运行批次
Raw 文件
质量问题
Manifest
```

第一版可以先做 SQLite `LIKE` + 前端高亮；后续再引入 SQLite FTS5 或 Tantivy/Meilisearch。

## 17. 后端数据模型

前端不应直接读取任意文件。建议新增只读 UI API 层。

### 17.1 推荐后端

```text
FastAPI：提供 JSON API 和本地静态文件服务
SQLite：读取 data_lake/collection/metadata/pitlake.sqlite
DuckDB：后续用于查询 raw JSON/CSV、生成聚合视图和 ad-hoc preview
Pydantic：定义 API response schema
```

### 17.2 推荐前端

```text
React + Vite
TanStack Table：高性能表格、排序、过滤、虚拟滚动
ECharts：K 线、折线、柱状、热力图、矩阵
Monaco / JSON viewer：raw JSON 和 payload 查看
PDF viewer：未来授权文档预览
```

如果只做 1-2 天快速原型，可以用 Streamlit 先验证信息架构；但正式版本仍建议 FastAPI + React，因为后续需要复杂 drilldown、状态矩阵、文档 viewer、局部刷新和更好的交互控制。

### 17.3 API 草案

```text
GET /api/overview?date=YYYY-MM-DD
GET /api/days/{date}
GET /api/datasets
GET /api/datasets/{logical_dataset}
GET /api/datasets/{logical_dataset}/items
GET /api/datasets/{logical_dataset}/coverage
GET /api/datasets/{logical_dataset}/quality
GET /api/datasets/{logical_dataset}/reconciliation
GET /api/sources
GET /api/sources/{source_id}
GET /api/runs
GET /api/runs/{run_id}
GET /api/symbols/{symbol}
GET /api/quality/findings
GET /api/reconciliation/reports
GET /api/raw
GET /api/raw/{raw_object_id}
GET /api/manifests
GET /api/manifests/{manifest_id}
GET /api/search?q=...
```

### 17.4 UI cache / 派生视图

为了让页面快，建议创建本地 UI cache，不进 git：

```text
data_lake/collection/ui_cache/pitlake_ui.sqlite
```

派生视图：

| 视图 | 用途 |
| --- | --- |
| `v_daily_source_status` | source x date 状态矩阵 |
| `v_daily_dataset_status` | logical_dataset x date 健康状态 |
| `v_dataset_latest_summary` | 数据资产目录 |
| `v_symbol_dataset_coverage` | 股票覆盖矩阵 |
| `v_quality_findings` | 统一质量问题列表 |
| `v_reconciliation_findings` | 统一对账问题列表 |
| `v_raw_browser` | raw 文件浏览 |
| `v_document_index` | 公告/政策/新闻/研报搜索 |
| `v_manifest_summary` | manifest 列表和统计 |

缓存刷新方式：

```text
pitlake ui-cache build --date YYYY-MM-DD
pitlake ui-cache build --all
pitlake ui --refresh-cache-on-start
```

## 18. 异常判定细则

### 18.1 Source 异常

红色：

```text
enabled source 当天应运行但没有成功 run
run.status != success 且 error_count > 0
run 成功但 raw_object=0，并且该 source 不允许空结果
run 成功但 manifest 未生成或 error_count > 0
```

黄色：

```text
active_shadow source 未运行
run 成功但 new_item_count 低于近 7 个同类日期基线
duplicate_count 突然升高
duration 超过历史 P95
source_health freshness 超过 warn SLO
```

灰色：

```text
disabled/planned source
非交易日无采集预期
该 source 在 schedule_policy 中当天无计划
```

### 18.2 Logical Dataset 异常

红色：

```text
P0 enabled dataset 当天无 raw_item_version
contract required fields 缺失
关键数值异常，例如 high < low、price_limit upper <= lower、负成交量
```

黄色：

```text
只有单一 source，无法对账
字段漂移但不影响 required fields
覆盖低于 watchlist / registry sample expectation
```

### 18.3 股票异常

红色：

```text
watchlist 或明确 universe 中的 symbol 在 P0 行情 dataset 缺失
该 symbol 行情关键字段缺失或数值异常
```

黄色：

```text
该 symbol 缺少非 P0 扩展 dataset
公告/新闻/研报无记录，但该 dataset 不保证每日必有事件
```

灰色：

```text
该 dataset 对该 symbol 不适用
事件型 dataset 当天没有事件，且 source 正常运行
```

## 19. 权限、安全和合规

第一版建议只读、本地、默认绑定 `127.0.0.1`。

必须遵守：

```text
不展示真实 credential，只展示 credential_ref
不把 .env、API key、cookie、付费数据正文写入 UI cache
HTML preview 使用 sandbox 或纯文本，避免执行远程脚本
大 raw 文件默认只预览前 N KB，避免浏览器卡死
付费/授权受限数据只显示 metadata，正文按 storage_permission 决定
所有 raw 文件只读，不提供删除、覆盖、编辑入口
```

如果未来部署到局域网或服务器，需要增加：

```text
登录鉴权
只读角色
访问日志
敏感 source 屏蔽
HTTPS 或反向代理
```

## 20. 分阶段落地计划

### 阶段 1：采集观测 MVP

目标：一眼看出今天是否异常。

范围：

```text
FastAPI 只读 API
React 首页
每日健康页
source x date 状态矩阵
logical_dataset 健康矩阵
运行详情页
质量报告 JSON 可视化
对账报告 JSON 可视化
```

验收：

```text
能打开最近日期总览
能看到 failed/warn/pass 状态
能从异常一路点到 run_id 和 raw_object
能展示 latest_quality_report 和 latest_reconciliation_report
不需要手动打开 data_lake 文件夹就能判断当天状态
```

### 阶段 2：数据资产和股票 drilldown

目标：用户想看哪个 dataset 或股票，就能直接点进去。

范围：

```text
数据资产目录
dataset detail 通用 tab
OHLCV K 线和表格
公告/政策/新闻 feed
指标类时间序列和透视表
股票详情页
watchlist 缺失检查
raw detail viewer
```

验收：

```text
输入股票代码能看到该股票所有已采 dataset 覆盖
打开 market_daily_ohlcv 能看到 K 线和 raw evidence
打开 announcement_index 能看到公告 feed 和 source link
质量异常能定位 affected item
```

### 阶段 3：质量治理增强

目标：从“看见问题”升级为“稳定判断问题”。

范围：

```text
source_health 写入和展示（已完成基础展示）
历史基线和 volume anomaly（已完成近 30 天只读基线）
schema drift 详情页（已完成汇总表，详情页待后续拆分）
dataset quality score（已完成基础评分）
issue 状态流转：已完成只读 open 派生队列；acknowledged / resolved 写入延后
外部告警入口链接（已显示本地 alerts.jsonl 状态和 webhook 配置口径）
UI cache 状态展示（已显示直接读取 metadata/report 的当前模式；增量刷新延后）
```

验收：

```text
近 7/30 天趋势可见
异常阈值可解释
问题可以标记已确认
每天打开首页即可知道要处理什么
```

### 阶段 4：可选 BI 和全文能力

目标：增强探索，不替代核心控制台。

范围：

```text
DuckDB semantic views
Superset/Metabase 连接指南
SQLite LIKE 文档搜索（FTS5 建索引后续再做）
PDF/HTML 文档 viewer（已做 raw HTML 文本预览和 PDF metadata 预览；嵌入式 PDF viewer 等授权全文后再做）
导出 CSV/JSON
```

验收：

```text
研究者可以用 BI 做临时图表
普通用户仍从 PitLake Console 看健康状态和证据链
```

## 21. 需要新增或调整的项目能力

前端设计暴露出几个采集层缺口：

1. `source_health` 表已建但当前为空，应把 `pitlake health-report` 结果纳入日常运行。
2. 缺少统一 security master / stock universe，因此“全市场股票缺失”暂时只能基于 watchlist 或 registry sample symbols 判断。
3. 现有 `quality_reports` 只覆盖 2026-04-26，本地 2026-04-27 有 manifest 和 reconciliation，但没有对应 quality report；前端应提示“quality report missing”而不是静默。
4. `lineage_event` 当前为空，第一版可先用 source -> raw_object -> raw_item_version -> manifest 的系统内 lineage 替代。
5. 如果要显示历史 volume anomaly，需要保留至少 7-30 天连续运行基线。
6. 文档全文/PDF preview 要受授权和 `storage_permission` 控制，不能默认下载或展示付费全文。

## 22. 推荐的第一版页面清单

第一版不要贪多，建议只做以下页面：

```text
/                         今日总览
/days/:date               每日健康
/datasets                 数据资产目录
/datasets/:logical_dataset 数据资产详情
/sources/:source_id        Source 详情
/runs/:run_id              Run 详情
/quality                   质量问题中心
/raw/:raw_object_id        Raw 详情
```

最小可用体验：

```text
用户打开首页，看到今天是 ok/warn/fail
点击 warn/fail，看到具体 logical_dataset 和 source
点击 source，看到失败 run 或异常 run
点击 run，看到 raw object、quality check 和 manifest
回到 dataset，能看样本、图表或文档 feed
搜索股票，能看到该股票覆盖了哪些 dataset，缺了哪些 dataset
```

## 23. 最终建议

做一个项目原生的 `PitLake Console`，而不是直接把文件扔给 BI 工具。

`PitLake Console` 的核心不是“好看地展示数据”，而是“让人知道数据湖今天是否可信，并能追溯每一条结论”。通用 BI 可以在后续承担自由分析，但采集控制台必须内建以下项目语义：

```text
source registry
dataset contract
enabled/shadow/planned 状态
run ledger
quality result
freshness SLO
reconciliation
manifest
first_seen_at
raw evidence
PIT append-only
```

只要第一版把“首页总览、每日健康、数据资产详情、股票覆盖、运行详情、raw 证据”这条路径打通，用户就不需要在 `data_lake/` 目录和 SQLite 表之间来回猜测，能在前端页面直接掌握采集情况和异常位置。
