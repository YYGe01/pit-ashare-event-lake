# A 股 PIT 长期数据采集架构总纲

> 更新时间：2026-04-26
> 目标：建立一个可长期运行、可审计、可回放、可替换数据源的 A 股多源数据采集框架，为后续量化研究、事件抽取、超图建模和 Qlib/回测流程提供高质量 point-in-time 数据底座。
> 边界：本文只设计“采集层和采集治理层”。事件抽取、实体链接、特征工程、模型训练和回测属于后处理/研究层。

阅读说明：

- 文档正文尽量使用中文解释。
- 字段名、目录名、配置名、API 名和表名保留英文，例如 `first_seen_at`、`source_registry`、`raw_object`。这些名称后续会直接进入代码、配置和数据表，统一使用英文更稳定。
- 第一次出现的重要英文术语会给出中文解释，后续可直接使用英文简称。

常用术语对照：

| 英文术语 | 中文含义 | 在本项目中的含义 |
| --- | --- | --- |
| PIT / point-in-time | 按当时可见时间记录 | 防止回测使用未来才知道的数据 |
| raw | 原始数据 | 未经语义加工的 API 响应、网页、PDF、CSV、附件等 |
| append-only | 只追加不覆盖 | 新版本新增记录，旧版本永远保留 |
| manifest | 采集清单 | 某一天已经发布的数据文件和元数据清单；必要时可扩展到小时级 |
| source | 具体数据源 | 某个网页、接口、RSS、文件目录或供应商接口 |
| provider | 数据供应商 | 巨潮、交易所、AkShare、Wind、Choice 等来源主体 |
| connector | 采集连接器 | 负责拉取某个 source 的代码模块 |
| logical dataset | 逻辑数据集 | 研究层看到的稳定数据产品，例如公告索引、日线行情、复权因子 |
| dataset contract | 数据契约 | 约束字段、主键、时间、质量规则和兼容性的规范 |
| quality gate | 质量门禁 | 数据发布前必须通过的校验流程 |
| lineage | 数据血缘 | 记录数据从哪个源、哪次运行、哪些输入生成 |
| replay / as-of query | 回放 / 按时点查询 | 按某个历史时刻还原当时系统可见的数据 |
| quarantine | 隔离区 | 保存未通过关键质量检查、默认不供研究层使用的数据 |
| shadow run | 影子运行 | 新数据源先并行采集和对账，不立刻替换主源 |

## 0. 现有 PIT 文档审阅结论

### 0.1 日/周频研究默认模式

本项目当前默认服务日频/周频量化研究：每日或每周生成候选股票和预测分，用于辅助调仓，而不是高频交易、分钟级择时或盘口微观结构研究。

因此，采集层默认资源应投向：

```text
A 股盘后日线、复权因子、交易日历、停复牌、涨跌停；
上市公司公告、财报披露文件、监管和政策信息；
宏观、商品、全球市场等日频外部变量；
能支撑严格 PIT 回放的 manifest、first_seen_at、source_publish_time 和 provider 版本。
```

分钟级 Level-1 快照、Level-2、tick、逐笔委托、盘口委托簿等数据不进入 P0。它们只能作为 P2 可选增强，并且应独立成子系统，避免把存储、授权、调度和质量成本提前压到主线框架上。

现有 `realtime_pit_data_collection_plan_zh.md` 的方向是正确的，尤其是以下判断必须保留：

- 采集层和后处理层分离。
- 原始数据 append-only，不覆盖历史版本。
- 每条数据必须记录 `first_seen_at`。
- 原始响应、原始文件、请求参数、哈希、manifest 都要保存。
- 补采数据不能伪装成实时可见数据。
- 后处理结果可以重算，原始采集事实不能重造。

但如果目标是“按年运行、频繁更换免费/付费数据源、服务长期量化研究”，现有方案还不够完整，主要缺口如下：

| 缺口 | 风险 | 改进方向 |
| --- | --- | --- |
| 缺少控制面 | 数据源、任务、授权、成本、优先级散落在代码里，后期难维护 | 建立数据源注册表、供应商注册表、数据契约和运行账本 |
| 缺少“逻辑数据集 vs 物理供应商”抽象 | 换数据源会污染下游表结构和研究代码 | 用逻辑数据集固定研究接口，用供应商适配器替换采集来源 |
| 缺少数据契约 | 字段变更、含义变更、单位变更容易静默污染数据 | 每个逻辑数据集维护数据契约、字段观测和质量规则 |
| 缺少质量门禁 | 爬虫成功不等于数据可信 | 引入硬性检查、软性检查、异常检测、跨源对账和隔离区 |
| 缺少发布机制 | 半成功数据可能被研究层读到 | 使用“暂存 -> 校验 -> 发布”的 Write-Audit-Publish 流程 |
| 缺少血缘和可观测性 | 出错时难以定位是源、连接器、解析器还是存储问题 | 记录数据集、任务、运行级数据血缘，建立数据源健康状态和告警 |
| 缺少供应商切换流程 | 免费源失效或付费源替换时容易断流 | 设计双源并行、影子对比、正式切换和回滚 |
| 缺少合规/授权细粒度记录 | 新闻、研报、社媒、付费数据版权风险高 | 在 source/provider 层记录授权、使用范围、留存策略、再分发限制 |
| 缺少成本治理 | 把分钟级/高频采集、付费 API、Level-2、新闻全文误放进 P0 会导致成本失控 | 记录供应商成本、额度、限速和价值优先级 |
| 缺少恢复演练 | 多年数据资产一旦损坏不可重来 | 设计校验、备份、恢复、重放、演练制度 |

因此，推荐把原方案升级为：

```text
PIT 采集湖仓
  = 数据源/供应商控制面
  + 采集连接器运行时
  + 不可变原始数据湖
  + 最小观测数据层
  + 质量门禁
  + 采集清单与数据血缘
  + 历史回放接口
  + 运维与治理体系
```

## 1. 总体设计原则

### 1.1 第一原则：PIT 可见性优先于字段漂亮

量化研究最怕未来函数。采集层最重要的事实不是“事件什么时候发生”，而是：

```text
我的系统在什么时候、通过什么来源、以什么请求、第一次看到了什么内容。
```

因此所有数据至少必须具备：

```text
source_id
provider_id
logical_dataset
source_item_key
source_publish_time
source_update_time
crawl_start_at
crawl_end_at
first_seen_at
stored_at
content_hash
raw_uri
run_id
is_backfilled
```

### 1.2 逻辑数据集稳定，供应商可替换

研究层不应该直接依赖 `akshare.xxx()`、`tushare.xxx()`、某个网页 DOM 或某个付费 API 字段名。采集层应拆成两层：

```text
逻辑数据集（logical dataset）：研究层看到的稳定数据产品
物理供应商（physical provider）：实际采集来源，可以免费、付费、备用、临时替换
```

例子：

```text
logical_dataset = announcement_index
  候选供应商（provider candidates）:
    cninfo
    sse_announcement
    szse_announcement
    bse_announcement
    wind_announcement
    choice_announcement
```

下游只认 `announcement_index` 的契约，不认具体供应商。供应商更换只影响 adapter 和 source mapping，不破坏研究接口。

### 1.3 采集层只做最低限度标准化

允许做：

- 时间、时区、证券代码、交易所代码标准化。
- 哈希、去重键、版本号。
- 原始字段到最小公共字段的映射。
- 文件类型、大小、MIME、编码识别。
- 数据质量和来源健康记录。

不允许做：

- 判断新闻利好利空。
- 推断影响股票。
- 生成事件 schema。
- 训练模型或写入预测结果。
- 为了下游方便而修改 `first_seen_at`。

### 1.4 所有写入可审计、可重放、可恢复

采集层每一次写入必须能回答：

```text
谁写的？
什么时候写的？
从哪里写的？
写了哪些对象？
对象哈希是什么？
有没有通过质量检查？
当日 manifest 是否包含它？
如果要回放 2026-04-26 15:00 的可见数据，能否精确恢复？
```

### 1.5 免费源优先起步，付费源作为可插拔增强

免费源用于快速积累和验证框架。付费源用于提升覆盖率、稳定性、历史深度、法律授权和跨源对账能力；高频能力只作为未来可选增强。架构上必须允许：

- 同一逻辑数据集同时接入多个 provider。
- 免费源和付费源并行采集、对账。
- 主源失败自动降级到备用源。
- 新源 shadow run 一段时间后再切换为主源。
- 旧源退役但历史 raw 数据和 mapping 保留。

## 2. 目标架构

```text
                  +-----------------------------+
                  | 控制面 Control Plane        |
                  | 数据源/供应商/数据契约      |
                  | 调度/额度/成本/策略         |
                  +--------------+--------------+
                                 |
                                 v
+----------------+     +---------+----------+     +------------------+
| 外部数据源     | --> | 连接器运行时       | --> | 暂存区           |
| External       |     | API/RSS/网页/文件  |     | raw 临时数据+账本|
+----------------+     +---------+----------+     +--------+---------+
                                 |                         |
                                 v                         v
                        +--------+----------+     +--------+---------+
                        | 不可变原始数据湖 | <-- | 质量门禁        |
                        | raw 对象存储      |     | 契约/检查        |
                        +--------+----------+     +--------+---------+
                                 |                         |
                                 v                         v
                        +--------+----------+     +--------+---------+
                        | 元数据存储        | --> | 已发布清单      |
                        | 运行/条目/文件    |     | manifests        |
                        +--------+----------+     +--------+---------+
                                 |
                                 v
                        +--------+----------+
                        | 历史回放接口      |
                        | as-of 查询        |
                        +--------+----------+
                                 |
                                 v
                        +--------+----------+
                        | 研究层            |
                        | 只读派生数据      |
                        +-------------------+
```

架构拆成 8 个子系统：

| 子系统 | 中文说明 |
| --- | --- |
| 控制面（Control Plane） | 管理数据源、供应商、授权、频率、优先级、数据契约、成本和任务配置 |
| 连接器运行时（Connector Runtime） | 执行 API、RSS、网页、文件、手工导入和供应商接口采集，负责限速、重试、幂等、断点续采 |
| 采集账本（Crawl Ledger） | 记录每次请求、响应状态、错误、分页、游标、重试和运行环境 |
| 不可变原始数据湖（Raw Immutable Lake） | 保存原始响应、HTML、JSON、CSV、PDF、压缩包、附件，不覆盖 |
| 最小观测层（Minimal Observed Layer） | 保存最小标准化索引，便于发现、检索、回放和下游读取 |
| 质量门禁（Quality Gate） | 做数据契约校验、硬约束、异常检测、跨源对账、隔离坏数据 |
| 采集清单与血缘（Manifest & Lineage） | 默认每日发布清单、哈希、血缘、质量报告和可回放快照；必要时扩展小时级 |
| 运维治理（Operations） | 监控、告警、备份、恢复演练、密钥管理、成本报表和合规审计 |

## 3. 分层数据模型

推荐采用湖仓的分层思想，但本项目的命名要更贴合 PIT 采集：

| 层 | 对应业界概念 | 是否可变 | 内容 | 使用者 |
| --- | --- | --- | --- | --- |
| `raw_immutable` | Bronze/raw | append-only | 原始响应、文件、请求、响应头、哈希 | 采集、审计、重放 |
| `observed_min` | Silver 的最小观测层 | append-only + 版本化 | 标准时间、证券代码、标题、URL、source key、raw 指针 | 后处理、检索、回放 |
| `quality_reports` | Data quality | append-only | 检查结果、异常、覆盖率、对账差异 | 运维、研究 |
| `published_manifests` | Snapshot/commit | append-only | 默认每日可见数据清单；必要时小时级 | 回放、审计、研究 |
| `derived` | Research/Gold | 可重算 | parsed text、events、features、labels、predictions | 研究层 |

采集项目只负责前四层。`derived` 可以放在同一个 `data_lake` 下，但必须与采集层有清晰边界。

## 4. 控制面设计

### 4.1 Source、Provider、Dataset 的区别

```text
logical_dataset（逻辑数据集）:
  稳定的数据产品名称，例如 market_daily_ohlcv、adjustment_factor、announcement_index。

provider（数据供应商）:
  一个供应商或来源实体，例如 akshare、tushare、cninfo、wind、choice。

source（具体数据源）:
  provider 下的具体接口、网页、RSS、文件目录或手工数据集。

connector（采集连接器）:
  代码实现，负责从 source 拉取数据。
```

示例：

```yaml
logical_dataset: announcement_index
contract_version: 1
providers:
  - provider_id: cninfo
    role: primary
    connector: cninfo_announcement_list
  - provider_id: sse
    role: supplemental
    connector: sse_announcement_list
  - provider_id: wind
    role: paid_fallback
    connector: wind_announcement_api
```

### 4.2 Source Registry 必备字段

```yaml
source_id: cninfo_announcement_list
provider_id: cninfo
logical_dataset: announcement_index
source_type: web_list
access_method: public_web
base_url: https://www.cninfo.com.cn/
auth_type: none
terms_url: https://www.cninfo.com.cn/new/commonUrl?url=disclosure/list/notice
robots_url: https://www.cninfo.com.cn/robots.txt
license_type: public_disclosure
redistribution_policy: raw_internal_only
allowed_frequency: 1h
priority: P0
trading_calendar: cn_ashare
active: true
owner: data_collection
adapter_class: CninfoAnnouncementConnector
contract_version: 1
rate_limit:
  requests_per_minute: 12
  burst: 3
retry_policy:
  max_retries: 3
  backoff: exponential
quality_profile: announcement_index_v1
retention_policy:
  raw: forever
  logs: 3y
cost:
  type: free
  monthly_budget_cny: 0
notes: "只保存内部研究用途，遵守网站条款和频率限制。"
```

### 4.3 Provider Registry 必备字段

```text
provider_id
provider_name
provider_type            public / open_source_lib / broker / exchange / paid_vendor / manual
legal_entity
homepage
contract_owner
auth_method
credential_ref
quota_policy
rate_limit_policy
license_scope
redistribution_scope
storage_permission       metadata_only / raw_allowed / raw_forbidden / derived_only
historical_depth
latency_expectation
support_contact
cost_model
renewal_date
risk_level
```

### 4.4 Dataset Contract 必备字段

每个 `logical_dataset` 都必须有数据契约。契约不是为了限制 raw 数据，而是为了约束 `observed_min` 和发布接口。

```text
logical_dataset
contract_version
primary_key_fields
required_fields
optional_fields
field_types
timezone_policy
identifier_policy
dedup_policy
versioning_policy
late_arrival_policy
quality_rules
compatibility_rules
downstream_consumers
```

示例：`announcement_index_v1`

```yaml
logical_dataset: announcement_index
primary_key_fields:
  - source_item_key
  - provider_id
required_fields:
  - announcement_id
  - title
  - instrument
  - exchange
  - source_publish_time
  - first_seen_at
  - raw_uri
  - content_hash
quality_rules:
  hard:
    - first_seen_at_not_null
    - raw_uri_exists
    - content_hash_not_null
    - title_not_empty
  soft:
    - source_publish_time_not_in_future_over_10m
    - pdf_size_greater_than_1kb_if_attachment
    - instrument_format_valid_if_present
compatibility:
  allow_add_optional_field: true
  require_review_for_required_field_change: true
```

## 5. PIT 时间模型

### 5.1 推荐字段

```text
source_event_time       事件实际发生时间，来源提供才记录
source_publish_time     来源声称发布时间
source_update_time      来源声称更新时间
source_effective_time   来源声称生效时间，例如指数成分生效日、公告生效日
crawl_start_at          请求开始时间
crawl_end_at            响应完成时间
first_seen_at           系统第一次看到该 item 的时间
stored_at               原始对象落盘时间
published_at            进入 manifest 的时间
market_available_at     研究层允许用于交易决策的最早时间
```

`market_available_at` 很重要。它不是来源字段，而是策略可用性规则。例如：

```text
交易日 14:59 首次看到的新闻，可能只能用于下一笔可交易时点。
盘后 20:00 发布的公告，通常只能用于下一交易日。
宏观数据 10:00 发布，但采集器 10:03 才首次看到，则回测中最早可用时间不能早于 10:03。
```

### 5.2 不能改写 `first_seen_at`

补采、纠错、重跑解析都不能把 `first_seen_at` 改早。正确做法：

```text
source_publish_time = 来源原始发布时间
first_seen_at = 本系统实际首次采到时间
is_backfilled = true
backfill_reason = crawler_failure / provider_delay / manual_repair / historical_import
```

### 5.3 双时间建模

建议至少保留两条时间轴：

```text
valid time: 来源声称的业务时间，例如公告发布时间、行情时间、财报报告期
transaction time: 本系统观察、存储、发布的时间
```

量化研究默认必须按 transaction time 做过滤：

```sql
where first_seen_at <= :as_of_time
  and published_at <= :as_of_time
```

业务分析才使用 valid time。

## 6. 采集流程

### 6.1 标准流程

```text
1. load source registry
2. acquire run lock
3. create crawl_run
4. discover list page / API page / file index
5. persist raw list response
6. extract candidate item keys
7. fetch detail / attachment when allowed
8. persist every raw response/file
9. compute content_hash and request_hash
10. update raw_item_version and raw_file
11. run minimal normalization
12. run data quality checks
13. quarantine failed records if needed
14. publish manifest only after checks
15. update source_health
16. emit lineage and metrics
```

### 6.2 Write-Audit-Publish

采集不能直接把半成品暴露给研究层。推荐流程：

```text
staging/
  connector writes raw files and metadata

audit/
  hash check, required fields, schema observation, freshness, counts

published/
  manifest points to accepted objects
```

质量失败时：

```text
raw 仍然保存；
observed_min 可进入 quarantine；
manifest 标记 status=partial 或 failed；
研究层默认不读取 failed/quarantine；
人工修复必须生成新的 repair_run，不覆盖旧记录。
```

### 6.3 幂等规则

每个连接器必须支持重复运行：

```text
同一 request_hash + response content_hash 不重复写 raw 文件；
同一 provider_id + source_item_key + content_hash 不重复生成 item version；
同一 source_item_key 内容变化时新增版本；
run_id 永远新增；
source_item_state 只指向 latest，但不删除历史。
```

## 7. 存储架构

### 7.1 V0：个人研究起步版

适合当前项目先落地：

```text
元数据存储：DuckDB 或 SQLite
原始文件存储：本地文件系统
分析表格式：Parquet
任务调度：Windows Task Scheduler / cron / APScheduler
日志：JSONL + loguru
质量检查：自写规则 + pytest
备份：本地硬盘 + 对象存储/云盘
```

优点是简单、低成本、可控。缺点是并发、权限、血缘和远程恢复较弱。

### 7.2 V1：稳定运行版

```text
元数据存储：PostgreSQL
原始对象存储：MinIO / S3 兼容存储
分析表格式：Parquet + DuckDB
任务调度：Dagster 或 Airflow
质量检查：Great Expectations / 自定义检查
数据血缘：可选 OpenLineage
监控：可选 Prometheus + Grafana
密钥管理：.env -> 密码管理器 / Vault
```

### 7.3 V2：长期资产版

```text
表格式：Apache Iceberg 或 Delta Lake
数据版本管理：lakeFS 或对象存储版本控制
数据目录：Iceberg REST Catalog / Hive Metastore / Glue 兼容目录
任务编排：Dagster assets 或 Airflow DAGs
计算引擎：本地研究用 DuckDB，规模变大后再用 Spark/Flink
质量门禁：Great Expectations + 自定义 PIT 检查
数据血缘：OpenLineage + 元数据目录
```

V2 不是第一天要做，但目录、manifest、schema 和接口应从第一天兼容未来升级。

### 7.4 推荐目录结构

```text
data_lake/
  collection/
    control/
      source_registry.yaml
      provider_registry.yaml
      dataset_contracts/
      schedule_policy.yaml
    raw_immutable/
      source=cninfo_announcement_list/
        dt=2026-04-26/
          *.json
          *.html
          *.pdf
    staging/
      run_id=.../
    observed_min/
      logical_dataset=announcement_index/
        dt=2026-04-26/
          part-*.parquet
    metadata/
      crawl_run.parquet
      request_ledger.parquet
      raw_object.parquet
      raw_item_version.parquet
      source_item_state.parquet
      quality_check_result.parquet
      lineage_event.parquet
    manifests/
      dt=2026-04-26/
        collection_manifest.json
        quality_manifest.json
        replay_snapshot.json
    quarantine/
      logical_dataset=announcement_index/
        dt=2026-04-26/
    logs/
      connector=cninfo_announcement_list/
        dt=2026-04-26/
          *.jsonl
    backups/
```

### 7.5 分区策略

raw 文件：

```text
source_id / dt
```

公告、新闻等日内低频轮询源可按需增加 `hour` 分区；不要把小时级分区作为所有数据源的默认要求。

observed_min：

```text
logical_dataset / dt
```

如果未来独立接入分钟级行情，可增加：

```text
logical_dataset / trading_date / instrument_bucket / hour
```

不要过早按股票代码生成大量小文件。单机阶段优先保证文件数量可控。

## 8. 核心元数据表

### 8.1 `crawl_run`

```text
run_id
source_id
provider_id
logical_dataset
connector_version
code_version
config_hash
scheduled_at
started_at
ended_at
status
trigger_type           schedule / manual / backfill / repair
is_backfill
backfill_window_start
backfill_window_end
request_count
success_count
failure_count
new_item_count
updated_item_count
duplicate_item_count
quarantine_count
error_summary
runtime_env
```

### 8.2 `request_ledger`

```text
request_id
run_id
source_id
request_method
request_url
request_params_json
request_headers_hash
request_body_hash
request_hash
started_at
ended_at
http_status
response_headers_json
response_size_bytes
response_content_hash
retry_count
rate_limit_remaining
cursor_or_page
raw_object_id
error_type
error_message
```

### 8.3 `raw_object`

```text
raw_object_id
run_id
source_id
provider_id
logical_dataset
object_type            api_response / html / pdf / csv / zip / image / binary
uri
storage_backend
content_hash
content_length
mime_type
encoding
compression
created_at
first_seen_at
request_id
retention_policy
legal_hold
```

### 8.4 `raw_item_version`

```text
item_version_id
logical_dataset
provider_id
source_id
source_item_key
canonical_item_key
version_no
content_hash
dedup_hash
raw_object_id
source_url
source_title
source_publish_time
source_update_time
first_seen_at
stored_at
is_backfilled
backfill_reason
observed_payload_json
quality_status
```

### 8.5 `source_item_state`

```text
logical_dataset
provider_id
source_item_key
canonical_item_key
first_item_version_id
latest_item_version_id
first_seen_at
last_seen_at
latest_content_hash
version_count
is_deleted_at_source
last_checked_at
```

### 8.6 `quality_check_result`

```text
check_id
run_id
logical_dataset
source_id
check_name
check_type              hard / soft / anomaly / reconciliation / pit
severity                critical / warning / info
status                  pass / fail / skipped
expected_value
observed_value
failed_count
sample_failed_keys
created_at
```

### 8.7 `collection_manifest`

```json
{
  "manifest_id": "2026-04-26-daily",
  "manifest_type": "daily",
  "as_of_start": "2026-04-26T00:00:00+08:00",
  "as_of_end": "2026-04-26T23:59:59+08:00",
  "created_at": "2026-04-27T00:10:00+08:00",
  "status": "complete",
  "datasets": [
    {
      "logical_dataset": "announcement_index",
      "contract_version": 1,
      "providers": ["cninfo", "sse", "szse"],
      "run_ids": ["..."],
      "new_item_count": 1280,
      "updated_item_count": 34,
      "quarantine_count": 2,
      "raw_object_count": 2600,
      "content_hash_root": "sha256:...",
      "quality_status": "pass"
    }
  ]
}
```

### 8.8 `lineage_event`

```text
lineage_event_id
event_time
job_name
run_id
input_datasets
output_datasets
input_manifest_ids
output_manifest_ids
source_code_version
config_hash
status
```

## 9. 数据质量体系

### 9.1 五类质量检查

| 类型 | 目的 | 示例 |
| --- | --- | --- |
| 硬性检查（Hard checks） | 不满足就不能发布 | raw 文件存在、content_hash 非空、first_seen_at 非空 |
| 软性检查（Soft checks） | 可发布但要告警 | 字段缺失率升高、标题过短、附件大小异常 |
| 异常检查（Anomaly checks） | 发现源异常 | 行情股票数突然少 50%、公告数异常为 0 |
| 跨源对账（Reconciliation checks） | 对比多个来源是否一致 | cninfo 与交易所公告数量差异、行情收盘价与备用源差异 |
| PIT 检查（PIT checks） | 防未来函数 | `first_seen_at` 晚于 `source_publish_time` 合理，不能被补采改早 |

### 9.2 质量门禁策略

```text
关键失败（critical fail）:
  raw 保留，observed_min 进入 quarantine，不进入 published manifest。

警告失败（warning fail）:
  进入 manifest，但标记 warning，并进入日报。

数据源异常（source anomaly）:
  本次 run 标记 degraded，触发备用源或人工检查。

供应商不一致（provider disagreement）:
  同时保留各 provider 版本，不在采集层强行判断谁对。
```

### 9.3 每日质量报告

每日报告至少包含：

```text
每个 logical_dataset 的新数据量、更新量、重复率
每个 provider 的成功率、延迟、失败原因
raw 文件缺失数、hash 缺失数、0 字节文件数
字段缺失率和 schema drift
跨源对账差异
backfill 比例
quarantine 样本
成本和 quota 使用情况
需要人工处理的 action items
```

## 10. 数据源资源规划

### 10.1 P0：第一阶段必须稳定采集

| 逻辑数据集 | 免费/公开来源 | 付费/高稳定来源 | 采集频率 | 备注 |
| --- | --- | --- | --- | --- |
| A 股日线/复权/基础行情 | AkShare、BaoStock、交易所公开文件 | Wind、Choice、iFinD、聚源、券商 API | 16:30、20:00、次日 08:30 兜底 | 日/周频模型的价格和成交量底座，免费源先起步，付费源用于对账和历史修正 |
| 交易日历/停复牌/涨跌停 | 交易所、AkShare、BaoStock | Wind、Choice、聚源 | 每日盘前 + 盘后 | 回测、标签和候选池最基础约束 |
| 上市公司公告/财报披露文件 | 巨潮、上交所、深交所、北交所 | Wind、Choice、iFinD、聚源 | 交易日每 1-2 小时，盘后 20:00/23:00 兜底 | 必须保存列表页、PDF 和附件 |
| 政策/监管新闻 | 证监会、交易所、人民银行、发改委、财政部、中国政府网 | Wind 新闻、Choice 新闻、iFinD | 每日 1-4 次，盘后兜底 | A 股政策驱动强，优先级高 |
| 商品/期货日频价格 | 上期所、大商所、郑商所、广期所、中金所公开数据 | Wind、Choice、Bloomberg、LSEG | 盘后结算价/收盘价 + 次日兜底 | 周期股、通胀、成本冲击 |
| 全球市场日频指标 | Yahoo Finance、Stooq、FRED、交易所公开数据 | Bloomberg、LSEG、FactSet | 海外收盘后 + 次日早晨 | 美股、美元、美债、VIX、原油、黄金 |

### 10.2 P1：框架稳定后接入

| 逻辑数据集 | 公开/低成本来源 | 付费/高难来源 | 难点 |
| --- | --- | --- | --- |
| 财务报表/财务指标 | 巨潮 PDF、交易所公告、AkShare/Tushare | Wind、Choice、iFinD、CSMAR、RESSET、Capital IQ | PIT 披露时间、修订版本、字段口径 |
| 指数成分/行业分类 | 中证指数、交易所、公开资料 | Wind、Choice、iFinD、申万授权数据 | 历史成分和分类版本不能用当前回填 |
| 概念板块/主题 | AkShare、公开网站 | Wind、Choice、iFinD、聚源 | 概念常有事后归因，PIT 难 |
| 融资融券/北向资金/龙虎榜 | 交易所、港交所、公开接口 | Wind、Choice、iFinD | 披露延迟、口径差异 |
| 基金持仓/机构行为 | 公告、基金季报 | Wind、Choice、CSMAR、RESSET | 季报披露滞后，不能按报告期使用 |
| 宏观指标/日历 | 国家统计局、央行、财政部、FRED、IMF、World Bank | Wind、CEIC、Bloomberg、LSEG | 修订数据、发布时间、节假日 |
| 天气/灾害 | Open-Meteo、NASA FIRMS、NOAA | 商业天气、卫星、遥感供应商 | 地理映射、分辨率、延迟 |
| 航运/能源 | 公开指数、交易所、EIA | Bloomberg、LSEG、Kpler、Vortexa、Clarksons | 费用高、授权复杂 |
| GDELT/全球事件摘要 | GDELT Project、GDELT DOC/GKG | GDELT Cloud、RavenPack、NewsAPI 付费计划 | 噪声高，需要实体映射和主题聚合，先做日频或 6 小时级汇总 |

### 10.3 P2：长期高成本/高难数据

| 数据 | 价值 | 难点 | 建议 |
| --- | --- | --- | --- |
| A 股分钟级/盘中快照 | 盘中状态和更细粒度回放 | 对日/周频调仓不是刚需，存储、授权和质量成本更高 | 不进入默认 P0，需要明确研究假设后独立接入 |
| Level-2 / tick / 逐笔委托 | 微观结构、盘口冲击 | 授权贵、数据量大、存储和回放复杂 | 不建议当前项目主线接入；如要做，作为独立子系统 |
| 全量新闻全文 | 事件覆盖完整 | 版权、去重、正文抓取、授权 | 优先元数据，全文只保存有授权内容 |
| 研报全文/电话会纪要 | 机构观点 | 版权和供应商限制 | 只在明确授权下存 raw |
| 社交媒体/论坛 | 情绪、热度 | 合规、隐私、反爬、噪声 | 先采公开聚合指标，不碰非公开用户数据 |
| 供应链/客户供应商数据库 | 事件传导 | 付费贵、版本化难 | 作为可替换 provider，不写死 |
| 卫星/遥感/另类数据 | 非公开视角 | 成本高、处理复杂、样本少 | 明确 alpha 假设后再接 |
| 专业新闻事件库 | 快速结构化事件 | 费用高、黑盒口径 | 与自建事件抽取并行对账 |

## 11. 连接器设计

### 11.1 连接器接口

每个 connector 实现同一套接口：

```text
load_config()
plan_requests(window, cursor)
execute_request(request)
persist_raw(response)
extract_item_candidates(raw)
fetch_detail(candidate)
normalize_min(raw_item)
emit_quality_metrics()
update_cursor()
```

### 11.2 连接器类型

```text
api_connector
rss_connector
web_list_connector
web_detail_connector
file_downloader
market_snapshot_collector
vendor_batch_importer
manual_dataset_importer
calendar_collector
```

### 11.3 必备能力

```text
timeout
retry with backoff
rate limit
domain-level concurrency
idempotency
cursor checkpoint
request/response persistence
raw file hashing
schema observation
structured logging
source health update
graceful degradation
```

### 11.4 Web/RSS/API 合规原则

- 优先官方公开 API、RSS、下载文件和授权数据。
- 不绕过登录、验证码、付费墙和反爬机制。
- 不高频压测网站。
- 对新闻、研报、社媒等版权内容记录 `storage_permission`。
- 原始全文默认内部研究使用，不对外再分发。

## 12. 调度和补采

### 12.1 调度维度

```text
交易日历（trading_calendar）: cn_ashare / global_24x7 / source_specific
交易时段（session）: pre_market / intraday / post_market / overnight
采集频率（frequency）: cron / interval / event_triggered
优先级（priority）: P0 / P1 / P2
新鲜度目标（freshness_slo）: 允许的最大延迟
额度预算（quota_budget）: 每日或每月可用 API 调用次数
```

### 12.2 优先级

```text
P0：盘后日线/复权、公告/财报披露文件、交易日历、停复牌/涨跌停、监管政策、核心商品和全球风险日频指标
P1：财务指标、宏观、资金、行业、概念、天气灾害、GDELT/财经新闻摘要
P2：分钟级快照、Level-2、tick、研报、社媒、另类数据、专业事件库
```

### 12.3 补采规则

补采必须单独标记：

```text
trigger_type = backfill
is_backfilled = true
backfill_window_start/end
backfill_reason
operator
```

补采数据可用于历史研究，但不能伪装成历史当时已知。研究层默认只使用：

```text
first_seen_at <= as_of_time
```

## 13. 供应商切换机制

### 13.1 切换场景

```text
免费源失效
字段含义变化
源延迟过大
API 额度不足
购买付费源
供应商合同到期
需要双源对账
```

### 13.2 标准切换流程

```text
1. 新 provider 注册到 provider_registry
2. 为同一 logical_dataset 建立 adapter
3. 以 role=shadow（影子运行）运行 7-30 天
4. 生成覆盖率、延迟、字段映射、跨源对账报告
5. 数据契约不破坏时升级为 supplemental（补充源）或 primary（主源）
6. 旧源降级为 fallback（备用源）或 retired（退役源）
7. 保留历史 source mapping 和 raw 数据
```

### 13.3 对账指标

```text
覆盖率（coverage_ratio）
延迟分布（latency_distribution）
缺失条目数（missing_item_count）
额外条目数（extra_item_count）
字段差异率（field_diff_rate）
价格差异，单位 bp（price_diff_bps）
公告 PDF 哈希匹配率（announcement_pdf_hash_match_rate）
字段结构漂移次数（schema_drift_count）
每一万条数据成本（cost_per_10k_items）
```

## 14. 可观测性和告警

### 14.1 核心指标

```text
数据源新鲜度分钟数（source_freshness_minutes）
采集成功率（crawl_success_rate）
新增条目数（new_item_count）
更新条目数（updated_item_count）
重复率（duplicate_rate）
隔离条目数（quarantine_count）
原始文件缺失数（raw_file_missing_count）
内容哈希缺失数（content_hash_missing_count）
字段结构变化次数（schema_change_count）
供应商延迟 p50/p95（provider_latency_p50/p95）
补采比例（backfill_ratio）
剩余额度（quota_remaining）
存储增长 GB（storage_growth_gb）
每日成本人民币（cost_cny_daily）
```

### 14.2 告警规则

```text
P0 source 连续失败 3 次
公告源交易日 2 小时无成功 run
日线行情股票数量低于近 20 日中位数 80%
raw 文件 0 字节
content_hash 缺失
manifest 未生成
备份失败
quota 低于 10%
付费源成本超预算
```

### 14.3 运行日报

每天生成：

```text
每日采集报告：collection_daily_report_YYYY-MM-DD.md
数据源健康状态：source_health_YYYY-MM-DD.json
质量清单：quality_manifest.json
成本报告：cost_report.json
待处理事项：action_items.json
```

## 15. 备份、恢复和安全

### 15.1 备份策略

```text
元数据（metadata）: 每日增量 + 每周全量
采集清单（manifest）: 每日多地备份
原始数据（raw）: 每周增量，重要源每日备份
数据源注册表/数据契约（source registry/contracts）: 跟随 git 版本管理
密钥凭据（credentials）: 不进入 git，只进入安全密钥管理
日志（logs）: 至少保留 1-3 年，P0 源更久
```

### 15.2 恢复演练

每月至少演练一次：

```text
从备份恢复 metadata
抽样校验 raw content_hash
重建某日 manifest
执行 as_of replay
对比恢复前后 row count/hash root
```

### 15.3 安全要求

- API key、cookie、供应商账号不进仓库。
- 付费数据原文按合同限制访问。
- 日志脱敏，不记录完整密钥。
- 供应商授权、合同范围、过期日必须进入 registry。
- 对外分享研究结果时确认是否包含不可再分发原文。

## 16. 回放接口

采集层必须提供研究层可用的 PIT 回放接口：

```text
list_manifests(date_range)
load_manifest(manifest_id)
get_raw_object(raw_object_id)
scan_observed_min(logical_dataset, as_of_time, filters)
build_replay_snapshot(as_of_time, logical_datasets)
export_for_qlib(snapshot_id)
```

查询语义：

```sql
select *
from observed_min
where logical_dataset = 'announcement_index'
  and first_seen_at <= :as_of_time
  and quality_status in ('pass', 'warning')
```

研究层不得直接扫描最新全量表做历史回测。

## 17. 与后处理文档的接口

给 `unified_event_hypergraph_alpha_research_zh.md` 的稳定输入：

```text
source_registry
provider_registry
dataset_contracts
crawl_run
request_ledger
raw_object
raw_item_version
observed_min
collection_manifest
quality_check_result
source_health
replay_snapshot
```

后处理层生成：

```text
parsed_document
events
entity_links
hyperedges
features
labels
predictions
backtest_reports
```

后处理层必须记录所用的：

```text
input_manifest_id
as_of_time
parser_version
prompt_version
model_version
feature_code_version
```

## 18. 实施路线

### 18.1 前 30 天

目标：采集骨架稳定。

```text
1. 建立 control/source_registry.yaml
2. 建立 provider_registry.yaml
3. 建立 dataset_contracts
4. 建立 data_lake/collection 目录
5. 实现 crawl_run、request_ledger、raw_object、raw_item_version
6. 接入 2 个 P0 源：公告 + A 股盘后日线/复权
7. 实现 raw 保存、hash、first_seen_at
8. 实现 daily manifest
9. 实现基础质量检查和 source health
10. 实现备份脚本
```

验收：

```text
连续 7 天自动采集；
所有 raw 有 content_hash；
所有 item 有 first_seen_at；
每日 manifest 可生成；
失败有日志；
补采有 is_backfilled。
```

### 18.2 31-90 天

目标：P0 数据源全覆盖，开始双源对账。

```text
1. 扩展公告到巨潮、上交所、深交所、北交所
2. 扩展盘后日线/复权到至少两个 provider
3. 接入政策/监管、商品日频、全球市场日频
4. 建立 quality report
5. 建立 cross-source reconciliation
6. 建立 source failover 流程
7. 建立 as_of replay API
8. 每月恢复演练
```

验收：

```text
连续 30 天 P0 不断流；
P0 源失败会告警；
可回放任意一天可见数据；
新旧 provider 可 shadow compare。
```

### 18.3 3-6 个月

目标：治理和研究接口成熟。

```text
1. PostgreSQL/MinIO 或等价升级
2. 引入 Dagster/Airflow 编排
3. 引入 Great Expectations 或等价质量框架
4. 引入 OpenLineage 或自建 lineage_event
5. 接入 P1 数据源：财务、宏观、资金、行业/概念
6. 建立 Qlib 导出
7. 建立供应商成本和授权台账
```

### 18.4 6-12 个月

目标：长期资产化。

```text
1. 评估 Iceberg/Delta/lakeFS
2. 接入付费源并与免费源对账
3. 建立统一 metadata catalog
4. 接入 P2 数据前先做 alpha 假设和成本评估
5. 完整 DR 演练
6. 建立数据源退役和合同续约流程
```

## 19. V0 最小可落地技术方案

当前仓库建议先做 V0：

```text
Python 3.11
DuckDB + Parquet
本地 raw 文件系统
YAML 注册表
APScheduler / Windows Task Scheduler
requests + beautifulsoup4 + lxml
pydantic 数据契约校验
pytest 质量检查
loguru JSONL 日志
```

原因：

- 复杂度低，适合个人长期研究。
- DuckDB/Parquet 足够支撑早期分析。
- 保持目录、manifest、metadata schema 与未来对象存储/湖仓兼容。
- 不急着上 Airflow/Spark/Kafka，避免运维成本超过数据价值。

## 20. V0 开发任务拆分

```text
src/pitlake/
  control/
    registry.py
    contracts.py
    schedules.py
  connectors/
    base.py
    market/
    announcements/
    policy/
    gdelt/
    commodities/
  storage/
    raw_store.py
    metadata_store.py
    manifest_store.py
  quality/
    checks.py
    report.py
  replay/
    asof.py
  ops/
    backup.py
    health.py
    alerts.py
  cli.py
```

优先实现顺序：

```text
1. 控制面注册表 + 数据契约加载器
2. 原始数据存储 + 哈希计算
3. 元数据存储
4. 基础连接器接口
5. 一个公告采集连接器
6. 一个盘后日线行情采集连接器
7. 每日采集清单
8. 质量检查
9. 按历史时点回放
10. 调度器
```

## 21. 架构验收标准

长期采集框架不是“爬到数据”就合格。合格标准：

```text
任意 raw 文件都能追溯 source、provider、request、run；
任意 observed item 都能找到 raw_uri 和 content_hash；
任意补采数据不会污染 first_seen_at；
任意一天都能生成 replay snapshot；
任意 provider 更换不会破坏 logical_dataset 契约；
任意 P0 源失败会告警；
任意 schema drift 会进入质量报告；
任意付费源都有授权、额度、成本和过期日记录；
任意研究结果能追溯 input_manifest_id；
备份能恢复，恢复后 hash 可校验。
```

## 22. 参考资料

以下资料用于本次架构设计取舍。英文名称是工具或项目原名，括号里是中文说明：

- Databricks Medallion Architecture（湖仓铜/银/金分层架构）：`https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion`
- Apache Airflow（批处理工作流调度和监控平台）：`https://airflow.apache.org/docs/apache-airflow/`
- Dagster（面向数据资产的数据编排和可观测平台）：`https://docs.dagster.io/`
- Delta Lake（支持事务、时间旅行和批流统一的数据湖表格式）：`https://docs.delta.io/`
- Apache Iceberg（支持 schema 演进、分区演进和时间旅行的数据湖表格式）：`https://iceberg.apache.org/docs/latest/`
- lakeFS（面向数据湖的数据版本管理工具，类似“数据版 Git”）：`https://docs.lakefs.io/`
- Great Expectations（数据质量校验和数据文档工具）：`https://docs.greatexpectations.io/`
- OpenLineage（开放的数据血缘采集标准）：`https://openlineage.io/`
- Scrapy（网页采集和爬虫框架）：`https://docs.scrapy.org/`
- GDELT Project（全球新闻和事件开放数据库）：`https://www.gdeltproject.org/`
- Qlib（微软开源的 AI 量化研究平台）：`https://qlib.readthedocs.io/`
- AkShare（开源金融数据接口库）：`https://akshare.akfamily.xyz/`
- Tushare Pro（金融数据接口服务）：`https://tushare.pro/document/2`
- BaoStock（证券数据接口服务）：`http://baostock.com/baostock/index.php`
- 巨潮资讯：`https://www.cninfo.com.cn/`
- 上交所信息披露：`https://www.sse.com.cn/disclosure/listedinfo/announcement/`
- 深交所信息披露：`https://www.szse.cn/disclosure/listed/notice/`
- 北交所信息披露：`https://www.bse.cn/disclosure/announcement.html`
- 中国证监会：`http://www.csrc.gov.cn/`
- 中国人民银行：`http://www.pbc.gov.cn/`
- 国家统计局：`https://www.stats.gov.cn/`
- 国家数据：`https://data.stats.gov.cn/`
- Open-Meteo：`https://open-meteo.com/en/docs`
- NASA FIRMS：`https://firms.modaps.eosdis.nasa.gov/`
- NOAA Climate Data Online：`https://www.ncdc.noaa.gov/cdo-web/webservices/v2`
