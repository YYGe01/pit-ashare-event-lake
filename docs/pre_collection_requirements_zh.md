# 正式建立采集框架前的输入清单

> 更新时间：2026-04-26  
> 目的：在正式写采集框架代码前，明确哪些信息必须由使用者先提供或确认，哪些可以后续补充，避免账号、授权、历史数据、部署环境和合规边界在实施中途阻塞。

本文只讨论采集框架建设前的准备事项，不要求把账号密码写进文档或聊天记录。所有密钥、Token、Cookie、供应商账号都应通过本地 `.env`、系统环境变量、密码管理器或密钥管理服务提供，文档中只记录密钥引用名和授权边界。

## 1. 开工前一定需要你提供或确认的事项

以下事项是 V0 采集框架开工前的硬前置。如果缺失，框架可以写骨架，但不能安全接入真实数据源。

| 类别 | 必须提供/确认什么 | 为什么必须先确认 |
| --- | --- | --- |
| 研究范围 | 当前只做 A 股日频/周频；是否包含北交所；是否需要指数、ETF、可转债、基金、港股、美股 | 决定 `logical_dataset`、交易日历、代码规范和采集优先级 |
| P0 数据集优先级 | 第一批必须接入哪些数据：日线/复权、交易日历、停复牌/涨跌停、公告、政策监管、商品、全球市场 | 决定第一版连接器、表结构、调度和质量门禁 |
| 数据源授权边界 | 每个候选源是否允许采集、保存 raw、保存全文、保存附件、内部研究使用、再分发 | 避免违反网站条款、供应商合同、版权或数据使用限制 |
| 需要登录/API Key 的来源 | Tushare、券商 API、付费数据商、需要注册的开放 API 是否可用；对应密钥引用名是什么 | 没有凭据就不能接入受保护接口；凭据方式会影响连接器设计 |
| 是否允许爬网页 | 对公开网页、RSS、下载文件的采集边界；是否禁止某些网站；是否允许低频轮询 | 决定使用 API/RSS/文件下载，还是完全避开网页采集 |
| 部署环境 | 运行机器、操作系统、是否长期在线、Python/Conda 是否可用、网络是否能访问目标源 | 决定调度方式、路径、依赖、超时和重试策略 |
| 数据存储位置 | `data_lake` 根目录、可用磁盘容量、是否使用移动硬盘/NAS/对象存储 | raw 文件不可变保存，磁盘和路径必须一开始定好 |
| 备份位置 | 本地备份、移动硬盘、云盘、对象存储或 NAS；备份频率和保留周期 | 原始数据丢失后无法重造，备份是采集层硬要求 |
| 告警方式 | 采集失败时通知到哪里：邮件、企业微信、飞书、Telegram、日志文件或暂不告警 | P0 源失败不能静默，否则 PIT 数据会断流 |
| 预算和额度 | 付费源月预算、API 调用额度、是否允许购买数据、单源失败时是否启用付费备用源 | 决定免费/付费源选择、限速、shadow run 和供应商切换策略 |

## 2. 关于账号密码、登录和 API Key

不是所有数据源都需要账号密码。第一阶段建议优先选择官方公开页面、公开下载文件、开放 API、RSS、AkShare/BaoStock 等低门槛来源，先把采集账本、raw 保存、manifest 和质量检查跑稳。

需要你提供凭据的常见情况：

- Tushare Pro Token。
- 券商 API 账号、Token、证书或客户端配置。
- Wind、Choice、iFinD、聚源、CSMAR、RESSET 等付费数据商账号或授权文件。
- 需要注册 Key 的开放 API，例如部分天气、灾害、宏观、新闻或遥感接口。
- 私有云盘、对象存储、数据库或通知通道的访问密钥。

不建议作为 P0 数据源的情况：

- 必须人工登录后才能看，且没有官方 API。
- 需要验证码、短信、人机验证或频繁 Cookie 刷新。
- 明确禁止自动化采集或禁止保存原文。
- 只能通过个人浏览器会话访问的付费内容。

凭据提供方式：

```text
不要把真实账号、密码、Token 写进 git、Markdown 文档或聊天记录。
只在文档中写 credential_ref，例如 TUSHARE_TOKEN、WIND_USERNAME、S3_ACCESS_KEY。
真实值放到本地 .env、系统环境变量、密码管理器或密钥管理服务。
日志必须脱敏，不能打印完整密钥。
```

示例：

```yaml
provider_id: tushare
auth_method: api_token
credential_ref: TUSHARE_TOKEN
storage_permission: raw_allowed
quota_policy: "按账号积分和接口限制执行"
```

## 3. 关于是否需要你先下载数据

默认不需要你先手工下载公开数据。采集框架的目标是自动从数据源拉取、保存 raw、记录 `first_seen_at` 和生成 manifest。

只有以下情况需要你先提供本地文件或下载包：

| 场景 | 是否需要你提供 | 需要附带的信息 |
| --- | --- | --- |
| 已购买历史数据包 | 需要 | 文件路径、供应商、合同允许用途、数据覆盖期、下载时间、字段说明 |
| 只能网页人工下载的官方文件 | 可能需要 | 下载 URL、下载时间、文件原名、页面截图或来源说明、更新频率 |
| 你已有旧 CSV/Excel/Parquet/PDF 数据 | 如果想纳入统一数据湖就需要 | 原始路径、来源、采集/下载时间、是否可重分发、字段含义 |
| 历史公告 PDF 或财报压缩包 | 如果用于历史回填就需要 | 来源、下载批次、文件哈希、覆盖日期、是否完整 |
| 手工维护的行业/概念/产业链表 | 需要版本化时需要 | 创建人、来源参考、版本号、生效时间、变更原因 |

历史导入必须和实时采集区分：

```text
historical_import / backfill 数据可以进入数据湖；
但 first_seen_at 必须记录为本系统导入或首次看到的时间；
不能把历史文件伪装成当时实时采到的数据。
```

## 4. 每个数据源接入前需要确认的模板

后续每接入一个 source，至少需要填清楚以下信息。公开源如果没有账号，可以把 `auth_type` 写成 `none`。

```yaml
source_id:
provider_id:
logical_dataset:
priority: P0 / P1 / P2
source_type: api / web_list / rss / file_download / vendor_batch / manual
base_url:
terms_url:
robots_url:
auth_type: none / api_key / account_password / token / vendor_client / manual_download
credential_ref:
allowed_frequency:
storage_permission: metadata_only / raw_allowed / raw_forbidden / derived_only
redistribution_policy: internal_only / allowed / forbidden / contract_specific
historical_depth:
expected_latency:
quota_policy:
cost_policy:
contact_or_owner:
notes:
```

其中最关键的是：

```text
auth_type
credential_ref
allowed_frequency
storage_permission
redistribution_policy
quota_policy
```

这些字段决定连接器能不能合法运行、能不能保存 raw、能不能保存全文或附件，以及采集频率是否会触发封禁或超额。

## 5. 运行环境和存储信息

开工前需要确认：

```text
data_lake 根路径
metadata 数据库路径
日志路径
备份路径
可用磁盘容量
是否每天自动运行
机器是否会休眠/断网/关机
是否需要跨机器迁移
是否需要对象存储或 NAS
```

V0 推荐：

```text
本地文件系统保存 raw；
DuckDB/SQLite 保存 metadata；
Parquet 保存结构化快照；
Windows Task Scheduler 或 APScheduler 做调度；
每日至少备份 metadata 和 manifest；
每周备份 raw 文件。
```

如果运行机器不是 7x24 在线，需要明确：

```text
哪些任务允许错过后补采；
补采窗口多长；
失败后是否自动重试；
补采数据是否默认进入 published manifest。
```

## 6. 合规、版权和使用边界

开工前必须确认以下原则：

- 不绕过登录、验证码、付费墙和反爬机制。
- 不高频压测网站。
- 不采集非公开个人数据。
- 新闻、研报、社媒、付费数据的全文保存必须看授权。
- 付费源的 raw、derived、截图、导出结果是否可保存和分享，以合同为准。
- 对外展示或分享研究结果时，不能泄露不可再分发的数据原文。

如果某个源授权不清楚，默认策略是：

```text
只保存 URL、标题、摘要、发布时间、来源、哈希和必要元数据；
不保存或不传播受版权保护的全文；
待授权确认后再启用 raw 全文保存。
```

## 7. 市场可用时间和回测边界

为了后续不产生未来函数，需要你确认研究使用规则：

```text
盘后公告是否只允许下一交易日使用；
盘中看到的政策/新闻是否允许当日使用，还是统一下一交易日使用；
宏观数据发布后是否按 first_seen_at 立即可用；
周频调仓使用哪一天、哪个时间点的数据快照；
遇到停牌、涨跌停、临时停复牌如何处理候选池。
```

这些规则会写入 `market_available_at` 或研究层的 as-of 过滤规则。采集层仍然只记录事实时间，不替研究层做收益判断。

## 8. 可以后续补充的事项

以下事项不阻塞 V0 开工，但越早确定越好：

- 是否接入 P1/P2 数据，例如分钟级、Level-2、研报、社媒、遥感、专业新闻事件库。
- 是否购买付费数据源做 cross-source reconciliation。
- 是否使用 PostgreSQL、MinIO、Dagster/Airflow、Great Expectations、OpenLineage。
- 是否建立可视化 dashboard。
- 是否导出到 Qlib 或其他研究平台。
- 是否做多机器部署和远程对象存储。

## 9. 如果你暂时不给额外信息，V0 默认假设

如果没有额外约束，V0 默认按以下保守配置推进：

```text
研究范围：A 股日频/周频，默认包含沪深北上市股票，不含港股、美股、Level-2、tick。
数据源：优先官方公开源、公开 API、RSS、AkShare/BaoStock；付费源先预留 adapter，不默认接入。
登录策略：不绕过登录和验证码；没有官方 API 的登录源不作为 P0。
数据保存：raw append-only，本地文件系统 + DuckDB/SQLite + Parquet。
版权策略：授权不清的新闻/研报只保存元数据、URL、摘要和哈希。
调度：盘后日线、公告、政策、商品、全球日频优先；分钟级不进入默认任务。
备份：metadata/manifest 每日，raw 每周。
告警：先落 JSONL 日志和本地日报；通知通道等你提供后再接入。
```

## 10. 最小开工确认清单

正式写第一版采集框架前，你至少需要回复或提供：

```text
1. 第一阶段只做 A 股日频/周频是否确认？
2. P0 数据集是否按：日线/复权、交易日历、停复牌/涨跌停、公告、政策监管、商品、全球市场？
3. 是否已有 Tushare、券商 API、Wind/Choice/iFinD 等账号或 Token？如果有，只提供 credential_ref，不要贴真实密钥。
4. 是否有必须优先接入或必须避开的数据源？
5. data_lake 希望放在哪个磁盘目录？大概可用容量多少？
6. 运行机器是否每天在线？是否会休眠？
7. 备份放在哪里？
8. 失败告警希望发到哪里？
9. 是否已有历史数据包、CSV、Excel、PDF、供应商导出文件需要导入？
10. 预算上限和付费源策略是什么？
```

这些答案确认后，就可以进入 V0 框架实现：注册表、数据契约、raw store、metadata store、连接器基类、manifest、质量检查和首批 P0 连接器。

## 11. 2026-04-26 已确认开工决策

使用者已经确认：

```text
第一阶段只做 A 股日频；
P0 数据集按日线/复权、交易日历、停复牌/涨跌停、公告、政策监管、商品、全球市场推进；
P0 稳定后再升级 P1/P2；
当前没有 Tushare、券商 API、Wind/Choice/iFinD 等账号或 Token；
当前没有必须优先接入或必须避开的数据源；
当前没有历史数据包、CSV、Excel、PDF 或供应商导出文件需要导入；
前期预算有限，优先免费源，付费源只预留接口；
当前只有 C 盘，V0 由框架决定默认 data_lake、备份和告警方案。
```

V0 当前执行决策见：

```text
docs/v0_runtime_decisions_zh.md
```

2026-04-26 已开始正式落地 V0 框架，并已用 AkShare `stock_zh_a_daily` 跑通首个 A 股日线真实采集闭环。详见 `docs/v0_runtime_decisions_zh.md` 的“首个真实采集闭环”章节。
