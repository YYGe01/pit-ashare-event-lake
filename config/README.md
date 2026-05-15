# QDC 配置说明

默认配置文件是：

```text
config/quant_data_center.yaml
```

日常完整采集可以直接运行；当前 `daily_pipeline.date_offset_days: -1` 表示默认采集昨天：

```powershell
conda run -n ai-trader qdc daily-pipeline
```

临时补跑某个日期时再显式覆盖：

```powershell
conda run -n ai-trader qdc daily-pipeline --date 2026-05-14
```

不建议长期把 `daily_pipeline.date` 写成固定日期，避免误跑旧日期。

当前主线每日文档采集优先直接运行：

```powershell
conda run -n ai-trader qdc crawl-daily
```

`crawl_daily` 是文档采集默认参数的主配置；`daily_pipeline --crawl-documents` 会复用这些默认值，除非 `daily_pipeline.crawl_*` 显式配置了覆盖值。

## project

| 参数 | 作用 |
| --- | --- |
| `name` | 项目名，仅用于运行信息和配置展示。 |
| `timezone` | 项目默认时区；未传 `--date` 时按该时区取当天。 |
| `phase` | 当前项目阶段标签，便于区分迁移、生产或测试环境。 |

## paths

| 参数 | 作用 |
| --- | --- |
| `data_root` | QDC 本地数据根目录。 |
| `database_path` | DuckDB 数据库文件路径。 |
| `raw_root` | raw 对象根目录。 |
| `parquet_root` | 同步出的 Parquet 根目录。 |
| `qlib_root` | QDC 导出的 Qlib 数据根目录。 |
| `logs_dir` | 日志目录。 |

## qlib_provider

| 参数 | 作用 |
| --- | --- |
| `provider_uri` | 外部 Qlib 基础行情 provider 路径。 |
| `required_fields` | `verify-qlib` 默认检查的基础字段。 |

## runtime

| 参数 | 作用 |
| --- | --- |
| `database_backend` | 当前只支持 `duckdb`。 |
| `file_format` | 当前只支持 `parquet`。 |
| `use_environment_proxy` | 是否让请求使用系统环境变量代理。默认 `false`。 |

## policy

| 参数 | 作用 |
| --- | --- |
| `prefer_free_sources` | 优先使用免费公开源。 |
| `paid_providers_enabled` | 是否启用付费源。不要在无凭据时打开。 |
| `raw_append_only` | raw 对象追加写入策略。 |
| `unknown_copyright_policy` | 未明确版权口径时的默认处理策略。 |

## daily_pipeline

这是 `qdc daily-pipeline` 的默认参数区。命令行传参优先级高于这里的配置。

`daily-pipeline` 仍保留为历史结构化诊断和一键兼容入口；当前主线文档采集默认参数放在 `crawl_daily`。本节的 `crawl_*` 参数默认建议保持 `null`，表示继承 `crawl_daily`；只有需要让 `daily-pipeline --crawl-documents` 与直接 `crawl-daily` 不同时，才在这里显式覆盖。

| 参数 | 作用 |
| --- | --- |
| `date` | 固定默认运行日期。建议保持 `null`，除非只做一次固定日期任务。 |
| `date_offset_days` | 相对运行日期偏移。`-1` 表示默认跑昨天，`0` 表示今天；命令行 `--date` 优先级更高。 |
| `universe` | 默认股票池；`all_a` / `all` / `ashare` 表示全 A。 |
| `source_id` | 结构化日频源默认主源。 |
| `source_ids` | 结构化日频源列表；覆盖 `source_id` 的单源模式。 |
| `symbols` | smoke 或临时任务的固定标的列表，逗号分隔；生产全市场保持 `null`。 |
| `all_market` | 是否强制按 `stock_basic` 全市场解析；`null` 时由 `universe` 判断。 |
| `skip_stock_basic_refresh` | 全市场解析前是否跳过 `stock_basic` 刷新。 |
| `batch_size` | 结构化日频任务按标的拆分的批大小。 |
| `limit_tasks` | 限制本次结构化日频任务数量；生产保持 `null`。 |
| `daily_parallelism` | 结构化日频任务并发数。 |
| `provider_uri` | Qlib 导出目标路径；`null` 时使用 `qlib_provider.provider_uri` 或命令默认。 |
| `export_start` | Qlib 导出起始日期；`null` 时默认等于运行日期。 |
| `market_name` | Qlib instruments 市场文件名；`null` 时按 universe 推导。 |
| `plan_only` | 只规划不执行。生产默认 `false`。 |
| `control_only` | 只跑控制表流程，不真实采集。生产默认 `false`。 |
| `watch` | 是否输出阶段进度到 stderr。 |
| `continue_on_failure` | 某阶段失败后是否继续后续阶段。 |
| `crawl_documents` | 是否在日频结构化任务后运行文档采集。 |
| `crawl_source_id` | 文档采集源过滤；`null` 表示默认文档源全跑。 |
| `crawl_limit_tasks` | 限制本次文档采集任务数量；生产保持 `null`。 |
| `crawl_page_size` | `daily-pipeline` 文档接口分页大小覆盖值；`null` 时继承 `crawl_daily.page_size`。 |
| `crawl_max_pages` | `daily-pipeline` 文档接口最大页数覆盖值；完整采集通常保持 `null`。 |
| `crawl_pdf_limit` | `daily-pipeline` 公告 PDF 下载数量上限覆盖值。 |
| `crawl_parallelism` | `daily-pipeline` 文档 source 并发覆盖值；`null` 时继承 `crawl_daily.parallel_sources`。 |
| `crawl_request_timeout_seconds` | `daily-pipeline` 单请求超时覆盖值；`null` 时继承 `crawl_daily.request_timeout_seconds`。 |
| `crawl_source_timeout_seconds` | `daily-pipeline` 单 source 总超时覆盖值；`null` 时继承 `crawl_daily.source_timeout_seconds` 和 source override。 |
| `crawl_instrument_parallelism` | `daily-pipeline` 标的循环源并发覆盖值。 |
| `crawl_instrument_limit` | `daily-pipeline` 隐式股票池上限覆盖值。 |
| `crawl_interaction_schedule` | `daily-pipeline` 互动问答调度策略覆盖值。 |
| `crawl_interaction_cold_no_data_days` | `daily-pipeline` cold 阈值覆盖值。 |
| `crawl_interaction_cold_check_interval_days` | `daily-pipeline` cold 复查间隔覆盖值。 |
| `crawl_interaction_cold_lookback_days` | `daily-pipeline` cold 复查窗口覆盖值。 |
| `crawl_interaction_unsupported_check_interval_days` | `daily-pipeline` unsupported 复查间隔覆盖值。 |
| `skip_crawl_pdf_download` | `daily-pipeline` 是否跳过公告 PDF 下载的覆盖值；`null` 时继承 `crawl_daily.skip_pdf_download`。 |
| `skip_factors` | 是否跳过因子构建。 |
| `skip_sync` | 是否跳过 Parquet 同步。 |
| `skip_quality` | 是否跳过质量检查。 |
| `skip_export` | 是否跳过 Qlib 导出。 |

## crawl_daily

这是直接 `qdc crawl-daily` 的默认参数区，也是文档采集默认参数的主配置。命令行传参优先级最高；没有命令行覆盖时，source 级 `source_overrides` 优先于全局值。

| 参数 | 作用 |
| --- | --- |
| `date` | 固定默认采集日期。建议保持 `null`，直接命令未传 `--date` 时按项目时区采集当天。 |
| `source_id` | 默认 source 过滤；`null` 表示运行默认每日源清单。 |
| `symbols` | 默认标的过滤；生产保持 `null`。 |
| `limit_tasks` | 限制本次文档采集任务数量；生产保持 `null`。 |
| `page_size` | 默认分页大小。当前全局为 `30`。 |
| `max_pages` | 最大页数限制；完整采集保持 `null`。 |
| `pdf_limit` | 公告 PDF 下载数量上限；默认不下载 PDF 时保持 `null`。 |
| `parallel_sources` | 多个文档 source 之间的并发数。 |
| `request_timeout_seconds` | 默认单请求超时。当前为 `30` 秒。 |
| `source_timeout_seconds` | 默认单 source 总超时。当前为 `900` 秒，避免 CNINFO 公告完整扫描被 180 秒默认值截断。 |
| `instrument_parallelism` | 互动问答等按标的循环源的标的并发数。 |
| `instrument_limit` | 互动问答隐式股票池上限；`0` 表示全 active instruments。 |
| `interaction_schedule` | 互动问答调度策略；默认 `cold-weekly`。 |
| `interaction_cold_no_data_days` | 连续多少次无目标日互动后进入 cold。 |
| `interaction_cold_check_interval_days` | cold 标的每隔多少自然日复查一次。 |
| `interaction_cold_lookback_days` | cold 复查时接受最近多少自然日的滚动窗口。 |
| `interaction_unsupported_check_interval_days` | `missing_org` / unsupported 标的复查间隔。 |
| `skip_pdf_download` | 是否跳过公告 PDF 下载。默认 `true`。 |
| `watch` | 是否默认输出 source 级进度。 |
| `plan_only` | 是否默认只规划不执行。生产默认 `false`。 |
| `control_only` | 是否默认只跑控制面不真实采集。生产默认 `false`。 |
| `source_overrides` | source 级运行参数覆盖。用于记录已验证的源特定稳定参数，例如 `sse_announcement.page_size=500`。 |

当前内置的 source 级参数来自 `2026-05-15` 真实采集：

| source_id | 参数 | 原因 |
| --- | --- | --- |
| `sse_announcement` | `page_size=500`、`request_timeout_seconds=120`、`source_timeout_seconds=900` | `page_size=30/100` 连续翻页时被远端断开；`page_size=500` 完整扫描成功，写入 748 条。 |
| `cninfo_investor_interaction` | `source_timeout_seconds=7200` | 全市场互动问答按标的循环，cold 调度下仍可能超过普通文档源 900 秒窗口。 |

## llm.text_event

| 参数 | 作用 |
| --- | --- |
| `provider` | 文本事件分类 provider，当前默认 `rule`。 |
| `model` | LLM provider 使用的模型名；规则模式下只作为配置占位。 |
| `api_key_file` | API key 文件路径；不要提交真实密钥。 |
| `api_key_env` | API key 环境变量名。 |
| `temperature` | LLM 分类温度。 |
| `max_tokens` | LLM 分类最大 token 数。 |

## universes

每个 universe 下面配置固定 `symbols` 列表，用于 smoke、小股票池实验或非全市场任务。

```yaml
universes:
  csi300:
    symbols:
      - SH600000
      - SZ000001
```

生产全 A 主链路优先使用 `daily_pipeline.universe: all_a` 和 `stock_basic`，不要手动维护全市场 symbols 列表。
