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
| `crawl_page_size` | 文档接口分页大小。 |
| `crawl_max_pages` | 文档接口最大页数；完整采集通常保持 `null`，由各源默认早停。 |
| `crawl_pdf_limit` | 公告 PDF 下载数量上限；默认不下载 PDF 时可保持 `null`。 |
| `crawl_parallelism` | 多个文档 source 之间的并发数。 |
| `crawl_request_timeout_seconds` | 单个 HTTP 请求超时时间。 |
| `crawl_source_timeout_seconds` | 单个文档 source 总超时时间。互动问答全市场需要较大值。 |
| `crawl_instrument_parallelism` | 互动问答等按标的循环源的标的并发数。 |
| `crawl_instrument_limit` | 互动问答隐式股票池上限；`0` 表示全 active instruments。 |
| `crawl_interaction_schedule` | 互动问答调度策略；默认 `cold-weekly`。如需严格每日全扫设为 `strict`。 |
| `crawl_interaction_cold_no_data_days` | 连续多少次无目标日互动后进入 cold。 |
| `crawl_interaction_cold_check_interval_days` | cold 标的每隔多少自然日复查一次。 |
| `crawl_interaction_cold_lookback_days` | cold 复查时接受最近多少自然日的滚动窗口。 |
| `crawl_interaction_unsupported_check_interval_days` | `missing_org` / unsupported 标的复查间隔。 |
| `skip_crawl_pdf_download` | 是否跳过公告 PDF 下载。默认 `true`。 |
| `skip_factors` | 是否跳过因子构建。 |
| `skip_sync` | 是否跳过 Parquet 同步。 |
| `skip_quality` | 是否跳过质量检查。 |
| `skip_export` | 是否跳过 Qlib 导出。 |

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
