# quant_data_center

本仓库当前主线是个人量化统一数据中心 `quant_data_center`。

目标是服务 Qlib 研究：采集 A 股每日数据、加工稳定日频因子，并导出 Qlib 可读数据。本仓库不做模型训练、组合回测、实盘下单或交易终端适配。

当前实施计划见 `docs/迁移实施计划.md`。第一次阅读项目时，建议先看 `docs/数据流阅读指南.md`，它按数据流解释原始留档层、上游快照层、统一研究层、因子、研究宽表层和 Qlib 数据目录导出的输入输出。控制台页面设计和后续标的画像规划见 `docs/控制台产品设计方案.md`。
每日收盘后自动采集、新闻公告爬虫和阶段状态统一见 `docs/每日自动采集实施计划.md`。

当前项目重心已经切到每日数据采集。历史回补能力保留为已有技术能力，但先冻结：默认不继续规划、执行或扩展 `plan-backfill` / `run-backfill`，除非明确为了解决既有队列或用户要求解冻。

## 当前入口

项目统一使用已存在的 `ai-trader` conda 环境；不要新建 `quant-data-center` conda 环境。需要同步依赖时，可在 `ai-trader` 中执行 editable install。

当前默认入口是每日采集链路：`daily-pipeline`、`crawl-daily`、`build-factors`、`sync-parquet`、`quality`、`export-qlib` 和 `console`。下面保留的回补命令仅作为历史能力参考，非当前优先事项。

```powershell
conda activate ai-trader
python -m pip install -e ".[market,dev]"
qdc validate-config
qdc init
qdc db-info
qdc smoke
qdc refresh-universe --universe csi300 --snapshot-date 2026-05-11
qdc list-universe --universe csi300
qdc plan-backfill --dataset daily_bar --source-id akshare --universe csi300 --start 2026-05-01 --end 2026-05-03 --batch-size 1 --chunk-days 2
qdc list-backfill --dataset daily_bar
qdc run-backfill --dataset daily_bar --limit-tasks 4 --control-only
qdc run-backfill --dataset daily_bar --retry-failed --limit-tasks 4
qdc recover-running --dataset daily_bar --older-than-minutes 15
qdc split-backfill --task-id <task_id> --batch-size 10
qdc plan-backfill --dataset trade_calendar --source-id akshare --start 2026-05-01 --end 2026-05-03
qdc run-backfill --dataset trade_calendar --limit-tasks 1
qdc daily --date 2026-05-11 --universe csi300 --control-only
qdc daily-pipeline --date 2026-05-11 --symbols "SH600000,SZ000001" --batch-size 1 --control-only
qdc daily-pipeline --date 2026-05-11 --symbols "SH600000,SZ000001" --batch-size 1 --control-only --crawl-documents --crawl-page-size 5 --crawl-max-pages 1 --crawl-pdf-limit 1
qdc daily-pipeline --batch-size 50
qdc --config config/quant_data_center_daily_only.yaml init
qdc --config config/quant_data_center_daily_only.yaml daily-pipeline
qdc crawl-plan --source-id cninfo_announcement --date 2026-05-11 --control-only
qdc crawl-plan --source-id sse_announcement --date 2026-05-11 --control-only
qdc crawl-plan --source-id sina_finance_news --date 2026-05-11 --control-only
qdc crawl-plan --source-id eastmoney_roll_news --date 2026-05-11 --control-only
qdc crawl-plan --source-id nbd_company_news --date 2026-05-11 --control-only
qdc crawl-run --source-id cninfo_announcement --control-only
qdc crawl-run --source-id sse_announcement --control-only
qdc crawl-run --source-id sina_finance_news --control-only
qdc crawl-run --source-id eastmoney_roll_news --control-only
qdc crawl-run --source-id nbd_company_news --control-only
qdc crawl-daily --date 2026-05-11 --control-only
qdc crawl-daily --date 2026-05-11 --source-id cninfo_announcement --page-size 2 --max-pages 1 --pdf-limit 1
qdc crawl-daily --date 2026-05-11 --source-id sse_announcement --page-size 2 --max-pages 1 --skip-pdf-download
qdc crawl-daily --date 2026-05-11 --source-id sina_finance_news --page-size 10 --max-pages 1
qdc crawl-daily --date 2026-05-11 --source-id eastmoney_roll_news --page-size 10 --max-pages 1
qdc crawl-daily --date 2026-05-11 --source-id nbd_company_news --symbols "SZ301421,SZ300001" --page-size 100 --max-pages 5
qdc daily-pipeline --date 2026-05-11 --symbols "SZ301421,SZ300001" --batch-size 1 --skip-stock-basic-refresh --no-crawl-documents --skip-factors --skip-sync --skip-quality --skip-export
qdc crawl-daily --date 2026-05-11 --source-id cninfo_announcement --symbols "SZ301421,SZ300001" --page-size 100 --max-pages 9 --parallel-sources 1
qdc crawl-daily --date 2026-05-11 --source-id nbd_company_news --symbols "SZ301421,SZ300001" --page-size 100 --max-pages 5 --parallel-sources 1
qdc build-factors --factor-set all --start 2026-05-01 --end 2026-05-03
qdc classify-text-event --document-type announcement --title "公司收到交易所监管问询函"
qdc sync-parquet --layer all
qdc quality --dataset daily_bar --start 2026-05-01 --end 2026-05-03
qdc export-qlib --start 2026-05-01 --end 2026-05-03 --provider-uri data/quant_data_center/qlib/cn_data --market-name qdc_smoke
qdc verify-qlib --start 2026-05-01 --end 2026-05-03 --instruments SH600000,SZ000001 --provider-uri data/quant_data_center/qlib/cn_data
qdc console --host 127.0.0.1 --port 8765
qrun config/qlib/workflow_config_lightgbm_alpha158_qdc_smoke.yaml
qrun config/qlib/workflow_config_lightgbm_alpha158_qdc_external.yaml
```

`run-backfill --control-only` 只验证任务状态流和水位表，不采集真实数据。

历史回补能力已实现但当前冻结。既有 AkShare 回补分支支持：

- 证券主数据 `stock_basic`
- 交易日历 `trade_calendar`
- 日线行情 `daily_bar`
- 复权因子 `adj_factor`
- 涨跌停价格 `price_limit`
- 交易状态 `trade_status`
- 公告 `announcement`
- 新闻 `news`

日线行情 `daily_bar`、复权因子 `adj_factor`、涨跌停价格 `price_limit` 可用 `--universe` 展开上游代码 `symbol`，也可以显式传入 `--symbols` 覆盖。历史回补命令仍保留 AkShare `announcement` / `news` 能力，但当前每日链路不再用 AkShare 作为公告新闻来源。`qdc refresh-universe` 可把 AkShare 指数成分快照写入 `qdc_silver.universe_constituent`，回补规划会优先使用最新快照；如果没有快照，再回退到配置里的静态样例。

`qdc daily-pipeline` 是收盘后日频自动化入口，默认使用 `all_a` 全 A 当前 active 标的：先刷新 `stock_basic`，再执行结构化单日采集、同日新闻公告爬虫、因子重建、Parquet 同步、质量检查和 Qlib provider 导出。每日结构化任务只规划 `trade_calendar`、`daily_bar`、`adj_factor`、`price_limit` 和 `trade_status`；默认配置会按 `source_ids: [akshare, eastmoney, sina]` 规划结构化备源，其中 Eastmoney 覆盖日线、复权、涨跌停和停牌，Sina 覆盖交易日历。公告新闻只走 `crawl-daily` 的 crawler-backed 源，避免 AkShare 元数据混入当日文档口径。配置里的 `daily_parallelism`、`crawl_parallelism`、`crawl_request_timeout_seconds` 和 `crawl_source_timeout_seconds` 控制并行度和源级超时；命令行同名参数可临时覆盖。`crawl_documents: true` 会在因子构建前执行 `crawl-daily` 的默认新闻公告源；需要临时只跑结构化链路时传 `--no-crawl-documents`。首次全市场运行前先用 `--symbols`、`--control-only` 和爬虫限量参数做 smoke。

如果暂时不使用历史回补数据，改用 `config/quant_data_center_daily_only.yaml`。该配置把 DuckDB、raw、Parquet、Qlib provider 和日志全部写到 `data/quant_data_center_daily_only/`，与当前 `data/quant_data_center/` 历史回补库隔离。`daily_pipeline` 段保存 `source_ids`、`batch_size`、`daily_parallelism`、`export_start`、`market_name`、非结构化文档爬虫和跳过步骤等默认参数；默认会采集公告和新闻文档，命令行传同名参数时会覆盖配置，例如 `--batch-size 20`、`--source-ids akshare` 或 `--no-crawl-documents`。

`qdc crawl-plan` / `qdc crawl-run` / `qdc crawl-daily` 是非结构化数据每日爬虫入口。当前默认源是 `cninfo_announcement`、`sse_announcement`、`sina_finance_news`、`eastmoney_roll_news` 和 `nbd_company_news`。公告源必须能按请求日期或返回字段确认当日发布日期；新闻源必须能解析完整 `YYYY-MM-DD HH:MM[:SS]` 发布时间，分不清发布日期的候选不启用。Sina/Eastmoney 是滚动页近实时口径，不适合作为隔天历史日期主源；NBD 使用日期分页做 metadata-only 历史日期样本。文档源按 source 并行执行；单源请求或整体超时会标记该源本次失败，其他成功源继续落 raw 和 silver。raw 侧除原始 JSON 外，会额外写直观文件包 `data/quant_data_center/raw/documents/<date>/<source>/<run>/manifest.json` 和 `records.jsonl`；新闻 `records.jsonl` 只写已映射到 instrument 的 normalized metadata 记录，整包上游响应只保留在 raw 对象中。DuckDB 继续做索引、控制表、silver 表、去重和导出。新闻源只保存 metadata/title/url，不保存版权不明确的正文。`build-factors` 会按 `publish_date + instrument + normalized_title` 对多源文档去重后再计数。可用 `--page-size` / `--max-pages` / `--pdf-limit` 做小范围真实 smoke；如只验证公告列表元数据，可加 `--skip-pdf-download`。正文抽取和更多备用源在后续阶段接入。

当前每日链路会写入原始 JSON、直观 `raw/documents` 文件包、上游快照 Parquet、`qdc_silver` DuckDB 表，并在 `qdc_meta.source_object` 登记文件索引。控制台每日宽表和处理后因子只消费 crawler-backed 文档源；点击公告/新闻标题会优先预览本地 PDF 或按标的归一化后的 `records.jsonl`，并标记“已保存正文文本 / 已保存 PDF 原文 / 仅元数据 / 本地未保存内容”。历史回补相关的 `qdc plan-backfill` / `qdc run-backfill` / `qdc run-backfill --retry-failed` 先冻结，不作为默认推进方向。`qdc build-factors` 默认用规则引擎基于 crawler-backed 新闻/公告生成日频 count、标题级情绪和事件因子，覆盖增长、风险、融资、合同、回购、股东增减持、监管、诉讼、业绩、质押和担保等事件；`qdc sync-parquet` 可同步统一研究层/研究宽表层 Parquet，`qdc quality` 可做基础质量检查，`qdc export-qlib` 可导出 Qlib day 数据目录，`qdc verify-qlib` 可用本地 Qlib 直接读取导出的数据目录做数据读取层冒烟验证。

LLM 抽取接口已预留为单条冒烟验证 `smoke` 命令，不参与 `build-factors` 全量构建。模型切换和数据提供方 `provider` 默认值统一放在 `config/quant_data_center.yaml` 的 `llm.text_event` 段：

```yaml
llm:
  text_event:
    provider: rule
    model: deepseek/deepseek-v4-flash
    api_key_file: data/quant_data_center/secrets/deepseek_api_key
    api_key_env: DEEPSEEK_API_KEY
    temperature: 0
    max_tokens: 512
```

`api_key_file` 指向 gitignored 的本地密钥文件，不提交真实 key。需要用 LLM 单条验证时，把 `provider` 改为 `llm` 或临时传 `--provider llm`：

```bash
qdc classify-text-event --provider llm --document-type announcement --title "公司拟回购股份"
```

长时间回补时，`qdc recover-running` 可把陈旧的运行中 `running` 任务标记为失败 `failed` 以便后续重试；`qdc split-backfill` 可把一个大的上游代码 `symbol` 批次任务拆成更小的待执行 `pending` 子任务。

`qdc console` 会启动一个本地只读 Web 控制台，默认访问 `http://127.0.0.1:8765/`。当前控制台聚焦每日采集：今日总览展示最近每日运行、每日链路阶段进度、数据水位和质量信号；数据预览支持按数据集查看最新记录，也支持按标的查看处理后日频因子或原始输入。页面先不展示历史回补队列，不触发采集或写库。采集过程中可以保持控制台打开；写入端遇到控制台瞬时只读锁会等待重试，控制台遇到采集写锁会显示“写入中”降级状态并继续刷新。

`qdc` 默认读取仓库内 `config/quant_data_center.yaml`；需要从其他目录运行或切换配置时，可设置 `QDC_CONFIG` 或传入 `--config`。

## 项目结构

```text
config/quant_data_center.yaml       QDC 运行配置
config/quant_data_center_daily_only.yaml  只积累每日新增数据的隔离配置
src/quant_data_center/              QDC 源码
src/quant_data_center/console_static/  本地只读控制台静态页面
tests/test_qdc_storage.py           当前 QDC 聚焦测试
docs/数据流阅读指南.md             数据流阅读指南
docs/迁移实施计划.md               迁移实施计划和当前状态
docs/工作日志/                     智能体工作记录
data/quant_data_center/             本地运行数据，已 gitignored
data/quant_data_center_daily_only/   daily-only 本地运行数据，已 gitignored
```

当前主线只保留 QDC 代码、配置、测试和文档。

## Qlib 联调

如需要在当前环境直接验证 Qlib 数据目录 provider，可安装本地 Qlib 源码和依赖。网络慢时优先使用国内 PyPI 源：

```bash
conda run -n ai-trader python -m pip install -e /root/code/qlib -i https://pypi.tuna.tsinghua.edu.cn/simple --extra-index-url https://mirrors.aliyun.com/pypi/simple
```

当前已用本地 `/root/code/qlib` 验证交易日历读取 `D.calendar`、标的列表读取 `D.list_instruments` 和特征读取 `D.features` 能读取 `qdc export-qlib` 产物。导出字段包含 Alpha158 默认需要的成交均价 `$vwap`，并包含新闻数量 `$news_count`、新闻情绪均值 `$news_sentiment_mean`、风险类新闻数量 `$news_risk_count`、融资类公告数量 `$announcement_financing_count` 等 QDC 外部日频因子；`--market-name` 可写出 Qlib 命名市场 `market` 文件。`config/qlib/workflow_config_lightgbm_alpha158_qdc_smoke.yaml` 可用当前 Qlib 数据目录 `provider` 跑通 LightGBM Alpha158 qrun 冒烟验证 `smoke`；`config/qlib/workflow_config_lightgbm_alpha158_qdc_csi300.yaml` 使用 `market=csi300` 跑正式股票池 baseline；`config/qlib/workflow_config_lightgbm_alpha158_qdc_csi300_external.yaml` 使用当前仓库的 `QdcAlpha158WithExternal` 处理器 `handler` 追加 QDC 外部因子。正式组合回测仍需要指数 benchmark 和组合分析配置。

## 验证

```powershell
qdc validate-config
qdc daily --date 2026-05-11 --universe csi300 --control-only
qdc daily-pipeline --date 2026-05-11 --symbols "SH600000,SZ000001" --batch-size 1 --control-only
qdc sync-parquet --layer all
qdc quality --dataset daily_bar
qdc verify-qlib --start 2024-01-02 --end 2024-01-02 --instruments SH600000,SZ000001 --provider-uri data/quant_data_center/qlib/cn_data
pytest
ruff check .
```

自动化环境未激活 shell 时：

```powershell
conda run -n ai-trader qdc validate-config
conda run -n ai-trader pytest
conda run -n ai-trader ruff check .
```
