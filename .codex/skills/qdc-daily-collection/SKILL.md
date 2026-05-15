---
name: qdc-daily-collection
description: Run and triage the quant_data_center daily collection workflow. Use when the user asks to perform QDC daily collection, collect yesterday's data, inspect a daily collection report, judge whether empty data is abnormal, fix confirmed collector/factor/config defects, rerun quality checks, or continue the collect-check-fix-rerun loop for this repository.
---

# QDC Daily Collection

## Workflow

默认用中文汇报。除非用户指定日期，按 `daily_pipeline.date_offset_days` 口径采集昨天。

先确认状态：

```powershell
git status --short
conda run -n ai-trader qdc validate-config
```

Windows/conda 输出 markdown 或中文内容时，如果遇到 GBK/UnicodeEncodeError，改用：

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONUTF8='1'
conda run --no-capture-output -n ai-trader ...
```

运行每日链路：

```powershell
conda run -n ai-trader qdc crawl-daily --date <date>
conda run -n ai-trader qdc build-factors --factor-set all --start <date> --end <date>
conda run -n ai-trader qdc sync-parquet --layer all
conda run -n ai-trader qdc quality --start <date> --end <date>
conda run -n ai-trader qdc daily-health --date <date> --format markdown
```

如果由 Codex 代跑且用户要求看到过程，优先用 `--no-capture-output`，把输出写入 `data/qdc_run_logs/<date>/`，并每 30 秒汇报当前命令、进程状态、`crawl_task`、`source_object` 或 `daily-health` 摘要；不要使用不存在的 `--watch` 参数。

如果用户只要求检查报告，不要重新采集；直接运行 `qdc daily-health --date <date>`，必要时再查控制表和 raw manifest。

## Quality Gate

以 `qdc daily-health` 为每日采集质量入口。它聚合：

- `crawl_task` / `crawl_run`：任务是否失败、未完成、未执行。
- raw document `manifest.json`：`provider_record_count`、`date_scan_complete`、解析失败率、映射率。
- silver 表：公告、新闻、研报、互动问答、公开舆情的当日入库行数。
- daily factor 表：对应 external factor 是否生成。
- `qdc quality` / `quality_issue`：字段合法性和因子合法性。
- 近 20 日 silver 行数中位数：识别突然低量。

默认判定：

```text
error:
  crawl task failed
  默认源未运行
  provider_record_count > 0 但 silver 行数为 0
  parse_failed_rate >= 20% 且 provider_record_count >= 10
  mapping_rate <= 30% 且 parsed_unique_record_count >= 10
  公告组 / 新闻组 / 公开舆情组在交易日全为空
  eastmoney_public_sentiment 交易日 silver 行数 < 500
  有文档 silver 行但对应 daily factor 行数为 0
  qdc quality 产生 error issue

warning:
  date_scan_complete=false
  pending/running task 未收敛
  可稀疏源为空，例如研报、Sina 新闻
  parse_failed_rate 在 5%-20%
  mapping_rate 在 30%-70%
  当日行数低于近 20 日中位数 30%
  build-factors / sync-parquet / quality 缺少 job_run 记录

ok:
  默认源已运行，关键组不为空，解析和映射率正常，factor 已生成，qdc quality 无 open issue。
```

`cninfo_investor_interaction` 是稀疏、cold-weekly 源；0 行可以是正常结果，不要仅因互动问答为空就改代码。

## Triage Rules

先按 `daily-health` 的 `code` 分类，再决定是否改代码：

- `source_not_run` / `crawl_task_unfinished`：先补跑或恢复任务，不改代码。
- `crawl_task_failed`：看 `last_error`。网络、超时、源站 502 先重跑；稳定复现再改超时、重试或源策略。
- 公告源 `cninfo_announcement` / `sse_announcement` 如果 `last_error=source timeout exceeded ...`，优先单源补跑并加大超时，例如 `qdc crawl-daily --date <date> --source-id cninfo_announcement --force --source-timeout-seconds 900`；确认仍复现再改代码。
- `empty_source_result` / `below_*_rows`：先确认是否周末、节假日、显式 `--max-pages`、显式 `--symbols` 或上游真实低量。
- `date_scan_incomplete`：正式采集不要用 `--max-pages` 限制；重跑该源。
- `provider_records_without_silver` / `high_parse_failed_rate`：优先查 collector normalization、字段变化、过滤条件、去重键；确认后做永久代码修复并补测试。
- 如果 raw/bronze 样本和 manifest 显示源站返回的 `publish_date` / `trade_date` 全部晚于目标日，例如目标日 `<date>` 但记录全是后一交易日，判定为源站已滚动或 latest-only 数据不可补；不要把后一日数据写入目标日，也不要跨日期采集来掩盖目标日报告，只记录结论并建议把该源调度提前到目标日收盘后或次日早间源站刷新前。
- `low_mapping_rate` / `elevated_mapping_failures`：查 `stock_basic`、简称歧义和 instrument 映射规则；不要把低置信新闻直接计入公司级因子。
- `documents_without_factor_rows`：先重跑 `build-factors`；仍为空再修 factor builder。
- `quality_issue:*`：读取 `observed_value`，判断是坏数据、解析 bug、因子 bug 还是环境问题。

只在确认是确定性代码、配置、schema 或因子逻辑缺陷时改代码。单次上游波动、节假日低量、源站真实无数据，只记录结论或重跑，不做业务代码绕行。

## Fix And Rerun Loop

修复后按最小范围重跑：

```powershell
conda run -n ai-trader qdc crawl-daily --date <date> --source-id <source_id> --force
conda run -n ai-trader qdc build-factors --factor-set all --start <date> --end <date>
conda run -n ai-trader qdc sync-parquet --layer all
conda run -n ai-trader qdc quality --start <date> --end <date>
conda run -n ai-trader qdc daily-health --date <date> --source-id <source_id> --format markdown
```

如果修复影响共享逻辑或多个源，去掉 `--source-id` 跑全量 `daily-health`。

结束条件：

- `daily-health status=ok`：可以结束。
- `status=warning`：只有在报告明确说明是节假日、稀疏源、上游真实低量或用户接受的降级时，才可结束，并在最终回复写明残余 warning。
- `status=error`：继续定位、修复或重跑；不要把 error 当作可用日报。

## Boundaries

- 不主动运行或扩展 `plan-backfill` / `run-backfill`。
- 不伪造 `collect_time`、`observed_at` 或历史可见性。
- 不提交 `.env`、token、cookie、付费数据或 `data/quant_data_center/` 运行数据。
- 如果产生代码、测试、配置或文档改动，按项目 Git 规则验证并提交；只 stage 本轮相关文件。
