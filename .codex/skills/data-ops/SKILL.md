# QDC Data Ops Skill

当任务来自 QDC 数据质量失败、采集异常、自动工单或相关 PR 时，先使用本 skill。目标是把问题分成“数据源/环境临时波动、配置/调度问题、解析或 schema 漂移、因子逻辑 bug、需要人工数据修复”几类，再决定是否改代码。

## 必做入口

1. 先看当前上下文和本地状态：

```powershell
git status --short
qdc db-info
qdc quality --start <date> --end <date>
qdc quality-issue-report --start <date> --end <date>
```

如果自动化环境没有激活 shell，使用：

```powershell
conda run -n ai-trader qdc quality --start <date> --end <date>
conda run -n ai-trader qdc quality-issue-report --start <date> --end <date>
```

2. 读取 Issue 正文中的 `Impact` 和 `Sample Issues`，再定位相关 raw bundle、silver 表、factor 输出和最近 `job_run` / `crawl_run`。
3. 不要把聊天记录当成唯一事实；用 DuckDB、raw 文件、manifest、测试和代码确认原因。

## 分类规则

- `upstream_data_or_network`: 上游接口超时、502、临时空页、源站当天未发布、网络或代理失败。默认不改业务代码；先重跑或记录源级观察。只有反复出现且可稳定复现时，才改超时、重试、降级或默认源策略。
- `config_or_schedule`: 日期窗口、source_id、page_size、instrument_limit、provider_uri、cutoff 或定时任务配置不合理。优先改配置、脚本或 README；不要用代码绕过配置错误。
- `parser_or_schema_drift`: 上游字段、HTML/JSON 结构、日期格式、股票映射或去重键变化。应做永久代码修复，补 collector normalization 和失败路径测试。
- `factor_logic_bug`: silver 数据正确，但 daily factor、情绪/主题分类、聚合或 Qlib 字段异常。应改 factor engine 或 export 逻辑，并补 focused test。
- `local_environment`: conda 环境、依赖、Qlib provider 或 DuckDB 文件锁问题。先修运行环境或串行化运行，不要把环境问题硬编码到业务逻辑。
- `manual_data_repair`: 只有在 raw 证据明确、幂等规则清楚、不会伪造 `collect_time` / `observed_at` 时才允许。raw/bronze 追加写入；不要覆盖历史对象。

## 临时修复和永久修复

临时处理适用：

- 单次上游波动、网络失败或源站当天真实无数据。
- 只需要重跑 `crawl-daily`、`build-factors`、`sync-parquet`、`quality`。
- 不改变 schema、CLI、因子口径或长期调度。

永久处理适用：

- 同类质量问题可稳定复现。
- 上游结构已经变化。
- 现有代码接受了脏数据、吞掉异常或写出了错误因子。
- 配置默认值会持续制造错误。
- 控制台、README 或实施计划的运行口径会误导后续操作。

永久处理必须补测试；公共 CLI、配置 key、schema、文件格式或工作流变化必须同步文档。

## 禁止项

- 不伪造历史 `collect_time`、`observed_at` 或“当时已看到”的事实。
- 不为了让质量检查通过而降低规则、删除 issue、吞异常或把坏数据改成 0。
- 不主动运行或扩展 `plan-backfill` / `run-backfill`，除非用户明确要求解冻。
- 不提交 `.env`、token、cookie、付费数据、真实凭据或 `data/quant_data_center/` 运行数据。
- 不回滚用户或其他 agent 的无关修改。

## 收尾验证

根据改动范围运行：

```powershell
conda run -n ai-trader qdc validate-config
conda run -n ai-trader qdc quality --start <date> --end <date>
conda run -n ai-trader pytest
conda run -n ai-trader ruff check .
```

如果只是文档或脚本变更，也至少运行相关单测或语法检查，并在最终说明中写清楚未运行的重型采集命令。
