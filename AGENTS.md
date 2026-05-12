# AGENTS.md

这是 Codex、Cursor 和其他 coding agent 共用的项目级指令文件。

## 语言规则

- 默认用中文和用户沟通。
- 项目文档、智能体工作日志、计划说明和解释性内容尽量用中文。
- 代码标识符、Python 模块名、CLI 命令名、配置 key、表名、dataset 名、schema 字段名保持英文。

## 项目背景

- 本仓库当前主线是 `quant_data_center`，用于建设个人量化统一数据中心。
- 目标服务 Qlib 研究：A 股每日数据采集、标准化清洗、稳定日频因子加工和 Qlib 导出。
- 当前项目重心是每日数据采集链路：`daily-pipeline`、`crawl-daily`、质量检查、Parquet 同步、Qlib 导出和控制台观察。
- 历史回补能力先冻结：不要主动规划、实现、扩展或执行 `plan-backfill` / `run-backfill` 相关工作，除非用户明确要求解冻或处理既有队列。
- 本仓库不做模型训练、组合回测、实盘下单或终端适配。
- 当前主线只保留 QDC 代码、配置、测试和文档。
- 当前项目统一使用已存在的 `ai-trader` conda 环境；不要创建或切换到 `quant-data-center` conda 环境。`quant-data-center` 仅保留为 Python 项目发行名。

## 开始工作时先做什么

如果用户说“继续”或没有给出更具体上下文，先查看：

```powershell
git status --short
Get-Content -Raw -Encoding UTF8 README.md
Get-Content -Raw -Encoding UTF8 docs\迁移实施计划.md
Get-Content -Raw -Encoding UTF8 config\quant_data_center.yaml
```

需要确认本地环境时，运行：

```powershell
conda activate ai-trader
qdc validate-config
pytest
ruff check .
```

如果 `qdc` 命令不可用，运行：

```powershell
python -m pip install -e ".[market,dev]"
```

## 常用命令

当前默认优先使用每日采集命令。回补命令只作为历史能力保留，非用户明确要求时不要主动执行。

```powershell
conda activate ai-trader
python -m pip install -e ".[market,dev]"
qdc validate-config
qdc init
qdc db-info
qdc smoke
qdc refresh-universe --universe csi300 --snapshot-date 2026-05-11
qdc daily --date 2026-05-11 --universe csi300 --control-only
qdc daily-pipeline --date 2026-05-11 --symbols "SH600000,SZ000001" --batch-size 1 --control-only
qdc daily-pipeline --date 2026-05-11 --symbols "SH600000,SZ000001" --batch-size 1 --crawl-documents --crawl-page-size 10 --crawl-max-pages 1 --crawl-pdf-limit 1 --export-start 2026-05-11 --market-name qdc_daily_smoke
qdc --config config/quant_data_center_daily_only.yaml daily-pipeline
qdc crawl-daily --date 2026-05-11 --control-only
qdc crawl-daily --date 2026-05-11 --source-id cninfo_announcement --page-size 2 --max-pages 1 --pdf-limit 1
qdc build-factors --factor-set all --start 2026-05-01 --end 2026-05-03
qdc sync-parquet --layer all
qdc quality --dataset daily_bar
qdc export-qlib --start 2026-05-01 --end 2026-05-03 --provider-uri data/quant_data_center/qlib/cn_data
qdc console --host 127.0.0.1 --port 8765
pytest
ruff check .
```

如果自动化环境没有激活 shell，可以使用：

```powershell
conda run -n ai-trader ...
```

## 状态和历史

- 当前实施计划放在 `docs/迁移实施计划.md`。
- 当前唯一每日采集状态入口放在 `docs/每日自动采集实施计划.md`。
- 智能体工作摘要写入 `docs/工作日志/YYYY年MM月DD日.md`。
- QDC 配置放在 `config/quant_data_center.yaml`。
- 本地运行数据放在 `data/quant_data_center/`，必须保持 gitignored。
- 不要把聊天记录当成唯一事实来源。

## 数据和凭据规则

- 不要提交 `.env`、API key、cookie、密码、供应商凭据或付费数据导出文件。
- 不要把真实凭据写进 Markdown、测试 fixture 或配置样例。
- raw/bronze 对象采用追加写入；不要覆盖历史对象。
- 历史补采必须保留真实系统采集时间，不要伪装成更早看到的数据。

## 工程规则

- 优先做小而可验证的增量。
- 引入新抽象前，先复用当前 `quant_data_center` 模式。
- 采集实现放在 `src/quant_data_center/collectors/`。
- 控制面配置放在 `config/quant_data_center.yaml`。
- 存储和控制表逻辑放在 `src/quant_data_center/storage/`。
- 回补和调度逻辑放在 `src/quant_data_center/jobs/`。
- 测试应覆盖 CLI、控制表、raw/bronze 写入、silver upsert、collector normalization 和失败路径。

## Git 规则

- 修改前后都检查 `git status --short`。
- 不要回滚用户或其他 agent 的无关修改。
- 未经用户明确要求，不要自动 commit。
- 运行生成的数据不要进 git。
- 准备提交或结束较大代码改动前，运行：

```powershell
conda run -n ai-trader qdc validate-config
conda run -n ai-trader pytest
conda run -n ai-trader ruff check .
```

## 文档规则

- 用户需要直接运行的命令，要更新到 `README.md`。
- 重大架构、存储、CLI 或工作流变化，要更新 `docs/迁移实施计划.md`。
- 每次智能体工作摘要写入 `docs/工作日志/YYYY年MM月DD日.md`。
- 文档中优先写清楚命令、路径和事实。

## 默认下一步

如果用户只说“继续”，且没有给出新优先级，按 `docs/迁移实施计划.md` 的未完成项继续：

1. 优先推进每日数据采集：`daily-pipeline`、`crawl-daily`、`build-factors`、`sync-parquet`、`quality`、`export-qlib`。
2. 跑通 all_a 单日真实采集和连续交易日稳定性记录。
3. 增强每日覆盖率、质量规则、失败恢复和控制台观察能力。

除非用户明确要求，暂不继续历史回补、历史训练底座补齐或回补队列消费。

<!-- agent-dev-rules:start -->
## 通用 Agent 提交规则

适用于 Codex、Cursor、Claude Code 和其他 coding agent。

### 通用工程规范

开发时默认遵守 `docs/智能体工程规范.md`。如果项目已有更具体的规范，以项目规范优先；没有项目规范时，按以下原则执行：

```text
小步改动：一次任务只解决一个清晰问题；功能、重构、格式化、迁移和大规模删除尽量拆成独立提交。
模块解耦：新增逻辑先放在明确职责模块内；避免跨层直接调用、循环依赖、全局可变状态和硬编码环境细节。
复杂度控制：单文件超过约 500 行、单函数超过约 80 行、类承担多个职责、嵌套层级超过 3 层时，必须主动评估拆分。
测试同行：新增或修改业务逻辑必须补充或更新测试；纯重构也要有现有测试或新增覆盖来证明行为不变。
契约稳定：公共 API、配置 key、数据 schema、CLI 参数、文件格式变更必须同步调用方、测试和文档。
错误处理：不要吞异常；错误信息要能定位上下文；重试、超时、空数据和边界输入要有明确行为。
安全边界：不要提交真实凭据、.env、cookie、token、密码、付费数据、运行数据或本地机器路径。
最小惊扰：不要顺手重写无关文件，不做无关格式化，不把个人偏好混入功能提交。
```

这些是软阈值，不是机械行数竞赛。拆分的标准是职责、可读性、测试性和评审成本；如果保留长文件/长函数更合理，必须在最终说明中解释原因。

### Git 收尾流程

只要本轮任务产生代码改动、测试改动、配置改动、文档改动或功能更新，结束前必须执行收尾流程：

```text
1. 运行 git status --short。
2. 识别本轮任务相关文件，只 stage 这些文件；禁止使用 git add .。
3. 不要提交用户或其他 agent 的无关改动。
4. 按项目文档运行必要验证命令；验证失败时不要提交，先说明失败和风险。
5. 使用 Conventional Commits 写 commit message。
6. 执行 git commit。
7. 最终回复说明 commit hash、验证命令和剩余未提交文件。
```

安全例外：

```text
如果存在无法判断归属的脏改、疑似凭据、.env、API key、cookie、密码、付费数据、运行数据目录或测试失败，不要提交。
如果用户明确要求“不要提交”，只保留工作区改动并说明验证结果。
```

推荐 commit message：

```text
<type>(<scope>): <summary>

- 主要改动
- 验证命令
- 风险或跳过项
```

常用 type：

```text
feat, fix, docs, test, refactor, chore, build, ci, perf
```
<!-- agent-dev-rules:end -->
