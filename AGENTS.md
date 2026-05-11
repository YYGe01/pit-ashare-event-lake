# AGENTS.md

这是 Codex、Cursor 和其他 coding agent 共用的项目级指令文件。

## 语言规则

- 默认用中文和用户沟通。
- 项目文档、agent 工作日志、计划说明和文档解释性内容尽量用中文。
- 代码标识符、Python 模块名、CLI 命令名、配置 key、表名、dataset 名、schema 字段名保持英文，避免破坏已有接口和数据契约。
- 引用外部 API 名称、源字段名、包名，或用户明确要求英文时，可以使用英文。

## 项目背景

- 本仓库用于建设 A 股日频 point-in-time 数据采集湖。
- V0 只做数据采集层：数据源注册表、数据契约、raw append-only 存储、metadata 账本、质量检查、manifest 和运维工具。
- 除非用户明确扩大范围，否则不要在本仓库实现事件抽取、特征工程、模型训练、回测或交易逻辑。
- V0 优先级是 A 股日频 P0 数据集：日线 OHLCV、复权因子、交易日历、交易状态、涨跌停、公告、政策监管、商品日频和全球市场日频。
- 当前第一个已跑通的真实连接器是 `akshare_market_daily_ohlcv`，使用 `akshare.stock_zh_a_daily` 实现。

## 开始工作时先做什么

如果用户说“继续之前的”或没有给出更具体上下文，先查看：

```powershell
git status --short
Get-Content -Raw -Encoding UTF8 README.md
Get-Content -Raw -Encoding UTF8 docs\v0_runtime_decisions_zh.md
Get-Content -Raw -Encoding UTF8 config\source_registry.yaml
```

需要确认本地环境时，运行：

```powershell
conda activate pit-ashare-event-lake
pitlake validate-config
pytest
ruff check .
```

如果 `pitlake` 命令不可用，运行：

```powershell
pip install -e .
```

## 常用命令

```powershell
conda env create -f environment.yml
conda activate pit-ashare-event-lake
pip install -e .
pitlake validate-config
pitlake init
pitlake smoke-run
pitlake run-enabled --start-date 20260424 --end-date 20260424 --limit-symbols 3 --manifest-date 2026-04-26
pytest
ruff check .
```

如果自动化环境没有激活 shell，可以使用：

```powershell
conda run -n pit-ashare-event-lake ...
```

Windows 上不要并行运行多个 `conda run`，它们可能争抢临时激活文件。

## 状态和历史

- 代码和配置历史放在 git。
- 项目决策放在 `docs/`，尤其是 `docs/v0_runtime_decisions_zh.md`。
- 当前和计划中的 source 放在 `config/source_registry.yaml`。
- 数据契约放在 `config/dataset_contracts/`。
- 运行事实放在 `data_lake/collection/metadata/pitlake.sqlite` 和 `data_lake/collection/published_manifests/`。
- agent 工作摘要写入 `docs/agent_journal/`。

不要把聊天记录当成唯一事实来源。

## 数据和凭据规则

- 不要提交 `.env`、API key、cookie、密码、供应商凭据或付费数据导出文件。
- 不要把真实凭据写进 Markdown、测试 fixture 或配置样例。
- 配置中只使用 `credential_ref`，例如 `TUSHARE_TOKEN`。
- `data_lake/` 是本地运行数据，必须保持 gitignored。
- raw 数据只追加不覆盖。不要覆盖历史 raw 文件，也不要修改 `first_seen_at`。
- 补采和历史导入必须保留真实系统观察时间，不能伪装成更早看到的数据。

## 工程规则

- 优先做小而可验证的增量，不做大范围重写。
- 引入新抽象前，先复用当前项目已有模式。
- 连接器实现放在 `src/pitlake/connectors/`。
- 控制面配置放在 `config/`，不要硬编码在连接器里。
- 启用新 `logical_dataset` 前，必须先有对应 dataset contract。
- 每个 enabled connector 必须写入 raw 数据、metadata 账本、quality result 和 manifest。
- 测试应覆盖 raw storage、metadata、manifest、contract 和 connector normalization。

## Git 规则

- 修改前后都检查 `git status --short`。
- 不要回滚用户或其他 agent 的无关修改。
- 未经用户明确要求，不要自动 commit。
- 运行生成的数据不要进 git。
- 准备提交或结束较大代码改动前，运行：

```powershell
pitlake validate-config
pytest
ruff check .
```

## 文档规则

- 用户需要直接运行的命令，要更新到 `README.md`。
- 重大 source、架构决策或已验证工作流变化，要更新 `docs/v0_runtime_decisions_zh.md`。
- 每次 agent 工作摘要写入 `docs/agent_journal/YYYY-MM-DD.md`。
- 文档中优先写清楚命令、路径和事实，少写泛泛解释。

## 默认下一步

如果用户只说“继续”，且没有给出新优先级，默认下一步是：

1. 实现 `trading_calendar`。
2. 实现 `price_limit` / `trade_status`。
3. 让多个 enabled P0 source 通过 `pitlake run-enabled` 一起运行。
4. 生成每日质量报告。
5. 简单市场约束数据稳定后，再接公告采集。


<!-- agent-dev-rules:start -->
## 通用 Agent 提交规则

适用于 Codex、Cursor、Claude Code 和其他 coding agent。

### 通用工程规范

开发时默认遵守 `docs/agent_engineering_standards_zh.md`。如果项目已有更具体的规范，以项目规范优先；没有项目规范时，按以下原则执行：

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
