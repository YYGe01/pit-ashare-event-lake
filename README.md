# quant_data_center

> 面向 Qlib 研究的 A 股每日事件与外部因子数据中心（非结构化数据主线）。

## 目录

- [项目简介](#项目简介)
- [核心能力](#核心能力)
- [快速开始](#快速开始)
- [每日采集工作流（推荐）](#每日采集工作流推荐)
- [常用命令](#常用命令)
- [项目结构](#项目结构)
- [验证与质量检查](#验证与质量检查)
- [文档与路线图](#文档与路线图)
- [贡献指南](#贡献指南)
- [安全与数据说明](#安全与数据说明)

## 项目简介

`quant_data_center`（QDC）聚焦 **A 股非结构化与另类数据** 的每日采集、标准化、日频因子构建与 Qlib 对齐导出。

- ✅ 本仓库主线：公告、新闻、研报、互动问答、公开舆情等数据。
- ✅ 目标产出：可供 Qlib 使用的 external factors。
- ❌ 不在本仓库范围：模型训练、组合回测、实盘下单、终端适配。
- ❌ 默认不推进：历史大规模回补（除非明确要求）。

> 说明：QDC 依赖外部 Qlib 基础行情底座（如 `cn_data`）做对齐，不负责重建完整 OHLCV 基础行情。

## 核心能力

- **每日采集**：统一执行 `crawl-daily`，支持多源采集与控制表管理。
- **因子加工**：基于文本/事件生成日频 external factors。
- **数据分层**：raw / bronze / silver / parquet 同步。
- **质量检查**：支持 dataset 级质量校验与每日健康摘要。
- **Qlib 导出**：将 external factors 对齐交易日历与 instruments。
- **控制台观察**：本地控制台查看任务状态与质量信号。

## 快速开始

### 1) 环境准备

项目统一使用已有 conda 环境 `ai-trader`：

```bash
conda activate ai-trader
python -m pip install -e ".[market,dev]"
```

如果自动化环境未激活 shell：

```bash
conda run -n ai-trader python -m pip install -e ".[market,dev]"
```

### 2) 基础检查

```bash
qdc validate-config
qdc init
qdc db-info
```

## 每日采集工作流（推荐）

推荐按阶段执行，便于定位失败点：

```bash
qdc crawl-daily --date 2026-05-13 --watch
qdc build-factors --factor-set all --start 2026-05-13 --end 2026-05-13
qdc sync-parquet --layer all
qdc quality --start 2026-05-13 --end 2026-05-13
qdc daily-health --date 2026-05-13 --format markdown
```

可选：启动控制台观察

```bash
qdc console --host 127.0.0.1 --port 8765
```

## 常用命令

### 单源 smoke / 补跑

```bash
qdc crawl-daily --date 2026-05-13 --source-id cninfo_announcement --page-size 10 --max-pages 1 --skip-pdf-download
qdc crawl-daily --date 2026-05-13 --source-id sse_announcement
qdc crawl-daily --date 2026-05-13 --source-id eastmoney_roll_news --page-size 100
qdc crawl-daily --date 2026-05-13 --source-id eastmoney_research_report --page-size 100
```

### 一键流水线（保留入口）

```bash
conda run -n ai-trader qdc daily-pipeline
```

### Qlib 基础底座轻量校验

```bash
qdc verify-qlib --provider-uri ~/.qlib/qlib_data/cn_data --start 2026-05-13 --end 2026-05-13 --instruments "SH600000,SZ000001" --fields '$close,$volume,$factor'
```

## 项目结构

```text
src/quant_data_center/                 QDC 源码
config/quant_data_center.yaml          主配置
config/quant_data_center_daily_only.yaml  历史/隔离配置
docs/迁移实施计划.md                  迁移总纲与边界
docs/每日自动采集实施计划.md          每日采集方案与状态
docs/工作日志/                         智能体工作日志
tests/                                 测试用例
data/quant_data_center/                本地运行数据（gitignored）
```

## 验证与质量检查

开发或提交前建议执行：

```bash
conda run -n ai-trader qdc validate-config
conda run -n ai-trader pytest
conda run -n ai-trader ruff check .
```

## 文档与路线图

- 总体迁移与边界：`docs/迁移实施计划.md`
- 每日采集主线：`docs/每日自动采集实施计划.md`
- 配置说明：`config/README.md`
- 日志记录：`docs/工作日志/`

## 贡献指南

欢迎通过 Issue / PR 参与改进。提交前请至少完成：

1. 保持改动小而可验证。
2. 仅提交与任务相关文件。
3. 通过基础验证命令（`validate-config` / `pytest` / `ruff check`）。
4. 不提交运行数据、凭据、密钥与本地环境文件。

## 安全与数据说明

- 禁止提交 `.env`、API key、cookie、密码、供应商凭据。
- `data/quant_data_center/` 为本地运行目录，必须保持 gitignored。
- raw/bronze 层采用追加写入，避免覆盖历史对象。

---

如果你是第一次接手本仓库，建议先从 `docs/每日自动采集实施计划.md` 的“当前状态/下一步”开始。
