# quant_data_center

本仓库当前主线是个人量化统一数据中心 `quant_data_center`。

目标是服务 Qlib 研究：采集 A 股数据、支持历史回补和每日增量、加工稳定日频因子，并导出 Qlib 可读数据。本仓库不做模型训练、组合回测、实盘下单或交易终端适配。

当前实施计划见 `docs/quant_data_center_migration_plan_zh.md`。

## 当前入口

```powershell
conda env create -f environment.yml
conda activate quant-data-center
pip install -e .
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
qdc plan-backfill --dataset trade_calendar --source-id akshare --start 2026-05-01 --end 2026-05-03
qdc run-backfill --dataset trade_calendar --limit-tasks 1
qdc daily --date 2026-05-11 --universe csi300 --control-only
qdc build-factors --factor-set all --start 2026-05-01 --end 2026-05-03
qdc sync-parquet --layer all
qdc quality --dataset daily_bar --start 2026-05-01 --end 2026-05-03
qdc export-qlib --start 2026-05-01 --end 2026-05-03 --provider-uri data/quant_data_center/qlib/cn_data --market-name qdc_smoke
qdc verify-qlib --start 2026-05-01 --end 2026-05-03 --instruments SH600000,SZ000001 --provider-uri data/quant_data_center/qlib/cn_data
qrun config/qlib/workflow_config_lightgbm_alpha158_qdc_smoke.yaml
```

`run-backfill --control-only` 只验证任务状态流和水位表，不采集真实数据。

当前真实 AkShare 回补已支持：

- `stock_basic`
- `trade_calendar`
- `daily_bar`
- `adj_factor`
- `price_limit`
- `trade_status`
- `announcement`
- `news`

`daily_bar`、`adj_factor`、`price_limit`、`news` 可用 `--universe` 展开 symbol，也可以显式传入 `--symbols` 覆盖。`qdc refresh-universe` 可把 AkShare 指数成分快照写入 `qdc_silver.universe_constituent`，回补规划会优先使用最新快照；如果没有快照，再回退到配置里的静态样例。

当前回补链路会写入 raw JSON、bronze Parquet、`qdc_silver` DuckDB 表，并在 `qdc_meta.source_object` 登记文件索引。`qdc run-backfill --retry-failed` 可显式重试失败任务。`qdc build-factors` 可生成新闻/公告日频 count 因子，`qdc sync-parquet` 可同步 silver/gold Parquet，`qdc quality` 可做基础质量检查，`qdc export-qlib` 可导出 Qlib day provider 目录，`qdc verify-qlib` 可用本地 Qlib 直接读取导出的 provider 做 data-layer smoke。

`qdc` 默认读取仓库内 `config/quant_data_center.yaml`；需要从其他目录运行或切换配置时，可设置 `QDC_CONFIG` 或传入 `--config`。

## 项目结构

```text
config/quant_data_center.yaml       QDC 运行配置
src/quant_data_center/              QDC 源码
tests/test_qdc_storage.py           当前 QDC 聚焦测试
docs/quant_data_center_migration_plan_zh.md  迁移实施计划和当前状态
docs/agent_journal/                 agent 工作记录
data/quant_data_center/             本地运行数据，已 gitignored
```

当前主线只保留 QDC 代码、配置、测试和文档。

## Qlib 联调

如需要在当前环境直接验证 Qlib provider，可安装本地 Qlib 源码和依赖。网络慢时优先使用国内 PyPI 源：

```bash
conda run -n ai-trader python -m pip install -e /root/code/qlib -i https://pypi.tuna.tsinghua.edu.cn/simple --extra-index-url https://mirrors.aliyun.com/pypi/simple
```

当前已用本地 `/root/code/qlib` 验证 `D.calendar`、`D.list_instruments` 和 `D.features` 能读取 `qdc export-qlib` 产物。导出字段包含 Alpha158 默认需要的 `$vwap`，`--market-name` 可写出 Qlib 命名 market 文件。`config/qlib/workflow_config_lightgbm_alpha158_qdc_smoke.yaml` 可用当前 provider 跑通 LightGBM Alpha158 qrun smoke；正式评估仍需要完整 universe 和稳定 train/valid/test 切分。

## 验证

```powershell
qdc validate-config
qdc daily --date 2026-05-11 --universe csi300 --control-only
qdc sync-parquet --layer all
qdc quality --dataset daily_bar
qdc verify-qlib --start 2024-01-02 --end 2024-01-02 --instruments SH600000,SZ000001 --provider-uri data/quant_data_center/qlib/cn_data
pytest
ruff check .
```

自动化环境未激活 shell 时：

```powershell
conda run -n quant-data-center qdc validate-config
conda run -n quant-data-center pytest
conda run -n quant-data-center ruff check .
```
