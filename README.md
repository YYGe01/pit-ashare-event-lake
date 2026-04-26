# A 股 PIT 事件数据湖

这是一个面向中国 A 股日频/周频量化研究的 point-in-time 数据采集项目。

当前默认研究节奏是每日或每周生成候选股票和预测分，用于辅助调仓；分钟级行情、Level-2、tick 和逐笔委托不属于第一阶段默认采集范围。

当前仓库只聚焦“数据采集层”：数据源注册、采集运行账本、原始数据追加保存、每日清单、监控、备份和审计。事件抽取、特征工程、模型训练和回测属于后续研究层。

## 设计文档

- `docs/realtime_pit_data_collection_plan_zh.md`：PIT 数据采集实施手册，说明如何保存原始数据、时间账本、核心表、目录和首月落地任务。
- `docs/pit_data_collection_architecture_zh.md`：长期采集架构总纲，说明数据源/供应商抽象、质量门禁、治理、运维和供应商切换机制。

## 环境

```powershell
conda env create -f environment.yml
conda activate pit-ashare-event-lake
```

## 范围

- 原始采集数据只追加保存，不覆盖历史版本。
- 每条数据都记录 `first_seen_at`，即系统第一次看到它的时间。
- 保存数据源元信息、原始响应、原始文件、内容哈希和每日采集清单。
- 下游解析、事件抽取、特征、模型和回测不写入采集层。
