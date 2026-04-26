# V0 运行决策记录

> 更新时间：2026-04-26  
> 目的：记录正式建立采集框架时已经确认的范围、默认路径、运行方式、备份、告警和付费源策略。后续如果迁移服务器、购买数据源或升级 P1/P2，应更新本文。

## 1. 已确认范围

当前 V0 只做 A 股日频采集框架，不做分钟级、Level-2、tick、逐笔委托或盘口数据。

第一阶段只做 P0 数据集：

```text
日线/复权
交易日历
停复牌/涨跌停
公告
政策监管
商品日频
全球市场日频
```

P0 稳定运行后，再升级 P1 和 P2。

## 2. 账号和付费源策略

当前没有 Tushare、券商 API、Wind、Choice、iFinD 等账号或 Token。

V0 策略：

```text
优先免费/公开数据源；
不启用需要账号的 provider；
不绕过登录、验证码、付费墙或反爬机制；
为 Tushare/Wind/Choice 等预留 provider 和 credential_ref；
等 P0 免费源跑稳后，再决定是否开通付费源用于对账、补全或稳定性提升。
```

真实密钥不写入 git、Markdown 或聊天记录。后续如开通账号，只在配置里使用 `credential_ref`，例如 `TUSHARE_TOKEN`。

## 3. 本地电脑还是服务器

当前建议分两阶段：

### 阶段一：本地电脑开发和验证

现在先用本地电脑即可，不需要立刻组服务器。原因：

```text
当前只做日频 P0，不需要 7x24 高频采集；
框架、注册表、数据契约、manifest、质量检查需要先验证；
前期预算有限，先把免费源跑通更重要；
本地调试连接器和排查字段变化更方便。
```

本地电脑运行条件：

```text
采集窗口内不要关机；
关闭自动休眠，至少保证 16:30、20:00、23:00、次日 08:30 这些窗口可运行；
网络要能访问目标数据源；
Windows Task Scheduler 或 APScheduler 后续负责定时运行；
C 盘空间要定期检查。
```

### 阶段二：P0 稳定后迁移到长期运行环境

当 P0 连续 7-30 天稳定后，建议迁移到更可靠的长期运行环境：

```text
优先方案：低功耗小主机 / NAS / 家用服务器 + 外接硬盘备份；
可选方案：低成本云服务器；
不建议长期只依赖经常关机或休眠的个人电脑。
```

选择建议：

| 方案 | 适合场景 | 风险 |
| --- | --- | --- |
| 本地电脑 | 开发、调试、前 2-4 周验证 | 休眠、关机、断网导致采集断流 |
| 小主机/NAS | 长期低成本运行、可接移动硬盘 | 需要自己维护电源、网络和备份 |
| 云服务器 | 7x24 稳定、远程方便 | 月费、磁盘费、部分国内源访问质量需要验证 |

当前决策：先本地运行，框架预留迁移能力；P0 连续稳定后再迁移。

## 4. 数据湖和备份路径

当前只有 C 盘，因此 V0 默认：

```text
data_lake 根目录：仓库内 data_lake/
metadata 数据库：data_lake/collection/metadata/pitlake.sqlite
raw 文件：data_lake/collection/raw_immutable/
manifest：data_lake/collection/published_manifests/
本地备份：data_lake/backups/local/
日志：data_lake/collection/logs/
```

`data_lake/` 已被 `.gitignore` 忽略，不进入 git。

备份策略：

```text
V0 当前：先做本地 metadata/manifest 备份；
P0 连接器开始真实运行后：metadata/manifest 每日备份，raw 每周备份；
P0 连续稳定后：增加外部硬盘、NAS、云盘或对象存储备份；
长期目标：至少一份不在 C 盘上的备份。
```

## 5. 告警策略

当前没有指定邮件、飞书、企业微信或 Telegram，因此 V0 默认：

```text
先写本地 JSONL 日志；
生成本地每日采集报告；
P0 真实采集开始后，source 连续失败、manifest 未生成、raw 为空、hash 缺失等情况进入报告；
等你提供通知通道后，再接邮件或 webhook。
```

本地告警不能替代长期运维告警。真实 P0 跑起来后，建议至少接一个外部通知通道。

## 6. 当前已落地的框架命令

在项目根目录运行：

```powershell
conda env create -f environment.yml
conda activate pit-ashare-event-lake
pip install -e .
pitlake validate-config
pitlake init
pitlake smoke-run
```

如果还没有安装包，也可以临时使用：

```powershell
$env:PYTHONPATH="src"
python -m pitlake.cli validate-config
python -m pitlake.cli init
python -m pitlake.cli smoke-run
```

`smoke-run` 不访问外网，只验证本地 raw 写入、metadata 账本、质量检查和 manifest 生成。

## 7. 下一步实现顺序

优先级如下：

```text
1. 完成连接器运行模板和任务调度封装；
2. 实现第一个真实 P0 连接器：A 股日线/复权或公告；
3. 为第一个连接器跑通 raw 保存、raw_item_version、quality gate、manifest；
4. 增加交易日历、停复牌/涨跌停；
5. 增加公告和政策监管；
6. 增加商品和全球市场日频；
7. 连续运行 7 天后，再考虑 shadow source、外部备份和外部告警。
```

## 8. 2026-04-26 首个真实采集闭环

已落地首个真实 P0 source：

```text
source_id: akshare_market_daily_ohlcv
logical_dataset: market_daily_ohlcv
provider_id: akshare
connector: pitlake.connectors.market.akshare_daily.AkshareMarketDailyConnector
akshare function: stock_zh_a_daily
default sample symbols: 000001, 600000, 300750
```

本地验证命令：

```powershell
pip install -e .
pitlake run-enabled --start-date 20260424 --end-date 20260424 --limit-symbols 3 --manifest-date 2026-04-26
```

验证结果：

```text
status: success
source_count: 1
request_count: 3
success_count: 3
error_count: 0
manifest generated: yes
```

说明：

```text
AkShare 的 stock_zh_a_hist / Eastmoney 历史接口在当前本地网络下会出现远端断开或代理连接失败；
因此 V0 首个连接器改用当前可访问的 stock_zh_a_daily；
后续应把 Eastmoney hist 接口作为单独 shadow source，不直接覆盖当前已跑通的 bootstrap source。
```

当前这一步已经跑通：

```text
config source -> connector runner -> AkShare request -> raw append-only store -> SQLite metadata -> quality checks -> raw_item_version -> daily manifest
```
