# AkShare 数据源风险评估

> 评估日期：2026-04-26  
> 适用范围：本仓库的 A 股日频 point-in-time 采集湖。  
> 结论摘要：AkShare 适合作为低成本 bootstrap 和历史补采来源，但不能单独视为严格 PIT 数据源。历史数据可以下载，也很有价值；未来数据风险必须由本项目的 `first_seen_at`、raw append-only、backfill 标记、披露时间对账和 shadow/official source 共同控制。

## 1. 总体结论

AkShare 的优势很明确：

- 覆盖广，能快速覆盖 A 股日线、复权行情、公告索引、财务指标、资金流、行业/概念、宏观、期货、海外市场、新闻和部分另类代理指标。
- 使用成本低，适合本项目早期建立 source registry、dataset contract、raw 存储、metadata 账本、quality report、manifest 和对账流程。
- 很多接口支持历史区间参数，例如 A 股历史行情 `stock_zh_a_hist` 支持 `start_date`、`end_date`、`adjust`，适合做历史补采和低成本样本构建。

但它也有硬边界：

- AkShare 是公开网页/公开接口聚合库，不是交易所、上市公司披露系统或付费金融数据商的 PIT 版本库。
- 大量历史接口返回的是“当前时点看到的历史数据”，不是“历史当时可见的数据版本”。
- 接口依赖上游网页结构和接口稳定性；AkShare 官方也提示网页改版可能导致接口异常，需要及时升级。
- 对严肃回测而言，不能把 AkShare 历史数据直接等同于历史当日可用数据。

因此，本项目应把 AkShare 定位为：

```text
bootstrap 主力源；
低成本历史 backfill 源；
部分数据集的 shadow/fallback 源；
进入生产级研究前必须被官方源、交易所源或付费源对账的来源。
```

## 2. 官方资料依据

本评估主要基于以下资料：

- AkShare 项目说明：`https://akshare.akfamily.xyz/introduction.html`
- AkShare 特别说明：`https://akshare.akfamily.xyz/special.html`
- AkShare 数据说明：`https://akshare.akfamily.xyz/data_tips.html`
- AkShare 股票数据文档：`https://akshare.akfamily.xyz/data/stock/stock.html`
- AkShare 期货数据文档：`https://akshare.akfamily.xyz/data/futures/futures.html`
- AkShare GitHub 仓库：`https://github.com/akfamily/akshare`

核心判断来自两类事实：

- 官方接口文档显示 AkShare 能下载多类历史数据，尤其是 `stock_zh_a_hist` 这类区间历史行情接口。
- 官方说明和接口示例也提示数据质量、上游变动、复权数据异常、网页接口不稳定等风险，这些风险会直接影响 PIT 研究。

## 3. 历史数据是否可以下载

可以。对本项目最关键的历史下载能力如下。

| 数据类别 | AkShare 历史能力 | 本项目可用性 |
| --- | --- | --- |
| A 股日线 OHLCV | `stock_zh_a_hist` 支持历史区间和复权参数；旧接口 `stock_zh_a_daily` 也可取日线 | 可用于历史 backfill；建议新增 `stock_zh_a_hist` connector，并保留现有 `stock_zh_a_daily` 作为兼容或 shadow |
| A 股复权行情 | `stock_zh_a_hist` 支持不复权、前复权、后复权 | 只能作为行情口径或辅助推导；不能直接把当前前复权历史序列当作过去可见序列 |
| 公告索引 | `stock_notice_report` 可查公告列表和链接 | 可用于公告索引 bootstrap；严格 PIT 仍需 CNINFO/SSE/SZSE/BSE 官方源对账 |
| 财务指标 | `stock_financial_analysis_indicator` 可取历史财务指标 | 可做财务指标 backfill；真实可用时间必须用公告披露时间或官方 filings 校验 |
| 交易日历 | `tool_trade_date_hist_sina` 返回交易日 | 可用于运行日历；非交易日完整性和临时休市应后续补官方交易所源 |
| 资金流/龙虎榜/融资融券/北向 | 多个东财、交易所或公开接口包装 | 可做 bootstrap；需要交易所、港交所或付费源对账 |
| 行业/概念成分 | 东财板块成分接口 | 只能保存采集时快照；不能反推历史成分有效期 |
| 基金持仓 | 公募持仓公开快照接口 | 可保存公开快照；披露时间、修订和完整持仓仍需官方/基金公告对账 |
| 期货日频 | 新浪期货或交易所相关接口 | 可 bootstrap；国内商品期货最终应优先接交易所结算数据 |
| 美股/全球市场 | 新浪、Yahoo、Stooq 等相关来源 | 可作全球风险指标样例；需确认许可、时区和调整口径 |
| 分钟线 | 有部分分钟接口，但通常不是完整深度历史 | 只适合 P2 样例，不应作为完整高频历史库 |
| 新闻/研报/热度 | 多数是当前列表、索引或聚合指标 | 只能保存观测时元数据；不下载或保存未授权全文 |

## 4. 是否全面和足够

对本项目当前阶段，答案分层看。

### 4.1 对 bootstrap 足够

AkShare 足够支撑本项目当前采集框架闭环：

```text
source registry
dataset contract
raw append-only
metadata ledger
quality report
manifest
reconciliation scaffold
backup/alert entry
```

目前仓库中大量 enabled source 已经是 AkShare bootstrap 源。它能让我们用低成本快速验证“长期自动采集任务”是否稳定。

### 4.2 对历史 backfill 基本够用

如果目标是补齐模型训练样本、做探索性研究、做非严格 PIT 的历史重建，AkShare 的历史数据很有价值。

但历史 backfill 必须明确标记：

```text
ingestion_mode = historical_backfill
first_seen_at = 本系统实际下载或导入时间
source_observation_date = provider 返回记录对应的交易日、报告期或公告日
market_available_at = 空或后续由官方披露源推导
```

不能把 2026-04-26 下载到的 2018 年财务指标伪装成 2018 年当时已经被本系统看到的数据。

### 4.3 对生产级严格 PIT 不够

严格 PIT 要回答的问题不是“这条数据代表哪一天”，而是：

```text
在历史上的某个决策时间点，研究系统是否已经有资格知道这条数据？
当时看到的版本是否就是今天下载到的这个版本？
如果后来修订、补发、重分类、复权调整，旧版本是否还能复原？
```

AkShare 多数历史接口不能单独回答这些问题，所以不能作为唯一生产级 PIT 依据。

### 4.4 为什么“今天下载的历史数据”不等于“历史当天看到的数据”

这里的风险不是说 AkShare 会故意给错数据，而是说：大多数公开历史接口默认返回的是“上游平台当前维护的最新版历史表”。这个表很适合查资料，但它通常不会保存“每个历史日期当时网页上长什么样、当时字段如何计算、当时哪些记录尚未披露、后来哪些记录被更正或删除”的完整版本历史。

直观理解：

```text
你在 2026-04-26 下载 2019-06-30 的数据。
这条记录的 observation_date 可能是 2019-06-30。
但这不代表它在 2019-06-30 当天已经可见，也不代表当时看到的值和 2026-04-26 下载的值完全一致。
```

本项目关心的是第二个问题：

```text
2019-07-01 早上做交易决策时，这条数据是否已经可见？
如果可见，当时的版本和值是否就是今天下载的这个版本？
```

#### 4.4.1 风险概率怎么理解

下面的概率是工程判断，不是精确统计。单条记录、单天、单字段的差异概率可能不高；但一旦扩展到全市场、十几年历史、多个数据集，差异几乎一定会出现。

| 风险等级 | 含义 | 典型判断 |
| --- | --- | --- |
| 极高 | 只要做长历史或全市场，基本必然遇到；某些场景单条记录也很容易错 | 前复权历史价格、当前概念成分回填历史、当前股票池回测历史 |
| 高 | 长历史中很常见，会显著影响回测或样本标签 | 财务指标修订、披露时间错配、基金持仓滞后、ST/退市状态回填 |
| 中 | 不一定每天发生，但遇到特殊事件、网页改版、口径调整时会发生 | 公告列表变更、宏观指标修订、资金流口径变化、龙虎榜披露时间 |
| 低 | 常规情况下差异较小，但不能假设不存在 | 已收盘后的不复权日线 OHLCV、普通交易日历 |

更重要的是：概率要按“研究问题”看。如果只是训练一个粗粒度模型，部分 backfill 偏差可以接受；如果声称做严格 PIT 回测，任何会提前暴露未来信息的字段都要控制。

#### 4.4.2 例子一：前复权价格会被未来公司行为改写

场景：

```text
某股票在 2020-01-02 收盘价为 10 元。
2021-06-01 公司发生分红送转。
2026-04-26 用 AkShare 下载 2020-01-02 的前复权价格。
```

今天下载到的 2020-01-02 前复权价，可能已经把 2021 年、2022 年、2023 年之后的分红送转全部折算进去了。这个价格在 2020-01-02 当天不可能被市场知道。

差异概率：

- 对不复权 OHLCV：低到中，主要风险是上游纠错或字段口径。
- 对前复权/后复权历史序列：极高。只要观察日之后发生过分红、送转、拆股、配股或除权除息，历史复权价就会变化。

容易发生的情况：

- 持有周期跨越分红送转。
- 使用 `adjust="qfq"` 或 `adjust="hfq"` 的整段历史行情。
- 用当前 qfq close 直接推导历史复权因子。

正确处理：

- 原始行情主表保存不复权 OHLCV。
- 复权因子单独保存，并按 `first_seen_at` 或公司行为披露时间版本化。
- 研究层按 `as_of_time` 生成当时可用的复权视图。

#### 4.4.3 例子二：财务报告期不是市场可用时间

场景：

```text
某公司 2023 年年报报告期是 2023-12-31。
公司实际在 2024-04-25 晚上披露年报。
你在 2026-04-26 下载历史财务指标，记录里 report_date = 2023-12-31。
```

如果回测系统把这条财务指标当作 2023-12-31 已知，就提前知道了 2024-04-25 才披露的信息。

差异概率：

- 披露时间错配：极高。财务数据几乎一定有报告期和披露期差异。
- 财务数值修订：中到高。发生业绩快报修正、会计差错更正、审计调整、重述、监管问询后修订时，今天下载的历史指标可能不同于首次披露版本。

容易发生的情况：

- 年报、半年报、季报。
- 业绩预告、业绩快报、正式财报之间数值不一致。
- 上市公司后续发布更正公告。
- 指标由上游平台二次计算，例如 ROE、毛利率、同比增速。

正确处理：

- `report_date` 只表示覆盖期间，不表示可用时间。
- 必须补 `published_at` 和 `market_available_at`。
- 没有披露时间前，只能作为 `historical_backfill` 或 approximate PIT。

#### 4.4.4 例子三：今天的股票列表不是历史股票池

场景：

```text
你在 2026-04-26 获取当前 A 股股票列表。
然后对这些股票下载 2015-2025 的历史行情。
```

这个样本会天然排除很多已经退市、长期停牌、被吸收合并、证券代码变更或上市失败的公司。回测只在“今天仍然存在的股票”上做，历史表现会被系统性美化。

差异概率：

- 用当前股票池回填十年以上历史：极高。
- 只看最近几个月的主板活跃股票：中。

容易发生的情况：

- 长周期回测。
- 因子选股只用当前可交易股票列表。
- 没有保存历史上市、暂停上市、退市、摘牌、代码变更、证券简称变更。

正确处理：

- 建立 as-of universe。
- 保存上市日期、退市日期、交易状态、证券状态和代码变更。
- 回测某一天只能使用那一天当时存在且可交易的股票池。

#### 4.4.5 例子四：当前概念成分会把未来叙事带回过去

场景：

```text
2026-04-26 下载“机器人概念”成分股。
其中某公司是在 2024 年因为业务转型才被市场归入机器人概念。
你把这个成分股列表用于 2020 年回测。
```

这样会让模型在 2020 年就知道 2024 年之后才形成的市场叙事。

差异概率：

- 概念板块：极高。概念定义经常随市场主题、平台维护和热点变化调整。
- 行业分类：中到高。标准行业相对稳定，但公司主营、分类标准和平台映射仍会变化。

容易发生的情况：

- AI、机器人、算力、低空经济、固态电池、中特估等主题概念。
- 使用当前行业/概念成分做历史分组收益。
- 把当前成分列表当作历史固定标签。

正确处理：

- 行业/概念成分只保存采集日 snapshot。
- 不从当前 snapshot 反推历史有效区间。
- 若要历史成分，必须有历史版本源或每日持续采集积累。

#### 4.4.6 例子五：ST、退市、涨跌停规则会随状态变化

场景：

```text
某股票 2022 年是普通股票，涨跌幅限制 10%。
2024 年被 ST，涨跌幅限制变为 5%。
你在 2026 年用当前 ST 状态去推算 2022 年涨跌停。
```

这会把 2024 年才知道的风险状态带回 2022 年。

差异概率：

- 单只长期股票：中。
- 全市场多年、包含 ST/退市整理/北交所/科创板/创业板规则切换：高。

容易发生的情况：

- 使用当前证券简称是否带 `ST` 来推断历史涨跌幅。
- 忽略创业板、科创板注册制改革前后的涨跌幅规则差异。
- 忽略新股上市初期、恢复上市、退市整理、停牌复牌等特殊规则。

正确处理：

- 涨跌停推算必须使用当日 as-of 证券状态。
- 特殊规则需要单独数据源对账。
- 当前 `price_limit` connector 只能作为 inferred bootstrap。

#### 4.4.7 例子六：公告日期不等于交易可用时间

场景：

```text
某公司公告日期显示为 2022-08-30。
公告实际在 2022-08-30 盘后发布。
你在回测中让模型在 2022-08-30 开盘前使用公告内容。
```

这就是未来函数。公告日期只说明公告归属日期或页面显示日期，不一定说明盘前、盘中还是盘后可用。

差异概率：

- 对日频收盘后调仓：中，需要判断公告发布时间是否早于调仓时间。
- 对盘前或盘中策略：高，必须精确到发布时间和可交易时间。

容易发生的情况：

- 晚间公告、临时停牌公告、重大事项公告。
- 财报披露集中期。
- 历史补采只拿到 `announcement_date`，没有精确 `published_at`。

正确处理：

- 保存公告列表中的日期，同时补官方详情页发布时间。
- 研究层用 `market_available_at`，例如盘后公告最早下一交易日可用。
- 没有发布时间时，不用于严格 PIT 事件回测。

#### 4.4.8 例子七：公告、新闻、研报链接和列表会变化

场景：

```text
2021 年某公告或研报列表中有一条链接。
2026 年再抓历史列表时，链接可能换域名、失效、被归档、被撤回或标题被修订。
```

今天下载的历史列表未必等于当时页面上展示的列表。公开网页常见的分页、搜索、排序和归档规则都会影响历史抓取结果。

差异概率：

- 列表 metadata：中。
- 全文、附件、PDF 链接：中到高。
- 被更正、撤回、补充的公告：低频但影响大。

容易发生的情况：

- 公告补充、更正、撤回。
- 新闻站点改版。
- 研报链接跳转、PDF 迁移、权限变化。
- 只抓搜索结果第一页或有限条数。

正确处理：

- raw 保存每次看到的列表响应。
- 重要公告源接官方详情页和附件 hash。
- 没有授权不保存研报或新闻全文。

#### 4.4.9 例子八：宏观指标和统计数据会修订

场景：

```text
某宏观指标 2020 年首次公布一个初值。
随后统计部门修订、换基期、调整季调算法或补充历史数据。
2026 年下载到的是修订后序列。
```

如果回测把修订后序列用于 2020 年决策，就可能提前知道后来的统计修订。

差异概率：

- 对官方宏观统计：中到高，取决于指标。
- 对不会频繁修订的简单公布值：中。

容易发生的情况：

- GDP、工业增加值、社融、货币、PMI 派生指标。
- 季调指标。
- 同比/环比由上游平台二次计算。
- 历史基准调整或统计口径变化。

正确处理：

- 保存每次采集的完整历史序列 raw。
- 不覆盖旧版本。
- 重要宏观指标需要 release calendar 和 vintage 数据源。

#### 4.4.10 例子九：基金持仓报告期和披露期差异很大

场景：

```text
基金 2023Q4 持仓报告期是 2023-12-31。
季报可能在 2024-01 下旬披露。
你在 2026 年下载到 2023-12-31 持仓，并在 2024-01-02 回测中使用。
```

这会提前知道基金在季度末的公开披露信息。

差异概率：

- 披露滞后：极高。
- 完整持仓和重仓股口径差异：高。

容易发生的情况：

- 用报告期末日期当作可用日期。
- 用重仓股公开数据推断完整持仓。
- 忽略年报/半年报/季报披露内容范围差异。

正确处理：

- `report_date` 和 `published_at` 分开。
- 只能在披露后使用。
- 不推断未披露完整持仓。

#### 4.4.11 例子十：资金流、热度、评论指标可能是当前算法重算的历史

场景：

```text
某平台 2024 年修改资金流算法或热度排名算法。
2026 年下载 2022 年历史资金流/热度。
```

如果上游没有保存旧算法版本，今天下载到的历史值可能是按新算法重算或新口径展示的结果。

差异概率：

- 平台算法型指标：中到高。
- 透明、交易所原始披露字段：低到中。

容易发生的情况：

- 主力资金流、超大单/大单分类。
- 人气榜、热度榜、评论聚合。
- 情绪、关注度、舆情类 proxy。

正确处理：

- 只解释为 provider 指标。
- 保存 `metric_payload` 和采集时间。
- 不把它们当作稳定金融事实字段。

#### 4.4.12 例子十一：交易日历一般低风险，但临时事件仍可能改变解释

场景：

```text
交易所历史交易日通常稳定。
但遇到临时休市、极端天气、重大事件或交易所规则调整时，普通交易日历接口可能只返回交易日列表，不返回为什么不开市。
```

差异概率：

- 普通交易日是否开市：低。
- 特殊休市原因、半日市、跨市场差异：中。

容易发生的情况：

- 港股、美股、A 股多市场混用。
- 国内外节假日和临时休市。
- 期货夜盘、商品交易所特殊安排。

正确处理：

- A 股日频可以先用 AkShare 日历。
- 后续补交易所官方日历、休市原因和市场维度。

#### 4.4.13 例子十二：不复权日线也不是零风险

场景：

```text
某股票 2020 年某日成交量或成交额上游记录有误。
平台在 2021 年修正历史数据。
你在 2026 年下载的是修正后版本。
```

这类修正不一定频繁，但全市场多年数据里一定要假设会遇到。

差异概率：

- 单只股票、普通交易日、OHLC 价格：低。
- 全市场长历史、成交额/换手率/流通股相关字段：中。

容易发生的情况：

- 停复牌、除权除息日前后。
- 成交额单位、成交量单位、换手率口径。
- 交易所或上游供应商后续纠错。

正确处理：

- raw append-only，不覆盖旧采集版本。
- 多源对账。
- 对关键字段做异常检测。

#### 4.4.14 直观风险排序

如果只问“今天下载历史数据，和历史当时可见版本不同的可能性”，可以按下面排序：

```text
几乎必然要严控：
前复权/后复权价格、财务披露数据、基金持仓、当前股票池、当前概念成分。

很容易在长历史中发生：
ST/退市状态、涨跌停规则、公告可用时间、宏观修订、行业分类变更。

中等风险，需要对账：
资金流、龙虎榜、北向资金、新闻列表、研报索引、商品期货结算口径。

相对低风险但不能忽略：
不复权日线 OHLCV、普通交易日历。
```

所以，最容易误用的不是“日线 close 这个数字”，而是围绕它做历史研究时使用的配套信息：股票池、复权、财务、公告、行业概念、证券状态和披露时间。

## 5. 未来数据风险总表

| 风险类别 | 典型数据 | 风险说明 | 本项目控制方式 |
| --- | --- | --- | --- |
| 前复权未来函数 | `qfq` 日线、用 qfq close 推导的复权因子 | 未来分红送转会改写过去价格；今天看到的历史前复权价包含后来发生的公司行为 | 原始行情优先保存不复权；复权因子作为独立数据集；研究层按 `as_of_time` 重建 |
| 财务披露时间错配 | 财务指标、利润表、资产负债表、现金流 | 报告期日期不等于市场可用时间；后来可能修订 | 必须记录公告披露时间；没有披露时间时只能标记为 backfill 或 approximate PIT |
| 历史版本不可复原 | 财务指标、宏观指标、基金持仓、板块成分 | 上游可能只返回当前修订后的历史序列 | raw append-only；重复采集保留版本；重要数据补官方源 |
| 幸存者偏差 | 当前股票列表、当前板块成分 | 用当前股票池回测历史会排除退市、改名、暂停上市标的 | 构建 as-of universe；接入上市/退市/证券状态源 |
| 板块概念回填 | 行业/概念成分 | 当前成分代表当前口径，不能代表历史某日口径 | 只作为采集时 snapshot；不推断历史有效区间 |
| 公告可用时间不准 | 公告索引、公告日期 | 公告日期不一定等于交易决策可用时间；盘后公告不能用于当天盘中或收盘前决策 | 引入 `published_at` 和 `market_available_at`；官方公告源对账 |
| 新闻时间不准 | 财经新闻、宏观新闻、研报索引 | 标题时间、抓取时间、实际发布时间、市场可用时间可能不同 | 保存 provider 时间和 `first_seen_at`；不做未经校验的事件映射 |
| 接口字段漂移 | 所有网页解析接口 | 上游网页改版会导致字段变化、空数据或解析错误 | quality report 检测字段/行数；connector 保留 raw payload 和 AkShare 函数名 |
| 数据口径不透明 | 资金流、热度、评论聚合 | 指标计算方式可能由上游平台决定，不完全公开 | 存入 `metric_payload`；只解释为 provider 指标，不擅自赋予金融含义 |
| 许可和存储权 | 新闻全文、研报 PDF、评论正文 | 公开可见不等于可批量保存或再分发 | 默认 metadata only；没有明确授权不保存全文 raw |
| 高频完整性不足 | 分钟线、tick、Level-2 | AkShare 部分分钟接口只适合近期或样例，不能替代 replay-grade 行情源 | P2 只保存样本；完整高频需单独付费源和高容量设计 |

## 6. 关键数据集风险细化

### 6.1 `market_daily_ohlcv`

当前项目已使用 `akshare.stock_zh_a_daily` 跑通 A 股日线 bootstrap。官方文档中更适合历史区间下载的是 `stock_zh_a_hist`，它支持起止日期和复权参数。

建议：

- 新增 `akshare_market_daily_ohlcv_hist` 或类似 source，使用 `stock_zh_a_hist` 做历史 backfill。
- 保留当前 `stock_zh_a_daily` connector，用于兼容和对账。
- 研究层默认使用不复权原始 OHLCV。
- `adjust=""` 的原始价格和成交量优先进入主行情数据集。
- `qfq`、`hfq` 数据只能作为复权口径或辅助，不直接混入主行情原始价格。

PIT 风险：

- 若历史回测直接使用今天下载的 qfq 序列，会把未来公司行为影响带回过去。
- 若股票池来自当前列表，会出现幸存者偏差。

### 6.2 `adjustment_factor`

当前项目的 V0 复权因子是用不复权 close 和前复权 close 比值推导。这个口径适合验证流程，但不是生产级公司行为因子。

建议：

- 把当前推导因子继续标记为 bootstrap/inferred。
- 后续接入官方公司行为、分红送转、除权除息或付费因子源。
- 复权因子必须按 observation time 版本化。

PIT 风险：

- 当前前复权历史价格会随未来除权除息变化。
- 单次历史 backfill 得到的是“今天这个时点的整段因子序列”，不是过去每一天当时能看到的因子序列。

### 6.3 `price_limit`

当前项目用前收盘价和板块规则推算涨跌停。这个口径适合 bootstrap，但特殊规则会带来偏差。

风险点：

- 新股、ST、退市整理、北交所、临停、恢复上市、特别处理切换等场景规则复杂。
- 用当前证券属性回填历史会泄漏未来状态。

建议：

- 保留当前 connector 为 inferred bootstrap。
- 后续接交易所、行情商或官方涨跌停字段对账。
- 把证券状态、板块、ST 状态、上市天数作为单独 as-of 输入，不在 connector 内硬编码长期假设。

### 6.4 `announcement_index`

AkShare 的公告接口适合保存公告索引和链接，但生产级公告数据应以 CNINFO、SSE、SZSE、BSE 等官方源为准。

PIT 风险：

- 公告日期不等于可交易使用时间。
- 历史补采公告只能证明“今天抓到了历史公告”，不能证明本系统在历史公告日已经看到。
- 公告可能有补充、更正、撤回或附件变化。

建议：

- 历史公告补采时保留 `announcement_date`、provider 时间字段、抓取时间和链接。
- 后续新增 `published_at`、`market_available_at`，按交易日收盘后、盘中、非交易日分别处理。
- 官方源对账前，不把公告作为严格事件 alpha 的唯一依据。

### 6.5 `financial_indicator`

财务指标是高风险区。报告期字段只说明财务覆盖期间，不说明市场何时知道。

PIT 风险：

- 年报、季报、业绩预告、业绩快报、修订公告的披露时间不同。
- 今天下载的历史财务指标可能包含后来修订。
- 用报告期末日期作为特征可用日期会严重提前。

建议：

- 财务指标继续保存 provider 原始列到 `metric_payload`。
- 用公告索引和官方 filings 补 `published_at`。
- 研究层只在 `market_available_at <= cutoff_time` 后使用。
- 没有披露时间的数据标记为 `approximate_pit=false` 或 `historical_backfill_only`。

### 6.6 `fund_holding`

基金持仓通常有报告期、披露期和真实可用时间的差异。

PIT 风险：

- 报告期末持仓不是当日可见信息。
- 季报、半年报、年报披露滞后。
- 部分接口可能只提供公开重仓或快照，不代表完整持仓历史。

建议：

- 保存为公开披露快照，不推断真实持仓连续时间。
- 后续用基金公告或官方披露源补 disclosure time。
- 研究层不能把报告期末持仓当作报告期末当天已知。

### 6.7 `industry_membership` 和 `concept_membership`

行业和概念成分是典型的快照数据。当前成分不能用于回填历史成分。

PIT 风险：

- 当前概念定义可能包含未来才加入的股票。
- 历史回测用当前概念成分，会把未来市场叙事带回过去。

建议：

- 只按 `snapshot_date` 保存采集时看到的成分。
- 不自动生成 `effective_from` / `effective_to`，除非有官方历史版本依据。
- 回测只能使用当时已采到的快照。

### 6.8 `capital_flow`

资金流、龙虎榜、融资融券、北向资金适合做 bootstrap，但口径需要对账。

PIT 风险：

- 各平台资金流算法不完全透明。
- 龙虎榜和融资融券有披露时间和交易日边界。
- 北向资金历史口径、汇总方式、假期和互联互通状态可能变化。

建议：

- provider 原始字段进入 `metric_payload`。
- 用 `flow_scope` 区分不同来源和口径。
- 后续对接交易所、港交所或付费源做关键字段对账。

### 6.9 `financial_news`、`policy_regulatory_doc` 和 `research_report_index`

新闻、政策、研报索引适合保存元数据；事件抽取、情绪打分和股票映射不属于本仓库采集层。

PIT 风险：

- 页面发布时间、文章发布时间、抓取时间和市场可用时间可能不同。
- 历史补采的新闻列表可能经过重新排序、下线、更新。
- 研报全文和新闻全文可能涉及授权限制。

建议：

- 默认 metadata only。
- 保存 `first_seen_at` 和 provider 时间字段。
- 没有明确授权不下载 PDF、新闻全文、研报全文或电话会纪要。
- 事件映射和情绪判断留给研究层，并且必须读取 as-of manifest。

### 6.10 `public_sentiment` 和 `social_media_aggregate`

热度排行和评论聚合只能作为 attention proxy，不应解释为真实情绪标签。

PIT 风险：

- 上游算法不透明。
- 历史快照通常不可完整复原。
- 当前热度排名回填历史没有意义。

建议：

- 只保存采集时快照。
- 不保存个人帖子正文或用户隐私信息。
- 不把 rank 直接命名为 sentiment score。

### 6.11 `market_minute_bar`

AkShare 的分钟线适合 P2 样例和低成本观察，不适合作为完整历史高频库。

PIT 风险：

- 历史覆盖可能有限。
- 缺少完整 tick、逐笔委托、盘口重建和交易所级别校验。
- 高频数据存储量和质量要求显著高于日频。

建议：

- 当前 P2 只保存少量样本。
- 若未来进入高频研究，应单独设计 replay-grade 数据湖，并使用付费或官方授权源。

## 7. 历史 backfill 规则

历史补采可以做，但必须遵守以下规则。

### 7.1 不伪造 `first_seen_at`

无论补采的是 2005 年行情、2015 年财务指标还是 2020 年公告，只要本系统是在 2026-04-26 才下载到，`first_seen_at` 就必须是 2026-04-26 或实际下载时间。

错误做法：

```text
把 2018-03-31 财务指标的 first_seen_at 写成 2018-03-31。
```

正确做法：

```text
report_date = 2018-03-31
first_seen_at = 2026-04-26T实际采集时间
ingestion_mode = historical_backfill
published_at = 后续从公告源推导，暂缺则为空
```

### 7.2 backfill 与 live observation 分离

建议在 metadata 或 observed payload 中增加或保留类似字段：

```text
ingestion_mode: live_observation | historical_backfill | vendor_snapshot_import
backfill_batch_id: 可选，标记同一批历史补采
provider_function: AkShare 函数名
provider_params: 请求参数
akshare_version: 本地 AkShare 版本
source_doc_url: 对应官方文档 URL
```

这样下游可以明确区分：

- 从今天起每日真实观察到的数据。
- 今天一次性下载的历史数据。
- 后续从其他 vendor 导入的历史快照。

### 7.3 回测默认只读 as-of 数据

严格回测应使用：

```sql
where first_seen_at <= :cutoff_time
```

对于财务、公告、新闻和基金持仓，还应进一步限制：

```text
market_available_at <= cutoff_time
```

如果没有 `market_available_at`，只能做非严格 PIT 或近似 PIT 实验，不能混称为严格 PIT。

## 8. 建议的项目落地方案

### 8.1 继续从今天开始做真实长期采集

从 2026-04-26 起持续每日采集是最干净的数据资产。每次运行都应保留：

```text
raw payload
request params
content_hash
first_seen_at
stored_at
manifest
quality result
```

这些数据以后可以作为严格 PIT 的核心。

### 8.2 新增 AkShare 历史行情 connector

建议后续新增一个独立 source：

```text
source_id: akshare_market_daily_ohlcv_hist
function: stock_zh_a_hist
logical_dataset: market_daily_ohlcv
ingestion_mode: historical_backfill
```

注意事项：

- 默认 `adjust=""`。
- `qfq` 和 `hfq` 单独采集或只进入辅助数据集。
- 与当前 `stock_zh_a_daily` 结果做对账。
- 对全市场补采前先小批量验证字段、速度、失败率、限流和 raw 文件体积。

### 8.3 复权因子改为独立治理对象

不要把复权后的价格直接当作原始行情。

建议长期结构：

```text
market_daily_ohlcv: 不复权原始 OHLCV
corporate_action: 分红、送转、配股、除权除息事件
adjustment_factor: 按观察时间版本化的复权因子
research view: 按 as_of_time 动态生成 qfq/hfq 研究视图
```

### 8.4 官方源和 shadow source 优先级

进入严肃研究前，至少应补以下对账：

| 数据集 | 建议对账源 |
| --- | --- |
| A 股日线 | BaoStock、Tushare、交易所或行情 vendor |
| 公告 | CNINFO、SSE、SZSE、BSE |
| 财务披露 | 公告原文、交易所披露、Tushare/Wind/Choice |
| 商品期货 | SHFE、DCE、CZCE、GFEX 官方数据 |
| 北向资金 | 港交所、交易所或付费 vendor |
| 高频行情 | Wind、券商、交易所授权或其他付费高频源 |

## 9. 推荐风险等级

| logical_dataset | 当前 AkShare 可用性 | PIT 风险等级 | 项目定位 |
| --- | --- | --- | --- |
| `market_daily_ohlcv` | 高 | 中 | 可 bootstrap；历史 backfill 可用；需 shadow 对账 |
| `adjustment_factor` | 中 | 高 | 只作 inferred bootstrap；需官方/付费因子源 |
| `trading_calendar` | 中 | 低到中 | 可运行；非交易日和临时休市后续补齐 |
| `trade_status` | 中 | 中到高 | 可 bootstrap；需官方停复牌源 |
| `price_limit` | 中 | 高 | 当前为推算口径；需官方/行情源对账 |
| `announcement_index` | 中 | 高 | 可做索引；官方公告源优先 |
| `policy_regulatory_doc` | 中 | 中 | 可做新闻/政策 bootstrap；权威源需 CSRC/gov/PBC |
| `commodity_daily` | 中 | 中 | 可 bootstrap；交易所结算源优先 |
| `global_market_daily` | 中 | 中 | 可作样例；需确认许可和时区 |
| `financial_indicator` | 高 | 高 | 可 backfill；必须补披露时间 |
| `macro_indicator` | 中 | 中到高 | 可 backfill；需发布时间和修订口径 |
| `capital_flow` | 中 | 中到高 | 可 bootstrap；需交易所/港交所/vendor 对账 |
| `fund_holding` | 中 | 高 | 只能按公开披露快照处理 |
| `industry_membership` | 中 | 高 | 只保存 snapshot，不能历史回填 |
| `concept_membership` | 中 | 高 | 只保存 snapshot，不能历史回填 |
| `financial_news` | 中 | 中到高 | metadata only；事件判断留给研究层 |
| `public_sentiment` | 中 | 高 | 只能作为 attention proxy |
| `market_minute_bar` | 低到中 | 高 | 样例源，不是完整高频库 |
| `research_report_index` | 中 | 高 | metadata only，不下载全文 |
| `social_media_aggregate` | 中 | 高 | 聚合指标快照，不保存正文 |

## 10. 可接受使用边界

AkShare 在本项目中的可接受用法：

- 用于搭建和验证采集框架。
- 用于从今天开始的持续 raw 观察。
- 用于历史 backfill，但明确标记为 backfill。
- 用于低成本 shadow/fallback。
- 用于发现字段、接口、数据量和运行频率问题。

不可接受用法：

- 把 AkShare 历史数据默认当作严格 PIT 数据。
- 把今天下载的历史 qfq 价格用于声称无未来函数的历史回测。
- 把报告期日期当作财务指标可用日期。
- 把当前股票列表、当前行业/概念成分用于历史股票池。
- 没有授权时保存研报、新闻、电话会或社媒全文。
- 忽略官方源或付费源对账，直接进入生产级研究结论。

## 11. 风险是否可以通过处理解决

结论：不是完全不能用。大部分风险可以通过工程治理显著降低；少数风险如果历史当时版本从未被我们采集、也没有 vendor/官方 vintage 数据，则不能从 AkShare 单源历史 backfill 中完全恢复。

应分三档处理：

| 档位 | 含义 | 可接受用途 |
| --- | --- | --- |
| 可完全控制 | 从今天开始持续采集，或拿到带 `as_of` / vintage / disclosure timestamp 的官方或 vendor 数据 | 可进入严格 PIT 回测 |
| 可近似控制 | 只有历史 backfill，但能补披露日期、上市退市、公司行为、官方公告等关键约束 | 可做 approximate PIT，实验必须标注 |
| 不可从 AkShare 单源恢复 | 上游只给当前最新版历史表，历史旧版本、旧算法、旧网页列表不存在 | 只能做非严格 PIT 研究或探索性分析 |

### 11.0 小白版：这些词到底是什么意思

这一节里出现的海外名字和英文词，不是说本项目要改做美股，也不是说 A 股不能处理。它们只是成熟市场里已经验证过的一套数据治理方法。A 股也适用，因为问题本质一样：回测时不能让模型提前看到未来才知道的信息。

几个核心词先翻译成人话：

| 词 | 小白解释 | A 股例子 |
| --- | --- | --- |
| PIT / point-in-time | 只使用“当时已经能看到”的数据 | 2024-04-25 晚上才披露的年报，不能在 2024-04-25 开盘前使用 |
| as-of | 截至某个时间点 | `as_of=2023-12-29 15:00` 表示只看这个时间之前已知的数据 |
| vintage | 某个数据在某个观察时点的版本 | 2024-01-15 看到的社融历史序列，和 2026-04-26 下载的社融历史序列可能不是同一版 |
| vendor | 数据供应商 | Wind、Choice、Tushare、交易所、CNINFO、AkShare 上游网站都可以理解为 provider/vendor |
| LSEG / S&P / CRSP / FRED | 海外成熟数据产品或数据库 | 这里只是借鉴它们的做法：保存版本、披露时间、上市退市、公司行为 |
| disclosure timestamp | 披露时间 | 公告实际发布时间、财报实际披露时间 |
| security master | 证券主数据 | 股票什么时候上市、退市、改名、改代码、属于哪个交易所 |
| corporate action | 公司行为 | 分红、送转、配股、除权除息、拆股、合并 |
| survivorship bias | 幸存者偏差 | 只用今天还活着的股票回测 2015 年，会漏掉后来退市的差公司 |

所以，不要被这些词吓到。落到本项目，其实就是五句话：

```text
1. 每条数据要知道“我们什么时候看到它”。
2. 每条数据要尽量知道“市场什么时候能用它”。
3. 股票池要知道每只股票当时是否存在、是否可交易。
4. 复权、分红、送转不能用今天的结果倒灌历史。
5. 找不到历史版本的数据，要诚实标记为历史补采，不能冒充严格 PIT。
```

### 11.0.1 这些方案适合 A 股吗

适合，但要按 A 股特性落地。

A 股和美股/宏观数据源不同，但风险类型完全类似：

| 风险 | 美股/海外成熟做法 | A 股对应做法 |
| --- | --- | --- |
| 财报什么时候可用 | SEC filed/accepted time、PIT fundamentals | CNINFO/SSE/SZSE/BSE 公告发布时间、财报公告日期、Tushare/Wind/Choice 披露字段 |
| 股票是否当时存在 | CRSP/QuantConnect security master | A 股上市日期、退市日期、暂停上市、摘牌、代码简称变更 |
| 复权价格是否包含未来分红 | split/dividend events、factor files | A 股分红送转、配股、除权除息、复权因子按观察时间保存 |
| 宏观数据是否修订 | FRED/ALFRED vintage | 中国宏观指标每次采集都保存一个版本；有官方发布时间则用官方发布时间 |
| 行业/概念是否未来才形成 | PIT classification / daily constituents | 每日采集行业/概念成分 snapshot；历史没有版本就不用来做严格回测 |

换句话说，海外资料只是证明“这些处理方法是成熟方案”。本项目真正执行时，仍然围绕 A 股公开源、交易所、CNINFO、AkShare、BaoStock、Tushare 或后续付费源来做。

### 11.1 这些方案不是本项目自创

行业里已有成熟做法，本项目只是把这些做法落到本地采集湖。

| 现有做法 | 代表资料 | 对本项目的启发 |
| --- | --- | --- |
| 经济数据 vintage / real-time period | FRED/ALFRED API 支持 `realtime_start`、`realtime_end`、`vintage_dates`，ALFRED 定位就是保存历史特定日期的数据版本 | 宏观数据不能只存 observation date，要存 data vintage |
| PIT fundamentals | LSEG Fundamentals Point in Time 提供带时间戳的财务数据，说明其用途是知道某天市场可见的数据；S&P/Compustat 也提供 point-in-time historical record | 财务数据要区分原始披露、重述、修订和可用时间 |
| 原始值不覆盖、原始版和重述版并存 | LSEG 说明 PIT fundamentals 中 original data 不覆盖，并提供 original/restated values | 本项目 raw append-only 和 raw_item_version 是正确方向 |
| 公司行为事件化 | QuantConnect/LEAN 对 split、dividend、symbol change、delisting 作为事件处理，并用 factor files / map files | 不要直接存一条“当前 qfq 历史价格”当事实，要存公司行为和因子版本 |
| security master / 无幸存者偏差 universe | QuantConnect US Equity Security Master 覆盖 split、dividend、delisting、merger、ticker change；CRSP survivor-bias-free 数据保留 active 和 delisted 对象以消除幸存者偏差 | A 股需要 instrument lifecycle、上市退市、代码变更、交易状态 |
| 披露时间字段 | Tushare 财务指标接口有 `ann_date` 和 `end_date`；Tushare 股票基础信息有 `list_status`、`list_date`、`delist_date`；SEC EDGAR 有 acceptance datetime 和 filed date 的概念 | 本项目要把 report date、announcement date、published_at、market_available_at 分开 |

参考链接：

- FRED series observations API：`https://fred.stlouisfed.org/docs/api/fred/series_observations.html`
- FRED real-time periods：`https://fred.stlouisfed.org/docs/api/fred/realtime_period.html`
- ALFRED：`https://alfred.stlouisfed.org/`
- LSEG Point in Time Fundamentals：`https://www.lseg.com/en/data-analytics/financial-data/company-data/fundamentals-data/point-in-time-fundamentals`
- S&P Global fundamental data：`https://www.spglobal.com/marketintelligence/en/solutions/fundamental-data`
- QuantConnect corporate actions：`https://www.quantconnect.com/docs/v2/writing-algorithms/securities/asset-classes/us-equity/corporate-actions`
- QuantConnect US Equity Security Master：`https://www.quantconnect.com/docs/v2/writing-algorithms/datasets/quantconnect/us-equity-security-master`
- CRSP survivor-bias-free database：`https://www.crsp.org/research/crsp-survivor-bias-free-us-mutual-funds/`
- Tushare 财务指标：`https://tushare.pro/document/2?doc_id=79`
- Tushare 股票基础信息：`https://tushare.pro/document/2?doc_id=25`
- SEC EDGAR API：`https://www.sec.gov/edgar/sec-api-documentation`
- SEC EDGAR timestamp FAQ：`https://www.sec.gov/about/webmaster-frequently-asked-questions`

### 11.2 本项目最小可实现方案

本项目不需要一次性做成 Wind/CRSP/Compustat 级别的 vendor 数据库，但可以分阶段实现可用的 PIT 治理。

第一阶段，所有 connector 都应统一补足或保留这些字段：

```text
first_seen_at            本系统第一次看到 raw 或 item 的时间，已经存在
stored_at                本系统落盘时间，已经存在
ingestion_mode           live_observation / historical_backfill / vendor_snapshot_import
provider_observation_at  provider 记录自身对应的日期，例如 trading_date、report_date、announcement_date
published_at             provider 或官方页面显示的发布时间，可为空
market_available_at      研究层最早允许使用的时间，可由规则推导
source_version           provider 版本或 AkShare 版本
provider_function        AkShare 函数名，例如 stock_zh_a_hist
provider_params          请求参数摘要
vintage_at               这批数据代表哪个观察版本；没有 vendor vintage 时等于 first_seen_at
```

第二阶段，增加几个治理型 logical_dataset：

```text
instrument_lifecycle     上市、退市、暂停上市、代码变更、简称变更、市场板块
corporate_action         分红、送转、配股、拆并股、除权除息、复权因子版本
disclosure_event         公告、财报、业绩预告、业绩快报、更正公告、披露时间
security_status_daily    ST、停牌、复牌、退市整理、涨跌幅规则适用状态
dataset_vintage_manifest 每次全量历史 backfill 或 vendor snapshot 的版本清单
```

第三阶段，所有研究层查询只通过 as-of 视图访问：

```sql
select *
from raw_item_version
where first_seen_at <= :cutoff_time
  and coalesce(json_extract(observed_payload, '$.market_available_at'), first_seen_at) <= :cutoff_time
```

如果同一个 `source_item_key` 有多个版本，则研究层取 `cutoff_time` 之前最后一次看到的版本，而不是取最新版本。

### 11.3 历史 backfill 的分级使用

历史 backfill 不应该一刀切禁止，而应给每批数据打使用等级。

| 等级 | 条件 | 可以做什么 | 不能做什么 |
| --- | --- | --- | --- |
| `pit_ready` | 有可靠 `first_seen_at`，有 `market_available_at`，且关键字段有官方或 vendor 对账 | 严格 PIT 回测 | 仍不能跳过质量检查 |
| `approximate_pit` | 有历史 backfill，能补公告日/上市退市/公司行为等关键约束，但缺少完整旧版本 | 近似 PIT 研究、稳健性测试 | 不能宣传为严格无未来函数 |
| `research_backfill` | 只有当前历史表，缺少披露时间、版本和可用时间 | 探索性训练、数据覆盖验证 | 不能用于严肃回测结论 |
| `metadata_only` | 只保存链接、标题、摘要或聚合指标 | 事件候选、监控、后续人工/模型处理 | 不能当作已经验证的事件标签 |

### 11.4 各类风险的具体解决方案

| 风险 | 能否解决 | 已有方案对应 | 本项目可实现方案 |
| --- | --- | --- | --- |
| 前复权/后复权未来函数 | 可以解决 | QuantConnect 用 raw/adjusted normalization、factor files、split/dividend events | `market_daily_ohlcv` 只存不复权主行情；新增 `corporate_action` 和版本化 `adjustment_factor`；研究层按 `as_of_time` 动态复权 |
| 财务报告期提前使用 | 可以解决可用时间；历史旧版本需 vendor/官方支持 | LSEG/S&P PIT fundamentals；Tushare `ann_date`/`end_date`；SEC acceptance datetime | `financial_indicator` 增加 `report_date`、`published_at`、`market_available_at`；用公告源补披露时间；没有披露时间时降级为 `research_backfill` |
| 财务重述和修订 | 部分解决；没有旧版本时不能完全恢复 | PIT fundamentals 保存 original/restated；vendor 不覆盖原始版 | 每次采集保存 raw 和 content_hash；同一报告期出现新 hash 就新增版本；重要标的用官方公告或付费源对账 |
| 当前股票池导致幸存者偏差 | 可以解决 | CRSP survivor-bias-free；QuantConnect security master；Tushare `list_status`/`list_date`/`delist_date` | 新增 `instrument_lifecycle`；历史回测 universe 按 `list_date <= date < delist_date` 和交易状态过滤 |
| 代码变更/简称变更 | 可以解决 | QuantConnect map files / symbol change events | `instrument_lifecycle` 里保留永久 `instrument_id` 和历史 symbol/name 映射；不要只用当前 6 位代码做长期身份 |
| 当前概念/行业回填历史 | 只能通过历史快照或 vendor PIT 分类解决 | 行业/指数成分数据通常按生效日期或每日快照使用 | 从今天起每日采集成分 snapshot；历史 backfill 不反推有效区间；没有历史版本时标记 `research_backfill` |
| ST、停复牌、涨跌停规则 | 可以大部分解决 | security master + corporate action/status events | 新增 `security_status_daily`；接 `trade_status`、ST 列表、上市天数、板块规则；`price_limit` 只用当日状态推算并与官方字段对账 |
| 公告日期不等于可用时间 | 可以解决 | SEC acceptance datetime；交易所/官方公告详情页发布时间 | `announcement_index` 保存 `announcement_date`、`published_at`；按 A 股交易日历生成 `market_available_at`，盘后公告最早下一交易日可用 |
| 公告/新闻/研报列表变化 | 部分解决；旧网页未保存则无法恢复 | EDGAR index、官方公告归档、raw 快照 | 从今天起多频抓取列表并保存 raw；历史补采只作为索引；重要公告保存详情页/附件 hash |
| 宏观修订 | 可以解决，如果有 vintage 源；AkShare 单源不能完全解决 | ALFRED/FRED `realtime_start`、`realtime_end`、`vintage_dates` | 每次采集宏观全历史序列都保存为新 raw vintage；若接 ALFRED 类源，使用 provider vintage；没有 vintage 时标记 approximate |
| 基金持仓披露滞后 | 可以解决可用时间；完整持仓需官方披露 | 财报/基金报告通常按披露日可用 | `fund_holding` 区分 `report_date` 和 `published_at`；只在披露后使用；不从重仓股推断完整持仓 |
| 资金流/热度/评论算法变化 | 不能从单源历史完全恢复，只能降低误用 | vendor 算法型指标通常需要 daily snapshot 或 PIT feed | 从今天起保存每日 snapshot；历史 backfill 只作为当前算法重算结果；字段保留在 `metric_payload`，不命名成稳定事实 |
| 不复权日线纠错 | 可以大部分解决 | 多源对账、版本化 raw、quality checks | AkShare + BaoStock/交易所/Tushare shadow 对账；同一日期同一股票出现新 hash 时保留版本并生成 reconciliation report |
| 高频分钟/tick 完整性 | AkShare 不能解决生产级需求 | replay-grade vendor feed、交易所授权行情 | P2 保持样例；生产级高频单独设计存储、授权、回放和容量，不混入日频 V0 |

### 11.4.1 A 股逐项落地方案和样例

下面按最容易误用的数据类型解释：具体怎么处理，处理后能用到什么程度。

#### A. 日线 OHLCV

目标：

```text
保存每只股票每天的不复权开高低收、成交量、成交额。
```

解决方案：

1. AkShare 历史补采可以用，但默认只采 `adjust=""` 的不复权行情。
2. 每条记录写入 `trading_date` 和真实 `first_seen_at`。
3. 历史补采统一标记 `ingestion_mode=historical_backfill`。
4. 从今天开始每天真实采集的数据标记 `ingestion_mode=live_observation`。
5. 后续用 BaoStock、交易所、Tushare 或其他源对同一股票同一天的 OHLCV 做对账。

例子：

```text
2026-04-26 下载 600000 在 2018-01-02 的不复权日线。
trading_date = 2018-01-02
first_seen_at = 2026-04-26T实际采集时间
ingestion_mode = historical_backfill
```

能不能用：

- 做历史覆盖、模型预训练、非严格 PIT 研究：可以。
- 做严格 PIT 回测：历史补采部分只能算 approximate；从 2026-04-26 以后持续采到的数据更接近严格 PIT。

#### B. 前复权/后复权价格

目标：

```text
不要把今天算出来的前复权历史价格，当成历史当天市场看到的价格。
```

解决方案：

1. 主行情表只存不复权价格。
2. 分红、送转、配股、除权除息单独存到 `corporate_action`。
3. 复权因子单独存到 `adjustment_factor`，并记录这个因子版本是什么时候采到的。
4. 回测时按照 `as_of_time` 动态生成当时允许使用的复权价格。

例子：

```text
某股票 2021-06-01 分红。
你在 2020-01-02 做回测时，不能使用包含 2021 分红影响的 qfq 价格。
正确做法是：2020-01-02 的研究视图只使用 2020-01-02 之前已知的公司行为。
```

能不能用：

- AkShare qfq/hfq 可以用于对照和探索。
- 严格 PIT 不应直接用今天下载的整段 qfq/hfq 序列。

#### C. 财务指标

目标：

```text
财务指标不能按报告期使用，要按披露后使用。
```

解决方案：

1. `report_date` 表示财报覆盖期，例如 2023-12-31。
2. `published_at` 表示公告实际披露时间，例如 2024-04-25 20:30。
3. `market_available_at` 表示研究系统最早可以使用的时间，例如 2024-04-26 09:30。
4. 如果 AkShare 财务指标没有披露时间，就用公告源 CNINFO/SSE/SZSE/BSE 去补。
5. 如果补不到披露时间，这批财务数据只能标记为 `research_backfill` 或 `approximate_pit`。

例子：

```text
浦发银行 2023 年年报：
report_date = 2023-12-31
published_at = 2024-04-25 盘后
market_available_at = 2024-04-26 开盘前
```

错误用法：

```text
在 2024-01-02 的模型里使用 2023 年年报净利润。
```

正确用法：

```text
2024-04-26 之后的模型才能使用这份年报指标。
```

能不能用：

- 补到披露时间并做对账：可以做较严格 PIT。
- 只有 AkShare 当前历史财务表：只能做非严格或近似 PIT。

#### D. 股票池和退市股票

目标：

```text
回测 2018 年时，只使用 2018 年当时已经上市且可交易的股票。
```

解决方案：

1. 建立 `instrument_lifecycle`。
2. 每只股票保留 `list_date`、`delist_date`、`list_status`、交易所、代码变更、简称变更。
3. 每个回测日先生成当天 universe，再取行情和特征。

例子：

```text
某股票 2020-05-10 上市。
回测 2019-12-31 时不能把它放进股票池。

某股票 2022-08-01 退市。
回测 2023-01-01 时不能把它当成可交易股票，但历史样本里不能删除它。
```

能不能用：

- 有上市退市和状态数据：可以解决幸存者偏差的大部分问题。
- 只用当前股票列表：不适合做严肃历史回测。

#### E. 行业和概念成分

目标：

```text
不要把今天的“机器人概念”“AI 概念”成分倒回 2020 年。
```

解决方案：

1. 从今天开始每日采集行业/概念成分 snapshot。
2. 每条成分记录保存 `snapshot_date` 和 `first_seen_at`。
3. 只有在某个回测日之前已经采集到的 snapshot，才能用于该回测日。
4. 历史没有 snapshot 的日期，不强行用当前成分回填。

例子：

```text
2026-04-26 采到“机器人概念”包含股票 A。
这只能证明 2026-04-26 这一天 AkShare/东财这样展示。
不能证明股票 A 在 2020 年就属于机器人概念。
```

能不能用：

- 从今天开始积累的 snapshot：未来可做 PIT。
- 今天下载的当前成分去回测多年历史：不能用于严格 PIT。

#### F. ST、停牌、涨跌停

目标：

```text
涨跌停和交易状态必须按当日状态判断，不能用当前状态回填历史。
```

解决方案：

1. 建立 `security_status_daily`。
2. 保存每天是否 ST、是否停牌、是否退市整理、适用哪个涨跌幅规则。
3. `price_limit` 只用当日状态推算。
4. 对特殊情况用交易所或行情源字段对账。

例子：

```text
某股票 2022 年不是 ST，涨跌幅 10%。
2024 年变成 ST，涨跌幅 5%。
回测 2022 年时必须用 10% 规则，不能因为今天看到它是 ST 就用 5%。
```

能不能用：

- 有当日状态数据：大部分可解决。
- 没有当日状态，只靠当前简称判断：高风险。

#### G. 公告和新闻

目标：

```text
公告不是看到公告日期就能用，要看发布时间和交易时间。
```

解决方案：

1. 公告索引保存 `announcement_date`。
2. 官方详情页或接口补 `published_at`。
3. 按交易日历推导 `market_available_at`。
4. 盘后公告通常下一交易日才可用于日频调仓。

例子：

```text
公告日期 = 2024-04-25
发布时间 = 2024-04-25 20:10
如果策略在 2024-04-25 收盘前决策，不能用。
如果策略在 2024-04-26 开盘前决策，可以用。
```

能不能用：

- 有官方发布时间：可以解决大部分可用时间问题。
- 只有公告日期，没有时间：只能保守设为下一交易日或降级使用。

#### H. 宏观数据

目标：

```text
宏观指标要区分“指标所属月份”和“公布/修订版本”。
```

解决方案：

1. `period` 表示指标属于哪个月，例如 2024-03。
2. `published_at` 表示什么时候公布。
3. 每次重新采集完整历史序列，都保存成一个新 `vintage_at`。
4. 如果历史值后来修订，旧版本和新版本都保留。

例子：

```text
2024-04-12 公布 2024-03 社融。
2026-04-26 下载历史社融序列。

period = 2024-03
first_seen_at = 2026-04-26
如果知道官方 release time，则 published_at = 2024-04-12
如果不知道，则不能假装 2024-04-12 就拿到了完全相同版本。
```

能不能用：

- 有官方发布时间和 vintage：可以严格处理。
- 只有 AkShare 当前历史序列：可做研究 backfill，但不要声称严格 PIT。

#### I. 基金持仓

目标：

```text
基金持仓按披露后使用，不按报告期末使用。
```

解决方案：

1. `report_date` 是基金报告期末。
2. `published_at` 是基金季报/半年报/年报披露时间。
3. `market_available_at` 是披露后可用于研究的时间。
4. 不用重仓股推断完整持仓。

例子：

```text
基金 2023Q4 持仓日期 = 2023-12-31。
季报披露日期 = 2024-01-22。
回测 2024-01-02 不能使用这份持仓。
```

能不能用：

- 补到披露时间：可以近似或严格使用。
- 只有报告期末持仓：不能用于严格 PIT。

#### J. 资金流、热度、评论

目标：

```text
这类指标通常是平台算法指标，只能当作 provider 当时展示的快照。
```

解决方案：

1. 从今天开始定时采集 snapshot。
2. 只解释为 `attention proxy` 或 provider 指标。
3. 不把今天下载的历史热度/资金流当作当年平台原算法结果。

例子：

```text
2026 年下载到 2022 年某股票“主力资金流”。
如果平台 2024 年改过算法，这个值可能不是 2022 年当时网页显示的值。
```

能不能用：

- 从今天开始保存的每日 snapshot：以后可用。
- 历史回填的算法指标：只能探索，不适合严格 PIT。

### 11.5 `market_available_at` 推导规则

`market_available_at` 是研究层防未来函数的核心字段。建议规则如下：

| 数据类型 | 推导规则 |
| --- | --- |
| 收盘后日线行情 | 当日收盘后、数据源稳定更新后可用；保守可设为下一交易日开盘前 |
| 盘后公告 | 下一交易日开盘前可用 |
| 盘中公告 | 公告发布时间之后可用；日频模型可保守设为下一交易日 |
| 年报/季报财务指标 | 正式公告 `published_at` 之后可用；若盘后发布，则下一交易日 |
| 宏观数据 | 官方 release time 之后可用；没有 release time 时使用 `first_seen_at` |
| 基金持仓 | 基金报告披露后可用，不是报告期末可用 |
| 行业/概念成分 | 本系统采集到该 snapshot 后可用，不反推历史 |
| 当前算法型指标 | 本系统采集到 snapshot 后可用，不用历史 backfill 冒充旧算法版本 |

保守策略：如果缺少精确 `published_at`，就不要把 `market_available_at` 推到历史更早时间；最多设为 `first_seen_at` 或标记为 unknown。

### 11.6 最小实现顺序

建议按以下顺序做，都是在当前项目架构内可实现的小步增量：

1. 给 AkShare 历史 backfill connector 增加 `ingestion_mode=historical_backfill` 和 `vintage_at=first_seen_at`。
2. 在 dataset contract 的 optional fields 中逐步加入 `published_at`、`market_available_at`、`ingestion_mode`、`vintage_at`。
3. 新增 `instrument_lifecycle` contract，先用免费源或 Tushare/BaoStock 候选字段补 `list_date`、`delist_date`、`list_status`。
4. 新增 `corporate_action` contract，先覆盖分红送转、除权除息、复权因子版本；现有 `adjustment_factor` 暂时标记为 inferred。
5. 公告源接 CNINFO/SSE/SZSE/BSE 详情页，提取或保留官方发布时间和附件 hash。
6. 新增 `asof` 查询工具，默认强制 `first_seen_at <= cutoff_time` 和 `market_available_at <= cutoff_time`。
7. 对 `market_daily_ohlcv` 开发 BaoStock/Tushare/交易所 shadow source，生成跨源 reconciliation report。
8. 对财务、基金、宏观类数据，建立 `pit_ready` / `approximate_pit` / `research_backfill` 标签。

### 11.7 哪些情况仍然不能完全解决

以下情况不能靠后处理完全修复：

- 2026-04-26 才下载到的历史概念成分，无法证明 2018 年也这样分类。
- 2026-04-26 下载的 qfq 历史价格，无法还原每个历史日期当时可见的 qfq 序列，除非有当时的因子 vintage。
- 上游平台 2024 年改了资金流算法，AkShare 只返回当前算法重算后的 2022 年历史值，无法从单源恢复旧算法值。
- 历史新闻/公告列表曾经显示但后来被删除、撤回或换链接，如果当时没有 raw 快照，就无法证明旧页面内容。
- 财务指标如果只拿到当前最新版，且没有原公告、修订公告或 PIT vendor，则不能还原首次披露值。

这类数据不是完全不能用，而是不能用于“严格 PIT”结论。正确做法是降级用途：

```text
严格 PIT 回测：不用。
近似 PIT 稳健性实验：可谨慎使用，并标注限制。
模型预训练或覆盖探索：可使用。
采集框架验证：可使用。
```

### 11.8 最终工程原则

解决方案可以概括为：

```text
能从今天开始观察的，就用 live observation 积累真 PIT；
能找到官方披露时间的，就补 published_at 和 market_available_at；
能找到历史生命周期的，就做 as-of universe；
能找到公司行为的，就不要依赖当前复权价；
能找到 vendor vintage 的，就接入 vintage；
找不到历史版本的，就诚实标记为 backfill，不冒充 PIT。
```

## 12. 最终判断

AkShare 值得继续作为本项目的核心 bootstrap 来源。它能显著降低早期建设成本，也能提供足够多的历史数据用于补齐样本和验证采集流程。

但本项目的核心价值不是“能不能下载历史数据”，而是“能不能证明某个历史决策时点之前系统已经合法、真实、可追溯地看到了哪些数据”。这件事不能由 AkShare 单独完成，必须由本项目的数据湖治理完成。

因此后续决策应保持：

```text
AkShare 负责低成本覆盖；
raw append-only 负责保留观察事实；
first_seen_at 负责防止伪造可见时间；
backfill 标记负责区分历史补采；
official/shadow source 负责对账；
研究层 as-of 查询负责避免未来数据泄漏。
```
