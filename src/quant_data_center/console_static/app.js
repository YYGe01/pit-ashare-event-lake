const FIELD_LABELS = {
  instrument: "标的",
  symbol: "代码",
  exchange: "交易所",
  name: "名称",
  industry: "行业",
  dataset: "数据集",
  state: "状态",
  total_batch_count: "总批次",
  success_count: "成功批次",
  running_count: "运行中",
  pending_count: "待执行",
  failed_count: "失败",
  stale_running_count: "疑似卡住",
  symbol_count: "批次标的数",
  symbol_preview: "标的预览",
  partition_key: "分区",
  attempt_count: "尝试次数",
  progress_percent: "进度",
  task_id: "任务 ID",
  updated_at: "更新时间",
  complete_percent: "完成率",
  row_count: "行数",
  instrument_count: "覆盖标的",
  expected_instrument_count: "应采标的",
  missing_instrument_count: "缺失标的",
  dimension: "字段维度",
  source_id: "数据源",
  success_instrument_count: "成功标的",
  failed_instrument_count: "失败标的",
  timeout_count: "超时次数",
  error_count: "错误次数",
  failed_task_count: "失败任务",
  raw_object_count: "raw 落地数",
  source_object_count: "对象数",
  silver_row_count: "入库行数",
  document_count: "文档数",
  dataset_count: "数据集数",
  datasets: "覆盖数据集",
  success_percent: "成功率",
  last_error: "最近错误",
  coverage_percent: "覆盖率",
  latest_updated_at: "最近更新",
  missing_dimensions: "缺失维度",
  severity: "严重级别",
  issue_type: "问题类型",
  status: "状态",
  entity_key: "对象",
  message: "说明",
  created_at: "创建时间",
  daily_bar: "日线行情",
  adj_factor: "复权因子",
  price_limit: "涨跌停",
  open: "开盘价",
  high: "最高价",
  low: "最低价",
  close: "收盘价",
  pre_close: "昨收价",
  volume: "成交量",
  amount: "成交额",
  vwap: "成交均价",
  factor_type: "复权类型",
  limit_up: "涨停价",
  limit_down: "跌停价",
  prev_close: "前收盘",
  limit_rule: "涨跌停规则",
  trade_status: "交易状态",
  halt_reason: "停牌原因",
  source_update_time: "状态日期",
  news_count: "新闻数量",
  announcement_count: "公告数量",
  research_report_count: "研报数量",
  investor_interaction_count: "互动问答数量",
  public_sentiment_count: "公开舆情数量",
  raw_news_count: "来源新闻数量",
  raw_announcement_count: "来源公告数量",
  raw_research_report_count: "来源研报数量",
  raw_investor_interaction_count: "来源互动问答数量",
  raw_public_sentiment_count: "来源公开舆情数量",
  news_sentiment_mean: "新闻情绪均值",
  news_positive_count: "新闻正面数",
  news_negative_count: "新闻负面数",
  news_weighted_sentiment_sum: "新闻加权情绪和",
  news_importance_sum: "新闻重要性和",
  news_growth_count: "新闻增长类",
  news_risk_count: "新闻风险类",
  news_financing_count: "新闻融资类",
  news_contract_count: "新闻合同类",
  news_buyback_count: "新闻回购类",
  news_shareholder_change_count: "新闻股东变动类",
  news_regulatory_count: "新闻监管类",
  news_litigation_count: "新闻诉讼类",
  news_performance_count: "新闻业绩类",
  announcement_sentiment_mean: "公告情绪均值",
  announcement_positive_count: "公告正面数",
  announcement_negative_count: "公告负面数",
  announcement_weighted_sentiment_sum: "公告加权情绪和",
  announcement_importance_sum: "公告重要性和",
  announcement_growth_count: "公告增长类",
  announcement_risk_count: "公告风险类",
  announcement_financing_count: "公告融资类",
  announcement_operation_count: "公告经营类",
  announcement_contract_count: "公告合同类",
  announcement_buyback_count: "公告回购类",
  announcement_shareholder_change_count: "公告股东变动类",
  announcement_regulatory_count: "公告监管类",
  announcement_litigation_count: "公告诉讼类",
  announcement_performance_count: "公告业绩类",
  research_institution_count: "研报机构数",
  research_analyst_count: "研报分析师数",
  research_rating_positive_count: "研报正向评级数",
  research_rating_neutral_count: "研报中性评级数",
  research_rating_negative_count: "研报负向评级数",
  research_risk_count: "研报风险提示数",
  research_topic_strength: "研报主题强度",
  research_sentiment_mean: "研报情绪均值",
  question_count: "互动问题数",
  reply_count: "互动回复数",
  reply_delay_hours_mean: "平均回复延迟(小时)",
  risk_topic_count: "互动风险主题数",
  new_business_topic_count: "互动新业务主题数",
  sentiment_mean: "互动情绪均值",
  public_sentiment_heat_mean: "舆情热度均值",
  public_sentiment_rank_best: "舆情最佳排名",
  public_sentiment_keyword_count: "舆情关键词数",
  public_sentiment_risk_topic_count: "舆情风险主题数",
  public_sentiment_new_business_topic_count: "舆情新业务主题数",
  public_sentiment_sentiment_mean: "舆情情绪均值",
  institution: "机构",
  analyst: "分析师",
  rating: "评级",
  rating_change: "评级变化",
  question_text: "问题",
  question_time: "提问时间",
  answer_text: "回复",
  answer_time: "回复时间",
  reply_status: "回复状态",
  reply_delay_hours: "回复延迟(小时)",
  questioner: "提问者",
  channel: "渠道",
  topic_tags: "主题",
  sentiment_score: "情绪分",
  platform: "平台",
  sentiment_type: "舆情类型",
  hot_rank: "热度排名",
  hot_score: "热度分",
  rank_change: "排名变化",
  keyword_text: "热门关键词",
  keyword_count: "关键词数",
};

const DATASET_LABELS = {
  daily_bar: "日线行情",
  adj_factor: "复权因子",
  price_limit: "涨跌停价格",
  trade_status: "交易状态",
  announcement: "公告明细",
  news: "新闻明细",
  research_report: "研报明细",
  investor_interaction: "互动问答明细",
  public_sentiment: "公开舆情明细",
  daily_news_factor: "新闻日频因子",
  daily_announcement_factor: "公告日频因子",
  daily_research_report_factor: "研报日频因子",
  daily_investor_interaction_factor: "互动问答日频因子",
  daily_public_sentiment_factor: "公开舆情日频因子",
};

const SOURCE_LABELS = {
  cninfo_announcement: "巨潮资讯公告",
  sse_announcement: "上交所公告",
  sina_finance_news: "新浪财经滚动新闻",
  eastmoney_roll_news: "东方财富滚动新闻",
  nbd_company_news: "每经公司新闻",
  eastmoney_research_report: "东方财富研报",
  cninfo_investor_interaction: "互动易问答",
  eastmoney_public_sentiment: "东方财富公开舆情",
  sina: "新浪财经",
  wallstreetcn: "华尔街见闻",
  "10jqka": "同花顺",
  eastmoney: "东方财富",
  yuncaijing: "云财经",
  fenghuang: "凤凰新闻",
  jinrongjie: "金融界",
  cls: "财联社",
  yicai: "第一财经",
};

const STATUS_LABELS = {
  complete: "已完成",
  success: "成功",
  running: "运行中",
  pending: "待执行",
  partial: "部分成功",
  blocked: "阻塞",
  failed: "失败",
  stopped: "已停止",
  empty: "暂无",
  ok: "正常",
  missing: "缺失",
  warning: "需关注",
  stale: "疑似卡住",
  provider_stale: "已过期",
  manual: "手动",
  matched: "已匹配",
};

const moneyFormatter = new Intl.NumberFormat("zh-CN");
const $ = (id) => document.getElementById(id);
const PAGE_SIZE = 10;
const DASHBOARD_SOURCE_IDS = [
  "cninfo_announcement",
  "sse_announcement",
  "eastmoney_roll_news",
  "sina_finance_news",
  "eastmoney_research_report",
  "cninfo_investor_interaction",
  "eastmoney_public_sentiment",
  "nbd_company_news",
];
const NEWS_SOURCE_IDS = ["eastmoney_roll_news", "sina_finance_news"];
const ANNOUNCEMENT_SOURCE_IDS = ["cninfo_announcement", "sse_announcement"];
const RESEARCH_REPORT_SOURCE_IDS = ["eastmoney_research_report"];
const INVESTOR_INTERACTION_SOURCE_IDS = ["cninfo_investor_interaction"];
const PUBLIC_SENTIMENT_SOURCE_IDS = ["eastmoney_public_sentiment"];
const FACTOR_TABLE_FIELDS = {
  daily_news_factor: [
    "news_count",
    "news_sentiment_mean",
    "news_positive_count",
    "news_negative_count",
    "news_growth_count",
    "news_risk_count",
    "news_financing_count",
    "news_weighted_sentiment_sum",
    "news_importance_sum",
    "news_contract_count",
    "news_buyback_count",
    "news_shareholder_change_count",
    "news_regulatory_count",
    "news_litigation_count",
    "news_performance_count",
  ],
  daily_announcement_factor: [
    "announcement_count",
    "announcement_growth_count",
    "announcement_risk_count",
    "announcement_financing_count",
    "announcement_operation_count",
    "announcement_sentiment_mean",
    "announcement_positive_count",
    "announcement_negative_count",
    "announcement_weighted_sentiment_sum",
    "announcement_importance_sum",
    "announcement_contract_count",
    "announcement_buyback_count",
    "announcement_shareholder_change_count",
    "announcement_regulatory_count",
    "announcement_litigation_count",
    "announcement_performance_count",
  ],
  daily_research_report_factor: [
    "research_report_count",
    "research_institution_count",
    "research_analyst_count",
    "research_rating_positive_count",
    "research_rating_neutral_count",
    "research_rating_negative_count",
    "research_risk_count",
    "research_topic_strength",
    "research_sentiment_mean",
  ],
  daily_investor_interaction_factor: [
    "question_count",
    "reply_count",
    "reply_delay_hours_mean",
    "risk_topic_count",
    "new_business_topic_count",
    "sentiment_mean",
  ],
  daily_public_sentiment_factor: [
    "public_sentiment_count",
    "public_sentiment_heat_mean",
    "public_sentiment_rank_best",
    "public_sentiment_keyword_count",
    "public_sentiment_risk_topic_count",
    "public_sentiment_new_business_topic_count",
    "public_sentiment_sentiment_mean",
  ],
};
const HANDLER_EXTERNAL_FIELDS = new Set([
  ...FACTOR_TABLE_FIELDS.daily_news_factor,
  ...FACTOR_TABLE_FIELDS.daily_announcement_factor,
  ...FACTOR_TABLE_FIELDS.daily_research_report_factor,
  ...FACTOR_TABLE_FIELDS.daily_investor_interaction_factor,
  ...FACTOR_TABLE_FIELDS.daily_public_sentiment_factor,
]);

let state = {
  activeSection: "dashboard",
  previewMode: "raw",
  statusPayload: null,
  previewPayload: null,
  runPayload: null,
  issuePage: 1,
  widePage: 1,
  sort: {},
  loading: false,
  previewLoading: false,
  runStarting: false,
  autoRefreshTimer: null,
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function number(value) {
  return moneyFormatter.format(Number(value || 0));
}

function percent(value) {
  const numeric = Number(value || 0);
  return `${numeric.toFixed(numeric % 1 === 0 ? 0 : 2)}%`;
}

function ratioPercent(value) {
  if (value === null || value === undefined || value === "") return "-";
  return percent(Number(value || 0) * 100);
}

function compact(value, maxLength = 120) {
  if (value === null || value === undefined || value === "") return "-";
  if (Array.isArray(value)) return value.length ? value.join(", ") : "-";
  const text = typeof value === "object" ? JSON.stringify(value) : String(value);
  return text.length > maxLength ? `${text.slice(0, maxLength)}...` : text;
}

function fieldLabel(key) {
  return FIELD_LABELS[key] || key;
}

function datasetLabel(value) {
  const key = String(value || "");
  return DATASET_LABELS[key] ? `${DATASET_LABELS[key]} (${key})` : key || "-";
}

function sourceLabel(value) {
  const ids = String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  if (!ids.length) return "-";
  return ids
    .map((id) => SOURCE_LABELS[id] ? `${SOURCE_LABELS[id]} (${id})` : id)
    .join(", ");
}

function statusLabel(value) {
  const key = String(value || "");
  return STATUS_LABELS[key] ? `${STATUS_LABELS[key]} (${key})` : key || "-";
}

function statusClass(value) {
  const key = String(value || "default").toLowerCase();
  if (["complete", "success", "ok", "matched"].includes(key)) return "success";
  if (["running"].includes(key)) return "running";
  if (["failed", "fail", "blocked", "missing", "stale"].includes(key)) return "danger";
  if (["pending", "empty", "partial", "warning", "stopped", "manual", "provider_stale"].includes(key)) return "warning";
  return "default";
}

function tag(value) {
  return `<span class="tag tag-${statusClass(value)}">${escapeHtml(statusLabel(value))}</span>`;
}

async function api(path, options = {}) {
  const response = await fetch(path, { cache: "no-store", ...options });
  const payload = await response.json();
  if (!response.ok || payload.status === "fail") {
    const error = new Error(payload.error || `HTTP ${response.status}`);
    error.httpStatus = response.status;
    throw error;
  }
  return payload;
}

function showError(error) {
  const banner = $("error-banner");
  banner.textContent = error ? error.message || String(error) : "";
  banner.classList.toggle("hidden", !error);
}

function friendlyError(error) {
  if (error?.httpStatus === 503) {
    return new Error("DuckDB 正在写入，页面保留上次结果并会在 15 秒内继续刷新。");
  }
  return error;
}

function activeDate() {
  return $("active-date").value;
}

function setActiveDate(date) {
  if (date && $("active-date").value !== date) {
    $("active-date").value = date;
  }
}

function table(columns, rows, emptyText = "暂无数据", options = {}) {
  if (!rows || rows.length === 0) {
    return `<div class="empty">${escapeHtml(emptyText)}</div>`;
  }
  const sort = options.sort || {};
  return `
    <div class="table-wrap">
      <table class="data-table">
        <thead><tr>${columns.map((column) => headerCell(column, sort, options.sortKind)).join("")}</tr></thead>
        <tbody>
          ${rows.map((row) => `<tr>${columns.map((column) => `<td>${cell(column, row)}</td>`).join("")}</tr>`).join("")}
        </tbody>
      </table>
    </div>
  `;
}

function headerCell(column, sort, sortKind) {
  if (!sortKind || column.sortable === false) {
    return `<th>${escapeHtml(column.label)}</th>`;
  }
  const active = sort.key === column.key;
  const direction = active ? sort.direction : "";
  const marker = active ? (direction === "asc" ? " ↑" : " ↓") : "";
  return `<th><button class="sort-button" data-sort-kind="${sortKind}" data-sort-key="${escapeHtml(column.key)}" type="button">${escapeHtml(column.label)}${marker}</button></th>`;
}

function filterInstrumentRows(rows, query) {
  const text = String(query || "").trim().toUpperCase();
  if (!text) return rows || [];
  return (rows || []).filter((row) => {
    const haystack = [
      row.instrument,
      row.symbol,
      row.exchange,
      row.name,
      row.industry,
    ].filter(Boolean).join(" ").toUpperCase();
    return haystack.includes(text);
  });
}

function paginateRows(rows, page) {
  const safePage = Math.max(1, Number(page || 1));
  const totalRows = rows.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / PAGE_SIZE));
  const currentPage = Math.min(safePage, totalPages);
  const start = (currentPage - 1) * PAGE_SIZE;
  return {
    rows: rows.slice(start, start + PAGE_SIZE),
    totalRows,
    totalPages,
    page: currentPage,
    start,
  };
}

function sortedRows(rows, sort) {
  if (!sort?.key) return rows;
  const direction = sort.direction === "asc" ? 1 : -1;
  const key = sort.key;
  return [...rows].sort((left, right) => compareValues(valueForSort(left[key]), valueForSort(right[key])) * direction);
}

function valueForSort(value) {
  if (Array.isArray(value)) return value.join(",");
  if (value === null || value === undefined || value === "") return null;
  const numeric = Number(value);
  if (!Number.isNaN(numeric) && String(value).trim?.() !== "") return numeric;
  return String(value).toUpperCase();
}

function compareValues(left, right) {
  if (left === null && right === null) return 0;
  if (left === null) return 1;
  if (right === null) return -1;
  if (typeof left === "number" && typeof right === "number") return left - right;
  return String(left).localeCompare(String(right), "zh-CN");
}

function pagination(kind, pageInfo) {
  const from = pageInfo.totalRows ? pageInfo.start + 1 : 0;
  const to = Math.min(pageInfo.start + PAGE_SIZE, pageInfo.totalRows);
  return `
    <div class="pagination-bar">
      <div class="pagination-info">显示 ${number(from)}-${number(to)} / ${number(pageInfo.totalRows)}，每页 ${number(PAGE_SIZE)} 行</div>
      <div class="pagination-actions">
        <button class="btn page-btn" data-page-kind="${kind}" data-page-delta="-1" type="button" ${pageInfo.page <= 1 ? "disabled" : ""}>上一页</button>
        <span class="pagination-page">第 ${number(pageInfo.page)} / ${number(pageInfo.totalPages)} 页</span>
        <button class="btn page-btn" data-page-kind="${kind}" data-page-delta="1" type="button" ${pageInfo.page >= pageInfo.totalPages ? "disabled" : ""}>下一页</button>
      </div>
    </div>
  `;
}

function cell(column, row) {
  if (column.html) return column.html(row);
  const raw = column.value ? column.value(row) : row[column.key];
  if (column.status) return tag(raw);
  const value = column.format ? column.format(raw, row) : raw;
  return escapeHtml(compact(value, column.maxLength || 120));
}

function summaryCard(label, value, foot = "", level = "") {
  return `
    <div class="summary-card ${level ? `summary-${level}` : ""}">
      <div class="summary-label">${escapeHtml(label)}</div>
      <div class="summary-value">${escapeHtml(value)}</div>
      ${foot ? `<div class="summary-foot">${escapeHtml(foot)}</div>` : ""}
    </div>
  `;
}

function renderStatus(payload) {
  state.statusPayload = payload;
  setActiveDate(payload.date);
  $("database-path").textContent = payload.database_path || "-";
  $("database-state").outerHTML = payload.database_exists
    ? '<span id="database-state" class="tag tag-success">已连接</span>'
    : '<span id="database-state" class="tag tag-warning">未初始化</span>';
  $("last-refresh").textContent = `刷新时间 ${new Date().toLocaleTimeString("zh-CN", { hour12: false })}`;
  renderDashboard(payload);
}

function renderDashboard(payload) {
  const collection = payload.collection || {};
  const batches = payload.batches || {};
  const reference = payload.reference || {};
  const verdict = payload.verdict || {};
  const quality = payload.quality_summary || {};
  const qlib = payload.qlib_provider_status || {};
  const sources = payload.source_summary_rows || [];
  const expected = Number(reference.expected_instrument_count || 0);
  const documentCoverage = documentCoverageFromDatasetRows(payload.dataset_rows || [], expected);
  const coreComplete = Number(collection.core_complete_instrument_count || 0);
  const problemCount = Number(collection.problem_instrument_count || 0);
  const readiness = Number(verdict.readiness_percent ?? collection.core_complete_percent ?? 0);
  const vendorFailures = sourceFailureCount(sources);
  const blockedBatches = Number(batches.failed_count || 0) + Number(batches.stale_running_count || 0);

  renderStatusStrip(payload);
  $("hero-state").textContent = verdictLevelLabel(verdict.level);
  $("hero-state").className = `hero-state tag tag-${verdictLevelClass(verdict.level)}`;
  $("hero-title").textContent = verdict.title || "等待采集状态";
  $("hero-summary").textContent = `当前日期：${payload.date || "-"}。${verdict.summary || "暂无结论。"}`;
  $("hero-action").textContent = verdict.next_action || "等待下一次刷新。";
  $("provider-note").textContent = providerNote(qlib, reference, readiness);
  if ($("hero-percent")) $("hero-percent").textContent = percent(readiness);
  if ($("hero-progress-fill")) $("hero-progress-fill").style.width = `${Math.max(0, Math.min(100, readiness))}%`;
  if ($("hero-meter-foot")) $("hero-meter-foot").textContent = `15 秒自动刷新 · ${payload.updated_at || "-"}`;

  $("kpi-grid").innerHTML = [
    summaryCard(
      "Qlib Provider",
      compact(qlib.calendar_latest_date || "-"),
      `预期 ${compact(qlib.expected_latest_date || "-")} · ${compact(qlib.provider_uri || "-", 48)}`,
      statusClass(qlib.status) === "success" ? "success" : statusClass(qlib.status) === "danger" ? "danger" : "warning",
    ),
    summaryCard("应采标的 (Expected)", number(expected), reference.source || "标的基准"),
    summaryCard("结构化诊断 (Legacy)", `${number(coreComplete)} / ${number(expected)}`, `完整率 ${percent(collection.core_complete_percent)}`, readiness >= 100 ? "success" : "warning"),
    summaryCard("问题标的 (Diagnostics)", number(problemCount), "仅用于历史结构化链路排查", problemCount ? "warning" : "success"),
    summaryCard("质量问题 (Quality)", number(quality.open_issue_count || 0), `失败维度 ${number(quality.failed_dimension_count || 0)}`, quality.open_issue_count ? "danger" : "success"),
    summaryCard("有公告标的", number(documentCoverage.announcement.instruments), `覆盖 ${documentCoverage.announcement.coverageText}`),
    summaryCard("有新闻标的", number(documentCoverage.news.instruments), `覆盖 ${documentCoverage.news.coverageText}`),
    summaryCard("有研报标的", number(documentCoverage.research_report.instruments), `覆盖 ${documentCoverage.research_report.coverageText}`),
    summaryCard("有互动标的", number(documentCoverage.investor_interaction.instruments), `覆盖 ${documentCoverage.investor_interaction.coverageText}`),
    summaryCard("供应商异常 (Vendor)", number(vendorFailures), "超时 + 错误", vendorFailures ? "danger" : "success"),
    summaryCard("阻塞批次 (Blocked)", number(blockedBatches), `失败 ${number(batches.failed_count)} / 卡住 ${number(batches.stale_running_count)}`, blockedBatches ? "danger" : "success"),
    summaryCard("运行中 (Running)", number(batches.running_count || 0), `待执行 ${number(batches.pending_count || 0)}`),
    summaryCard("总批次 (Batches)", number(batches.total_batch_count || 0), `成功 ${number(batches.success_count || 0)}`),
  ].join("");

  renderStageBoard(payload.stage_rows || []);
  renderSourceHealth(sources);
  renderFactorStatus(payload.dataset_rows || []);
  renderActionCommands(payload);
  renderRunStatus(state.runPayload);
  renderQualitySummary(quality, payload.quality_issue_rows || []);
  renderSourceSummary(sources);
  renderBatchTable(payload.batch_rows || [], payload.batch_task_rows || [], payload.crawl_task_rows || []);
  renderDatasetCoverage(payload.dataset_rows || []);
  renderSourceDimensions(payload.source_dimension_rows || []);
  renderIssueTable(payload.issue_rows || []);
}

function verdictLevelLabel(level) {
  const labels = {
    success: "可用",
    running: "采集中",
    warning: "需确认",
    danger: "需处理",
  };
  return labels[level] || "未知";
}

function verdictLevelClass(level) {
  if (level === "success") return "success";
  if (level === "running") return "running";
  if (level === "danger") return "danger";
  return "warning";
}

function sourceFailureCount(rows) {
  return (rows || []).reduce((total, row) => total + Number(row.timeout_count || 0) + Number(row.error_count || 0), 0);
}

function providerNote(qlib, reference, readiness) {
  const latest = qlib.calendar_latest_date || "-";
  const expected = qlib.expected_latest_date || "-";
  const suffix = statusClass(qlib.status) === "success"
    ? "基础行情 provider 可作为 external factor 对齐底座。"
    : "QDC 文档因子可继续采集，但训练底座不可声明可用。";
  return `Provider 最新日历：${latest}，目标日期：${expected}。基准：${reference.source || "unknown"}；首页可用性 ${percent(readiness)}。${suffix}`;
}

function renderStatusStrip(payload) {
  const qlib = payload.qlib_provider_status || {};
  const sources = payload.source_summary_rows || [];
  const quality = payload.quality_summary || {};
  const expected = Number(payload.reference?.expected_instrument_count || 0);
  const documentCoverage = documentCoverageFromDatasetRows(payload.dataset_rows || [], expected);
  const factorRows = factorStatusRows(payload.dataset_rows || [], payload.date);
  const factorStatus = factorRows.some((row) => Number(row.row_count || 0) > 0) ? "ok" : "warning";
  const items = [
    statusStripItem("Qlib provider", qlibStripStatus(qlib), qlib.calendar_latest_date || "-"),
    statusStripItem("公告", sourceGroupStatus(sources, ANNOUNCEMENT_SOURCE_IDS), documentCoverageFoot(documentCoverage, "announcement")),
    statusStripItem("新闻", sourceGroupStatus(sources, NEWS_SOURCE_IDS), documentCoverageFoot(documentCoverage, "news")),
    statusStripItem("研报", sourceGroupStatus(sources, RESEARCH_REPORT_SOURCE_IDS), documentCoverageFoot(documentCoverage, "research_report")),
    statusStripItem("互动", sourceGroupStatus(sources, INVESTOR_INTERACTION_SOURCE_IDS), documentCoverageFoot(documentCoverage, "investor_interaction")),
    statusStripItem("舆情", sourceGroupStatus(sources, PUBLIC_SENTIMENT_SOURCE_IDS), documentCoverageFoot(documentCoverage, "public_sentiment")),
    statusStripItem("因子", factorStatus, `${number(factorRows.reduce((sum, row) => sum + Number(row.row_count || 0), 0))} 行`),
    statusStripItem("质量", quality.status === "success" ? "ok" : quality.status || "pending", `${number(quality.open_issue_count || 0)} 个未关闭问题`),
  ];
  $("status-strip").innerHTML = items.map(renderStatusPill).join("");
}

function statusStripItem(label, status, foot) {
  return { label, status, foot };
}

function renderStatusPill(item) {
  const cls = item.status === "stale" ? "warning" : statusClass(item.status);
  return `
    <div class="status-pill status-pill-${cls}">
      <div class="status-pill-label">${escapeHtml(item.label)}</div>
      <div class="status-pill-value">${escapeHtml(statusLabel(item.status))}</div>
      <div class="status-pill-foot">${escapeHtml(item.foot || "-")}</div>
    </div>
  `;
}

function qlibStripStatus(qlib) {
  const status = String(qlib.status || "missing");
  const stale = (qlib.issues || []).some((issue) => issue.issue_type === "stale_calendar");
  if (status === "warning" && stale) return "provider_stale";
  return status;
}

function sourceGroupStatus(rows, sourceIds) {
  const selected = sourceIds.map((sourceId) => (rows || []).find((row) => row.source_id === sourceId)).filter(Boolean);
  if (!selected.length) return "empty";
  if (selected.some((row) => row.state === "failed")) return "failed";
  if (selected.some((row) => row.state === "empty" || row.state === "partial")) return "warning";
  if (selected.some((row) => row.state === "success")) return "ok";
  return "empty";
}

function sourceGroupFoot(rows, sourceIds) {
  const selected = sourceIds.map((sourceId) => (rows || []).find((row) => row.source_id === sourceId)).filter(Boolean);
  const silverRows = selected.reduce((sum, row) => sum + Number(row.silver_row_count || 0), 0);
  const errors = selected.reduce((sum, row) => sum + Number(row.timeout_count || 0) + Number(row.error_count || 0), 0);
  return `入库 ${number(silverRows)} · 异常 ${number(errors)}`;
}

function documentCoverageFromDatasetRows(datasetRows, expected) {
  const byDataset = new Map((datasetRows || []).map((row) => [row.dataset, row]));
  return {
    announcement: documentCoverageItem(byDataset.get("announcement"), expected),
    news: documentCoverageItem(byDataset.get("news"), expected),
    research_report: documentCoverageItem(byDataset.get("research_report"), expected),
    investor_interaction: documentCoverageItem(byDataset.get("investor_interaction"), expected),
    public_sentiment: documentCoverageItem(byDataset.get("public_sentiment"), expected),
  };
}

function documentCoverageItem(row, expected) {
  const instruments = Number(row?.instrument_count || 0);
  const rows = Number(row?.row_count || 0);
  return {
    instruments,
    rows,
    coverageText: expected > 0 ? `${number(instruments)} / ${number(expected)} 标的` : `${number(instruments)} 标的`,
  };
}

function documentCoverageFoot(coverage, kind) {
  const item = coverage[kind] || { instruments: 0, rows: 0, coverageText: "0 标的" };
  return `标的 ${item.coverageText} · 入库 ${number(item.rows)}`;
}

function renderSourceHealth(rows) {
  const target = $("source-health-table");
  if (!target) return;
  const bySource = new Map((rows || []).map((row) => [row.source_id, row]));
  const displayRows = sortedRows(
    DASHBOARD_SOURCE_IDS.map((sourceId) => normalizeSourceHealthRow(sourceId, bySource.get(sourceId))),
    state.sort.sourceHealth,
  );
  target.innerHTML = table(
    [
      { key: "source_id", label: "source_id", format: sourceLabel, maxLength: 120 },
      { key: "state", label: "状态", status: true },
      { key: "provider_record_count", label: "provider", value: (row) => sourceHealthNumber(row, "provider_record_count") },
      { key: "silver_row_count", label: "入库", value: (row) => sourceHealthNumber(row, "silver_row_count") },
      { key: "mapping_rate", label: "映射率", value: (row) => row.state === "manual" ? "-" : ratioPercent(row.mapping_rate) },
      { key: "empty_result_count", label: "空结果", value: (row) => sourceHealthNumber(row, "empty_result_count") },
      { key: "duplicate_rate", label: "重复率", value: (row) => row.state === "manual" ? "-" : ratioPercent(row.duplicate_rate) },
      { key: "parse_failed_rate", label: "解析失败", value: (row) => row.state === "manual" ? "-" : ratioPercent(row.parse_failed_rate) },
      { key: "last_error", label: "最近错误", value: (row) => row.last_error || (row.state === "manual" ? "已退出默认" : "-"), maxLength: 160 },
    ],
    displayRows,
    "当前日期暂无源级健康记录。",
    { sortKind: "sourceHealth", sort: state.sort.sourceHealth || {} },
  );
}

function normalizeSourceHealthRow(sourceId, row) {
  if (row) return row;
  return {
    source_id: sourceId,
    state: sourceId === "nbd_company_news" ? "manual" : "empty",
    provider_record_count: sourceId === "nbd_company_news" ? null : 0,
    silver_row_count: sourceId === "nbd_company_news" ? null : 0,
    empty_result_count: sourceId === "nbd_company_news" ? null : 0,
    duplicate_rate: null,
    parse_failed_rate: null,
    mapping_rate: null,
    last_error: null,
  };
}

function sourceHealthNumber(row, key) {
  if (row.state === "manual") return "-";
  const value = row[key];
  return value === null || value === undefined || value === "" ? "-" : number(value);
}

function renderFactorStatus(datasetRows) {
  const target = $("factor-status-table");
  if (!target) return;
  const rows = sortedRows(factorStatusRows(datasetRows, state.statusPayload?.date), state.sort.factorStatus);
  target.innerHTML = table(
    [
      { key: "dataset", label: "factor table", format: datasetLabel, maxLength: 120 },
      { key: "row_count", label: "行数", value: (row) => number(row.row_count || 0) },
      { key: "instrument_count", label: "标的数", value: (row) => number(row.instrument_count || 0) },
      { key: "date_range", label: "日期范围", maxLength: 80 },
      { key: "handler_status", label: "Qlib handler", html: (row) => tag(row.handler_status) },
    ],
    rows,
    "当前日期暂无 external factor。",
    { sortKind: "factorStatus", sort: state.sort.factorStatus || {} },
  );
}

function factorStatusRows(datasetRows, date) {
  const byDataset = new Map((datasetRows || []).map((row) => [row.dataset, row]));
  return [
    "daily_announcement_factor",
    "daily_news_factor",
    "daily_research_report_factor",
    "daily_investor_interaction_factor",
    "daily_public_sentiment_factor",
  ].map((dataset) => {
    const row = byDataset.get(dataset) || {};
    const fields = FACTOR_TABLE_FIELDS[dataset] || [];
    const missing = fields.filter((field) => !HANDLER_EXTERNAL_FIELDS.has(field));
    return {
      dataset,
      row_count: Number(row.row_count || 0),
      instrument_count: Number(row.instrument_count || 0),
      date_range: Number(row.row_count || 0) ? (date || "-") : "-",
      handler_status: missing.length ? "missing" : "matched",
    };
  });
}

function renderActionCommands(payload) {
  const target = $("action-command-list");
  if (!target) return;
  const date = payload.date || activeDate() || "";
  const qlib = payload.qlib_provider_status || {};
  const providerUri = qlib.provider_uri || "~/.qlib/qlib_data/cn_data";
  const instruments = (qlib.sample_instruments || []).slice(0, 2).map((item) => String(item).toUpperCase()).join(",") || "SH600000,SZ000001";
  const commands = [
    `qdc verify-qlib --provider-uri ${providerUri} --start ${date} --end ${date} --instruments "${instruments}" --fields '$close,$volume,$factor'`,
    `qdc crawl-daily --date ${date} --source-id cninfo_announcement --page-size 20 --max-pages 1 --skip-pdf-download`,
    `qdc crawl-daily --date ${date} --source-id eastmoney_research_report --page-size 50`,
    `qdc crawl-daily --date ${date} --source-id cninfo_investor_interaction --symbols SZ002594 --page-size 20 --max-pages 1`,
    `qdc crawl-daily --date ${date} --source-id eastmoney_public_sentiment --page-size 20 --max-pages 1`,
    `qdc build-factors --factor-set all --start ${date} --end ${date}`,
  ];
  target.innerHTML = commands.map((command) => `
    <div class="action-command">
      <button class="btn" type="button" data-copy-command="${escapeHtml(command)}">复制命令</button>
      <code>${escapeHtml(command)}</code>
    </div>
  `).join("");
}

function renderRunStatus(payload) {
  const target = $("run-status-card");
  if (!target) return;
  const run = payload?.run;
  const running = Boolean(payload?.running);
  const button = $("run-daily-button");
  if (button) button.disabled = running || state.runStarting;
  if (!run) {
    target.innerHTML = `${renderRunProgressBoard()}<div class="empty compact-empty">当前没有由控制台启动的采集任务。</div>`;
    return;
  }
  const command = (run.command || []).join(" ");
  const logs = runLogLines(run).slice(-60);
  const runStatus = run.status || "pending";
  const batches = state.statusPayload?.batches || {};
  const stopTime = run.stop_requested_at ? ` · 停止请求 ${escapeHtml(run.stop_requested_at)}` : "";
  target.innerHTML = `
    <div class="run-status-head run-status-${statusClass(runStatus)}">
      <div>
        <div class="run-status-title"><span class="live-dot live-dot-${statusClass(runStatus)}"></span>最近执行 ${tag(runStatus)}</div>
        <div class="run-status-meta">开始 ${escapeHtml(run.start_at || "-")} · 结束 ${escapeHtml(run.end_at || "-")} · 返回码 ${escapeHtml(run.return_code ?? "-")}${stopTime}</div>
      </div>
      <div class="run-actions">
        ${running ? '<button class="btn btn-danger" id="stop-run-btn" type="button">停止采集</button>' : ""}
        <button class="btn" id="refresh-run-btn" type="button">刷新执行状态</button>
      </div>
    </div>
    ${renderRunProgressBoard()}
    <div class="run-resume-strip">
      <div>
        <span class="run-resume-label">断点续跑批次</span>
        <strong>${number(batches.success_count || 0)} / ${number(batches.total_batch_count || 0)}</strong>
      </div>
      <span>运行中 ${number(batches.running_count || 0)}</span>
      <span>待执行 ${number(batches.pending_count || 0)}</span>
      <span>失败 ${number(batches.failed_count || 0)}</span>
      <span>疑似卡住 ${number(batches.stale_running_count || 0)}</span>
    </div>
    <div class="run-command-block">
      <div class="run-block-label">执行命令</div>
      <div class="run-command">${escapeHtml(command)}</div>
    </div>
    <div class="run-log-block">
      <div class="run-log-head">
        <span>实时日志</span>
        <span>${number(logs.length)} 行</span>
      </div>
      ${logs.length ? `<pre class="run-log">${escapeHtml(logs.join("\n"))}</pre>` : '<div class="empty compact-empty">暂无日志输出。</div>'}
    </div>
  `;
  $("refresh-run-btn")?.addEventListener("click", refreshRunStatus);
  $("stop-run-btn")?.addEventListener("click", stopDailyPipeline);
  const logBox = target.querySelector(".run-log");
  if (logBox) logBox.scrollTop = logBox.scrollHeight;
}

function renderRunProgressBoard() {
  const status = state.statusPayload || {};
  const structured = summarizeTaskProgress(status.batch_task_rows || [], status.batches || {});
  const crawlers = summarizeTaskProgress(status.crawl_task_rows || [], null);
  return `
    <div class="run-progress-grid">
      ${runProgressItem("文档源批次", crawlers)}
      ${runProgressItem("结构化诊断批次", structured)}
    </div>
  `;
}

function summarizeTaskProgress(rows, fallback) {
  if (!rows.length && fallback) {
    const total = Number(fallback.total_batch_count || 0);
    const success = Number(fallback.success_count || 0);
    const running = Number(fallback.running_count || 0);
    const pending = Number(fallback.pending_count || 0);
    const failed = Number(fallback.failed_count || 0) + Number(fallback.stale_running_count || 0);
    return {
      total,
      success,
      running,
      pending,
      failed,
      progress: Number(fallback.complete_percent || 0),
      state: fallback.state || "empty",
    };
  }
  const total = rows.length;
  const success = rows.filter((row) => row.state === "success").length;
  const running = rows.filter((row) => row.state === "running").length;
  const pending = rows.filter((row) => row.state === "pending").length;
  const failed = rows.filter((row) => ["failed", "stale", "blocked"].includes(row.state)).length;
  const progress = total
    ? rows.reduce((sum, row) => sum + Number(row.progress_percent || 0), 0) / total
    : 0;
  const stateName = failed ? "failed" : running ? "running" : pending ? "pending" : success && success === total ? "success" : "empty";
  return { total, success, running, pending, failed, progress, state: stateName };
}

function runProgressItem(label, summary) {
  const progress = Math.max(0, Math.min(100, Number(summary.progress || 0)));
  return `
    <div class="run-progress-item run-progress-${statusClass(summary.state)}">
      <div class="run-progress-head">
        <strong>${escapeHtml(label)}</strong>
        ${tag(summary.state || "empty")}
      </div>
      <div class="progress-bar"><div class="progress-fill progress-${statusClass(summary.state)}" style="width:${progress}%"></div></div>
      <div class="run-progress-foot">
        ${percent(progress)} · 成功 ${number(summary.success)} / 总 ${number(summary.total)} · 运行 ${number(summary.running)} · 待执行 ${number(summary.pending)} · 失败 ${number(summary.failed)}
      </div>
    </div>
  `;
}

function runLogLines(run) {
  const combined = run.log_tail || [];
  if (combined.length) {
    return combined
      .map((item) => typeof item === "string" ? item : item?.text)
      .filter(Boolean);
  }
  return [...(run.stderr_tail || []), ...(run.stdout_tail || [])].filter(Boolean);
}

function renderStageBoard(rows) {
  $("stage-board").innerHTML = rows.length
    ? rows.map(renderStageItem).join("")
    : '<div class="empty">当前日期暂无采集阶段记录。</div>';
}

function renderStageItem(row) {
  const progress = row.progress_percent === null || row.progress_percent === undefined ? null : Number(row.progress_percent || 0);
  const progressHtml = progress === null
    ? '<div class="stage-progress-note">事件型数据不按覆盖率判断</div>'
    : `<div class="progress-bar"><div class="progress-fill" style="width:${Math.max(0, Math.min(100, progress))}%"></div></div>`;
  return `
    <article class="stage-item stage-${statusClass(row.status)}">
      <div class="stage-head">
        <h3>${escapeHtml(row.label || "-")}</h3>
        ${tag(row.status || "pending")}
      </div>
      <div class="stage-primary">${escapeHtml(row.primary || "-")}</div>
      <div class="stage-secondary">${escapeHtml(row.secondary || "")}</div>
      ${progressHtml}
    </article>
  `;
}

function renderQualitySummary(summary, issueRows) {
  const rows = summary.rows || [];
  $("quality-grid").innerHTML = rows.length
    ? rows.map(renderQualityItem).join("")
    : '<div class="empty">当前日期暂无质量体检结果。</div>';
  $("quality-issue-table").innerHTML = table(
    [
      { key: "dataset", label: fieldLabel("dataset"), format: datasetLabel, maxLength: 90 },
      { key: "source_id", label: fieldLabel("source_id"), format: sourceLabel, maxLength: 180 },
      { key: "severity", label: fieldLabel("severity"), maxLength: 80 },
      { key: "issue_type", label: fieldLabel("issue_type"), maxLength: 140 },
      { key: "entity_key", label: fieldLabel("entity_key"), maxLength: 160 },
      { key: "message", label: fieldLabel("message"), maxLength: 260 },
      { key: "created_at", label: fieldLabel("created_at"), maxLength: 100 },
    ],
    issueRows,
    "当前日期没有未关闭质量问题。",
    { sortKind: "qualityIssue", sort: state.sort.qualityIssue || {} },
  );
}

function renderQualityItem(row) {
  const status = row.status || "success";
  return `
    <article class="quality-item quality-${statusClass(status)}">
      <div class="quality-head">
        <h3>${escapeHtml(row.label || row.dimension || "-")}</h3>
        ${tag(status)}
      </div>
      <div class="quality-number">${number(row.affected_count || 0)}</div>
      <div class="quality-foot">${escapeHtml(row.sample || "未发现问题")}</div>
    </article>
  `;
}

function renderSourceSummary(rows) {
  $("source-summary-grid").innerHTML = rows.length
    ? rows.slice(0, 8).map(renderSourceCard).join("")
    : '<div class="empty">当前日期暂无供应商采集记录。</div>';
}

function renderSourceCard(row) {
  const failures = Number(row.timeout_count || 0) + Number(row.error_count || 0);
  const foot = row.last_error || `raw ${number(row.raw_object_count || 0)} · 入库 ${number(row.silver_row_count || 0)} 行`;
  return `
    <article class="source-card source-${statusClass(row.state)}">
      <div class="source-head">
        <h3>${escapeHtml(sourceLabel(row.source_id))}</h3>
        ${tag(row.state || "empty")}
      </div>
      <div class="source-metrics">
        <span>失败标的 ${number(row.failed_instrument_count || 0)}</span>
        <span>异常 ${number(failures)}</span>
        <span>数据集 ${number(row.dataset_count || 0)}</span>
      </div>
      <div class="source-foot">${escapeHtml(compact(foot, 180))}</div>
    </article>
  `;
}

function renderBatchTable(rows, taskRows = [], crawlRows = []) {
  const sorted = sortedRows(rows, state.sort.batch);
  const taskSorted = sortedRows(taskRows, state.sort.batchTask);
  const crawlSorted = sortedRows(crawlRows, state.sort.crawlTask);
  $("batch-table").innerHTML = `
    <div class="batch-section">
      <h3>数据集批次汇总</h3>
      ${table(
    [
      { key: "dataset", label: fieldLabel("dataset"), format: datasetLabel, maxLength: 90 },
      { key: "state", label: fieldLabel("state"), status: true },
      { key: "total_batch_count", label: fieldLabel("total_batch_count"), value: (row) => number(row.total_batch_count) },
      { key: "success_count", label: fieldLabel("success_count"), value: (row) => number(row.success_count) },
      { key: "running_count", label: fieldLabel("running_count"), value: (row) => number(row.running_count) },
      { key: "pending_count", label: fieldLabel("pending_count"), value: (row) => number(row.pending_count) },
      { key: "failed_count", label: fieldLabel("failed_count"), value: (row) => number(row.failed_count) },
      { key: "stale_running_count", label: fieldLabel("stale_running_count"), value: (row) => number(row.stale_running_count) },
      { key: "symbol_count", label: fieldLabel("symbol_count"), value: (row) => number(row.symbol_count) },
      { key: "complete_percent", label: fieldLabel("complete_percent"), value: (row) => percent(row.complete_percent) },
      { key: "latest_updated_at", label: fieldLabel("latest_updated_at"), maxLength: 100 },
    ],
    sorted,
    "当前日期暂无每日采集批次。",
    { sortKind: "batch", sort: state.sort.batch || {} },
  )}
    </div>
    <div class="batch-section">
      <h3>结构化采集批次</h3>
      ${renderTaskProgressTable(taskSorted, "batchTask", "当前日期暂无结构化采集批次。")}
    </div>
    <div class="batch-section">
      <h3>文档源爬虫批次</h3>
      ${renderCrawlProgressTable(crawlSorted, "crawlTask", "当前日期暂无文档源爬虫批次。")}
    </div>
  `;
}

function renderTaskProgressTable(rows, sortKind, emptyText) {
  return table(
    [
      { key: "dataset", label: fieldLabel("dataset"), format: datasetLabel, maxLength: 90 },
      { key: "source_id", label: fieldLabel("source_id"), format: sourceLabel, maxLength: 140 },
      { key: "state", label: fieldLabel("state"), status: true },
      { key: "progress_percent", label: fieldLabel("progress_percent"), html: progressCell },
      { key: "symbol_count", label: fieldLabel("symbol_count"), value: (row) => number(row.symbol_count) },
      { key: "symbol_preview", label: fieldLabel("symbol_preview"), maxLength: 220 },
      { key: "attempt_count", label: fieldLabel("attempt_count"), value: (row) => number(row.attempt_count) },
      { key: "updated_at", label: fieldLabel("updated_at"), maxLength: 100 },
      { key: "last_error", label: fieldLabel("last_error"), maxLength: 260 },
    ],
    rows,
    emptyText,
    { sortKind, sort: state.sort[sortKind] || {} },
  );
}

function renderCrawlProgressTable(rows, sortKind, emptyText) {
  return table(
    [
      { key: "dataset", label: fieldLabel("dataset"), format: datasetLabel, maxLength: 90 },
      { key: "source_id", label: fieldLabel("source_id"), format: sourceLabel, maxLength: 180 },
      { key: "state", label: fieldLabel("state"), status: true },
      { key: "progress_percent", label: fieldLabel("progress_percent"), html: progressCell },
      { key: "partition_key", label: fieldLabel("partition_key"), maxLength: 160 },
      { key: "attempt_count", label: fieldLabel("attempt_count"), value: (row) => number(row.attempt_count) },
      { key: "updated_at", label: fieldLabel("updated_at"), maxLength: 100 },
      { key: "last_error", label: fieldLabel("last_error"), maxLength: 260 },
    ],
    rows,
    emptyText,
    { sortKind, sort: state.sort[sortKind] || {} },
  );
}

function progressCell(row) {
  const progress = Math.max(0, Math.min(100, Number(row.progress_percent || 0)));
  return `
    <div class="task-progress-cell">
      <div class="progress-bar"><div class="progress-fill progress-${statusClass(row.state)}" style="width:${progress}%"></div></div>
      <span>${percent(progress)}</span>
    </div>
  `;
}

function renderDatasetCoverage(rows) {
  const sorted = sortedRows(rows, state.sort.coverage);
  $("dataset-coverage-table").innerHTML = table(
    [
      { key: "dataset", label: fieldLabel("dataset"), format: datasetLabel, maxLength: 90 },
      { key: "row_count", label: fieldLabel("row_count"), value: (row) => number(row.row_count) },
      { key: "instrument_count", label: fieldLabel("instrument_count"), value: (row) => row.instrument_count === null ? "-" : number(row.instrument_count) },
      { key: "expected_instrument_count", label: fieldLabel("expected_instrument_count"), value: (row) => row.expected_instrument_count === null ? "-" : number(row.expected_instrument_count) },
      { key: "missing_instrument_count", label: fieldLabel("missing_instrument_count"), value: (row) => row.missing_instrument_count === null ? "-" : number(row.missing_instrument_count) },
      { key: "coverage_percent", label: fieldLabel("coverage_percent"), value: (row) => row.coverage_percent === null ? "-" : percent(row.coverage_percent) },
      { key: "latest_updated_at", label: fieldLabel("latest_updated_at"), maxLength: 100 },
    ],
    sorted,
    "暂无覆盖统计。",
    { sortKind: "coverage", sort: state.sort.coverage || {} },
  );
}

function renderSourceDimensions(rows) {
  const sorted = sortedRows(rows, state.sort.sourceDimension);
  $("source-dimension-table").innerHTML = table(
    [
      { key: "dataset", label: fieldLabel("dataset"), format: datasetLabel, maxLength: 90 },
      { key: "dimension", label: fieldLabel("dimension"), maxLength: 120 },
      { key: "source_id", label: fieldLabel("source_id"), format: sourceLabel, maxLength: 220 },
      { key: "expected_instrument_count", label: fieldLabel("expected_instrument_count"), value: (row) => row.expected_instrument_count === null ? "-" : number(row.expected_instrument_count) },
      { key: "success_instrument_count", label: fieldLabel("success_instrument_count"), value: (row) => row.success_instrument_count === null ? "-" : number(row.success_instrument_count) },
      { key: "failed_instrument_count", label: fieldLabel("failed_instrument_count"), value: (row) => row.failed_instrument_count === null ? "-" : number(row.failed_instrument_count) },
      { key: "timeout_count", label: fieldLabel("timeout_count"), value: (row) => number(row.timeout_count) },
      { key: "error_count", label: fieldLabel("error_count"), value: (row) => number(row.error_count) },
      { key: "last_error", label: fieldLabel("last_error"), maxLength: 260 },
    ],
    sorted,
    "当前日期暂无按源字段失败统计。",
    { sortKind: "sourceDimension", sort: state.sort.sourceDimension || {} },
  );
}

function renderIssueTable(rows) {
  const filtered = filterInstrumentRows(rows, $("issue-search")?.value);
  const sorted = sortedRows(filtered, state.sort.issue);
  const pageInfo = paginateRows(sorted, state.issuePage);
  state.issuePage = pageInfo.page;
  $("issue-table").innerHTML = pagination("issue", pageInfo) + table(
    [
      { key: "instrument", label: fieldLabel("instrument") },
      { key: "symbol", label: fieldLabel("symbol") },
      { key: "name", label: fieldLabel("name") },
      { key: "industry", label: fieldLabel("industry") },
      { key: "daily_bar", label: fieldLabel("daily_bar"), status: true },
      { key: "adj_factor", label: fieldLabel("adj_factor"), status: true },
      { key: "price_limit", label: fieldLabel("price_limit"), status: true },
      { key: "missing_dimensions", label: fieldLabel("missing_dimensions"), format: (value) => (value || []).map(datasetLabel).join(", "), maxLength: 220 },
    ],
    pageInfo.rows,
    "当前日期没有核心缺失标的。",
    { sortKind: "issue", sort: state.sort.issue || {} },
  );
}

async function refreshStatus() {
  if (state.loading) return;
  state.loading = true;
  showError(null);
  try {
    const query = new URLSearchParams();
    if (activeDate()) query.set("date", activeDate());
    const [payload, runPayload] = await Promise.all([
      api(`/api/daily-collection-status?${query.toString()}`),
      api("/api/daily-pipeline-run"),
    ]);
    state.runPayload = runPayload;
    renderStatus(payload);
  } catch (error) {
    showError(friendlyError(error));
  } finally {
    state.loading = false;
  }
}

async function refreshRunStatus() {
  try {
    state.runPayload = await api("/api/daily-pipeline-run");
    renderRunStatus(state.runPayload);
  } catch (error) {
    showError(friendlyError(error));
  }
}

async function startDailyPipeline() {
  if (state.runStarting) return;
  state.runStarting = true;
  showError(null);
  renderRunStatus(state.runPayload);
  try {
    const payload = {
      workflow: $("run-workflow").value,
      date: activeDate() || null,
      source_id: $("run-source-id").value,
      symbols: $("run-symbols").value.trim(),
      batch_size: Number($("run-batch-size").value || 50),
      page_size: Number($("run-batch-size").value || 50),
      max_pages: Number($("run-max-pages").value || 1),
      refresh_stock_basic: $("run-refresh-stock-basic").checked,
      crawl_documents: $("run-crawl-documents").checked,
      download_pdfs: $("run-download-pdfs").checked,
      skip_crawl_pdf_download: !$("run-download-pdfs").checked,
      control_only: $("run-control-only").checked,
    };
    state.runPayload = await api("/api/daily-pipeline-run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    renderRunStatus(state.runPayload);
    await refreshStatus();
  } catch (error) {
    showError(friendlyError(error));
  } finally {
    state.runStarting = false;
    renderRunStatus(state.runPayload);
  }
}

async function stopDailyPipeline() {
  showError(null);
  try {
    state.runPayload = await api("/api/daily-pipeline-stop", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    renderRunStatus(state.runPayload);
    await refreshStatus();
  } catch (error) {
    showError(friendlyError(error));
  }
}

async function refreshPreview() {
  if (state.previewLoading) return;
  state.previewLoading = true;
  try {
    const query = new URLSearchParams();
    if (activeDate()) query.set("date", activeDate());
    query.set("mode", state.previewMode);
    query.set("limit", "6000");
    if (!state.previewPayload) {
      $("wide-preview-table").innerHTML = '<div class="empty">加载宽表...</div>';
    }
    const payload = await api(`/api/daily-wide-preview?${query.toString()}`);
    renderPreview(payload);
  } catch (error) {
    showError(friendlyError(error));
  } finally {
    state.previewLoading = false;
  }
}

function renderPreview(payload) {
  state.previewPayload = payload;
  setActiveDate(payload.date);
  const filteredRows = filterInstrumentRows(payload.rows || [], $("wide-search")?.value);
  const sorted = sortedRows(filteredRows, state.sort.wide);
  const pageInfo = paginateRows(sorted, state.widePage);
  state.widePage = pageInfo.page;
  const modeText = payload.mode === "factor" ? "处理后因子宽表" : "原始输入宽表";
  const documentCoverage = documentCoverageFromPreviewRows(filteredRows, payload.mode);
  $("preview-summary").innerHTML = [
    summaryCard("基准日期", payload.date || "-", modeText),
    summaryCard("匹配标的", number(filteredRows.length), `总返回 ${number(payload.row_count || 0)}，隐藏 ${number(payload.hidden_count || 0)}`),
    summaryCard("有公告标的", number(documentCoverage.announcement), `当前列表中 ${number(filteredRows.length - documentCoverage.announcement)} 个为 0`),
    summaryCard("有新闻标的", number(documentCoverage.news), `当前列表中 ${number(filteredRows.length - documentCoverage.news)} 个为 0`),
    summaryCard("有研报标的", number(documentCoverage.research_report), `当前列表中 ${number(filteredRows.length - documentCoverage.research_report)} 个为 0`),
    summaryCard("有互动标的", number(documentCoverage.investor_interaction), `当前列表中 ${number(filteredRows.length - documentCoverage.investor_interaction)} 个为 0`),
    summaryCard("有舆情标的", number(documentCoverage.public_sentiment), `当前列表中 ${number(filteredRows.length - documentCoverage.public_sentiment)} 个为 0`),
    summaryCard("标的来源", payload.reference_source || "-", "每行一个 instrument"),
    summaryCard("刷新", new Date().toLocaleTimeString("zh-CN", { hour12: false }), "15 秒自动更新当前页"),
  ].join("");
  const columns = (payload.columns || []).map((key) => ({
    key,
    label: fieldLabel(key),
    html: documentCountRenderer(key),
    maxLength: 140,
  }));
  $("wide-preview-table").innerHTML = pagination("wide", pageInfo) + table(
    columns,
    pageInfo.rows,
    "当前日期没有宽表数据。",
    { sortKind: "wide", sort: state.sort.wide || {} },
  );
}

function documentCoverageFromPreviewRows(rows, mode) {
  const newsKey = mode === "factor" ? "raw_news_count" : "news_count";
  const announcementKey = mode === "factor" ? "raw_announcement_count" : "announcement_count";
  const researchReportKey = mode === "factor" ? "raw_research_report_count" : "research_report_count";
  const investorInteractionKey = mode === "factor" ? "raw_investor_interaction_count" : "investor_interaction_count";
  const publicSentimentKey = mode === "factor" ? "raw_public_sentiment_count" : "public_sentiment_count";
  return {
    news: rows.filter((row) => Number(row[newsKey] || 0) > 0).length,
    announcement: rows.filter((row) => Number(row[announcementKey] || 0) > 0).length,
    research_report: rows.filter((row) => Number(row[researchReportKey] || 0) > 0).length,
    investor_interaction: rows.filter((row) => Number(row[investorInteractionKey] || 0) > 0).length,
    public_sentiment: rows.filter((row) => Number(row[publicSentimentKey] || 0) > 0).length,
  };
}

function documentCountRenderer(key) {
  const newsKeys = new Set(["news_count", "raw_news_count"]);
  const announcementKeys = new Set(["announcement_count", "raw_announcement_count"]);
  const researchReportKeys = new Set(["research_report_count", "raw_research_report_count"]);
  const investorInteractionKeys = new Set(["investor_interaction_count", "raw_investor_interaction_count"]);
  const publicSentimentKeys = new Set(["public_sentiment_count", "raw_public_sentiment_count"]);
  if (
    !newsKeys.has(key)
    && !announcementKeys.has(key)
    && !researchReportKeys.has(key)
    && !investorInteractionKeys.has(key)
    && !publicSentimentKeys.has(key)
  ) return null;
  return (row) => {
    const count = Number(row[key] || 0);
    if (!count) return "0";
    const kind = newsKeys.has(key)
      ? "news"
      : researchReportKeys.has(key)
        ? "research_report"
        : investorInteractionKeys.has(key)
          ? "investor_interaction"
          : publicSentimentKeys.has(key)
            ? "public_sentiment"
            : "announcement";
    return `<button class="link-button document-count" data-kind="${kind}" data-instrument="${escapeHtml(row.instrument)}" type="button">${number(count)} 条</button>`;
  };
}

function openDocuments(kind, instrument) {
  const row = (state.previewPayload?.rows || []).find((item) => item.instrument === instrument);
  if (!row) return;
  const docs = kind === "news"
    ? row._news_documents || []
    : kind === "research_report"
      ? row._research_report_documents || []
      : kind === "investor_interaction"
        ? row._investor_interaction_documents || []
        : kind === "public_sentiment"
          ? row._public_sentiment_documents || []
          : row._announcement_documents || [];
  const title = kind === "news"
    ? "新闻来源明细"
    : kind === "research_report"
      ? "研报来源明细"
      : kind === "investor_interaction"
        ? "互动问答明细"
        : kind === "public_sentiment"
          ? "公开舆情明细"
          : "公告来源明细";
  $("document-modal-title").textContent = `${instrument} ${title}`;
  $("document-modal-summary").textContent = `${state.previewPayload.date || "-"} · ${docs.length} 条已加载明细`;
  $("document-modal-body").innerHTML = docs.length
    ? docs.map(renderDocumentItem).join("")
    : '<div class="empty">该数量来自因子或聚合统计，当前没有加载到来源明细。</div>';
  $("document-modal").classList.remove("hidden");
}

function renderDocumentItem(document) {
  const title = escapeHtml(document.title || "-");
  const externalUrl = document.url ? String(document.url) : "";
  const localUrl = document.local_url ? String(document.local_url) : "";
  const sourceIds = Array.isArray(document.source_ids) && document.source_ids.length
    ? document.source_ids.join(", ")
    : document.source_id || "-";
  const sourceText = sourceLabel(sourceIds);
  const titleControl = localUrl
    ? `<button class="document-title-button" data-preview-url="${escapeHtml(localUrl)}" type="button" aria-expanded="false">${title}</button>`
    : `<span>${title}</span>`;
  const contentLabel = document.content_label || "本地未保存正文或原文";
  const contentStatus = document.content_status || "missing_local_content";
  const localAction = localUrl
    ? `<button class="link-button document-preview-button" data-preview-url="${escapeHtml(localUrl)}" type="button" aria-expanded="false">预览本地数据</button>`
    : "";
  const externalAction = externalUrl
    ? `<a href="${escapeHtml(externalUrl)}" target="_blank" rel="noreferrer">外部来源</a>`
    : "";
  const bodyText = document.body_text || document.answer_text
    ? `<pre class="document-body-text">${escapeHtml(document.body_text || document.answer_text)}</pre>`
    : "";
  const detailFields = [
    "institution",
    "analyst",
    "rating",
    "rating_change",
    "industry",
    "answer_time",
    "question_time",
    "reply_status",
    "reply_delay_hours",
    "questioner",
    "channel",
    "topic_tags",
    "sentiment_score",
    "platform",
    "sentiment_type",
    "hot_rank",
    "hot_score",
    "rank_change",
    "keyword_text",
    "keyword_count",
    "risk_topic_count",
    "new_business_topic_count",
  ]
    .filter((key) => document[key] !== null && document[key] !== undefined && document[key] !== "")
    .map((key) => `<span>${escapeHtml(fieldLabel(key))}: ${escapeHtml(compact(document[key], 80))}</span>`);
  const details = detailFields.length
    ? `<p class="document-meta document-extra-meta">${detailFields.join(" · ")}</p>`
    : "";
  const preview = localUrl
    ? `<iframe class="document-local-preview hidden" data-document-preview title="本地数据预览"></iframe>`
    : "";
  return `
    <article class="document-item">
      <h3>${titleControl}</h3>
      <p class="document-meta">${escapeHtml(document.publish_date || "-")} · ${escapeHtml(sourceText)}</p>
      ${details}
      <p class="document-content-status document-content-${escapeHtml(contentStatus)}">${escapeHtml(contentLabel)}</p>
      ${bodyText}
      <div class="document-actions">${localAction}${externalAction}</div>
      ${preview}
    </article>
  `;
}

function toggleDocumentPreview(button) {
  const article = button.closest(".document-item");
  const frame = article?.querySelector("[data-document-preview]");
  const url = button.dataset.previewUrl;
  if (!frame || !url) return;
  const isHidden = frame.classList.contains("hidden");
  article.querySelectorAll("[data-preview-url]").forEach((item) => {
    item.setAttribute("aria-expanded", isHidden ? "true" : "false");
  });
  if (isHidden) {
    if (!frame.src) frame.src = url;
    frame.classList.remove("hidden");
  } else {
    frame.classList.add("hidden");
  }
}

function closeModal() {
  $("document-modal").classList.add("hidden");
}

async function copyCommand(button) {
  const command = button.dataset.copyCommand || "";
  if (!command) return;
  try {
    await navigator.clipboard.writeText(command);
    const previous = button.textContent;
    button.textContent = "已复制";
    window.setTimeout(() => {
      button.textContent = previous || "复制命令";
    }, 1200);
  } catch (error) {
    showError(`复制失败：${friendlyError(error)}`);
  }
}

function syncRunControls() {
  const workflow = $("run-workflow")?.value || "crawl_daily";
  const isCrawlDaily = workflow === "crawl_daily";
  setControlDisabled("run-source-id", !isCrawlDaily);
  setControlDisabled("run-max-pages", !isCrawlDaily);
  setControlDisabled("run-download-pdfs", !isCrawlDaily);
  setControlDisabled("run-refresh-stock-basic", isCrawlDaily);
  setControlDisabled("run-crawl-documents", isCrawlDaily);
  if ($("run-daily-button")) {
    $("run-daily-button").textContent = isCrawlDaily ? "启动采集" : "启动流水线";
  }
}

function setControlDisabled(id, disabled) {
  const control = $(id);
  if (!control) return;
  control.disabled = disabled;
  control.closest(".run-field, .check-field")?.classList.toggle("disabled", disabled);
}

function bindNav() {
  document.querySelectorAll(".nav-item").forEach((button) => {
    button.addEventListener("click", () => {
      state.activeSection = button.dataset.section;
      document.querySelectorAll(".nav-item").forEach((item) => item.classList.toggle("active", item === button));
      document.querySelectorAll(".page-section").forEach((section) => section.classList.toggle("active", section.id === state.activeSection));
      $("page-title").textContent = state.activeSection === "data" ? "数据预览" : "今日总览";
      $("page-summary").textContent = state.activeSection === "data"
        ? "按日期查看原始输入和处理后因子两张大宽表。"
        : "先看今天能不能用，再看采集进度、质量体检和供应商采集质量。";
      if (state.activeSection === "data") refreshPreview();
    });
  });
}

function bindControls() {
  $("refresh-btn").addEventListener("click", () => {
    refreshStatus();
    if (state.activeSection === "data") refreshPreview();
  });
  $("run-daily-button").addEventListener("click", startDailyPipeline);
  $("run-workflow").addEventListener("change", syncRunControls);
  $("active-date").addEventListener("change", () => {
    state.previewPayload = null;
    state.issuePage = 1;
    state.widePage = 1;
    refreshStatus();
    refreshPreview();
  });
  $("issue-search").addEventListener("input", () => {
    state.issuePage = 1;
    renderIssueTable(state.statusPayload?.issue_rows || []);
  });
  $("wide-search").addEventListener("input", () => {
    state.widePage = 1;
    if (state.previewPayload) renderPreview(state.previewPayload);
  });
  document.querySelectorAll(".segment-button").forEach((button) => {
    button.addEventListener("click", () => {
      state.previewMode = button.dataset.previewMode;
      document.querySelectorAll(".segment-button").forEach((item) => item.classList.toggle("active", item === button));
      state.previewPayload = null;
      state.widePage = 1;
      refreshPreview();
    });
  });
  document.addEventListener("click", (event) => {
    const copyButton = event.target.closest("[data-copy-command]");
    if (copyButton) {
      copyCommand(copyButton);
      return;
    }
    const docButton = event.target.closest(".document-count");
    if (docButton) {
      openDocuments(docButton.dataset.kind, docButton.dataset.instrument);
      return;
    }
    const previewButton = event.target.closest("[data-preview-url]");
    if (previewButton) {
      toggleDocumentPreview(previewButton);
      return;
    }
    const pageButton = event.target.closest(".page-btn");
    if (pageButton) {
      const delta = Number(pageButton.dataset.pageDelta || 0);
      if (pageButton.dataset.pageKind === "issue") {
        state.issuePage += delta;
        renderIssueTable(state.statusPayload?.issue_rows || []);
      }
      if (pageButton.dataset.pageKind === "wide") {
        state.widePage += delta;
        if (state.previewPayload) renderPreview(state.previewPayload);
      }
      return;
    }
    const sortButton = event.target.closest(".sort-button");
    if (sortButton) {
      toggleSort(sortButton.dataset.sortKind, sortButton.dataset.sortKey);
      return;
    }
    if (event.target.closest("[data-close-modal]")) closeModal();
  });
  syncRunControls();
}

function toggleSort(kind, key) {
  const current = state.sort[kind] || {};
  const direction = current.key === key && current.direction === "asc" ? "desc" : "asc";
  state.sort[kind] = { key, direction };
  if (kind === "issue") {
    state.issuePage = 1;
    renderIssueTable(state.statusPayload?.issue_rows || []);
    return;
  }
  if (kind === "wide") {
    state.widePage = 1;
    if (state.previewPayload) renderPreview(state.previewPayload);
    return;
  }
  if (kind === "batch") {
    renderBatchTable(
      state.statusPayload?.batch_rows || [],
      state.statusPayload?.batch_task_rows || [],
      state.statusPayload?.crawl_task_rows || [],
    );
    return;
  }
  if (kind === "batchTask" || kind === "crawlTask") {
    renderBatchTable(
      state.statusPayload?.batch_rows || [],
      state.statusPayload?.batch_task_rows || [],
      state.statusPayload?.crawl_task_rows || [],
    );
    return;
  }
  if (kind === "coverage") {
    renderDatasetCoverage(state.statusPayload?.dataset_rows || []);
    return;
  }
  if (kind === "sourceHealth") {
    renderSourceHealth(state.statusPayload?.source_summary_rows || []);
    return;
  }
  if (kind === "factorStatus") {
    renderFactorStatus(state.statusPayload?.dataset_rows || []);
    return;
  }
  if (kind === "qualityIssue") {
    renderQualitySummary(
      state.statusPayload?.quality_summary || {},
      state.statusPayload?.quality_issue_rows || [],
    );
  }
}

function init() {
  bindNav();
  bindControls();
  refreshStatus().then(refreshPreview);
  state.autoRefreshTimer = window.setInterval(() => {
    refreshStatus();
    if (state.activeSection === "data") refreshPreview();
  }, 15000);
  window.addEventListener("beforeunload", () => window.clearInterval(state.autoRefreshTimer));
}

init();
