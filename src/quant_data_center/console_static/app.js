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
  last_error: "最近错误",
  coverage_percent: "覆盖率",
  latest_updated_at: "最近更新",
  missing_dimensions: "缺失维度",
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
  raw_news_count: "来源新闻数量",
  raw_announcement_count: "来源公告数量",
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
};

const DATASET_LABELS = {
  daily_bar: "日线行情",
  adj_factor: "复权因子",
  price_limit: "涨跌停价格",
  trade_status: "交易状态",
  announcement: "公告明细",
  news: "新闻明细",
  daily_news_factor: "新闻日频因子",
  daily_announcement_factor: "公告日频因子",
};

const SOURCE_LABELS = {
  cninfo_announcement: "巨潮资讯公告",
  sse_announcement: "上交所公告",
  sina_finance_news: "新浪财经滚动新闻",
  eastmoney_roll_news: "东方财富滚动新闻",
  nbd_company_news: "每经公司新闻",
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
  blocked: "阻塞",
  failed: "失败",
  empty: "暂无",
  ok: "正常",
  missing: "缺失",
};

const moneyFormatter = new Intl.NumberFormat("zh-CN");
const $ = (id) => document.getElementById(id);
const PAGE_SIZE = 10;

let state = {
  activeSection: "dashboard",
  previewMode: "raw",
  statusPayload: null,
  previewPayload: null,
  issuePage: 1,
  widePage: 1,
  sort: {},
  loading: false,
  previewLoading: false,
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
  if (["complete", "success", "ok"].includes(key)) return "success";
  if (["running"].includes(key)) return "running";
  if (["failed", "blocked", "missing"].includes(key)) return "danger";
  if (["pending", "empty"].includes(key)) return "warning";
  return "default";
}

function tag(value) {
  return `<span class="tag tag-${statusClass(value)}">${escapeHtml(statusLabel(value))}</span>`;
}

async function api(path) {
  const response = await fetch(path, { cache: "no-store" });
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
  const collected = Number(collection.collected_instrument_count || 0);
  const expected = Number(reference.expected_instrument_count || 0);
  const remaining = Number(collection.remaining_instrument_count || 0);
  const collectionPercent = Number(collection.collection_percent || 0);

  $("hero-date").textContent = payload.date || "-";
  $("hero-summary").textContent = `按 ${reference.source || "unknown"} 作为应采标的基准：已采 ${number(collected)} / ${number(expected)}，剩余 ${number(remaining)}。`;
  $("hero-percent").textContent = percent(collectionPercent);
  $("hero-progress-fill").style.width = `${Math.max(0, Math.min(100, collectionPercent))}%`;
  $("hero-meter-foot").textContent = `15 秒自动刷新 · ${payload.updated_at || "-"}`;

  $("kpi-grid").innerHTML = [
    summaryCard("应采标的", number(expected), reference.source || "标的基准"),
    summaryCard("已采标的", number(collected), "以 daily_bar 当日覆盖为主", "success"),
    summaryCard("未采标的", number(remaining), "应采 - 已采", remaining ? "danger" : "success"),
    summaryCard("核心完整", number(collection.core_complete_instrument_count || 0), `完整率 ${percent(collection.core_complete_percent)}`),
    summaryCard("总批次", number(batches.total_batch_count || 0), `成功 ${number(batches.success_count)} / 运行中 ${number(batches.running_count)}`),
    summaryCard("待执行批次", number(batches.pending_count || 0), "等待每日采集消费"),
    summaryCard("失败批次", number(batches.failed_count || 0), `卡住 ${number(batches.stale_running_count)}`, batches.failed_count ? "danger" : ""),
    summaryCard("问题标的", number(collection.problem_instrument_count || 0), "缺 daily_bar / adj_factor / price_limit", collection.problem_instrument_count ? "danger" : "success"),
  ].join("");

  renderBatchTable(payload.batch_rows || []);
  renderDatasetCoverage(payload.dataset_rows || []);
  renderSourceDimensions(payload.source_dimension_rows || []);
  renderIssueTable(payload.issue_rows || []);
}

function renderBatchTable(rows) {
  const sorted = sortedRows(rows, state.sort.batch);
  $("batch-table").innerHTML = table(
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
  );
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
    const payload = await api(`/api/daily-collection-status?${query.toString()}`);
    renderStatus(payload);
  } catch (error) {
    showError(friendlyError(error));
  } finally {
    state.loading = false;
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
  $("preview-summary").innerHTML = [
    summaryCard("基准日期", payload.date || "-", modeText),
    summaryCard("匹配标的", number(filteredRows.length), `总返回 ${number(payload.row_count || 0)}，隐藏 ${number(payload.hidden_count || 0)}`),
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

function documentCountRenderer(key) {
  const newsKeys = new Set(["news_count", "raw_news_count"]);
  const announcementKeys = new Set(["announcement_count", "raw_announcement_count"]);
  if (!newsKeys.has(key) && !announcementKeys.has(key)) return null;
  return (row) => {
    const count = Number(row[key] || 0);
    if (!count) return "0";
    const kind = newsKeys.has(key) ? "news" : "announcement";
    return `<button class="link-button document-count" data-kind="${kind}" data-instrument="${escapeHtml(row.instrument)}" type="button">${number(count)} 条</button>`;
  };
}

function openDocuments(kind, instrument) {
  const row = (state.previewPayload?.rows || []).find((item) => item.instrument === instrument);
  if (!row) return;
  const docs = kind === "news" ? row._news_documents || [] : row._announcement_documents || [];
  const title = kind === "news" ? "新闻来源明细" : "公告来源明细";
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
  const bodyText = document.body_text
    ? `<pre class="document-body-text">${escapeHtml(document.body_text)}</pre>`
    : "";
  const preview = localUrl
    ? `<iframe class="document-local-preview hidden" data-document-preview title="本地数据预览"></iframe>`
    : "";
  return `
    <article class="document-item">
      <h3>${titleControl}</h3>
      <p class="document-meta">${escapeHtml(document.publish_date || "-")} · ${escapeHtml(sourceText)}</p>
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

function bindNav() {
  document.querySelectorAll(".nav-item").forEach((button) => {
    button.addEventListener("click", () => {
      state.activeSection = button.dataset.section;
      document.querySelectorAll(".nav-item").forEach((item) => item.classList.toggle("active", item === button));
      document.querySelectorAll(".page-section").forEach((section) => section.classList.toggle("active", section.id === state.activeSection));
      $("page-title").textContent = state.activeSection === "data" ? "数据预览" : "今日总览";
      $("page-summary").textContent = state.activeSection === "data"
        ? "按日期查看原始输入和处理后因子两张大宽表。"
        : "按日期查看每日 5000 多只标的的采集进度、批次状态、缺失标的和覆盖总表。";
      if (state.activeSection === "data") refreshPreview();
    });
  });
}

function bindControls() {
  $("refresh-btn").addEventListener("click", () => {
    refreshStatus();
    if (state.activeSection === "data") refreshPreview();
  });
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
    renderBatchTable(state.statusPayload?.batch_rows || []);
    return;
  }
  if (kind === "coverage") {
    renderDatasetCoverage(state.statusPayload?.dataset_rows || []);
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
