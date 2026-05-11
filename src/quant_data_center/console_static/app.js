const datasets = [
  "stock_basic",
  "universe_constituent",
  "trade_calendar",
  "daily_bar",
  "adj_factor",
  "price_limit",
  "trade_status",
  "announcement",
  "news",
  "daily_news_factor",
  "daily_announcement_factor",
];

const rawPreviewDatasets = [
  "stock_basic",
  "universe_constituent",
  "daily_bar",
  "adj_factor",
  "price_limit",
  "trade_status",
  "announcement",
  "news",
];

const CLIENT_TABLE_PAGE_SIZE = 5;

const pageTitles = {
  dashboard: "总览",
  backfill: "回补任务",
  dataset: "数据预览",
  quality: "质量检查",
  qlib: "Qlib 导出",
};

const pageSummaries = {
  dashboard: "先判断当前采集是否正常，再查看卡住的数据集、队列进度和最近运行记录。",
  backfill: "查看历史回补队列，定位哪些日期、标的和数据集还在等待、运行或失败。",
  dataset: "查看采集源表覆盖、原始日频缺口、单标的原始记录和处理后的日频因子。",
  quality: "集中查看质量问题，优先处理未关闭和高严重级别异常。",
  qlib: "确认研究数据是否已经导出为 Qlib 可读 provider，并检查最近导出结果。",
};

const statusOrder = [
  "blocked",
  "failed",
  "running",
  "pending",
  "superseded",
  "success",
  "complete",
  "open",
  "closed",
];
const moneyFormatter = new Intl.NumberFormat("zh-CN");

const datasetLabels = {
  stock_basic: "证券主数据",
  universe_constituent: "股票池成分",
  trade_calendar: "交易日历",
  daily_bar: "日线行情",
  adj_factor: "复权因子",
  price_limit: "涨跌停价格",
  trade_status: "交易状态",
  announcement: "公告",
  news: "新闻",
  daily_news_factor: "新闻日频因子",
  daily_announcement_factor: "公告日频因子",
  qlib_export: "Qlib 导出",
};

const datasetDescriptions = {
  stock_basic: "证券主数据：股票代码、交易所、名称等基础信息。",
  universe_constituent: "股票池成分：某个股票池在某天包含哪些标的。",
  trade_calendar: "交易日历：哪些日期开市、上一交易日和下一交易日。",
  daily_bar: "日线行情：每个交易日每只股票的开高低收、成交量、成交额和成交均价。",
  adj_factor: "复权因子：处理分红、送转、拆股等导致的价格断点。",
  price_limit: "涨跌停价格：每个交易日的涨停价、跌停价和规则。",
  trade_status: "交易状态：正常交易、停牌和停牌原因。",
  announcement: "公告：按发布日期保存的公告标题和链接，仍是事件来源明细。",
  news: "新闻：按发布日期保存的新闻标题和链接，仍是事件来源明细。",
  daily_news_factor: "新闻日频因子：把新闻标题按交易日和标的聚合成数量、情绪和事件计数。",
  daily_announcement_factor: "公告日频因子：把公告标题按交易日和标的聚合成公告数量和事件计数。",
};

const statusLabels = {
  blocked: "阻塞",
  failed: "失败",
  running: "运行中",
  pending: "待执行",
  superseded: "已拆分替代",
  success: "成功",
  complete: "已完成",
  open: "未关闭",
  closed: "已关闭",
  warning: "警告",
  error: "错误",
};

const statusDescriptions = {
  blocked: "存在失败或超时运行任务，进度不会自然完成，需要先处理。",
  failed: "任务执行失败，查看最后错误，修复后重试。",
  running: "任务正在执行；如果更新时间长期不变，可能需要 recover-running。",
  pending: "任务已排队，还没有被 run-backfill 消费。",
  superseded: "任务已经被拆分替代，不再计入总进度。",
  success: "任务已经成功完成。",
  complete: "这组任务全部成功。",
  open: "问题仍未关闭，需要继续处理。",
  closed: "问题已经关闭，只作历史记录。",
};

const coverageKindLabels = {
  required_daily: "原始日频",
  sparse_source: "原始事件",
  sparse_factor: "处理后因子",
  metadata: "基础资料",
  missing_table: "表未创建",
  missing_database: "数据库未初始化",
};

const overviewSourceDatasets = [
  "stock_basic",
  "universe_constituent",
  "trade_calendar",
  "daily_bar",
  "adj_factor",
  "price_limit",
  "trade_status",
  "announcement",
  "news",
];

const overviewInstrumentDimensions = [
  "stock_basic",
  "universe_constituent",
  "daily_bar",
  "adj_factor",
  "price_limit",
  "trade_status",
  "news",
  "announcement",
];

const fixedExpectedInstrumentDimensions = new Set([
  "stock_basic",
  "universe_constituent",
  "daily_bar",
  "adj_factor",
  "price_limit",
]);

const layerLabels = {
  raw: "原始留档层",
  bronze: "上游快照层",
  silver: "统一研究层",
  gold: "研究宽表层",
  qlib: "Qlib 数据目录",
};

const sourceLabels = {
  akshare: "AkShare",
  qdc: "QDC",
};

const universeLabels = {
  csi300: "沪深300",
};

const fieldLabels = {
  status: "状态",
  severity: "严重级别",
  job_type: "运行类型",
  dataset: "数据集",
  source_id: "数据来源",
  universe: "股票池",
  start: "开始",
  end: "结束",
  start_date: "开始日期",
  end_date: "结束日期",
  created_at: "创建时间",
  updated_at: "更新时间",
  error: "错误",
  error_message: "错误信息",
  function: "上游函数",
  params: "采集参数",
  parameter_summary: "采集参数",
  record_count: "原始记录数",
  record_set: "记录集",
  row_index: "原始行号",
  value: "原始值",
  factor_input: "后续用途",
  symbols: "标的",
  symbol_batch_json: "标的批次",
  attempt: "尝试次数",
  attempt_count: "尝试次数",
  last_error: "最后错误",
  issue_type: "问题类型",
  entity_key: "实体键",
  message: "说明",
  layer: "层级",
  uri: "文件路径",
  size_bytes: "大小",
  bytes: "字节",
  provider: "数据目录",
  files: "文件数",
  trade_date: "交易日",
  instrument: "标的",
  symbol: "上游代码",
  exchange: "交易所",
  name: "名称",
  open: "开盘价",
  high: "最高价",
  low: "最低价",
  close: "收盘价",
  pre_close: "昨收价",
  volume: "成交量",
  amount: "成交额",
  vwap: "成交均价",
  adj_factor: "复权因子",
  factor_type: "因子类型",
  limit_up: "涨停价",
  limit_down: "跌停价",
  prev_close: "前收盘价",
  limit_rule: "涨跌停规则",
  trade_status: "交易状态",
  halt_reason: "停牌原因",
  source_update_time: "来源更新时间",
  calendar_id: "日历编号",
  is_open: "是否开市",
  pre_trade_date: "上一交易日",
  next_trade_date: "下一交易日",
  publish_date: "发布日期",
  snapshot_date: "快照日期",
  list_date: "上市日期",
  delist_date: "退市日期",
  title: "标题",
  url: "链接",
  source: "来源",
  weight: "权重",
  announcement_id: "公告编号",
  news_id: "新闻编号",
  news_count: "新闻数量",
  announcement_count: "公告数量",
  news_sentiment_mean: "新闻情绪均值",
  announcement_sentiment_mean: "公告情绪均值",
  news_weighted_sentiment_sum: "新闻加权情绪和",
  news_importance_sum: "新闻重要性和",
  announcement_weighted_sentiment_sum: "公告加权情绪和",
  announcement_importance_sum: "公告重要性和",
  news_positive_count: "新闻正面数",
  news_negative_count: "新闻负面数",
  news_growth_count: "新闻增长类",
  news_risk_count: "新闻风险类",
  news_financing_count: "新闻融资类",
  news_contract_count: "新闻合同类",
  news_buyback_count: "新闻回购类",
  news_shareholder_change_count: "新闻股东变动类",
  news_regulatory_count: "新闻监管类",
  news_litigation_count: "新闻诉讼类",
  news_performance_count: "新闻业绩类",
  announcement_positive_count: "公告正面数",
  announcement_negative_count: "公告负面数",
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
  daily_bar_source_id: "行情来源",
  adj_factor_source_id: "复权来源",
  price_limit_source_id: "涨跌停来源",
  trade_status_source_id: "交易状态来源",
  daily_news_factor_source_id: "新闻因子来源",
  daily_announcement_factor_source_id: "公告因子来源",
  row_count: "行数",
  source_ids: "数据源",
  min_date: "最早日期",
  max_date: "最晚日期",
  date_count: "日期数",
  instruments_with_rows: "有数据标的",
  instruments_missing: "缺失标的",
  missing_daily_rows: "缺失日频行",
  daily_coverage_percent: "日频覆盖率",
  coverage_kind: "维度类型",
  industry: "行业",
  is_active: "是否活跃",
  universes: "股票池",
  stock_basic_present: "证券主数据",
  universe_constituent_present: "股票池成分",
  raw_missing_daily_rows: "原始日频缺口",
  core_missing_days: "原始日频缺口",
  trade_status_days: "交易状态天数",
  news_rows: "新闻明细",
  announcement_rows: "公告明细",
  daily_news_factor_days: "新闻因子天数",
  daily_announcement_factor_days: "公告因子天数",
  factor_news_count: "新闻因子计数",
  factor_announcement_count: "公告因子计数",
};

const factorColumnGroups = [
  {
    title: "行情价格",
    description: "Qlib 基础价格和成交字段，几乎每个交易日都应该有。",
    fields: ["open", "high", "low", "close", "pre_close", "volume", "amount", "vwap"],
  },
  {
    title: "复权和交易限制",
    description: "处理价格连续性、涨跌停约束和停牌状态。",
    fields: [
      "adj_factor",
      "factor_type",
      "limit_up",
      "limit_down",
      "prev_close",
      "limit_rule",
      "trade_status",
      "halt_reason",
      "source_update_time",
    ],
  },
  {
    title: "新闻相关因子",
    description: "由新闻标题按交易日聚合得到，天然可能稀疏。",
    fields: [
      "news_count",
      "news_sentiment_mean",
      "news_positive_count",
      "news_negative_count",
      "news_weighted_sentiment_sum",
      "news_importance_sum",
      "news_growth_count",
      "news_risk_count",
      "news_financing_count",
      "news_contract_count",
      "news_buyback_count",
      "news_shareholder_change_count",
      "news_regulatory_count",
      "news_litigation_count",
      "news_performance_count",
    ],
  },
  {
    title: "公告相关因子",
    description: "由公告标题按交易日聚合得到，重点看事件类型和情绪。",
    fields: [
      "announcement_count",
      "announcement_sentiment_mean",
      "announcement_positive_count",
      "announcement_negative_count",
      "announcement_weighted_sentiment_sum",
      "announcement_importance_sum",
      "announcement_growth_count",
      "announcement_risk_count",
      "announcement_financing_count",
      "announcement_operation_count",
      "announcement_contract_count",
      "announcement_buyback_count",
      "announcement_shareholder_change_count",
      "announcement_regulatory_count",
      "announcement_litigation_count",
      "announcement_performance_count",
    ],
  },
  {
    title: "来源追踪",
    description: "不是训练因子，用来确认每列来自哪个采集源。",
    fields: [
      "daily_bar_source_id",
      "adj_factor_source_id",
      "price_limit_source_id",
      "trade_status_source_id",
      "daily_news_factor_source_id",
      "daily_announcement_factor_source_id",
    ],
  },
];

const factorWideColumns = [
  "trade_date",
  ...factorColumnGroups.flatMap((group) => group.fields),
];

const rawWideColumnOrder = [
  "dataset",
  "factor_input",
  "trade_date",
  "publish_date",
  "snapshot_date",
  "instrument",
  "symbol",
  "name",
  "exchange",
  "industry",
  "list_date",
  "delist_date",
  "open",
  "high",
  "low",
  "close",
  "volume",
  "amount",
  "adj_factor",
  "limit_up",
  "limit_down",
  "prev_close",
  "limit_rule",
  "trade_status",
  "halt_reason",
  "source_update_time",
  "title",
  "url",
  "source",
  "weight",
];

const $ = (id) => document.getElementById(id);

function setHtml(id, html) {
  const target = $(id);
  if (target) {
    target.innerHTML = html;
  }
}

let overview = null;
let activeSection = "dashboard";
let activePreviewMode = "raw";
let autoRefreshTimer = null;
let instrumentSearchTimer = null;
let instrumentOptions = [];
let factorPreviewPage = 1;
let tableRenderSequence = 0;

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function compact(value, maxLength = 120) {
  if (value === null || value === undefined || value === "") {
    return "";
  }
  if (Array.isArray(value)) {
    return value.join(", ");
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  const text = String(value);
  return text.length > maxLength ? `${text.slice(0, maxLength)}...` : text;
}

function number(value) {
  return moneyFormatter.format(Number(value || 0));
}

function labelWithCode(map, value) {
  const key = String(value ?? "").trim();
  if (!key) {
    return "-";
  }
  const label = map[key];
  return label ? `${label} (${key})` : key;
}

function datasetLabel(value) {
  return labelWithCode(datasetLabels, value);
}

function sourceLabel(value) {
  return labelWithCode(sourceLabels, value);
}

function universeLabel(value) {
  return labelWithCode(universeLabels, value);
}

function fieldLabel(value) {
  return labelWithCode(fieldLabels, value);
}

function tokenLabel(value) {
  const key = String(value ?? "").trim();
  if (!key) {
    return "-";
  }
  if (statusLabels[key]) {
    return `${statusLabels[key]} (${key})`;
  }
  if (layerLabels[key]) {
    return `${layerLabels[key]} (${key})`;
  }
  if (datasetLabels[key]) {
    return `${datasetLabels[key]} (${key})`;
  }
  return key;
}

function coverageKindLabel(value) {
  const key = String(value ?? "").trim();
  return coverageKindLabels[key] || key || "-";
}

function statusDescription(value) {
  const key = String(value ?? "").trim();
  return statusDescriptions[key] || "";
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

function setLoading(targetId) {
  const target = $(targetId);
  if (!target.innerHTML.trim()) {
    target.innerHTML = '<div class="empty">加载中</div>';
  }
}

function tag(value) {
  const label = escapeHtml(tokenLabel(value));
  const type = String(value || "default").toLowerCase().replaceAll("_", "-");
  return `<span class="tag tag-${type}">${label}</span>`;
}

function table(columns, rows, emptyText = "暂无数据", options = {}) {
  if (!rows || rows.length === 0) {
    return `<div class="empty">${escapeHtml(emptyText)}</div>`;
  }
  const pageSize = Number(options.pageSize || CLIENT_TABLE_PAGE_SIZE);
  const shouldPaginate = options.paginate !== false && rows.length > pageSize;
  const tableId = `client-table-${++tableRenderSequence}`;
  const totalPages = Math.ceil(rows.length / pageSize);
  const head = columns.map((column) => `<th>${escapeHtml(column.label)}</th>`).join("");
  const body = rows
    .map((row, index) => {
      const cells = columns
        .map((column) => {
          const raw = column.value ? column.value(row) : row[column.key];
          const display = column.format ? column.format(raw, row) : raw;
          const value = column.html
            ? column.html(row, raw)
            : column.status
              ? tag(raw)
              : escapeHtml(compact(display, column.maxLength));
          const title = escapeHtml(compact(display, 400));
          return `<td title="${title}">${value}</td>`;
        })
        .join("");
      const rowClass = shouldPaginate && index >= pageSize ? ' class="client-page-hidden"' : "";
      return `<tr data-client-row-index="${index}"${rowClass}>${cells}</tr>`;
    })
    .join("");
  const pagination = shouldPaginate
    ? clientTablePagination({ tableId, page: 1, totalPages, totalRows: rows.length, pageSize })
    : "";
  return `
    <div id="${tableId}" class="client-table" data-current-page="1" data-page-size="${pageSize}" data-total-rows="${rows.length}">
      <div class="table-wrap"><table class="data-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>
      ${pagination}
    </div>
  `;
}

function tableSummary(text) {
  return `<div class="table-summary">${escapeHtml(text)}</div>`;
}

function clientTablePagination({ tableId, page, totalPages, totalRows, pageSize }) {
  const start = totalRows ? (page - 1) * pageSize + 1 : 0;
  const end = Math.min(page * pageSize, totalRows);
  return `
    <div class="pagination-bar client-table-pagination">
      <button class="btn" data-table-page-action="previous" data-table-id="${tableId}" type="button" disabled>上一页</button>
      <span class="pagination-info" data-table-page-info="${tableId}">第 ${number(page)} / ${number(totalPages)} 页，${number(start)}-${number(end)} / ${number(totalRows)} 行</span>
      <button class="btn" data-table-page-action="next" data-table-id="${tableId}" type="button">下一页</button>
    </div>
  `;
}

function bindTablePagination() {
  document.addEventListener("click", (event) => {
    const button = event.target.closest("[data-table-page-action]");
    if (!button) {
      return;
    }
    updateClientTablePage(button.dataset.tableId, button.dataset.tablePageAction);
  });
}

function updateClientTablePage(tableId, action) {
  const root = $(tableId);
  if (!root) {
    return;
  }
  const pageSize = Number(root.dataset.pageSize || CLIENT_TABLE_PAGE_SIZE);
  const totalRows = Number(root.dataset.totalRows || 0);
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
  const currentPage = Number(root.dataset.currentPage || 1);
  const nextPage = action === "next"
    ? Math.min(totalPages, currentPage + 1)
    : Math.max(1, currentPage - 1);
  const startIndex = (nextPage - 1) * pageSize;
  const endIndex = startIndex + pageSize;
  root.dataset.currentPage = String(nextPage);
  root.querySelectorAll("[data-client-row-index]").forEach((row) => {
    const index = Number(row.dataset.clientRowIndex || 0);
    row.classList.toggle("client-page-hidden", index < startIndex || index >= endIndex);
  });
  root.querySelectorAll("[data-table-page-action='previous']").forEach((button) => {
    button.disabled = nextPage <= 1;
  });
  root.querySelectorAll("[data-table-page-action='next']").forEach((button) => {
    button.disabled = nextPage >= totalPages;
  });
  const info = root.querySelector(`[data-table-page-info="${tableId}"]`);
  if (info) {
    const start = totalRows ? startIndex + 1 : 0;
    const end = Math.min(endIndex, totalRows);
    info.textContent = `第 ${number(nextPage)} / ${number(totalPages)} 页，${number(start)}-${number(end)} / ${number(totalRows)} 行`;
  }
}

function populateSelect(selectId, options, allLabel, labelFn = (option) => option) {
  const select = $(selectId);
  const current = select.value;
  select.innerHTML = "";
  if (allLabel) {
    select.add(new Option(allLabel, ""));
  }
  options.forEach((option) => select.add(new Option(labelFn(option), option)));
  if ([...select.options].some((option) => option.value === current)) {
    select.value = current;
  }
}

function bindNav() {
  document.querySelectorAll(".nav-item").forEach((button) => {
    button.addEventListener("click", () => {
      activeSection = button.dataset.section;
      document.querySelectorAll(".nav-item").forEach((item) => item.classList.remove("active"));
      document.querySelectorAll(".page-section").forEach((section) => {
        section.classList.remove("active");
      });
      button.classList.add("active");
      $(activeSection).classList.add("active");
      $("page-title").textContent = pageTitles[activeSection];
      $("page-summary").textContent = pageSummaries[activeSection];
    });
  });
}

function bindPreviewMode() {
  document.querySelectorAll("[data-preview-mode]").forEach((button) => {
    button.addEventListener("click", () => {
      setPreviewMode(button.dataset.previewMode);
    });
  });
}

function setPreviewMode(mode) {
  activePreviewMode = ["coverage", "raw", "factor"].includes(mode) ? mode : "coverage";
  document.querySelectorAll("[data-preview-mode]").forEach((button) => {
    button.classList.toggle("active", button.dataset.previewMode === activePreviewMode);
  });
  $("coverage-preview-mode").classList.toggle("hidden", activePreviewMode !== "coverage");
  $("instrument-preview-panel").classList.toggle("hidden", activePreviewMode === "coverage");
  $("raw-preview-mode").classList.toggle("hidden", activePreviewMode !== "raw");
  $("factor-preview-mode").classList.toggle("hidden", activePreviewMode !== "factor");
  if (activePreviewMode === "raw") {
    loadRawInstrumentPreview();
  } else if (activePreviewMode === "factor") {
    loadFactorPreview();
  }
}

function bindFilters() {
  ["task-dataset", "task-status", "task-limit"].forEach((id) => {
    $(id).addEventListener("change", loadBackfillTasks);
  });
  ["quality-dataset", "quality-status", "quality-limit"].forEach((id) => {
    $(id).addEventListener("change", loadQualityIssues);
  });
  $("preview-instrument").addEventListener("input", () => {
    window.clearTimeout(instrumentSearchTimer);
    instrumentSearchTimer = window.setTimeout(
      () => loadInstrumentOptions($("preview-instrument").value.trim()),
      250,
    );
  });
  $("preview-instrument").addEventListener("change", () => {
    factorPreviewPage = 1;
    loadActivePreview();
  });
  ["preview-start", "preview-end", "preview-limit"].forEach((id) => {
    $(id).addEventListener("change", () => {
      factorPreviewPage = 1;
      loadActivePreview();
    });
  });
  $("refresh-btn").addEventListener("click", refreshAll);
}

function renderOverview(payload) {
  overview = payload;
  $("database-path").textContent = payload.database_path || "";
  $("database-state").textContent = payload.database_exists ? "DuckDB 已连接" : "DuckDB 未初始化";
  $("database-state").className = payload.database_exists
    ? "tag tag-success"
    : "tag tag-warning";

  const tableCounts = payload.table_counts || {};
  const silverCounts = payload.silver_table_counts || {};
  const statusCounts = payload.backfill_status_counts || {};
  const qualityCounts = payload.quality_status_counts || {};
  const openIssues = qualityCounts.open || 0;

  const silverTotal = Object.values(silverCounts).reduce((sum, value) => sum + Number(value), 0);
  const backfillTotal = Object.values(statusCounts).reduce((sum, value) => sum + Number(value), 0);
  const sourceTotal = tableCounts.source_object || 0;
  const progressRows = payload.backfill_progress || [];
  const blockedCount = progressRows.reduce((sum, row) => sum + Number(row.blocked_count || 0), 0);
  const successTasks = progressRows.reduce((sum, row) => sum + Number(row.success_count || 0), 0);
  const pendingTasks = progressRows.reduce((sum, row) => sum + Number(row.pending_count || 0), 0);
  const runningTasks = progressRows.reduce((sum, row) => sum + Number(row.running_count || 0), 0);
  const staleTasks = progressRows.reduce((sum, row) => sum + Number(row.stale_running_count || 0), 0);
  const totalTasks = progressRows.reduce((sum, row) => sum + Number(row.total_task_count || 0), 0);
  const successPercent = totalTasks ? Math.round((successTasks / totalTasks) * 100) : 0;
  const latestWatermark = latestWatermarkDate(payload.watermarks || []);
  const latestExport = (payload.latest_qlib_exports || [])[0];

  $("overview-health").innerHTML = [
    healthItem(
      "采集是否卡住",
      collectionHealthLabel({ blockedCount, runningTasks, pendingTasks, totalTasks }),
      collectionHealthHint({ blockedCount, staleTasks, runningTasks, pendingTasks, totalTasks }),
      blockedCount ? "danger" : runningTasks || pendingTasks ? "warning" : "success",
    ),
    healthItem(
      "数据覆盖到哪里",
      latestWatermark || "暂无水位",
      latestWatermark
        ? `已登记 ${number((payload.watermarks || []).length)} 条数据水位，说明采集至少覆盖到这些日期。`
        : "还没有 dataset_watermark，通常表示还没成功跑完任何采集任务。",
      latestWatermark ? "success" : "warning",
    ),
    healthItem(
      "Qlib 是否可用",
      latestExport ? tokenLabel(latestExport.status) : "暂无导出",
      latestExport
        ? `最近导出覆盖 ${latestExport.start_date || "-"} 到 ${latestExport.end_date || "-"}，可继续用 verify-qlib 验证。`
        : "还没有 export_qlib 记录，Qlib 暂时没有新的 provider 可用。",
      latestExport?.status === "success" ? "success" : "warning",
    ),
  ].join("");

  $("kpi-grid").innerHTML = [
    kpi("统一研究层行数", silverTotal, "已经清洗进 qdc_silver 的总行数"),
    kpi("回补任务", backfillTotal, `成功 ${number(successTasks)} / 总计 ${number(totalTasks)}，完成率 ${successPercent}%`),
    kpi("运行记录", tableCounts.job_run || 0, "CLI 作业记录，包括 daily、build-factors、export-qlib"),
    kpi("阻塞任务", blockedCount, `失败 ${number(statusCounts.failed || 0)}，超时运行 ${number(staleTasks)}`),
    kpi("源文件索引", sourceTotal, "已经登记的 raw、bronze、gold、qlib 文件"),
    kpi("水位记录", tableCounts.dataset_watermark || 0, "每个数据集已覆盖的日期范围"),
    kpi("Qlib 导出", (payload.latest_qlib_exports || []).length, "最近 export_qlib 作业数量"),
    kpi("质量问题", openIssues, "仍未关闭的质量问题"),
  ].join("");

  $("status-overview").innerHTML = [
    statusBars("回补任务", statusCounts),
    statusBars("运行记录", payload.job_status_counts || {}),
    statusBars("质量问题", qualityCounts),
    statusBars("文件层级", payload.source_layer_counts || {}),
  ].join("");

  renderProgress(progressRows);
  renderDataCoverage(payload.data_coverage || {});
  renderWatermarks(payload.watermarks || []);
  renderRecentJobs(payload.latest_job_runs || []);
  renderQlibJobs(payload.latest_qlib_exports || []);
}

function latestWatermarkDate(rows) {
  const dates = rows.map((row) => row.max_date).filter(Boolean).sort();
  return dates.length ? dates[dates.length - 1] : "";
}

function collectionHealthLabel({ blockedCount, runningTasks, pendingTasks, totalTasks }) {
  if (blockedCount) {
    return `${number(blockedCount)} 个阻塞`;
  }
  if (runningTasks) {
    return `${number(runningTasks)} 个运行中`;
  }
  if (pendingTasks) {
    return `${number(pendingTasks)} 个待执行`;
  }
  if (totalTasks) {
    return "任务已完成";
  }
  return "暂无回补任务";
}

function collectionHealthHint({ blockedCount, staleTasks, runningTasks, pendingTasks, totalTasks }) {
  if (blockedCount) {
    return `先看回补任务页的 failed 和 stale running。超时运行 ${number(staleTasks)} 个。`;
  }
  if (runningTasks) {
    return "正在推进，保持观察更新时间和最近运行记录即可。";
  }
  if (pendingTasks) {
    return "任务已经排队，下一步运行 run-backfill 消费队列。";
  }
  if (totalTasks) {
    return "当前回补队列已经没有待处理任务。";
  }
  return "还没有计划回补任务，可先用 plan-backfill 生成队列。";
}

function healthItem(label, value, hint, tone) {
  return `
    <div class="health-card health-${escapeHtml(tone)}">
      <div class="health-label">${escapeHtml(label)}</div>
      <div class="health-value">${escapeHtml(value)}</div>
      <div class="health-hint">${escapeHtml(hint)}</div>
    </div>
  `;
}

function kpi(label, value, foot) {
  return `
    <div class="kpi-card">
      <div class="kpi-label">${escapeHtml(label)}</div>
      <div class="kpi-value">${number(value)}</div>
      <div class="kpi-foot">${escapeHtml(foot)}</div>
    </div>
  `;
}

function statusBars(label, counts) {
  const entries = Object.entries(counts || {});
  if (entries.length === 0) {
    return `
      <div class="status-group">
        <div class="status-group-header">
          <span>${escapeHtml(label)}</span>
          <span class="muted">暂无记录</span>
        </div>
      </div>
    `;
  }
  const total = entries.reduce((sum, [, count]) => sum + Number(count), 0) || 1;
  const rows = entries
    .sort(([left], [right]) => statusOrder.indexOf(left) - statusOrder.indexOf(right))
    .map(([status, count]) => {
      const width = Math.max(2, (Number(count) / total) * 100);
      const cls = status.toLowerCase().replaceAll("_", "-");
      return `
        <div class="status-row">
          <div>
            ${tag(status)}
            <div class="status-help">${escapeHtml(statusDescription(status))}</div>
          </div>
          <div class="bar"><div class="bar-fill ${cls}" style="width:${width}%"></div></div>
          <div class="muted">${number(count)}</div>
        </div>
      `;
    })
    .join("");
  return `
    <div class="status-group">
      <div class="status-group-header">
        <span>${escapeHtml(label)}</span>
        <span class="muted">共 ${number(total)} 条</span>
      </div>
      ${rows}
    </div>
  `;
}

function renderDataCoverage(coverage) {
  const reference = coverage.reference || {};
  const instrumentSummary = coverage.instrument_summary || {};
  const datasetRows = coverage.dataset_rows || [];
  const sourceDatasetRows = datasetRows.filter((row) => overviewSourceDatasets.includes(row.dataset));
  const instrumentRows = coverage.instrument_rows || [];
  const referenceRange = [reference.min_trade_date, reference.max_trade_date]
    .filter(Boolean)
    .join(" - ");

  const coverageSummaryHtml = [
    coverageCard(
      "当前日期范围",
      referenceRange || "-",
      `${number(reference.trade_date_count || 0)} 个交易日，来源：${reference.trade_date_source || "none"}`,
    ),
    coverageCard(
      "参考标的",
      number(reference.instrument_count || 0),
      `来源：${reference.instrument_source || "none"}`,
    ),
    coverageCard(
      "原始日频完整",
      `${number(instrumentSummary.complete_instruments || 0)} / ${number(instrumentSummary.total_instruments || 0)}`,
      `完整率 ${number(instrumentSummary.complete_percent || 0)}%`,
    ),
    coverageCard(
      "原始日频缺口",
      `${number(instrumentSummary.missing_daily_rows || 0)} 行`,
      missingByDimensionText(instrumentSummary.missing_by_dimension || {}),
    ),
  ].join("");
  setHtml("coverage-summary", coverageSummaryHtml);

  const datasetCoverageHtml = tableSummary(
    "这里只展示采集得到的基础、行情和事件源表；处理后的新闻/公告日频因子不放在覆盖表。日线、复权、涨跌停按当前日期范围判断缺口。",
  ) + table(
    [
      { key: "dataset", label: fieldLabel("dataset"), format: datasetLabel },
      { key: "coverage_kind", label: fieldLabel("coverage_kind"), format: coverageKindLabel },
      { key: "row_count", label: fieldLabel("row_count") },
      { key: "min_date", label: "日期范围", value: dateRangeValue },
      { key: "date_count", label: fieldLabel("date_count") },
      { key: "instruments_with_rows", label: "标的覆盖", value: instrumentCoverageValue },
      { key: "missing_daily_rows", label: "日频缺口", value: dailyMissingValue },
      { key: "source_ids", label: fieldLabel("source_ids"), format: sourceSummary },
    ],
    sourceDatasetRows,
    "暂无数据维度覆盖信息。",
  );
  setHtml("dataset-coverage-table", datasetCoverageHtml);

  const hidden = Number(coverage.hidden_instrument_count || 0);
  const instrumentCoverageSummaryHtml = overviewInstrumentDimensions.map((dataset) => {
    const total = Number(instrumentSummary.total_instruments || 0);
    const available = Number(instrumentSummary.available_by_dimension?.[dataset] || 0);
    const missing = fixedExpectedInstrumentDimensions.has(dataset)
      ? instrumentMissingCount(dataset, instrumentSummary, total, available)
      : null;
    const value = missing === null
      ? `${number(available)} 个标的`
      : `${number(Math.max(total - missing, 0))} 完整`;
    const foot = missing === null
      ? "有记录，不按无事件判缺失"
      : `缺失 ${number(missing)} 个标的`;
    return coverageCard(
      datasetLabel(dataset),
      value,
      foot,
    );
  }).join("");
  setHtml("instrument-coverage-summary", instrumentCoverageSummaryHtml);

  const instrumentCoverageTableHtml = tableSummary(
    hidden
      ? `下表只展示原始维度。原始日频缺口是日线、复权、涨跌停在当前日期范围内缺了多少行，另有 ${number(hidden)} 个标的未展示。`
      : "下表只展示原始维度。原始日频缺口是日线、复权、涨跌停在当前日期范围内缺了多少行。",
  ) + table(
    [
      { key: "instrument", label: fieldLabel("instrument") },
      { key: "name", label: fieldLabel("name"), format: (value) => value || "-" },
      { key: "symbol", label: fieldLabel("symbol") },
      { key: "exchange", label: fieldLabel("exchange") },
      { key: "industry", label: fieldLabel("industry"), format: (value) => value || "-" },
      { key: "universes", label: fieldLabel("universes"), value: universesValue, maxLength: 120 },
      {
        key: "raw_missing_daily_rows",
        label: fieldLabel("raw_missing_daily_rows"),
        value: (row) => `${number(row.raw_missing_daily_rows ?? row.core_missing_days ?? 0)} 行`,
      },
      ...overviewInstrumentDimensions.map((dataset) => ({
        key: dataset,
        label: datasetLabel(dataset),
        value: (row) => dimensionStatusValue(row.dimension_statuses?.[dataset]),
        maxLength: 180,
      })),
    ],
    instrumentRows,
    "暂无标的完整度明细。",
  );
  setHtml("instrument-coverage-table", instrumentCoverageTableHtml);
}

function coverageCard(label, value, foot) {
  return `
    <div class="coverage-card">
      <div class="coverage-label">${escapeHtml(label)}</div>
      <div class="coverage-value">${escapeHtml(value)}</div>
      <div class="coverage-foot">${escapeHtml(foot || "")}</div>
    </div>
  `;
}

function sourceSummary(sources) {
  if (!Array.isArray(sources) || !sources.length) {
    return "-";
  }
  return sources
    .map((source) => `${sourceLabel(source.source_id)} ${number(source.row_count)}`)
    .join("；");
}

function dateRangeValue(row) {
  const range = [row.min_date, row.max_date].filter(Boolean).join(" - ");
  return range || "-";
}

function instrumentCoverageValue(row) {
  if (row.instruments_with_rows === null || row.instruments_with_rows === undefined) {
    return "-";
  }
  const total = row.reference_instrument_count || 0;
  return total
    ? `${number(row.instruments_with_rows)} / ${number(total)}`
    : number(row.instruments_with_rows);
}

function dailyMissingValue(row) {
  if (row.missing_daily_rows === null || row.missing_daily_rows === undefined) {
    return "-";
  }
  return `缺 ${number(row.missing_daily_rows)} 行`;
}

function missingByDimensionText(missingByDimension) {
  const entries = Object.entries(missingByDimension || {});
  if (!entries.length) {
    return "暂无核心维度统计";
  }
  return entries
    .map(([dataset, count]) => `${datasetLabel(dataset)} 缺 ${number(count)}`)
    .join("；");
}

function instrumentMissingCount(dataset, summary, total, available) {
  if (summary.missing_by_dimension && dataset in summary.missing_by_dimension) {
    return Number(summary.missing_by_dimension[dataset] || 0);
  }
  return Math.max(total - available, 0);
}

function universesValue(row) {
  if (!Array.isArray(row.universes) || !row.universes.length) {
    return "-";
  }
  return row.universes.map(universeLabel).join("；");
}

function dimensionStatusValue(status) {
  if (!status) {
    return "-";
  }
  const unit = status.unit || "";
  const observed = `${number(status.observed || 0)} ${unit}`.trim();
  if (status.expected !== null && status.expected !== undefined) {
    const expected = `${number(status.expected)} ${unit}`.trim();
    return `${observed}/${expected}`;
  }
  return observed;
}

function renderWatermarks(rows) {
  if (!rows.length) {
    $("watermark-list").innerHTML = '<div class="empty">暂无数据水位。成功跑完采集后，这里会显示每个数据集覆盖的日期范围。</div>';
    return;
  }
  $("watermark-list").innerHTML = rows
    .slice(0, 8)
    .map((row) => {
      const title = [
        datasetLabel(row.dataset),
        sourceLabel(row.source_id),
        row.universe ? universeLabel(row.universe) : "",
      ].filter(Boolean).join(" / ");
      const range = [row.min_date, row.max_date].filter(Boolean).join(" - ");
      return `
        <div class="list-item">
          <div class="list-title">${escapeHtml(title)}</div>
          <div class="list-meta">覆盖日期：${escapeHtml(range || "-")}</div>
          <div class="list-meta">更新时间：${escapeHtml(row.updated_at || "")}</div>
        </div>
      `;
    })
    .join("");
}

function renderProgress(rows) {
  if (!rows.length) {
    $("progress-list").innerHTML = '<div class="empty">暂无回补任务。可先用 plan-backfill 生成任务，再用 run-backfill 执行。</div>';
    return;
  }
  $("progress-list").innerHTML = rows
    .slice(0, 8)
    .map((row) => {
      const percent = Number(row.success_percent || 0);
      const title = [
        datasetLabel(row.dataset),
        sourceLabel(row.source_id),
        row.universe ? universeLabel(row.universe) : "",
      ].filter(Boolean).join(" / ");
      const range = [row.min_date, row.max_date].filter(Boolean).join(" - ");
      const stale = Number(row.stale_running_count || 0);
      const state = String(row.state || "empty");
      const staleText = stale
        ? `<div class="progress-warning">有 ${number(stale)} 个运行中任务超过 15 分钟未更新，建议先确认回补进程是否还在。</div>`
        : "";
      return `
        <div class="progress-item">
          <div class="progress-title">
            <span class="list-title">${escapeHtml(title)}</span>
            ${tag(state)}
          </div>
          <div class="progress-bar">
            <div class="progress-fill ${escapeHtml(state)}" style="width:${percent}%"></div>
          </div>
          <div class="progress-meta">
            <span>${number(row.success_count)} / ${number(row.total_task_count)} 个任务</span>
            <span>${percent}%</span>
          </div>
          <dl class="progress-facts">
            <div><dt>日期范围</dt><dd>${escapeHtml(range || "-")}</dd></div>
            <div><dt>成功</dt><dd>${number(row.success_count)}</dd></div>
            <div><dt>待执行</dt><dd>${number(row.pending_count)}</dd></div>
            <div><dt>运行中</dt><dd>${number(row.running_count)}</dd></div>
            <div><dt>失败</dt><dd>${number(row.failed_count)}</dd></div>
          </dl>
          ${staleText}
          <div class="next-step">${escapeHtml(progressNextStep(row))}</div>
        </div>
      `;
    })
    .join("");
}

function progressNextStep(row) {
  const state = String(row.state || "");
  if (state === "blocked") {
    if (Number(row.failed_count || 0)) {
      return "下一步：进入回补任务页，筛选 failed，查看最后错误并重试。";
    }
    return "下一步：确认回补进程是否还在；如果已经停止，执行 recover-running。";
  }
  if (state === "running") {
    return "下一步：继续观察更新时间和最近运行记录，不需要手动干预。";
  }
  if (state === "pending") {
    return "下一步：运行 run-backfill 消费待执行任务。";
  }
  if (state === "complete") {
    return "下一步：可继续 build-factors、sync-parquet 或 export-qlib。";
  }
  return "下一步：先计划回补任务，或查看是否缺少控制表。";
}

function renderRecentJobs(rows) {
  $("recent-jobs").innerHTML = tableSummary(
    `最近 ${number(rows.length)} 条作业记录。失败时先看错误信息，再回到对应 CLI 处理。`,
  ) + table(
    [
      { key: "status", label: fieldLabel("status"), status: true },
      { key: "job_type", label: fieldLabel("job_type") },
      { key: "dataset", label: fieldLabel("dataset"), format: datasetLabel },
      { key: "source_id", label: fieldLabel("source_id"), format: sourceLabel },
      { key: "start_date", label: fieldLabel("start_date") },
      { key: "end_date", label: fieldLabel("end_date") },
      { key: "created_at", label: fieldLabel("created_at") },
      { key: "error_message", label: fieldLabel("error_message"), maxLength: 90 },
    ],
    rows,
    "暂无运行记录。执行 daily、run-backfill、build-factors 或 export-qlib 后会出现。",
  );
}

async function loadBackfillTasks() {
  try {
    setLoading("task-table");
    const query = new URLSearchParams();
    appendQuery(query, "dataset", $("task-dataset").value);
    appendQuery(query, "status", $("task-status").value);
    appendQuery(query, "limit", $("task-limit").value);
    const payload = await api(`/api/backfill-tasks?${query.toString()}`);
    $("task-table").innerHTML = tableSummary(
      `当前筛选返回 ${number(payload.task_count)} 个任务。优先看 failed、running 和 last_error。`,
    ) + table(
      [
        { key: "status", label: fieldLabel("status"), status: true },
        { key: "dataset", label: fieldLabel("dataset"), format: datasetLabel },
        { key: "source_id", label: fieldLabel("source_id"), format: sourceLabel },
        { key: "universe", label: fieldLabel("universe"), format: universeLabel },
        { key: "start_date", label: fieldLabel("start_date") },
        { key: "end_date", label: fieldLabel("end_date") },
        { key: "symbol_batch_json", label: fieldLabel("symbol_batch_json"), maxLength: 80 },
        { key: "attempt_count", label: fieldLabel("attempt_count") },
        { key: "updated_at", label: fieldLabel("updated_at") },
        { key: "last_error", label: fieldLabel("last_error"), maxLength: 120 },
      ],
      payload.tasks,
      "当前筛选条件下没有回补任务。",
    );
  } catch (error) {
    showError(friendlyError(error));
  }
}

function loadActivePreview() {
  if (activePreviewMode === "raw") {
    return loadRawInstrumentPreview();
  }
  if (activePreviewMode === "factor") {
    return loadFactorPreview();
  }
  return Promise.resolve();
}

async function loadInstrumentOptions(query = "") {
  try {
    const params = new URLSearchParams();
    appendQuery(params, "query", query);
    appendQuery(params, "limit", "80");
    const payload = await api(`/api/instruments?${params.toString()}`);
    instrumentOptions = payload.instruments || [];
    const options = $("instrument-options");
    options.innerHTML = instrumentOptions
      .map((item) => {
        const label = item.label || item.instrument;
        return `<option value="${escapeHtml(item.instrument)}" label="${escapeHtml(label)}"></option>`;
      })
      .join("");
  } catch (error) {
    showError(friendlyError(error));
  }
}

function selectedPreviewInstrument() {
  const value = $("preview-instrument").value.trim();
  if (!value) {
    return "";
  }
  const upper = value.toUpperCase();
  const exact = instrumentOptions.find((item) => String(item.instrument || "").toUpperCase() === upper);
  if (exact) {
    return exact.instrument;
  }
  const matches = instrumentOptions.filter((item) => {
    const label = `${item.instrument || ""} ${item.symbol || ""} ${item.name || ""} ${item.industry || ""}`.toUpperCase();
    return label.includes(upper);
  });
  return matches.length === 1 ? matches[0].instrument : value;
}

async function loadRawInstrumentPreview() {
  try {
    if (activePreviewMode !== "raw") {
      return;
    }
    const instrument = selectedPreviewInstrument();
    if (!instrument) {
      $("preview-selected").innerHTML = "<strong>先选择标的：</strong>可输入代码、名称或行业搜索。";
      $("raw-preview-explanation").classList.add("hidden");
      $("raw-preview-summary").classList.add("hidden");
      $("raw-preview-table").innerHTML = '<div class="empty">请输入标的后查看原始采集数据。</div>';
      return;
    }
    setLoading("raw-preview-table");
    const query = new URLSearchParams();
    appendQuery(query, "instrument", instrument);
    appendQuery(query, "start", $("preview-start").value.trim());
    appendQuery(query, "end", $("preview-end").value.trim());
    appendQuery(query, "limit", $("preview-limit").value);
    const payload = await api(`/api/raw-instrument-preview?${query.toString()}`);
    renderRawInstrumentPreview(payload);
  } catch (error) {
    showError(friendlyError(error));
  }
}

function renderRawInstrumentPreview(payload) {
  $("preview-selected").innerHTML = `<strong>当前标的：</strong>${escapeHtml(payload.instrument)}。筛选日期 ${escapeHtml(payload.start || "-")} 到 ${escapeHtml(payload.end || "-")}。`;
  $("raw-preview-explanation").classList.add("hidden");
  $("raw-preview-summary").classList.add("hidden");
  $("raw-preview-table").innerHTML = renderRawWideTable(payload.sections || []);
}

function renderRawWideTable(sections) {
  const rows = flattenRawRows(sections);
  if (!rows.length) {
    return '<div class="empty">当前标的和日期范围没有匹配的 raw 采集记录。</div>';
  }
  const columns = rawVisibleColumns(rows);
  return table(columns, rows, "当前标的和日期范围没有匹配的 raw 采集记录。", {
    pageSize: Number($("preview-limit").value || CLIENT_TABLE_PAGE_SIZE),
  });
}

function flattenRawRows(sections) {
  return sections.flatMap((section) =>
    (section.rows || []).map((row) => ({
      dataset: section.dataset,
      ...row,
    })),
  );
}

function rawVisibleColumns(rows) {
  const seen = new Set(rows.flatMap((row) => Object.keys(row)));
  const ordered = rawWideColumnOrder.filter((key) => seen.has(key));
  const extras = [...seen].filter((key) => !rawWideColumnOrder.includes(key)).sort();
  return [...ordered, ...extras].map((key) => ({
    key,
    label: fieldLabel(key),
    format: key === "dataset" ? datasetLabel : undefined,
    maxLength: key === "title" ? 180 : 120,
  }));
}

async function loadFactorPreview() {
  try {
    if (activePreviewMode !== "factor") {
      return;
    }
    const instrument = selectedPreviewInstrument();
    if (!instrument) {
      $("preview-selected").innerHTML = "<strong>先选择标的：</strong>可输入代码、名称或行业搜索。";
      $("factor-preview-explanation").innerHTML = "";
      $("factor-preview-summary").innerHTML = "";
      $("factor-preview-table").innerHTML = '<div class="empty">请输入标的后查看处理后因子。</div>';
      return;
    }
    setLoading("factor-preview-table");
    const query = new URLSearchParams();
    appendQuery(query, "instrument", instrument);
    appendQuery(query, "start", $("preview-start").value.trim());
    appendQuery(query, "end", $("preview-end").value.trim());
    appendQuery(query, "limit", $("preview-limit").value);
    const pageSize = Number($("preview-limit").value || CLIENT_TABLE_PAGE_SIZE);
    appendQuery(query, "offset", String(Math.max(factorPreviewPage - 1, 0) * pageSize));
    const payload = await api(`/api/factor-preview?${query.toString()}`);
    renderFactorPreview(payload);
  } catch (error) {
    showError(friendlyError(error));
  }
}

function renderFactorPreview(payload) {
  const summary = payload.summary || {};
  const range = [summary.min_trade_date, summary.max_trade_date].filter(Boolean).join(" - ");
  $("preview-selected").innerHTML = `<strong>当前标的：</strong>${escapeHtml(payload.instrument)}。筛选日期 ${escapeHtml(payload.start || "-")} 到 ${escapeHtml(payload.end || "-")}。`;
  $("factor-preview-explanation").innerHTML = "<strong>处理后因子：</strong>这里展示按交易日对齐后的研究字段，会进入 gold 宽表或 Qlib 外部因子。";
  $("factor-preview-summary").innerHTML = [
    coverageCard("当前页交易日", number(summary.trade_date_count || 0), range || "当前筛选无日频记录"),
    coverageCard("核心完整天数", number(summary.core_complete_days || 0), "同时有行情、复权因子和涨跌停"),
    coverageCard("新闻记录", number(summary.news_rows || 0), `日频新闻计数 ${number(summary.factor_news_count || 0)}`),
    coverageCard("公告记录", number(summary.announcement_rows || 0), `日频公告计数 ${number(summary.factor_announcement_count || 0)}`),
  ].join("");
  $("factor-preview-table").innerHTML = renderFactorWidePreview(payload);
  bindFactorPagination(payload);
}

function renderFactorWidePreview(payload) {
  const rows = payload.timeline_rows || [];
  const newsByDate = groupDocumentsByTradeDate(payload.news_rows || []);
  const announcementsByDate = groupDocumentsByTradeDate(payload.announcement_rows || []);
  const page = Number(payload.page || 1);
  const pageSize = Number(payload.page_size || payload.limit || rows.length || 0);
  const offset = Number(payload.offset || 0);
  const rangeStart = rows.length ? offset + 1 : 0;
  const rangeEnd = offset + rows.length;
  const columns = factorWideColumns.map((key) => ({
    key,
    label: fieldLabel(key),
    format: key.endsWith("_source_id") ? sourceLabel : undefined,
    html: factorDocumentCellRenderer(key, newsByDate, announcementsByDate),
    maxLength: key === "halt_reason" ? 120 : 90,
  }));
  return [
    renderFactorCategories(),
    tableSummary(
      `第 ${number(page)} 页，每页 ${number(pageSize)} 个交易日；当前显示第 ${number(rangeStart)}-${number(rangeEnd)} 条。横向滚动可查看全部因子列。`,
    ),
    renderFactorPagination(payload),
    table(columns, rows, "当前筛选条件下没有处理后因子。", { paginate: false }),
    renderFactorPagination(payload),
  ].join("");
}

function factorDocumentCellRenderer(key, newsByDate, announcementsByDate) {
  if (key === "news_count") {
    return (row) => documentCountCell(row, "news_count", newsByDate, "news_id");
  }
  if (key === "announcement_count") {
    return (row) =>
      documentCountCell(row, "announcement_count", announcementsByDate, "announcement_id");
  }
  return undefined;
}

function groupDocumentsByTradeDate(rows) {
  return rows.reduce((groups, row) => {
    const tradeDate = String(row.trade_date || row.publish_date || "");
    if (!tradeDate) {
      return groups;
    }
    groups[tradeDate] = groups[tradeDate] || [];
    groups[tradeDate].push(row);
    return groups;
  }, {});
}

function documentCountCell(row, countKey, documentsByDate, idField) {
  const count = Number(row[countKey] || 0);
  if (!count) {
    return "0";
  }
  const documents = documentsByDate[String(row.trade_date || "")] || [];
  if (!documents.length) {
    return escapeHtml(number(count));
  }
  return `
    <details class="document-details">
      <summary>${escapeHtml(number(count))} 条</summary>
      <div class="document-detail-list">
        ${documents.map((document) => renderDocumentDetail(document, idField)).join("")}
      </div>
    </details>
  `;
}

function renderDocumentDetail(document, idField) {
  const title = escapeHtml(document.title || "-");
  const publishDate = escapeHtml(document.publish_date || "-");
  const source = escapeHtml(sourceLabel(document.source_id));
  const idText = escapeHtml(document[idField] || "");
  const url = document.url ? String(document.url) : "";
  const link = url
    ? `<a href="${escapeHtml(url)}" target="_blank" rel="noreferrer">${title}</a>`
    : title;
  return `
    <div class="document-detail-item">
      <div class="document-detail-title">${link}</div>
      <div class="document-detail-meta">${publishDate} / ${source}${idText ? ` / ${idText}` : ""}</div>
    </div>
  `;
}

function renderFactorCategories() {
  return `
    <div class="factor-category-grid">
      ${factorColumnGroups.map(renderFactorCategory).join("")}
    </div>
  `;
}

function renderFactorCategory(group) {
  const fields = group.fields
    .map((field) => `<span class="factor-chip">${escapeHtml(fieldLabel(field))}</span>`)
    .join("");
  return `
    <section class="factor-category-card">
      <div class="factor-category-title">${escapeHtml(group.title)}</div>
      <p>${escapeHtml(group.description)}</p>
      <div class="factor-chip-list">${fields}</div>
    </section>
  `;
}

function renderFactorPagination(payload) {
  const page = Number(payload.page || 1);
  const hasPrevious = Boolean(payload.has_previous);
  const hasNext = Boolean(payload.has_next);
  return `
    <div class="pagination-bar">
      <button class="btn factor-prev-page" type="button" ${hasPrevious ? "" : "disabled"}>上一页</button>
      <span class="pagination-info">第 ${number(page)} 页</span>
      <button class="btn factor-next-page" type="button" ${hasNext ? "" : "disabled"}>下一页</button>
    </div>
  `;
}

function bindFactorPagination(payload) {
  factorPreviewPage = Number(payload.page || factorPreviewPage || 1);
  const previousButtons = document.querySelectorAll(".factor-prev-page");
  const nextButtons = document.querySelectorAll(".factor-next-page");
  previousButtons.forEach((button) => {
    button.addEventListener("click", () => {
      factorPreviewPage = Math.max(1, factorPreviewPage - 1);
      loadFactorPreview();
    });
  });
  nextButtons.forEach((button) => {
    button.addEventListener("click", () => {
      factorPreviewPage += 1;
      loadFactorPreview();
    });
  });
}

async function loadQualityIssues() {
  try {
    setLoading("quality-table");
    const query = new URLSearchParams();
    appendQuery(query, "dataset", $("quality-dataset").value);
    appendQuery(query, "status", $("quality-status").value);
    appendQuery(query, "limit", $("quality-limit").value);
    const payload = await api(`/api/quality-issues?${query.toString()}`);
    $("quality-table").innerHTML = tableSummary(
      `当前筛选返回 ${number(payload.issue_count)} 条质量问题。建议先处理 open 状态。`,
    ) + table(
      [
        { key: "status", label: fieldLabel("status"), status: true },
        { key: "severity", label: fieldLabel("severity"), status: true },
        { key: "dataset", label: fieldLabel("dataset"), format: datasetLabel },
        { key: "source_id", label: fieldLabel("source_id"), format: sourceLabel },
        { key: "issue_type", label: fieldLabel("issue_type") },
        { key: "entity_key", label: fieldLabel("entity_key") },
        { key: "message", label: fieldLabel("message"), maxLength: 140 },
        { key: "created_at", label: fieldLabel("created_at") },
      ],
      payload.issues,
      "当前筛选条件下没有质量问题。",
    );
  } catch (error) {
    showError(friendlyError(error));
  }
}

async function loadQlibObjects() {
  try {
    setLoading("qlib-objects");
    const query = new URLSearchParams();
    appendQuery(query, "dataset", "qlib_export");
    appendQuery(query, "limit", "80");
    const payload = await api(`/api/source-objects?${query.toString()}`);
    $("qlib-objects").innerHTML = tableSummary(
      `已登记 ${number(payload.object_count)} 个 Qlib 导出文件索引。`,
    ) + table(
      [
        { key: "layer", label: fieldLabel("layer"), status: true },
        { key: "uri", label: fieldLabel("uri"), maxLength: 120 },
        { key: "size_bytes", label: fieldLabel("bytes") },
        { key: "created_at", label: fieldLabel("created_at") },
      ],
      payload.objects,
      "暂无 Qlib 导出文件。先执行 export-qlib。",
    );
  } catch (error) {
    showError(friendlyError(error));
  }
}

function renderQlibJobs(rows) {
  $("qlib-jobs").innerHTML = tableSummary(
    `最近 ${number(rows.length)} 次 Qlib 导出记录。成功后再用 verify-qlib 验证可读性。`,
  ) + table(
    [
      { key: "status", label: fieldLabel("status"), status: true },
      { key: "start_date", label: fieldLabel("start_date") },
      { key: "end_date", label: fieldLabel("end_date") },
      {
        key: "parameters_json",
        label: fieldLabel("provider"),
        maxLength: 110,
        value: (row) => row.parameters_json?.provider_uri || "",
      },
      {
        key: "parameters_json",
        label: fieldLabel("files"),
        value: (row) => row.parameters_json?.file_count || 0,
      },
      { key: "created_at", label: fieldLabel("created_at") },
    ],
    rows,
    "暂无 Qlib 导出记录。先执行 export-qlib。",
  );
}

function appendQuery(query, key, value) {
  if (value !== null && value !== undefined && value !== "") {
    query.set(key, value);
  }
}

async function refreshAll() {
  showError(null);
  try {
    if (!overview) {
      setLoading("recent-jobs");
      setLoading("watermark-list");
      setLoading("progress-list");
    }
    const payload = await api("/api/overview");
    renderOverview(payload);
    await Promise.all([
      loadBackfillTasks(),
      loadInstrumentOptions($("preview-instrument").value.trim()),
      loadActivePreview(),
      loadQualityIssues(),
      loadQlibObjects(),
    ]);
  } catch (error) {
    showError(friendlyError(error));
  }
}

function friendlyError(error) {
  if (error?.httpStatus === 503) {
    return new Error("DuckDB 正在写入，页面已保留上次快照并会自动刷新。");
  }
  return error;
}

function init() {
  populateSelect("task-dataset", datasets, "全部数据集", datasetLabel);
  populateSelect("quality-dataset", datasets, "全部数据集", datasetLabel);
  $("preview-instrument").value = "SH600000";
  bindNav();
  bindPreviewMode();
  bindTablePagination();
  bindFilters();
  bindTablePagination();
  refreshAll();
  autoRefreshTimer = window.setInterval(refreshAll, 15000);
  window.addEventListener("beforeunload", () => window.clearInterval(autoRefreshTimer));
}

init();
