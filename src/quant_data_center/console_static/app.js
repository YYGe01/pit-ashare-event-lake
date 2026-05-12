const DAILY_DATASETS = [
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

const INSTRUMENT_FILTER_DATASETS = new Set([
  "universe_constituent",
  "daily_bar",
  "adj_factor",
  "price_limit",
  "trade_status",
  "announcement",
  "news",
  "daily_news_factor",
  "daily_announcement_factor",
]);

const DATE_FILTER_DATASETS = new Set([
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
]);

const DAILY_JOB_TYPES = new Set([
  "daily",
  "daily_pipeline",
  "quality",
  "export_qlib",
  "build_factors",
  "sync_parquet",
]);

const DATASET_LABELS = {
  stock_basic: "证券主数据",
  universe_constituent: "股票池成分",
  trade_calendar: "交易日历",
  daily_bar: "日线行情",
  adj_factor: "复权因子",
  price_limit: "涨跌停价格",
  trade_status: "交易状态",
  announcement: "公告明细",
  news: "新闻明细",
  daily_news_factor: "新闻日频因子",
  daily_announcement_factor: "公告日频因子",
};

const JOB_LABELS = {
  daily: "每日结构化采集",
  daily_pipeline: "每日流水线",
  quality: "质量检查",
  export_qlib: "Qlib 导出",
  build_factors: "因子加工",
  sync_parquet: "Parquet 同步",
};

const FIELD_LABELS = {
  status: "状态",
  job_type: "作业类型",
  dataset: "数据集",
  source_id: "数据源",
  universe: "股票池",
  start_date: "开始日期",
  end_date: "结束日期",
  created_at: "创建时间",
  updated_at: "更新时间",
  start_at: "开始时间",
  end_at: "结束时间",
  error_message: "错误信息",
  parameters_json: "参数",
  min_date: "最早日期",
  max_date: "最晚日期",
  row_count: "行数",
  total_row_count: "总行数",
  filtered_row_count: "筛选行数",
  date_count: "日期数",
  instrument_count: "标的数",
  source_ids: "来源",
  trade_date: "交易日",
  publish_date: "发布日期",
  snapshot_date: "快照日期",
  instrument: "标的",
  symbol: "代码",
  exchange: "交易所",
  name: "名称",
  industry: "行业",
  open: "开盘",
  high: "最高",
  low: "最低",
  close: "收盘",
  pre_close: "昨收",
  volume: "成交量",
  amount: "成交额",
  vwap: "成交均价",
  adj_factor: "复权因子",
  factor_type: "因子类型",
  limit_up: "涨停价",
  limit_down: "跌停价",
  prev_close: "前收盘",
  limit_rule: "涨跌停规则",
  trade_status: "交易状态",
  halt_reason: "停牌原因",
  news_count: "新闻数量",
  announcement_count: "公告数量",
  title: "标题",
  url: "链接",
  severity: "严重级别",
  issue_type: "问题类型",
  entity_key: "实体键",
  message: "说明",
};

const STATUS_LABELS = {
  ok: "正常",
  success: "成功",
  failed: "失败",
  partial: "部分完成",
  running: "运行中",
  pending: "等待数据",
  warning: "需关注",
  open: "未关闭",
  closed: "已关闭",
  missing: "缺失",
  observed: "已观测",
  complete: "完整",
  empty: "暂无",
};

const STAGES = [
  {
    id: "base",
    title: "基础资料",
    description: "标的、股票池和交易日历是每日链路的定位基准。",
    datasets: ["stock_basic", "universe_constituent", "trade_calendar"],
  },
  {
    id: "core",
    title: "核心日频",
    description: "行情、复权和涨跌停是训练与导出的硬依赖。",
    datasets: ["daily_bar", "adj_factor", "price_limit"],
  },
  {
    id: "state",
    title: "交易状态",
    description: "停复牌和异常交易状态用于解释缺口。",
    datasets: ["trade_status"],
  },
  {
    id: "events",
    title: "事件源",
    description: "公告和新闻为日频文本因子提供输入。",
    datasets: ["announcement", "news"],
  },
  {
    id: "factors",
    title: "文本因子",
    description: "把事件源聚合到 instrument + trade_date。",
    datasets: ["daily_news_factor", "daily_announcement_factor"],
  },
];

const PAGE_COPY = {
  dashboard: {
    title: "今日总览",
    summary: "只看每日采集链路：运行状态、数据水位、单标的数据预览和质量信号。",
  },
  data: {
    title: "数据预览",
    summary: "查看每日相关数据集覆盖、最新记录，以及单标的原始输入和处理后因子。",
  },
  quality: {
    title: "质量信号",
    summary: "优先处理未关闭问题，避免异常数据进入后续研究和导出。",
  },
};

const moneyFormatter = new Intl.NumberFormat("zh-CN");
const $ = (id) => document.getElementById(id);

let state = {
  overview: null,
  jobs: [],
  qualityIssues: [],
  activeSection: "dashboard",
  activeInstrumentMode: "factor",
  dateDefaultsApplied: false,
  autoRefreshTimer: null,
  instrumentSearchTimer: null,
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatNumber(value) {
  return moneyFormatter.format(Number(value || 0));
}

function compact(value, maxLength = 120) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  if (Array.isArray(value)) {
    return value.length ? value.join(", ") : "-";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  const text = String(value);
  return text.length > maxLength ? `${text.slice(0, maxLength)}...` : text;
}

function datasetLabel(value) {
  const key = String(value || "");
  return DATASET_LABELS[key] ? `${DATASET_LABELS[key]} (${key})` : key || "-";
}

function jobLabel(value) {
  const key = String(value || "");
  return JOB_LABELS[key] ? `${JOB_LABELS[key]} (${key})` : key || "-";
}

function fieldLabel(value) {
  const key = String(value || "");
  return FIELD_LABELS[key] || key || "-";
}

function statusLabel(value) {
  const key = String(value || "");
  return STATUS_LABELS[key] ? `${STATUS_LABELS[key]} (${key})` : key || "-";
}

function statusClass(value) {
  const key = String(value || "default").toLowerCase().replaceAll("_", "-");
  if (["success", "ok", "complete", "observed"].includes(key)) {
    return "success";
  }
  if (["failed", "missing"].includes(key)) {
    return "danger";
  }
  if (["partial", "warning", "pending", "empty"].includes(key)) {
    return "warning";
  }
  if (["running", "open"].includes(key)) {
    return "running";
  }
  return key;
}

function tag(value) {
  return `<span class="tag tag-${statusClass(value)}">${escapeHtml(statusLabel(value))}</span>`;
}

function setHidden(id, hidden) {
  const target = $(id);
  if (target) {
    target.classList.toggle("hidden", hidden);
  }
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
    return new Error("DuckDB 正在写入，页面保留上次结果并会继续刷新。");
  }
  return error;
}

function appendQuery(query, key, value) {
  if (value !== null && value !== undefined && String(value).trim() !== "") {
    query.set(key, String(value).trim());
  }
}

function table(columns, rows, emptyText = "暂无数据") {
  if (!rows || rows.length === 0) {
    return `<div class="empty">${escapeHtml(emptyText)}</div>`;
  }
  return `
    <div class="table-wrap">
      <table class="data-table">
        <thead>
          <tr>${columns.map((column) => `<th>${escapeHtml(column.label)}</th>`).join("")}</tr>
        </thead>
        <tbody>
          ${rows.map((row) => `
            <tr>
              ${columns.map((column) => `<td>${cell(column, row)}</td>`).join("")}
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>
  `;
}

function cell(column, row) {
  if (column.html) {
    return column.html(row);
  }
  const raw = column.value ? column.value(row) : row[column.key];
  if (column.status) {
    return tag(raw);
  }
  const value = column.format ? column.format(raw, row) : raw;
  const text = compact(value, column.maxLength || 120);
  if (column.url && raw) {
    return `<a href="${escapeHtml(String(raw))}" target="_blank" rel="noreferrer">${escapeHtml(text)}</a>`;
  }
  return escapeHtml(text);
}

function summaryCard(label, value, foot = "") {
  return `
    <div class="summary-card">
      <div class="summary-label">${escapeHtml(label)}</div>
      <div class="summary-value">${escapeHtml(value)}</div>
      ${foot ? `<div class="summary-foot">${escapeHtml(foot)}</div>` : ""}
    </div>
  `;
}

function kpiCard(label, value, foot = "", level = "") {
  return `
    <div class="kpi-card ${level ? `kpi-${level}` : ""}">
      <div class="kpi-label">${escapeHtml(label)}</div>
      <div class="kpi-value">${escapeHtml(value)}</div>
      ${foot ? `<div class="kpi-foot">${escapeHtml(foot)}</div>` : ""}
    </div>
  `;
}

function progressBar(percent, status) {
  const safePercent = Math.max(0, Math.min(100, Number(percent || 0)));
  return `
    <div class="progress-bar">
      <div class="progress-fill ${statusClass(status)}" style="width: ${safePercent}%"></div>
    </div>
  `;
}

function latestDailyDate(overview, jobs) {
  const dailyJob = jobs.find((job) => ["daily_pipeline", "daily"].includes(job.job_type));
  if (dailyJob?.end_date || dailyJob?.start_date) {
    return dailyJob.end_date || dailyJob.start_date;
  }
  const watermarkDates = (overview?.watermarks || [])
    .map((row) => row.max_date)
    .filter(Boolean)
    .sort();
  return watermarkDates.at(-1) || null;
}

function latestWatermarkByDataset(overview) {
  const result = {};
  (overview?.watermarks || []).forEach((row) => {
    if (!DAILY_DATASETS.includes(row.dataset)) {
      return;
    }
    const current = result[row.dataset];
    if (!current || String(row.max_date || "") > String(current.max_date || "")) {
      result[row.dataset] = row;
    }
  });
  return result;
}

function coverageRowByDataset(overview) {
  const result = {};
  (overview?.data_coverage?.dataset_rows || []).forEach((row) => {
    result[row.dataset] = row;
  });
  return result;
}

function compareDate(left, right) {
  if (!left || !right) {
    return false;
  }
  return String(left) >= String(right);
}

function datasetState(dataset, targetDate, coverageRows, watermarks, counts) {
  const coverage = coverageRows[dataset] || {};
  const watermark = watermarks[dataset] || {};
  const rowCount = Number(coverage.row_count ?? counts[dataset] ?? 0);
  const maxDate = coverage.max_date || watermark.max_date || null;
  if (!rowCount) {
    return { status: "pending", rowCount, maxDate, note: "暂无行" };
  }
  if (!targetDate || !maxDate) {
    return { status: "success", rowCount, maxDate, note: "已有数据" };
  }
  if (compareDate(maxDate, targetDate)) {
    return { status: "success", rowCount, maxDate, note: `已到 ${maxDate}` };
  }
  return { status: "warning", rowCount, maxDate, note: `水位 ${maxDate}` };
}

function renderDashboard() {
  const overview = state.overview;
  const jobs = state.jobs;
  const targetDate = latestDailyDate(overview, jobs);
  const latestDailyJob = jobs.find((job) => ["daily_pipeline", "daily"].includes(job.job_type));
  const openIssues = state.qualityIssues.length;
  const counts = overview?.silver_table_counts || {};
  const dailyRows = DAILY_DATASETS.reduce((sum, dataset) => sum + Number(counts[dataset] || 0), 0);
  const latestExport = (overview?.latest_qlib_exports || [])[0];

  $("database-state").outerHTML = overview?.database_exists
    ? '<span id="database-state" class="tag tag-success">已连接</span>'
    : '<span id="database-state" class="tag tag-warning">未初始化</span>';
  $("database-path").textContent = overview?.database_path || "-";
  $("last-refresh").textContent = `刷新时间 ${new Date().toLocaleTimeString("zh-CN", { hour12: false })}`;

  $("hero-date").textContent = targetDate || "暂无每日日期";
  $("hero-status").className = `hero-status hero-${statusClass(latestDailyJob?.status || "pending")}`;
  $("hero-status").textContent = latestDailyJob ? statusLabel(latestDailyJob.status) : "尚未运行";
  $("hero-summary").textContent = latestDailyJob
    ? `${jobLabel(latestDailyJob.job_type)}，${latestDailyJob.start_date || "-"} 到 ${latestDailyJob.end_date || "-"}，创建于 ${latestDailyJob.created_at || "-"}。`
    : "还没有每日采集作业记录；页面将展示已有数据水位。";

  $("kpi-grid").innerHTML = [
    kpiCard("最近每日日期", targetDate || "-", "来自 daily / daily_pipeline 作业或数据水位"),
    kpiCard("最近运行状态", latestDailyJob ? statusLabel(latestDailyJob.status) : "未运行", latestDailyJob ? jobLabel(latestDailyJob.job_type) : "暂无作业记录", statusClass(latestDailyJob?.status)),
    kpiCard("每日相关行数", formatNumber(dailyRows), "Silver 层每日相关数据集总行数"),
    kpiCard("未关闭质量问题", formatNumber(openIssues), "open 状态 issue", openIssues ? "danger" : "success"),
    kpiCard("最近 Qlib 导出", latestExport ? statusLabel(latestExport.status) : "暂无", latestExport?.end_date ? `覆盖到 ${latestExport.end_date}` : "仅展示每日链路产物"),
  ].join("");

  renderStages(targetDate);
  renderRecentJobs();
  renderWatermarks();
}

function renderStages(targetDate) {
  const overview = state.overview;
  const coverageRows = coverageRowByDataset(overview);
  const watermarks = latestWatermarkByDataset(overview);
  const counts = overview?.silver_table_counts || {};
  $("daily-stage-grid").innerHTML = STAGES.map((stage) => {
    const datasetStates = stage.datasets.map((dataset) => ({
      dataset,
      ...datasetState(dataset, targetDate, coverageRows, watermarks, counts),
    }));
    const completeCount = datasetStates.filter((item) => item.status === "success").length;
    const hasWarning = datasetStates.some((item) => item.status === "warning");
    const stageStatus = completeCount === datasetStates.length
      ? "success"
      : hasWarning || completeCount
        ? "warning"
        : "pending";
    const percent = datasetStates.length ? (completeCount / datasetStates.length) * 100 : 0;
    return `
      <article class="stage-card stage-${statusClass(stageStatus)}">
        <div class="stage-head">
          <div>
            <h3>${escapeHtml(stage.title)}</h3>
            <p>${escapeHtml(stage.description)}</p>
          </div>
          ${tag(stageStatus)}
        </div>
        ${progressBar(percent, stageStatus)}
        <div class="stage-datasets">
          ${datasetStates.map((item) => `
            <div class="dataset-pill dataset-${statusClass(item.status)}">
              <b>${escapeHtml(DATASET_LABELS[item.dataset] || item.dataset)}</b>
              <span>${escapeHtml(item.note)} · ${formatNumber(item.rowCount)} 行</span>
            </div>
          `).join("")}
        </div>
      </article>
    `;
  }).join("");
}

function visibleJobs() {
  return state.jobs.filter((job) => DAILY_JOB_TYPES.has(job.job_type));
}

function renderRecentJobs() {
  const jobs = visibleJobs().slice(0, 16);
  $("recent-jobs").innerHTML = table(
    [
      { key: "status", label: fieldLabel("status"), status: true },
      { key: "job_type", label: fieldLabel("job_type"), format: jobLabel, maxLength: 80 },
      { key: "dataset", label: fieldLabel("dataset"), format: datasetLabel },
      { key: "start_date", label: fieldLabel("start_date") },
      { key: "end_date", label: fieldLabel("end_date") },
      {
        key: "parameters_json",
        label: "进度",
        value: (row) => {
          const params = row.parameters_json || {};
          const parts = [];
          if (params.planned_count !== undefined) parts.push(`计划 ${formatNumber(params.planned_count)}`);
          if (params.ran_count !== undefined) parts.push(`执行 ${formatNumber(params.ran_count)}`);
          if (params.crawl_ran_count !== undefined) parts.push(`爬虫 ${formatNumber(params.crawl_ran_count)}`);
          if (params.quality_status) parts.push(`质量 ${params.quality_status}`);
          return parts.join(" / ") || "-";
        },
        maxLength: 120,
      },
      { key: "created_at", label: fieldLabel("created_at"), maxLength: 100 },
      { key: "error_message", label: fieldLabel("error_message"), maxLength: 160 },
    ],
    jobs,
    "暂无每日链路作业记录。",
  );
}

function renderWatermarks() {
  const rows = (state.overview?.watermarks || [])
    .filter((row) => DAILY_DATASETS.includes(row.dataset))
    .sort((left, right) => String(right.max_date || "").localeCompare(String(left.max_date || "")))
    .slice(0, 12);
  if (!rows.length) {
    $("watermark-list").innerHTML = '<div class="empty">暂无数据水位。</div>';
    return;
  }
  $("watermark-list").innerHTML = rows.map((row) => `
    <div class="list-item">
      <div class="list-title">${escapeHtml(datasetLabel(row.dataset))}</div>
      <div class="list-meta">${escapeHtml(row.min_date || "-")} 到 ${escapeHtml(row.max_date || "-")}</div>
      <div class="list-meta">${escapeHtml(row.source_id || "-")} · ${escapeHtml(row.universe || "默认")} · ${escapeHtml(row.last_success_at || row.updated_at || "-")}</div>
    </div>
  `).join("");
}

function renderCoverage() {
  const coverage = state.overview?.data_coverage || {};
  const reference = coverage.reference || {};
  const summary = coverage.instrument_summary || {};
  $("coverage-reference").innerHTML = [
    summaryCard("参考标的", formatNumber(reference.instrument_count || 0), reference.instrument_source || "none"),
    summaryCard("交易日", formatNumber(reference.trade_date_count || 0), `${reference.min_trade_date || "-"} 到 ${reference.max_trade_date || "-"}`),
    summaryCard("完整标的", formatNumber(summary.complete_instruments || 0), `完整率 ${summary.complete_percent ?? 0}%`),
    summaryCard("核心缺口", formatNumber(summary.missing_daily_rows || 0), "daily_bar / adj_factor / price_limit"),
  ].join("");

  const rows = (coverage.dataset_rows || []).filter((row) => DAILY_DATASETS.includes(row.dataset));
  $("coverage-dataset-table").innerHTML = table(
    [
      { key: "dataset", label: fieldLabel("dataset"), format: datasetLabel, maxLength: 90 },
      { key: "row_count", label: fieldLabel("row_count"), value: (row) => formatNumber(row.row_count) },
      { key: "min_date", label: fieldLabel("min_date") },
      { key: "max_date", label: fieldLabel("max_date") },
      { key: "date_count", label: fieldLabel("date_count"), value: (row) => row.date_count === null ? "-" : formatNumber(row.date_count) },
      { key: "instrument_count", label: fieldLabel("instrument_count"), value: (row) => row.instrument_count === null ? "-" : formatNumber(row.instrument_count) },
      { key: "daily_coverage_percent", label: "核心覆盖率", value: (row) => row.daily_coverage_percent === null ? "-" : `${row.daily_coverage_percent}%` },
    ],
    rows,
    "暂无覆盖数据。",
  );
}

async function loadDatasetPreview() {
  try {
    const dataset = $("dataset-select").value;
    const query = new URLSearchParams();
    appendQuery(query, "dataset", dataset);
    if (INSTRUMENT_FILTER_DATASETS.has(dataset)) {
      appendQuery(query, "instrument", $("dataset-instrument").value);
    }
    if (DATE_FILTER_DATASETS.has(dataset)) {
      appendQuery(query, "start", $("dataset-start").value);
      appendQuery(query, "end", $("dataset-end").value);
    }
    appendQuery(query, "limit", $("dataset-limit").value);
    $("dataset-preview-table").innerHTML = '<div class="empty">加载数据集预览...</div>';
    const payload = await api(`/api/dataset-preview?${query.toString()}`);
    renderDatasetPreview(payload);
  } catch (error) {
    showError(friendlyError(error));
  }
}

function renderDatasetPreview(payload) {
  const summary = payload.summary || {};
  $("dataset-preview-summary").innerHTML = [
    summaryCard("数据集", datasetLabel(payload.dataset), payload.supports_instrument_filter ? "支持标的筛选" : "全表预览"),
    summaryCard("筛选行数", formatNumber(summary.filtered_row_count || 0), `总行数 ${formatNumber(summary.total_row_count || 0)}`),
    summaryCard("日期范围", `${summary.min_date || "-"} 到 ${summary.max_date || "-"}`, `日期数 ${summary.date_count ?? "-"}`),
    summaryCard("标的数", summary.instrument_count === null ? "-" : formatNumber(summary.instrument_count || 0), (summary.source_ids || []).map((item) => item.source_id).join(", ") || "无 source_id"),
  ].join("");
  const columns = (payload.columns?.length ? payload.columns : Object.keys(payload.rows?.[0] || {})).map((key) => ({
    key,
    label: fieldLabel(key),
    format: key === "dataset" ? datasetLabel : undefined,
    url: key === "url",
    maxLength: key === "title" ? 180 : 120,
  }));
  $("dataset-preview-table").innerHTML = table(columns, payload.rows || [], "当前筛选条件下没有记录。");
}

async function loadInstrumentPreview() {
  try {
    const instrument = $("preview-instrument").value.trim();
    if (!instrument) {
      $("instrument-preview-table").innerHTML = '<div class="empty">请输入 instrument 后查看。</div>';
      return;
    }
    const query = new URLSearchParams();
    appendQuery(query, "instrument", instrument);
    appendQuery(query, "start", $("preview-start").value);
    appendQuery(query, "end", $("preview-end").value);
    appendQuery(query, "limit", $("preview-limit").value);
    $("instrument-preview-table").innerHTML = '<div class="empty">加载标的预览...</div>';
    const endpoint = state.activeInstrumentMode === "raw" ? "/api/raw-instrument-preview" : "/api/factor-preview";
    const payload = await api(`${endpoint}?${query.toString()}`);
    if (state.activeInstrumentMode === "raw") {
      renderRawInstrumentPreview(payload);
    } else {
      renderFactorPreview(payload);
    }
  } catch (error) {
    showError(friendlyError(error));
  }
}

function renderFactorPreview(payload) {
  const summary = payload.summary || {};
  $("instrument-preview-summary").innerHTML = [
    summaryCard("标的", payload.instrument || "-", `${payload.start || "-"} 到 ${payload.end || "-"}`),
    summaryCard("日频行", formatNumber(summary.trade_date_count || 0), `${summary.min_trade_date || "-"} 到 ${summary.max_trade_date || "-"}`),
    summaryCard("核心完整天数", formatNumber(summary.core_complete_days || 0), "行情 + 复权 + 涨跌停"),
    summaryCard("事件因子", `${formatNumber(summary.factor_news_count || 0)} / ${formatNumber(summary.factor_announcement_count || 0)}`, "新闻 / 公告计数"),
  ].join("");
  const preferred = [
    "trade_date",
    "instrument",
    "close",
    "volume",
    "amount",
    "adj_factor",
    "limit_up",
    "limit_down",
    "trade_status",
    "news_count",
    "announcement_count",
  ];
  const keys = uniqueKeys(payload.timeline_rows || [], preferred);
  $("instrument-preview-table").innerHTML = table(
    keys.map((key) => ({ key, label: fieldLabel(key), maxLength: 120 })),
    payload.timeline_rows || [],
    "当前标的没有处理后因子记录。",
  );
}

function renderRawInstrumentPreview(payload) {
  const summary = payload.summary || {};
  $("instrument-preview-summary").innerHTML = [
    summaryCard("标的", payload.instrument || "-", `${payload.start || "-"} 到 ${payload.end || "-"}`),
    summaryCard("数据集", formatNumber(summary.dataset_count || 0), (summary.datasets || []).map((item) => DATASET_LABELS[item] || item).join(", ") || "无"),
    summaryCard("raw 对象", formatNumber(summary.object_count || 0), "最近 raw JSON 扫描结果"),
    summaryCard("匹配行", formatNumber(summary.row_count || 0), "按标的和日期过滤"),
  ].join("");
  const sections = payload.sections || [];
  if (!sections.length) {
    $("instrument-preview-table").innerHTML = '<div class="empty">当前标的没有匹配的原始输入。</div>';
    return;
  }
  $("instrument-preview-table").innerHTML = sections.map((section) => {
    const columns = (section.columns?.length ? section.columns : Object.keys(section.rows?.[0] || {})).map((key) => ({
      key,
      label: fieldLabel(key),
      url: key === "url",
      maxLength: key === "title" ? 180 : 120,
    }));
    return `
      <div class="preview-section">
        <div class="preview-section-title">${escapeHtml(datasetLabel(section.dataset))} · ${formatNumber(section.row_count || 0)} 行 · ${formatNumber(section.object_count || 0)} 个 raw 对象</div>
        ${table(columns, section.rows || [], "该数据集没有匹配行。")}
      </div>
    `;
  }).join("");
}

function uniqueKeys(rows, preferred) {
  const seen = new Set();
  const keys = [];
  preferred.forEach((key) => {
    if (rows.some((row) => row[key] !== undefined)) {
      seen.add(key);
      keys.push(key);
    }
  });
  rows.forEach((row) => {
    Object.keys(row).forEach((key) => {
      if (!seen.has(key)) {
        seen.add(key);
        keys.push(key);
      }
    });
  });
  return keys;
}

function renderQuality() {
  const issues = state.qualityIssues;
  const bySeverity = issues.reduce((groups, issue) => {
    const key = issue.severity || "unknown";
    groups[key] = (groups[key] || 0) + 1;
    return groups;
  }, {});
  const affectedDatasets = new Set(issues.map((issue) => issue.dataset).filter(Boolean));
  $("quality-kpis").innerHTML = [
    summaryCard("未关闭问题", formatNumber(issues.length), "open 状态"),
    summaryCard("严重级别", Object.entries(bySeverity).map(([key, value]) => `${key}:${value}`).join(" / ") || "无", "按 severity 汇总"),
    summaryCard("影响数据集", formatNumber(affectedDatasets.size), [...affectedDatasets].map((item) => DATASET_LABELS[item] || item).join(", ") || "无"),
    summaryCard("建议", issues.length ? "先处理最新问题" : "当前无 open 问题", "每日链路跑完后复查"),
  ].join("");
  $("quality-table").innerHTML = table(
    [
      { key: "severity", label: fieldLabel("severity"), status: true },
      { key: "dataset", label: fieldLabel("dataset"), format: datasetLabel },
      { key: "source_id", label: fieldLabel("source_id") },
      { key: "issue_type", label: fieldLabel("issue_type") },
      { key: "entity_key", label: fieldLabel("entity_key"), maxLength: 120 },
      { key: "message", label: fieldLabel("message"), maxLength: 180 },
      { key: "created_at", label: fieldLabel("created_at") },
    ],
    issues,
    "当前没有未关闭质量问题。",
  );
}

async function loadInstrumentOptions(query = "") {
  const params = new URLSearchParams();
  appendQuery(params, "query", query);
  appendQuery(params, "limit", "200");
  const payload = await api(`/api/instruments?${params.toString()}`);
  const options = (payload.instruments || []).map((item) => `
    <option value="${escapeHtml(item.instrument)}" label="${escapeHtml(item.label || item.instrument)}"></option>
  `).join("");
  $("instrument-options").innerHTML = options;
}

function applyDateDefaults() {
  if (state.dateDefaultsApplied) {
    return;
  }
  const targetDate = latestDailyDate(state.overview, state.jobs);
  if (!targetDate) {
    return;
  }
  ["dataset-start", "dataset-end", "preview-start", "preview-end"].forEach((id) => {
    if (!$(id).value) {
      $(id).value = targetDate;
    }
  });
  state.dateDefaultsApplied = true;
}

async function refreshAll() {
  showError(null);
  try {
    if (!state.overview) {
      $("recent-jobs").innerHTML = '<div class="empty">加载运行记录...</div>';
      $("watermark-list").innerHTML = '<div class="empty">加载数据水位...</div>';
      $("daily-stage-grid").innerHTML = '<div class="empty">加载进度...</div>';
    }
    const [overview, jobPayload, qualityPayload] = await Promise.all([
      api("/api/overview"),
      api("/api/job-runs?limit=80"),
      api("/api/quality-issues?status=open&limit=80"),
    ]);
    state.overview = overview;
    state.jobs = jobPayload.jobs || [];
    state.qualityIssues = qualityPayload.issues || [];
    applyDateDefaults();
    renderDashboard();
    renderCoverage();
    renderQuality();
  } catch (error) {
    showError(friendlyError(error));
  }
}

function populateDatasetSelect() {
  $("dataset-select").innerHTML = DAILY_DATASETS.map((dataset) => `
    <option value="${escapeHtml(dataset)}">${escapeHtml(datasetLabel(dataset))}</option>
  `).join("");
  $("dataset-select").value = "daily_bar";
}

function bindNav() {
  document.querySelectorAll(".nav-item").forEach((button) => {
    button.addEventListener("click", () => {
      state.activeSection = button.dataset.section;
      document.querySelectorAll(".nav-item").forEach((item) => item.classList.toggle("active", item === button));
      document.querySelectorAll(".page-section").forEach((section) => section.classList.toggle("active", section.id === state.activeSection));
      const copy = PAGE_COPY[state.activeSection] || PAGE_COPY.dashboard;
      $("page-title").textContent = copy.title;
      $("page-summary").textContent = copy.summary;
    });
  });
}

function bindControls() {
  $("refresh-btn").addEventListener("click", refreshAll);
  $("load-dataset-preview").addEventListener("click", loadDatasetPreview);
  $("load-instrument-preview").addEventListener("click", loadInstrumentPreview);
  $("dataset-select").addEventListener("change", loadDatasetPreview);

  ["preview-instrument", "dataset-instrument"].forEach((id) => {
    $(id).addEventListener("input", (event) => {
      window.clearTimeout(state.instrumentSearchTimer);
      state.instrumentSearchTimer = window.setTimeout(() => {
        loadInstrumentOptions(event.target.value.trim()).catch((error) => showError(friendlyError(error)));
      }, 250);
    });
  });

  document.querySelectorAll(".segment-button").forEach((button) => {
    button.addEventListener("click", () => {
      state.activeInstrumentMode = button.dataset.instrumentMode;
      document.querySelectorAll(".segment-button").forEach((item) => item.classList.toggle("active", item === button));
      loadInstrumentPreview();
    });
  });
}

function init() {
  populateDatasetSelect();
  $("preview-instrument").value = "SH600000";
  bindNav();
  bindControls();
  refreshAll().then(() => {
    loadInstrumentOptions().catch((error) => showError(friendlyError(error)));
    loadDatasetPreview();
    loadInstrumentPreview();
  });
  state.autoRefreshTimer = window.setInterval(refreshAll, 15000);
  window.addEventListener("beforeunload", () => window.clearInterval(state.autoRefreshTimer));
}

init();
