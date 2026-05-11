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

const pageTitles = {
  dashboard: "总览",
  backfill: "回补任务",
  dataset: "数据预览",
  quality: "质量检查",
  qlib: "Qlib 导出",
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
  title: "标题",
  url: "链接",
  announcement_id: "公告编号",
  news_id: "新闻编号",
  news_count: "新闻数量",
  announcement_count: "公告数量",
  news_sentiment_mean: "新闻情绪均值",
  announcement_sentiment_mean: "公告情绪均值",
};

const $ = (id) => document.getElementById(id);

let overview = null;
let activeSection = "dashboard";
let autoRefreshTimer = null;

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

function table(columns, rows, emptyText = "暂无数据") {
  if (!rows || rows.length === 0) {
    return `<div class="empty">${escapeHtml(emptyText)}</div>`;
  }
  const head = columns.map((column) => `<th>${escapeHtml(column.label)}</th>`).join("");
  const body = rows
    .map((row) => {
      const cells = columns
        .map((column) => {
          const raw = column.value ? column.value(row) : row[column.key];
          const display = column.format ? column.format(raw, row) : raw;
          const value = column.status ? tag(raw) : escapeHtml(compact(display, column.maxLength));
          const title = escapeHtml(compact(display, 400));
          return `<td title="${title}">${value}</td>`;
        })
        .join("");
      return `<tr>${cells}</tr>`;
    })
    .join("");
  return `<div class="table-wrap"><table class="data-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
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
    });
  });
}

function bindFilters() {
  ["task-dataset", "task-status", "task-limit"].forEach((id) => {
    $(id).addEventListener("change", loadBackfillTasks);
  });
  ["quality-dataset", "quality-status", "quality-limit"].forEach((id) => {
    $(id).addEventListener("change", loadQualityIssues);
  });
  ["preview-dataset", "preview-limit"].forEach((id) => {
    $(id).addEventListener("change", loadDatasetPreview);
  });
  ["preview-instrument", "preview-start", "preview-end"].forEach((id) => {
    $(id).addEventListener("change", loadDatasetPreview);
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
  const totalTasks = progressRows.reduce((sum, row) => sum + Number(row.total_task_count || 0), 0);
  const successPercent = totalTasks ? Math.round((successTasks / totalTasks) * 100) : 0;

  $("kpi-grid").innerHTML = [
    kpi("统一研究层行数", silverTotal, `${Object.keys(silverCounts).length} 个表`),
    kpi("回补任务", backfillTotal, `完成率 ${successPercent}%`),
    kpi("运行记录", tableCounts.job_run || 0, "作业记录 job_run"),
    kpi("阻塞任务", blockedCount, `失败 ${number(statusCounts.failed || 0)}`),
    kpi("源文件索引", sourceTotal, "source_object"),
    kpi("水位记录", tableCounts.dataset_watermark || 0, "dataset_watermark"),
    kpi("Qlib 导出", (payload.latest_qlib_exports || []).length, "最近导出"),
    kpi("质量问题", openIssues, "未关闭 open"),
  ].join("");

  $("status-overview").innerHTML = [
    statusBars("回补任务", statusCounts),
    statusBars("运行记录", payload.job_status_counts || {}),
    statusBars("质量问题", qualityCounts),
    statusBars("文件层级", payload.source_layer_counts || {}),
  ].join("");

  renderProgress(progressRows);
  renderWatermarks(payload.watermarks || []);
  renderRecentJobs(payload.latest_job_runs || []);
  renderQlibJobs(payload.latest_qlib_exports || []);
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
      <div class="status-row">
        <div class="list-title">${escapeHtml(label)}</div>
        <div class="bar"><div class="bar-fill" style="width:0"></div></div>
        <div class="muted">0</div>
      </div>
    `;
  }
  const total = entries.reduce((sum, [, count]) => sum + Number(count), 0) || 1;
  return entries
    .sort(([left], [right]) => statusOrder.indexOf(left) - statusOrder.indexOf(right))
    .map(([status, count]) => {
      const width = Math.max(2, (Number(count) / total) * 100);
      const cls = status.toLowerCase().replaceAll("_", "-");
      return `
        <div class="status-row">
          <div>${escapeHtml(label)} ${tag(status)}</div>
          <div class="bar"><div class="bar-fill ${cls}" style="width:${width}%"></div></div>
          <div class="muted">${number(count)}</div>
        </div>
      `;
    })
    .join("");
}

function renderWatermarks(rows) {
  if (!rows.length) {
    $("watermark-list").innerHTML = '<div class="empty">暂无数据</div>';
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
          <div class="list-meta">${escapeHtml(range || "-")}</div>
          <div class="list-meta">${escapeHtml(row.updated_at || "")}</div>
        </div>
      `;
    })
    .join("");
}

function renderProgress(rows) {
  if (!rows.length) {
    $("progress-list").innerHTML = '<div class="empty">暂无任务</div>';
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
      const meta = [
        `成功 ${number(row.success_count)}`,
        `待执行 ${number(row.pending_count)}`,
        `运行中 ${number(row.running_count)}`,
        `失败 ${number(row.failed_count)}`,
      ].join(" · ");
      const stale = Number(row.stale_running_count || 0);
      const staleText = stale ? `<div class="list-meta danger">超时运行中 ${number(stale)}</div>` : "";
      return `
        <div class="progress-item">
          <div class="progress-title">
            <span class="list-title">${escapeHtml(title)}</span>
            ${tag(row.state)}
          </div>
          <div class="progress-bar">
            <div class="progress-fill ${escapeHtml(row.state)}" style="width:${percent}%"></div>
          </div>
          <div class="progress-meta">
            <span>${number(row.success_count)} / ${number(row.total_task_count)} 个任务</span>
            <span>${percent}%</span>
          </div>
          <div class="list-meta">${escapeHtml(range)}</div>
          <div class="list-meta">${escapeHtml(meta)}</div>
          ${staleText}
        </div>
      `;
    })
    .join("");
}

function renderRecentJobs(rows) {
  $("recent-jobs").innerHTML = table(
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
    $("task-table").innerHTML = table(
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
    );
  } catch (error) {
    showError(friendlyError(error));
  }
}

async function loadDatasetPreview() {
  try {
    setLoading("preview-table");
    const query = new URLSearchParams();
    appendQuery(query, "dataset", $("preview-dataset").value);
    appendQuery(query, "instrument", $("preview-instrument").value.trim());
    appendQuery(query, "start", $("preview-start").value.trim());
    appendQuery(query, "end", $("preview-end").value.trim());
    appendQuery(query, "limit", $("preview-limit").value);
    const payload = await api(`/api/dataset-preview?${query.toString()}`);
    const columns = payload.columns.slice(0, 18).map((column) => ({
      key: column,
      label: fieldLabel(column),
    }));
    $("preview-table").innerHTML = table(columns, payload.rows);
  } catch (error) {
    showError(friendlyError(error));
  }
}

async function loadQualityIssues() {
  try {
    setLoading("quality-table");
    const query = new URLSearchParams();
    appendQuery(query, "dataset", $("quality-dataset").value);
    appendQuery(query, "status", $("quality-status").value);
    appendQuery(query, "limit", $("quality-limit").value);
    const payload = await api(`/api/quality-issues?${query.toString()}`);
    $("quality-table").innerHTML = table(
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
    $("qlib-objects").innerHTML = table(
      [
        { key: "layer", label: fieldLabel("layer"), status: true },
        { key: "uri", label: fieldLabel("uri"), maxLength: 120 },
        { key: "size_bytes", label: fieldLabel("bytes") },
        { key: "created_at", label: fieldLabel("created_at") },
      ],
      payload.objects,
    );
  } catch (error) {
    showError(friendlyError(error));
  }
}

function renderQlibJobs(rows) {
  $("qlib-jobs").innerHTML = table(
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
      loadDatasetPreview(),
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
  populateSelect("preview-dataset", datasets, null, datasetLabel);
  $("preview-dataset").value = "daily_bar";
  bindNav();
  bindFilters();
  refreshAll();
  autoRefreshTimer = window.setInterval(refreshAll, 15000);
  window.addEventListener("beforeunload", () => window.clearInterval(autoRefreshTimer));
}

init();
