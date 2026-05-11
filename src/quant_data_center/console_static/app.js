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

const $ = (id) => document.getElementById(id);

let overview = null;
let activeSection = "dashboard";

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

async function api(path) {
  const response = await fetch(path, { cache: "no-store" });
  const payload = await response.json();
  if (!response.ok || payload.status === "fail") {
    throw new Error(payload.error || `HTTP ${response.status}`);
  }
  return payload;
}

function showError(error) {
  const banner = $("error-banner");
  banner.textContent = error ? error.message || String(error) : "";
  banner.classList.toggle("hidden", !error);
}

function setLoading(targetId) {
  $(targetId).innerHTML = '<div class="empty">加载中</div>';
}

function tag(value) {
  const label = escapeHtml(value || "-");
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
          const value = column.status ? tag(raw) : escapeHtml(compact(raw, column.maxLength));
          const title = escapeHtml(compact(raw, 400));
          return `<td title="${title}">${value}</td>`;
        })
        .join("");
      return `<tr>${cells}</tr>`;
    })
    .join("");
  return `<div class="table-wrap"><table class="data-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}

function populateSelect(selectId, options, allLabel) {
  const select = $(selectId);
  const current = select.value;
  select.innerHTML = "";
  if (allLabel) {
    select.add(new Option(allLabel, ""));
  }
  options.forEach((option) => select.add(new Option(option, option)));
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
    kpi("Silver 行数", silverTotal, `${Object.keys(silverCounts).length} tables`),
    kpi("回补任务", backfillTotal, `完成率 ${successPercent}%`),
    kpi("运行记录", tableCounts.job_run || 0, "job_run"),
    kpi("阻塞任务", blockedCount, `failed ${number(statusCounts.failed || 0)}`),
    kpi("源文件索引", sourceTotal, "source_object"),
    kpi("水位记录", tableCounts.dataset_watermark || 0, "dataset_watermark"),
    kpi("Qlib 导出", (payload.latest_qlib_exports || []).length, "latest jobs"),
    kpi("质量问题", openIssues, "open"),
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
      const title = [row.dataset, row.source_id, row.universe].filter(Boolean).join(" / ");
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
      const title = [row.dataset, row.source_id, row.universe].filter(Boolean).join(" / ");
      const range = [row.min_date, row.max_date].filter(Boolean).join(" - ");
      const meta = [
        `success ${number(row.success_count)}`,
        `pending ${number(row.pending_count)}`,
        `running ${number(row.running_count)}`,
        `failed ${number(row.failed_count)}`,
      ].join(" · ");
      const stale = Number(row.stale_running_count || 0);
      const staleText = stale ? `<div class="list-meta danger">stale running ${number(stale)}</div>` : "";
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
            <span>${number(row.success_count)} / ${number(row.total_task_count)} tasks</span>
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
      { key: "status", label: "status", status: true },
      { key: "job_type", label: "job_type" },
      { key: "dataset", label: "dataset" },
      { key: "source_id", label: "source_id" },
      { key: "start_date", label: "start" },
      { key: "end_date", label: "end" },
      { key: "created_at", label: "created_at" },
      { key: "error_message", label: "error", maxLength: 90 },
    ],
    rows,
  );
}

async function loadBackfillTasks() {
  setLoading("task-table");
  const query = new URLSearchParams();
  appendQuery(query, "dataset", $("task-dataset").value);
  appendQuery(query, "status", $("task-status").value);
  appendQuery(query, "limit", $("task-limit").value);
  const payload = await api(`/api/backfill-tasks?${query.toString()}`);
  $("task-table").innerHTML = table(
    [
      { key: "status", label: "status", status: true },
      { key: "dataset", label: "dataset" },
      { key: "source_id", label: "source_id" },
      { key: "universe", label: "universe" },
      { key: "start_date", label: "start" },
      { key: "end_date", label: "end" },
      { key: "symbol_batch_json", label: "symbols", maxLength: 80 },
      { key: "attempt_count", label: "attempt" },
      { key: "updated_at", label: "updated_at" },
      { key: "last_error", label: "last_error", maxLength: 120 },
    ],
    payload.tasks,
  );
}

async function loadDatasetPreview() {
  setLoading("preview-table");
  const query = new URLSearchParams();
  appendQuery(query, "dataset", $("preview-dataset").value);
  appendQuery(query, "instrument", $("preview-instrument").value.trim());
  appendQuery(query, "start", $("preview-start").value.trim());
  appendQuery(query, "end", $("preview-end").value.trim());
  appendQuery(query, "limit", $("preview-limit").value);
  const payload = await api(`/api/dataset-preview?${query.toString()}`);
  const columns = payload.columns.slice(0, 18).map((column) => ({ key: column, label: column }));
  $("preview-table").innerHTML = table(columns, payload.rows);
}

async function loadQualityIssues() {
  setLoading("quality-table");
  const query = new URLSearchParams();
  appendQuery(query, "dataset", $("quality-dataset").value);
  appendQuery(query, "status", $("quality-status").value);
  appendQuery(query, "limit", $("quality-limit").value);
  const payload = await api(`/api/quality-issues?${query.toString()}`);
  $("quality-table").innerHTML = table(
    [
      { key: "status", label: "status", status: true },
      { key: "severity", label: "severity", status: true },
      { key: "dataset", label: "dataset" },
      { key: "source_id", label: "source_id" },
      { key: "issue_type", label: "issue_type" },
      { key: "entity_key", label: "entity_key" },
      { key: "message", label: "message", maxLength: 140 },
      { key: "created_at", label: "created_at" },
    ],
    payload.issues,
  );
}

async function loadQlibObjects() {
  setLoading("qlib-objects");
  const query = new URLSearchParams();
  appendQuery(query, "dataset", "qlib_export");
  appendQuery(query, "limit", "80");
  const payload = await api(`/api/source-objects?${query.toString()}`);
  $("qlib-objects").innerHTML = table(
    [
      { key: "layer", label: "layer", status: true },
      { key: "uri", label: "uri", maxLength: 120 },
      { key: "size_bytes", label: "bytes" },
      { key: "created_at", label: "created_at" },
    ],
    payload.objects,
  );
}

function renderQlibJobs(rows) {
  $("qlib-jobs").innerHTML = table(
    [
      { key: "status", label: "status", status: true },
      { key: "start_date", label: "start" },
      { key: "end_date", label: "end" },
      {
        key: "parameters_json",
        label: "provider",
        maxLength: 110,
        value: (row) => row.parameters_json?.provider_uri || "",
      },
      {
        key: "parameters_json",
        label: "files",
        value: (row) => row.parameters_json?.file_count || 0,
      },
      { key: "created_at", label: "created_at" },
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
    setLoading("recent-jobs");
    setLoading("watermark-list");
    setLoading("progress-list");
    const payload = await api("/api/overview");
    renderOverview(payload);
    await Promise.all([
      loadBackfillTasks(),
      loadDatasetPreview(),
      loadQualityIssues(),
      loadQlibObjects(),
    ]);
  } catch (error) {
    showError(error);
  }
}

function init() {
  populateSelect("task-dataset", datasets, "全部 dataset");
  populateSelect("quality-dataset", datasets, "全部 dataset");
  populateSelect("preview-dataset", datasets, null);
  $("preview-dataset").value = "daily_bar";
  bindNav();
  bindFilters();
  refreshAll();
}

init();
