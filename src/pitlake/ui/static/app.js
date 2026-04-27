const state = {
  date: null,
  overview: null,
  view: "overview",
};

const app = document.querySelector("#app");
const dateSelect = document.querySelector("#dateSelect");
const subtitle = document.querySelector("#subtitle");
const drawer = document.querySelector("#drawer");
const drawerTitle = document.querySelector("#drawerTitle");
const drawerBody = document.querySelector("#drawerBody");

document.querySelector("#refreshBtn").addEventListener("click", () => loadOverview(state.date));
document.querySelector("#drawerClose").addEventListener("click", closeDrawer);
document.querySelectorAll(".tab").forEach((button) => {
  button.addEventListener("click", () => switchView(button.dataset.view));
});

app.addEventListener("click", async (event) => {
  const target = event.target.closest("[data-action]");
  if (!target) return;
  const action = target.dataset.action;
  const id = target.dataset.id;
  if (action === "dataset") {
    const payload = await api(`/api/datasets/${encodeURIComponent(id)}?date=${state.date}`);
    openDrawer(`数据资产：${id}`, renderDatasetDetail(payload));
  }
  if (action === "source") {
    const payload = await api(`/api/sources/${encodeURIComponent(id)}?date=${state.date}`);
    openDrawer(`Source：${id}`, renderSourceDetail(payload));
  }
  if (action === "run") {
    const payload = await api(`/api/runs/${encodeURIComponent(id)}`);
    openDrawer(`Run：${id}`, renderRunDetail(payload));
  }
  if (action === "raw") {
    const payload = await api(`/api/raw/${encodeURIComponent(id)}`);
    openDrawer(`Raw：${id}`, renderRawDetail(payload));
  }
});

dateSelect.addEventListener("change", () => loadOverview(dateSelect.value));

loadOverview();

async function loadOverview(date) {
  app.innerHTML = `<section class="section"><p class="empty">正在加载...</p></section>`;
  const qs = date ? `?date=${encodeURIComponent(date)}` : "";
  state.overview = await api(`/api/overview${qs}`);
  state.date = state.overview.report_date;
  renderDateSelect();
  subtitle.textContent = `当前日期 ${state.date || "无"}，状态 ${state.overview.status}`;
  switchView(state.view, { keepTab: true });
}

function renderDateSelect() {
  dateSelect.innerHTML = "";
  for (const date of state.overview.available_dates || []) {
    const option = document.createElement("option");
    option.value = date;
    option.textContent = date;
    option.selected = date === state.date;
    dateSelect.appendChild(option);
  }
}

function switchView(view, options = {}) {
  state.view = view;
  if (!options.keepTab) {
    document.querySelectorAll(".tab").forEach((button) => {
      button.classList.toggle("active", button.dataset.view === view);
    });
  } else {
    document.querySelectorAll(".tab").forEach((button) => {
      button.classList.toggle("active", button.dataset.view === state.view);
    });
  }
  const renderers = {
    overview: renderOverview,
    datasets: renderDatasets,
    sources: renderSources,
    runs: renderRuns,
    quality: renderQuality,
    raw: renderRaw,
    search: renderSearch,
  };
  renderers[view]();
}

function renderOverview() {
  const overview = state.overview;
  app.innerHTML = `
    <section class="section">
      <div class="toolbar">
        <h2>采集健康总览</h2>
        ${statusBadge(overview.status)}
      </div>
      <div class="metric-grid">
        ${metric("Source", overview.summary.source_count)}
        ${metric("Enabled", overview.summary.enabled_source_count)}
        ${metric("Runs", overview.summary.run_count)}
        ${metric("Failed Runs", overview.summary.failed_run_count)}
        ${metric("Raw Objects", overview.summary.raw_object_count)}
        ${metric("Items", overview.summary.item_version_count)}
        ${metric("Dataset Fail", overview.summary.dataset_fail_count)}
        ${metric("Issues", overview.summary.issue_count)}
      </div>
    </section>
    <section class="section">
      <h2>异常优先队列</h2>
      ${renderIssues(overview.issues)}
    </section>
    <section class="section">
      <h2>Logical Dataset 健康</h2>
      ${datasetTable(overview.datasets)}
    </section>
    <section class="section">
      <h2>Source 采集状态</h2>
      ${sourceTable(overview.sources)}
    </section>
  `;
}

function renderDatasets() {
  app.innerHTML = `
    <section class="section">
      <div class="toolbar">
        <h2>数据资产目录</h2>
        <p>点击 logical_dataset 查看质量、运行、样本和 raw 证据。</p>
      </div>
      ${datasetTable(state.overview.datasets)}
    </section>
  `;
}

function renderSources() {
  app.innerHTML = `
    <section class="section">
      <div class="toolbar">
        <h2>Source 目录</h2>
        <p>点击 source_id 查看 registry、health、runs 和 raw objects。</p>
      </div>
      ${sourceTable(state.overview.sources)}
    </section>
  `;
}

async function renderRuns() {
  const payload = await api(`/api/runs?date=${state.date}&limit=200`);
  app.innerHTML = `
    <section class="section">
      <h2>运行批次</h2>
      ${table(payload.runs, [
        ["run_id", "run_id", (row) => actionButton("run", row.run_id, short(row.run_id))],
        ["source_id", "source_id", (row) => actionButton("source", row.source_id, row.source_id)],
        ["logical_dataset", "logical_dataset", (row) => datasetButton(row.logical_dataset)],
        ["status", "status", (row) => statusBadge(row.status)],
        ["start_at", "start_at"],
        ["end_at", "end_at"],
        ["new_item_count", "new"],
        ["duplicate_count", "dup"],
        ["error_count", "errors"],
      ])}
    </section>
  `;
}

async function renderQuality() {
  const payload = await api(`/api/quality?date=${state.date}`);
  app.innerHTML = `
    <section class="section">
      <div class="toolbar">
        <h2>质量问题中心</h2>
        ${statusBadge(payload.report_meta.status)}
      </div>
      <h3>Quality Findings</h3>
      ${table(payload.quality_findings, [
        ["severity", "severity", (row) => statusBadge(row.severity)],
        ["logical_dataset", "logical_dataset", (row) => datasetButton(row.logical_dataset)],
        ["source_id", "source_id", (row) => sourceMaybe(row.source_id)],
        ["finding_type", "type"],
        ["message", "message"],
        ["failed_count", "failed"],
      ])}
      <h3>Failed Checks</h3>
      ${table(payload.failed_checks, [
        ["severity", "severity", (row) => statusBadge(row.severity)],
        ["logical_dataset", "logical_dataset", (row) => datasetButton(row.logical_dataset)],
        ["source_id", "source_id", (row) => sourceMaybe(row.source_id)],
        ["check_name", "check"],
        ["status", "status", (row) => statusBadge(row.status)],
        ["failed_count", "failed"],
        ["created_at", "created_at"],
      ])}
    </section>
  `;
}

async function renderRaw() {
  const payload = await api(`/api/raw?date=${state.date}&limit=200`);
  app.innerHTML = `
    <section class="section">
      <h2>Raw 文件浏览</h2>
      ${rawTable(payload.raw_objects)}
    </section>
  `;
}

function renderSearch() {
  app.innerHTML = `
    <section class="section">
      <h2>全局搜索</h2>
      <div class="search-row">
        <input id="searchInput" placeholder="source、dataset、股票、标题、run_id、raw hash" />
        <button id="searchBtn" type="button">搜索</button>
      </div>
      <div id="searchResults" class="table-wrap"></div>
    </section>
  `;
  document.querySelector("#searchBtn").addEventListener("click", runSearch);
  document.querySelector("#searchInput").addEventListener("keydown", (event) => {
    if (event.key === "Enter") runSearch();
  });
}

async function runSearch() {
  const input = document.querySelector("#searchInput");
  const payload = await api(`/api/search?q=${encodeURIComponent(input.value)}&limit=80`);
  document.querySelector("#searchResults").outerHTML = table(payload.results, [
    ["type", "type"],
    ["title", "title", (row) => resultButton(row)],
    ["subtitle", "subtitle"],
  ]);
}

function renderDatasetDetail(payload) {
  return `
    <div class="metric-grid">
      ${metric("状态", payload.summary?.status || "missing")}
      ${metric("展示类型", payload.view_type)}
      ${metric("Items", payload.summary?.item_version_count || 0)}
      ${metric("Runs", payload.summary?.run_count || 0)}
    </div>
    <h3>Source</h3>
    ${table(payload.sources, [
      ["source_id", "source_id", (row) => actionButton("source", row.source_id, row.source_id)],
      ["provider_id", "provider"],
      ["enabled", "enabled"],
      ["implementation_status", "status"],
      ["priority", "priority"],
    ])}
    <h3>样本数据</h3>
    ${itemsTable(payload.items)}
    <h3>质量 Findings</h3>
    ${table(payload.quality_findings, [
      ["severity", "severity", (row) => statusBadge(row.severity)],
      ["finding_type", "type"],
      ["message", "message"],
      ["failed_count", "failed"],
    ])}
    <h3>Raw Evidence</h3>
    ${rawTable(payload.raw_objects)}
    <h3>Contract</h3>
    <pre>${escapeHtml(JSON.stringify(payload.contract, null, 2))}</pre>
  `;
}

function renderSourceDetail(payload) {
  return `
    <div class="metric-grid">
      ${metric("Runs", payload.summary.run_count_all_time)}
      ${metric("Success", payload.summary.success_run_count_all_time)}
      ${metric("Failed", payload.summary.failed_run_count_all_time)}
      ${metric("Date Items", payload.summary.item_version_count_on_date)}
    </div>
    <h3>Health</h3>
    <pre>${escapeHtml(JSON.stringify(payload.health || {}, null, 2))}</pre>
    <h3>Registry</h3>
    <pre>${escapeHtml(JSON.stringify(payload.config || {}, null, 2))}</pre>
    <h3>Runs</h3>
    ${table(payload.runs, [
      ["run_id", "run_id", (row) => actionButton("run", row.run_id, short(row.run_id))],
      ["status", "status", (row) => statusBadge(row.status)],
      ["start_at", "start_at"],
      ["new_item_count", "new"],
      ["duplicate_count", "dup"],
      ["error_count", "errors"],
    ])}
    <h3>Raw Objects</h3>
    ${rawTable(payload.raw_objects)}
  `;
}

function renderRunDetail(payload) {
  if (!payload.found) return `<p class="empty">未找到 run。</p>`;
  return `
    <h3>Run</h3>
    <pre>${escapeHtml(JSON.stringify(payload.run, null, 2))}</pre>
    <h3>Raw Objects</h3>
    ${rawTable(payload.raw_objects)}
    <h3>Quality Checks</h3>
    ${table(payload.quality_checks, [
      ["severity", "severity", (row) => statusBadge(row.severity)],
      ["check_name", "check"],
      ["status", "status", (row) => statusBadge(row.status)],
      ["failed_count", "failed"],
      ["observed_value", "observed"],
    ])}
    <h3>Items</h3>
    ${itemsTable(payload.items)}
  `;
}

function renderRawDetail(payload) {
  if (!payload.found) return `<p class="empty">未找到 raw object。</p>`;
  const preview = payload.preview || {};
  return `
    <h3>Metadata</h3>
    <pre>${escapeHtml(JSON.stringify(payload.raw_object, null, 2))}</pre>
    <h3>Preview</h3>
    <pre>${escapeHtml(preview.json ? JSON.stringify(preview.json, null, 2) : preview.text || "")}</pre>
    <h3>Linked Items</h3>
    ${itemsTable(payload.items)}
  `;
}

function datasetTable(rows) {
  return table(rows, [
    ["status", "status", (row) => statusBadge(row.status)],
    ["logical_dataset", "logical_dataset", (row) => datasetButton(row.logical_dataset)],
    ["label", "中文名称"],
    ["priority", "priority"],
    ["view_type", "view"],
    ["enabled_source_count", "enabled"],
    ["source_count", "sources"],
    ["run_count", "runs"],
    ["item_version_count", "items"],
    ["quality_status", "quality", (row) => statusBadge(row.quality_status)],
    ["reconciliation_status", "reconcile", (row) => statusBadge(row.reconciliation_status)],
  ]);
}

function sourceTable(rows) {
  return table(rows, [
    ["status", "status", (row) => statusBadge(row.status)],
    ["source_id", "source_id", (row) => actionButton("source", row.source_id, row.source_id)],
    ["logical_dataset", "logical_dataset", (row) => datasetButton(row.logical_dataset)],
    ["provider_id", "provider"],
    ["priority", "priority"],
    ["enabled", "enabled"],
    ["implementation_status", "implementation"],
    ["run_count", "runs"],
    ["item_version_count", "items"],
    ["last_run_at", "last_run"],
  ]);
}

function rawTable(rows) {
  return table(rows, [
    ["raw_object_id", "raw_object_id", (row) => actionButton("raw", row.raw_object_id, short(row.raw_object_id))],
    ["source_id", "source_id", (row) => sourceMaybe(row.source_id)],
    ["logical_dataset", "logical_dataset", (row) => datasetButton(row.logical_dataset)],
    ["mime_type", "mime"],
    ["size_bytes", "bytes"],
    ["stored_at", "stored_at"],
    ["content_hash", "hash", (row) => short(row.content_hash)],
  ]);
}

function itemsTable(rows) {
  return table(rows, [
    ["logical_dataset", "logical_dataset", (row) => datasetButton(row.logical_dataset)],
    ["source_id", "source_id", (row) => sourceMaybe(row.source_id)],
    ["source_item_key", "item_key"],
    ["title", "title"],
    ["source_publish_time", "publish_time"],
    ["first_seen_at", "first_seen"],
    ["quality_status", "quality", (row) => statusBadge(row.quality_status)],
  ]);
}

function table(rows, columns) {
  if (!rows || rows.length === 0) return `<p class="empty">没有数据。</p>`;
  return `
    <div class="table-wrap">
      <table>
        <thead>
          <tr>${columns.map((column) => `<th>${escapeHtml(column[1])}</th>`).join("")}</tr>
        </thead>
        <tbody>
          ${rows
            .map(
              (row) => `
                <tr>
                  ${columns
                    .map((column) => `<td>${cell(row, column)}</td>`)
                    .join("")}
                </tr>
              `,
            )
            .join("")}
        </tbody>
      </table>
    </div>
  `;
}

function cell(row, column) {
  if (typeof column[2] === "function") return column[2](row);
  const value = row[column[0]];
  if (value === null || value === undefined || value === "") return "";
  if (typeof value === "object") return `<pre>${escapeHtml(JSON.stringify(value, null, 2))}</pre>`;
  return escapeHtml(String(value));
}

function renderIssues(issues) {
  if (!issues || issues.length === 0) return `<p class="empty">没有发现异常。</p>`;
  return `
    <div class="issue-list">
      ${issues
        .map(
          (issue) => `
            <div class="issue">
              <div>${statusBadge(issue.severity)}</div>
              <div>
                <p class="issue-title">${escapeHtml(issue.title || issue.kind)}</p>
                <p class="issue-detail">${escapeHtml(issue.detail || "")}</p>
              </div>
            </div>
          `,
        )
        .join("")}
    </div>
  `;
}

function metric(label, value) {
  return `
    <div class="metric">
      <div class="label">${escapeHtml(label)}</div>
      <div class="value">${escapeHtml(String(value ?? ""))}</div>
    </div>
  `;
}

function statusBadge(status) {
  const normalized = normalizeStatus(status);
  return `<span class="status ${normalized}">${escapeHtml(status || "missing")}</span>`;
}

function normalizeStatus(status) {
  const value = String(status || "missing").toLowerCase();
  if (["pass", "ok", "success", "complete", "stored"].includes(value)) return "pass";
  if (["fail", "failed", "error", "critical"].includes(value)) return "fail";
  if (["warn", "warning", "partial", "missing"].includes(value)) return "warn";
  return "neutral";
}

function datasetButton(id) {
  if (!id) return "";
  return actionButton("dataset", id, id);
}

function sourceMaybe(id) {
  if (!id) return "";
  return actionButton("source", id, id);
}

function resultButton(row) {
  if (row.type === "dataset") return datasetButton(row.id);
  if (row.type === "source") return sourceMaybe(row.id);
  if (row.type === "run") return actionButton("run", row.id, row.title);
  return escapeHtml(row.title || row.id || "");
}

function actionButton(action, id, text) {
  return `<button class="link-button" data-action="${escapeHtml(action)}" data-id="${escapeHtml(id)}" type="button">${escapeHtml(text || id)}</button>`;
}

function short(value) {
  if (!value) return "";
  const text = String(value);
  return text.length > 12 ? text.slice(0, 12) : text;
}

function openDrawer(title, body) {
  drawerTitle.textContent = title;
  drawerBody.innerHTML = body;
  drawer.classList.add("open");
}

function closeDrawer() {
  drawer.classList.remove("open");
}

async function api(path) {
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text);
  }
  return response.json();
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
