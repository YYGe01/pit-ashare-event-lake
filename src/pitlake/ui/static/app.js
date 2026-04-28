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
  if (action === "symbol") {
    const payload = await api(`/api/symbols/${encodeURIComponent(id)}?date=${state.date}`);
    openDrawer(`股票：${id}`, renderSymbolDetail(payload));
  }
  if (action === "manifest") {
    const payload = await api(`/api/manifests/${encodeURIComponent(id)}`);
    openDrawer(`Manifest：${id}`, renderManifestDetail(payload));
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
    symbols: renderSymbols,
    sources: renderSources,
    runs: renderRuns,
    quality: renderQuality,
    governance: renderGovernance,
    reconciliation: renderReconciliation,
    raw: renderRaw,
    manifests: renderManifests,
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
      <h2>Source x 日期状态矩阵</h2>
      ${sourceMatrix(overview.source_matrix)}
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

function renderSymbols() {
  const symbols = state.overview.symbol_universe?.symbols || [];
  app.innerHTML = `
    <section class="section">
      <div class="toolbar">
        <h2>股票覆盖</h2>
        <p>当前口径是 registry sample symbols 加当天已观测 item，不代表全市场 universe。</p>
      </div>
      <div class="search-row">
        <input id="symbolInput" list="symbolOptions" placeholder="输入股票代码，如 600000" />
        <datalist id="symbolOptions">
          ${symbols.map((row) => `<option value="${escapeHtml(row.symbol)}"></option>`).join("")}
        </datalist>
        <button id="symbolBtn" type="button">查看</button>
      </div>
      ${table(symbols, [
        ["symbol", "symbol", (row) => actionButton("symbol", row.symbol, row.symbol)],
        ["scope", "scope"],
        ["registry_datasets", "registry_datasets", (row) => listText(row.registry_datasets)],
        ["observed_datasets", "observed_datasets", (row) => listText(row.observed_datasets)],
      ])}
    </section>
  `;
  document.querySelector("#symbolBtn").addEventListener("click", openSymbolFromInput);
  document.querySelector("#symbolInput").addEventListener("keydown", (event) => {
    if (event.key === "Enter") openSymbolFromInput();
  });
}

async function openSymbolFromInput() {
  const input = document.querySelector("#symbolInput");
  const value = input.value.trim();
  if (!value) return;
  const payload = await api(`/api/symbols/${encodeURIComponent(value)}?date=${state.date}`);
  openDrawer(`股票：${value}`, renderSymbolDetail(payload));
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

async function renderGovernance() {
  const payload = await api(`/api/governance?date=${state.date}`);
  app.innerHTML = `
    <section class="section">
      <div class="toolbar">
        <h2>质量治理</h2>
        <div class="metric-inline">
          ${miniMetric("Health Pass", payload.source_health_summary.pass_count)}
          ${miniMetric("Health Warn", payload.source_health_summary.warn_count)}
          ${miniMetric("Health Fail", payload.source_health_summary.fail_count)}
          ${miniMetric("Health Missing", payload.source_health_summary.missing_count)}
        </div>
      </div>
      <h3>Dataset Quality Score</h3>
      ${table(payload.dataset_scores, [
        ["status", "status", (row) => statusBadge(row.status)],
        ["logical_dataset", "logical_dataset", (row) => datasetButton(row.logical_dataset)],
        ["label", "中文名称"],
        ["quality_score", "score"],
        ["dataset_status", "dataset", (row) => statusBadge(row.dataset_status)],
        ["quality_status", "quality", (row) => statusBadge(row.quality_status)],
        ["reconciliation_status", "reconcile", (row) => statusBadge(row.reconciliation_status)],
        ["item_version_count", "items"],
        ["run_count", "runs"],
        ["factors", "factors", (row) => listText(row.factors)],
      ])}
      <h3>Volume Baseline</h3>
      ${table(payload.volume_baselines, [
        ["status", "status", (row) => statusBadge(row.status)],
        ["logical_dataset", "logical_dataset", (row) => datasetButton(row.logical_dataset)],
        ["current_count", "current"],
        ["baseline_average", "baseline_avg"],
        ["baseline_days", "days"],
        ["ratio_to_baseline", "ratio", (row) => formatRatio(row.ratio_to_baseline)],
        ["history", "recent_history", (row) => historyText(row.history)],
        ["message", "message"],
      ])}
      <h3>Schema Drift</h3>
      ${table(payload.schema_drift, [
        ["status", "status", (row) => statusBadge(row.status)],
        ["logical_dataset", "logical_dataset", (row) => datasetButton(row.logical_dataset)],
        ["source_id", "source_id", (row) => sourceMaybe(row.source_id)],
        ["unknown_fields", "unknown_fields", (row) => listText(row.unknown_fields)],
        ["failed_count", "failed"],
        ["sample_failed_keys", "samples", (row) => listText(row.sample_failed_keys)],
      ])}
      <h3>Source Health</h3>
      ${table(payload.source_health, [
        ["status", "status", (row) => statusBadge(row.status)],
        ["source_id", "source_id", (row) => sourceMaybe(row.source_id)],
        ["logical_dataset", "logical_dataset", (row) => datasetButton(row.logical_dataset)],
        ["freshness_minutes", "freshness_min"],
        ["success_rate_24h", "success_24h", (row) => formatPercent(row.success_rate_24h)],
        ["new_items_24h", "new_24h"],
        ["last_success_time", "last_success"],
        ["notes", "notes"],
      ])}
    </section>
  `;
}

async function renderReconciliation() {
  const payload = await api(`/api/reconciliation?date=${state.date}`);
  app.innerHTML = `
    <section class="section">
      <div class="toolbar">
        <h2>对账中心</h2>
        ${statusBadge(payload.report_meta.status)}
      </div>
      <h3>Dataset 状态</h3>
      ${table(payload.datasets, [
        ["status", "status", (row) => statusBadge(row.status)],
        ["logical_dataset", "logical_dataset", (row) => datasetButton(row.logical_dataset)],
        ["active_sources", "active_sources", (row) => listText(row.active_sources)],
        ["planned_counterparty_sources", "counterparty", (row) => listText(row.planned_counterparty_sources)],
        ["findings", "findings", (row) => String(row.findings?.length || 0)],
        ["compared_group_count", "compared"],
      ])}
      <h3>Findings</h3>
      ${table(payload.findings, [
        ["severity", "severity", (row) => statusBadge(row.severity)],
        ["logical_dataset", "logical_dataset", (row) => datasetButton(row.logical_dataset)],
        ["finding_type", "type"],
        ["message", "message"],
        ["identity", "identity"],
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

async function renderManifests() {
  const payload = await api(`/api/manifests?limit=200`);
  app.innerHTML = `
    <section class="section">
      <h2>Manifest 快照</h2>
      ${table(payload.manifests, [
        ["status", "status", (row) => statusBadge(row.status)],
        ["manifest_id", "manifest_id", (row) => actionButton("manifest", row.manifest_id, short(row.manifest_id, 22))],
        ["manifest_date", "date"],
        ["manifest_type", "type"],
        ["run_count", "runs"],
        ["raw_object_count", "raw"],
        ["new_item_count", "new_items"],
        ["error_count", "errors"],
        ["created_at", "created_at"],
      ])}
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
    <h3>Explore</h3>
    ${renderExplore(payload)}
    <h3>Coverage</h3>
    ${renderCoverage(payload.coverage)}
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
    <h3>Reconciliation</h3>
    <pre>${escapeHtml(JSON.stringify(payload.reconciliation || {}, null, 2))}</pre>
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

function renderSymbolDetail(payload) {
  return `
    <div class="metric-grid">
      ${metric("Symbol", payload.normalized_symbol)}
      ${metric("Coverage Scope", payload.coverage_scope)}
      ${metric("Datasets", payload.coverage.length)}
      ${metric("Items", payload.items.length)}
    </div>
    <h3>Dataset Coverage</h3>
    ${table(payload.coverage, [
      ["status", "status", (row) => statusBadge(row.status)],
      ["logical_dataset", "logical_dataset", (row) => datasetButton(row.logical_dataset)],
      ["label", "中文名称"],
      ["item_count", "items"],
      ["expected_source_count", "expected_sources"],
      ["latest_first_seen_at", "latest_first_seen"],
    ])}
    <h3>Items</h3>
    ${itemsTable(payload.items)}
    <h3>Quality Checks</h3>
    ${table(payload.quality_checks, [
      ["severity", "severity", (row) => statusBadge(row.severity)],
      ["logical_dataset", "logical_dataset", (row) => datasetButton(row.logical_dataset)],
      ["source_id", "source_id", (row) => sourceMaybe(row.source_id)],
      ["check_name", "check"],
      ["status", "status", (row) => statusBadge(row.status)],
      ["sample_failed_keys", "sample_keys"],
    ])}
    <h3>Raw Evidence</h3>
    ${rawTable(payload.raw_objects)}
  `;
}

function renderManifestDetail(payload) {
  if (!payload.found) return `<p class="empty">未找到 manifest。</p>`;
  return `
    <h3>Manifest Metadata</h3>
    <pre>${escapeHtml(JSON.stringify(payload.manifest, null, 2))}</pre>
    <h3>Payload</h3>
    <pre>${escapeHtml(JSON.stringify(payload.payload || {}, null, 2))}</pre>
  `;
}

function renderExplore(payload) {
  if (payload.view_type === "document_feed") {
    return documentFeed(payload.items);
  }
  if (payload.logical_dataset === "market_daily_ohlcv") {
    return ohlcvTable(payload.items);
  }
  if (payload.logical_dataset === "trading_calendar") {
    return compactPayloadTable(payload.items, ["calendar_id", "date", "is_trading_day"]);
  }
  return compactPayloadTable(payload.items);
}

function renderCoverage(coverage) {
  if (!coverage) return `<p class="empty">没有覆盖数据。</p>`;
  return `
    <p class="hint">缺失检查口径：${escapeHtml(coverage.coverage_scope)}</p>
    <h4>按日期</h4>
    ${table(coverage.date_counts, [
      ["date", "date"],
      ["item_count", "items"],
    ])}
    <h4>按 Source</h4>
    ${table(coverage.source_counts, [
      ["source_id", "source_id", (row) => sourceMaybe(row.source_id)],
      ["item_count", "items"],
    ])}
    <h4>按 Symbol</h4>
    ${table(coverage.symbol_counts, [
      ["status", "status", (row) => statusBadge(row.status)],
      ["symbol", "symbol", (row) => actionButton("symbol", row.symbol, row.symbol)],
      ["item_count", "items"],
      ["expected_source_count", "expected_sources"],
    ])}
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
    ["raw_object_id", "raw", (row) => actionButton("raw", row.raw_object_id, short(row.raw_object_id))],
  ]);
}

function sourceMatrix(matrix) {
  if (!matrix?.rows?.length) return `<p class="empty">没有矩阵数据。</p>`;
  const rows = matrix.rows.filter((row) => row.enabled || row.cells.some((cell) => cell.run_count > 0));
  if (!rows.length) return `<p class="empty">没有 enabled source 或运行记录。</p>`;
  return `
    <div class="matrix-wrap">
      <table class="matrix-table">
        <thead>
          <tr>
            <th>source_id</th>
            <th>dataset</th>
            ${matrix.dates.map((date) => `<th>${escapeHtml(date)}</th>`).join("")}
          </tr>
        </thead>
        <tbody>
          ${rows
            .map(
              (row) => `
                <tr>
                  <td>${sourceMaybe(row.source_id)}</td>
                  <td>${datasetButton(row.logical_dataset)}</td>
                  ${row.cells
                    .map((cell) => {
                      const firstRun = cell.run_ids?.[0];
                      const label = `${cell.status || "missing"} / ${cell.run_count}`;
                      return `<td>${firstRun ? actionButton("run", firstRun, label) : statusBadge(cell.status)}</td>`;
                    })
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

function documentFeed(items) {
  if (!items || items.length === 0) return `<p class="empty">没有文档数据。</p>`;
  return `
    <div class="feed-list">
      ${items
        .map((item) => {
          const payload = item.observed_payload || {};
          const url = item.source_url || payload.source_url || payload.url || "";
          return `
            <article class="feed-item">
              <div class="feed-main">
                <h4>${escapeHtml(item.title || payload.title || item.source_item_key)}</h4>
                <p>${escapeHtml(item.source_publish_time || payload.publish_time || item.first_seen_at || "")}</p>
                <p>${escapeHtml(item.logical_dataset)} / ${escapeHtml(item.source_id)}</p>
              </div>
              <div class="feed-actions">
                ${url ? `<a href="${escapeHtml(url)}" target="_blank" rel="noreferrer">来源</a>` : ""}
                ${actionButton("raw", item.raw_object_id, "Raw")}
              </div>
            </article>
          `;
        })
        .join("")}
    </div>
  `;
}

function ohlcvTable(items) {
  return compactPayloadTable(items, [
    "instrument",
    "trading_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
  ]);
}

function compactPayloadTable(items, fields) {
  if (!items || items.length === 0) return `<p class="empty">没有样本数据。</p>`;
  const inferredFields = fields || inferPayloadFields(items);
  const columns = inferredFields.map((field) => [
    field,
    field,
    (row) => escapeHtml(row.observed_payload?.[field] ?? row[field] ?? ""),
  ]);
  columns.push(["raw_object_id", "raw", (row) => actionButton("raw", row.raw_object_id, "Raw")]);
  return table(items, columns);
}

function inferPayloadFields(items) {
  const fields = [];
  for (const item of items.slice(0, 20)) {
    for (const field of Object.keys(item.observed_payload || {})) {
      if (!fields.includes(field)) fields.push(field);
      if (fields.length >= 10) return fields;
    }
  }
  return fields.length ? fields : ["source_item_key"];
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
                <p class="issue-actions">
                  ${issue.logical_dataset ? datasetButton(issue.logical_dataset) : ""}
                  ${issue.source_id ? sourceMaybe(issue.source_id) : ""}
                  ${issue.run_id ? actionButton("run", issue.run_id, "Run") : ""}
                </p>
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

function miniMetric(label, value) {
  return `<span class="mini-metric"><strong>${escapeHtml(String(value ?? 0))}</strong>${escapeHtml(label)}</span>`;
}

function statusBadge(status) {
  const normalized = normalizeStatus(status);
  return `<span class="status ${normalized}">${escapeHtml(status || "missing")}</span>`;
}

function normalizeStatus(status) {
  const value = String(status || "missing").toLowerCase();
  if (["pass", "ok", "success", "complete", "stored", "present", "observed"].includes(value)) return "pass";
  if (["fail", "failed", "error", "critical"].includes(value)) return "fail";
  if (["warn", "warning", "partial", "missing"].includes(value)) return "warn";
  if (["not_expected", "not_applicable", "skipped", "not_enough_history"].includes(value)) return "neutral";
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
  if (row.type === "raw") return actionButton("raw", row.id, short(row.title));
  if (row.type === "symbol") return actionButton("symbol", row.id, row.title);
  if (row.type === "manifest") return actionButton("manifest", row.id, short(row.title, 22));
  return escapeHtml(row.title || row.id || "");
}

function actionButton(action, id, text) {
  return `<button class="link-button" data-action="${escapeHtml(action)}" data-id="${escapeHtml(id)}" type="button">${escapeHtml(text || id)}</button>`;
}

function short(value, length = 12) {
  if (!value) return "";
  const text = String(value);
  return text.length > length ? text.slice(0, length) : text;
}

function listText(value) {
  if (!Array.isArray(value)) return escapeHtml(value || "");
  return escapeHtml(value.join(", "));
}

function historyText(history) {
  if (!Array.isArray(history) || history.length === 0) return "";
  return escapeHtml(history.slice(0, 7).map((row) => `${row.date}:${row.item_count}`).join(" | "));
}

function formatRatio(value) {
  if (value === null || value === undefined || value === "") return "";
  return `${Number(value).toFixed(2)}x`;
}

function formatPercent(value) {
  if (value === null || value === undefined || value === "") return "";
  return `${(Number(value) * 100).toFixed(0)}%`;
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
