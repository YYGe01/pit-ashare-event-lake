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

const pageSummaries = {
  dashboard: "先判断当前采集是否正常，再查看卡住的数据集和最近运行记录。",
  backfill: "查看历史回补队列，定位哪些日期、标的和数据集还在等待、运行或失败。",
  dataset: "直接查看统一研究层 qdc_silver 表，确认某个标的和日期的数据是否已可用于研究。",
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

function tableSummary(text) {
  return `<div class="table-summary">${escapeHtml(text)}</div>`;
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

async function loadDatasetPreview() {
  try {
    setLoading("preview-table");
    const query = new URLSearchParams();
    appendQuery(query, "dataset", $("preview-dataset").value);
    appendQuery(query, "instrument", $("preview-instrument").value.trim());
    appendQuery(query, "start", $("preview-start").value.trim());
    appendQuery(query, "end", $("preview-end").value.trim());
    appendQuery(query, "limit", $("preview-limit").value);
    const selectedDataset = $("preview-dataset").value;
    $("preview-explanation").innerHTML = `<strong>当前数据集：</strong>${escapeHtml(datasetLabel(selectedDataset))}。${escapeHtml(datasetDescriptions[selectedDataset] || "")}`;
    const payload = await api(`/api/dataset-preview?${query.toString()}`);
    const columns = payload.columns.slice(0, 18).map((column) => ({
      key: column,
      label: fieldLabel(column),
    }));
    $("preview-table").innerHTML = tableSummary(
      `当前显示 ${number(payload.row_count)} 行，最多展示前 ${number($("preview-limit").value)} 行。`,
    ) + table(columns, payload.rows, "当前筛选条件下没有数据。换个日期、标的或数据集再看。");
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
