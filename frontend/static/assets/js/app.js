const REFRESH_INTERVAL_MS = 5000;

const routes = ["dashboard", "health", "settings", "diagnostics"];

const state = {
  live: null,
  ready: null,
  version: null,
  runtime: null,
  startup: null,
  settings: null,
  schema: null,
  lastUpdatedAt: null,
  error: null,
};

const nodes = {
  refreshButton: document.querySelector("#refreshButton"),
  lastUpdated: document.querySelector("#lastUpdated"),
  liveStatus: document.querySelector("#liveStatus"),
  readyStatus: document.querySelector("#readyStatus"),
  versionValue: document.querySelector("#versionValue"),
  portValue: document.querySelector("#portValue"),
  launchSummary: document.querySelector("#launchSummary"),
  checkGrid: document.querySelector("#checkGrid"),
  pathList: document.querySelector("#pathList"),
  providerList: document.querySelector("#providerList"),
  healthSummary: document.querySelector("#healthSummary"),
  workerSummary: document.querySelector("#workerSummary"),
  failedChecks: document.querySelector("#failedChecks"),
  settingsList: document.querySelector("#settingsList"),
  schemaList: document.querySelector("#schemaList"),
  startupTimeline: document.querySelector("#startupTimeline"),
  startupJson: document.querySelector("#startupJson"),
  runtimeJson: document.querySelector("#runtimeJson"),
  routeLinks: [...document.querySelectorAll(".route-link")],
  views: Object.fromEntries(routes.map((route) => [route, document.querySelector(`#view-${route}`)])),
};

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function prettyDate(value) {
  if (!value) {
    return "Unavailable";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(date);
}

function prettyJson(value) {
  return JSON.stringify(value ?? {}, null, 2);
}

function statusMeta(ok, pendingLabel = "Pending") {
  if (ok === true) {
    return { label: "Healthy", className: "status-ok" };
  }
  if (ok === false) {
    return { label: "Attention", className: "status-error" };
  }
  return { label: pendingLabel, className: "status-unknown" };
}

function indicatorMeta(ok) {
  if (ok === true) {
    return { label: "Pass", className: "indicator indicator-ok" };
  }
  if (ok === false) {
    return { label: "Fail", className: "indicator indicator-error" };
  }
  return { label: "Waiting", className: "indicator indicator-warn" };
}

async function fetchJson(path) {
  const response = await fetch(path, {
    headers: { Accept: "application/json" },
    cache: "no-store",
  });
  const payload = await response.json().catch(() => ({}));
  return {
    ok: response.ok,
    status: response.status,
    payload,
  };
}

async function refreshState() {
  nodes.refreshButton.disabled = true;
  nodes.refreshButton.textContent = "Refreshing...";

  try {
    const [live, ready, version, startup, runtime, settings, schema] = await Promise.all([
      fetchJson("/api/v1/health/live"),
      fetchJson("/api/v1/health/ready"),
      fetchJson("/api/v1/version"),
      fetchJson("/api/v1/diagnostics/startup"),
      fetchJson("/api/v1/diagnostics/runtime"),
      fetchJson("/api/v1/settings/effective"),
      fetchJson("/api/v1/settings/schema"),
    ]);

    state.live = live;
    state.ready = ready;
    state.version = version;
    state.startup = startup;
    state.runtime = runtime;
    state.settings = settings;
    state.schema = schema;
    state.lastUpdatedAt = new Date().toISOString();
    state.error = null;
  } catch (error) {
    state.error = error instanceof Error ? error.message : String(error);
  } finally {
    render();
    nodes.refreshButton.disabled = false;
    nodes.refreshButton.textContent = "Refresh diagnostics";
  }
}

function renderStatusPill(node, ok, labelWhenUnknown) {
  const meta = statusMeta(ok, labelWhenUnknown);
  node.className = `status-pill ${meta.className}`;
  node.textContent = meta.label;
}

function renderSummaryList(node, items) {
  node.innerHTML = items
    .map(
      (item) => `
        <article class="stack-item">
          <strong>${escapeHtml(item.label)}</strong>
          <span>${escapeHtml(item.value)}</span>
        </article>
      `,
    )
    .join("");
}

function renderDefinitionGrid(node, entries) {
  node.innerHTML = entries
    .map(
      ([label, value]) => `
        <article class="definition-item">
          <strong>${escapeHtml(label)}</strong>
          <span>${escapeHtml(value)}</span>
        </article>
      `,
    )
    .join("");
}

function renderProviders(node, providers = {}) {
  node.innerHTML = Object.entries(providers)
    .map(([name, data]) => {
      const enabled = Boolean(data.enabled);
      const apiKey = data.api_key || "Not set";
      return `
        <article class="provider-item">
          <strong>${escapeHtml(name)}</strong>
          <span>${enabled ? "Enabled in config" : "Disabled in config"}</span>
          <span>${escapeHtml(apiKey)}</span>
        </article>
      `;
    })
    .join("");
}

function renderCheckGrid(node, checks = {}) {
  node.innerHTML = Object.entries(checks)
    .map(([name, value]) => {
      const indicator = indicatorMeta(value);
      return `
        <article class="check-item">
          <strong>
            ${escapeHtml(name)}
            <span class="${indicator.className}">${escapeHtml(indicator.label)}</span>
          </strong>
          <span>${value === true ? "Requirement satisfied." : value === false ? "Requirement is failing." : "Waiting for signal."}</span>
        </article>
      `;
    })
    .join("");
}

function renderSchema(node, schema = {}) {
  node.innerHTML = Object.entries(schema)
    .map(([sectionName, data]) => {
      const fields = Object.entries(data.fields || {})
        .map(([field, description]) => `<li><strong>${escapeHtml(field)}</strong>: ${escapeHtml(description)}</li>`)
        .join("");
      return `
        <article class="schema-item">
          <strong>${escapeHtml(sectionName)}</strong>
          <p>${data.required ? "Required section." : "Optional section."}</p>
          <ul>${fields}</ul>
        </article>
      `;
    })
    .join("");
}

function renderTimeline(node, stages = []) {
  node.innerHTML = stages
    .map((stage) => {
      const meta = stage.status === "ok" ? "status-ok" : stage.status === "error" ? "status-error" : "status-warn";
      return `
        <li>
          <span class="status-pill ${meta}">${escapeHtml(stage.status || "unknown")}</span>
          <strong>${escapeHtml(stage.stage || "stage")}</strong>
          <p>${escapeHtml(stage.message || "No message recorded.")}</p>
          <p>${escapeHtml(prettyDate(stage.timestamp))}</p>
        </li>
      `;
    })
    .join("");
}

function renderFailedChecks(node, failedChecks = []) {
  if (!failedChecks.length) {
    node.innerHTML = '<li class="chip" style="background: rgba(52, 211, 153, 0.12); border-color: rgba(52, 211, 153, 0.22); color: var(--ok);">No active blockers</li>';
    return;
  }
  node.innerHTML = failedChecks.map((check) => `<li class="chip">${escapeHtml(check)}</li>`).join("");
}

function renderRoute() {
  const route = routes.includes(window.location.hash.replace("#/", "")) ? window.location.hash.replace("#/", "") : "dashboard";
  for (const [name, node] of Object.entries(nodes.views)) {
    const active = name === route;
    node.hidden = !active;
    node.classList.toggle("view-active", active);
  }
  for (const link of nodes.routeLinks) {
    link.dataset.active = String(link.dataset.route === route);
  }
}

function render() {
  const runtime = state.runtime?.payload ?? {};
  const startup = state.startup?.payload ?? {};
  const settings = state.settings?.payload ?? {};
  const schema = state.schema?.payload ?? {};
  const liveOk = state.live?.ok ?? null;
  const readyOk = state.ready?.payload?.ok ?? null;

  renderStatusPill(nodes.liveStatus, liveOk, "Checking");
  renderStatusPill(nodes.readyStatus, readyOk, "Checking");

  nodes.versionValue.textContent = runtime.app?.version || state.version?.payload?.version || "Unknown";
  nodes.portValue.textContent = runtime.server?.port ? String(runtime.server.port) : "Unknown";
  nodes.lastUpdated.textContent = state.error
    ? `Refresh error: ${state.error}`
    : `Last refreshed ${prettyDate(state.lastUpdatedAt)}`;

  renderSummaryList(nodes.launchSummary, [
    { label: "App", value: `${runtime.app?.name || "Market Bot"} ${runtime.app?.sprint || ""}`.trim() },
    { label: "Started", value: prettyDate(runtime.server?.started_at || startup.started_at) },
    { label: "Launcher URL", value: startup.launcher_url || "Not recorded yet" },
    { label: "Schema version", value: runtime.database?.schema_version || "Unknown" },
    { label: "Config path", value: runtime.config_path || "Unknown" },
  ]);

  renderCheckGrid(nodes.checkGrid, runtime.checks || {});
  renderDefinitionGrid(nodes.pathList, Object.entries(runtime.paths || {}));
  renderProviders(nodes.providerList, settings.providers || {});

  renderSummaryList(nodes.healthSummary, [
    { label: "Live endpoint", value: state.live?.status ? `HTTP ${state.live.status}` : "Unavailable" },
    { label: "Ready endpoint", value: state.ready?.status ? `HTTP ${state.ready.status}` : "Unavailable" },
    { label: "Database", value: runtime.database?.message || "Unknown" },
    { label: "Applied migrations", value: String(runtime.database?.applied_migrations || 0) },
    { label: "Backend PID", value: runtime.server?.pid ? String(runtime.server.pid) : "Unknown" },
  ]);

  renderSummaryList(nodes.workerSummary, [
    { label: "Worker state", value: runtime.worker_status?.state || "Unknown" },
    { label: "Worker PID", value: runtime.worker_status?.pid ? String(runtime.worker_status.pid) : "Unknown" },
    { label: "Last heartbeat", value: prettyDate(runtime.worker_status?.last_heartbeat_at) },
    { label: "Worker database check", value: String(runtime.worker_status?.database_message || "Unknown") },
  ]);

  renderFailedChecks(nodes.failedChecks, runtime.failed_checks || state.ready?.payload?.failed_checks || []);
  renderDefinitionGrid(
    nodes.settingsList,
    Object.entries(flattenSettings(settings)).map(([key, value]) => [key, String(value)]),
  );
  renderSchema(nodes.schemaList, schema);
  renderTimeline(nodes.startupTimeline, startup.stages || []);
  nodes.startupJson.textContent = prettyJson(startup);
  nodes.runtimeJson.textContent = prettyJson(runtime);

  if (state.error) {
    nodes.healthSummary.insertAdjacentHTML(
      "afterbegin",
      `<div class="error-banner">Diagnostics refresh failed: ${escapeHtml(state.error)}</div>`,
    );
  }

  renderRoute();
}

function flattenSettings(value, prefix = "", output = {}) {
  if (Array.isArray(value)) {
    output[prefix] = value.join(", ");
    return output;
  }
  if (value && typeof value === "object") {
    for (const [key, item] of Object.entries(value)) {
      const nextPrefix = prefix ? `${prefix}.${key}` : key;
      flattenSettings(item, nextPrefix, output);
    }
    return output;
  }
  output[prefix] = value ?? "";
  return output;
}

nodes.refreshButton.addEventListener("click", () => {
  void refreshState();
});

window.addEventListener("hashchange", renderRoute);

if (!window.location.hash) {
  window.location.hash = "#/dashboard";
}

renderRoute();
void refreshState();
window.setInterval(() => {
  void refreshState();
}, REFRESH_INTERVAL_MS);
