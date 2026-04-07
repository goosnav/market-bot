const REFRESH_INTERVAL_MS = 5000;

const routes = ["dashboard", "warehouse", "studio", "campaigns", "health", "settings", "diagnostics"];

const state = {
  live: null,
  ready: null,
  version: null,
  runtime: null,
  startup: null,
  settings: null,
  schema: null,
  warehouse: null,
  studio: null,
  campaigns: null,
  campaignPreview: null,
  selectedCampaignId: null,
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
  warehouseSummary: document.querySelector("#warehouseSummary"),
  importJobs: document.querySelector("#importJobs"),
  warehouseLeads: document.querySelector("#warehouseLeads"),
  savedFilters: document.querySelector("#savedFilters"),
  warehouseNotice: document.querySelector("#warehouseNotice"),
  studioSummary: document.querySelector("#studioSummary"),
  studioArtifacts: document.querySelector("#studioArtifacts"),
  studioTemplates: document.querySelector("#studioTemplates"),
  studioOffers: document.querySelector("#studioOffers"),
  studioPlaybooks: document.querySelector("#studioPlaybooks"),
  studioNotice: document.querySelector("#studioNotice"),
  offerProfileForm: document.querySelector("#offerProfileForm"),
  offerName: document.querySelector("#offerName"),
  offerValueProp: document.querySelector("#offerValueProp"),
  offerCta: document.querySelector("#offerCta"),
  offerAllowedClaims: document.querySelector("#offerAllowedClaims"),
  offerDisallowedClaims: document.querySelector("#offerDisallowedClaims"),
  playbookForm: document.querySelector("#playbookForm"),
  playbookName: document.querySelector("#playbookName"),
  playbookTone: document.querySelector("#playbookTone"),
  playbookPains: document.querySelector("#playbookPains"),
  playbookSubjects: document.querySelector("#playbookSubjects"),
  playbookDisallowedLanguage: document.querySelector("#playbookDisallowedLanguage"),
  templateForm: document.querySelector("#templateForm"),
  templateName: document.querySelector("#templateName"),
  templateDescription: document.querySelector("#templateDescription"),
  templateSubject: document.querySelector("#templateSubject"),
  templateIntro: document.querySelector("#templateIntro"),
  templateAiInstruction: document.querySelector("#templateAiInstruction"),
  templateAiFallback: document.querySelector("#templateAiFallback"),
  templateOfferLine: document.querySelector("#templateOfferLine"),
  templateCtaLine: document.querySelector("#templateCtaLine"),
  renderForm: document.querySelector("#renderForm"),
  renderLeadId: document.querySelector("#renderLeadId"),
  renderTemplateId: document.querySelector("#renderTemplateId"),
  renderOfferId: document.querySelector("#renderOfferId"),
  renderPlaybookId: document.querySelector("#renderPlaybookId"),
  renderDisabledBlocks: document.querySelector("#renderDisabledBlocks"),
  renderDeterministic: document.querySelector("#renderDeterministic"),
  regenerateForm: document.querySelector("#regenerateForm"),
  regenerateArtifactId: document.querySelector("#regenerateArtifactId"),
  regenerateBlockKeys: document.querySelector("#regenerateBlockKeys"),
  campaignSummary: document.querySelector("#campaignSummary"),
  campaignNotice: document.querySelector("#campaignNotice"),
  campaignList: document.querySelector("#campaignList"),
  providerAccountForm: document.querySelector("#providerAccountForm"),
  providerDisplayName: document.querySelector("#providerDisplayName"),
  providerEmailAddress: document.querySelector("#providerEmailAddress"),
  providerName: document.querySelector("#providerName"),
  providerDailyCap: document.querySelector("#providerDailyCap"),
  campaignBuildForm: document.querySelector("#campaignBuildForm"),
  campaignName: document.querySelector("#campaignName"),
  campaignTimezone: document.querySelector("#campaignTimezone"),
  campaignSavedFilterId: document.querySelector("#campaignSavedFilterId"),
  campaignOfferId: document.querySelector("#campaignOfferId"),
  campaignPlaybookId: document.querySelector("#campaignPlaybookId"),
  campaignProviderAccountIds: document.querySelector("#campaignProviderAccountIds"),
  campaignStepOneTemplateId: document.querySelector("#campaignStepOneTemplateId"),
  campaignStepOneDelayDays: document.querySelector("#campaignStepOneDelayDays"),
  campaignStepTwoTemplateId: document.querySelector("#campaignStepTwoTemplateId"),
  campaignStepTwoDelayDays: document.querySelector("#campaignStepTwoDelayDays"),
  campaignStartAt: document.querySelector("#campaignStartAt"),
  campaignApprovalMode: document.querySelector("#campaignApprovalMode"),
  campaignReplyMode: document.querySelector("#campaignReplyMode"),
  campaignSendWindowStart: document.querySelector("#campaignSendWindowStart"),
  campaignSendWindowEnd: document.querySelector("#campaignSendWindowEnd"),
  campaignQuietHoursStart: document.querySelector("#campaignQuietHoursStart"),
  campaignQuietHoursEnd: document.querySelector("#campaignQuietHoursEnd"),
  campaignPreviewCampaignId: document.querySelector("#campaignPreviewCampaignId"),
  campaignPreviewRows: document.querySelector("#campaignPreviewRows"),
  campaignPreviewEditForm: document.querySelector("#campaignPreviewEditForm"),
  campaignPreviewMessageId: document.querySelector("#campaignPreviewMessageId"),
  campaignEditedSubject: document.querySelector("#campaignEditedSubject"),
  campaignEditedBody: document.querySelector("#campaignEditedBody"),
  campaignPreviewRegenerateForm: document.querySelector("#campaignPreviewRegenerateForm"),
  campaignRegenerateMessageId: document.querySelector("#campaignRegenerateMessageId"),
  campaignRegenerateBlockKeys: document.querySelector("#campaignRegenerateBlockKeys"),
  campaignApproveForm: document.querySelector("#campaignApproveForm"),
  campaignApproveId: document.querySelector("#campaignApproveId"),
  csvImportForm: document.querySelector("#csvImportForm"),
  csvListName: document.querySelector("#csvListName"),
  csvSource: document.querySelector("#csvSource"),
  csvImportText: document.querySelector("#csvImportText"),
  manualLeadForm: document.querySelector("#manualLeadForm"),
  manualFullName: document.querySelector("#manualFullName"),
  manualEmail: document.querySelector("#manualEmail"),
  manualCompany: document.querySelector("#manualCompany"),
  manualDomain: document.querySelector("#manualDomain"),
  manualTitle: document.querySelector("#manualTitle"),
  manualListName: document.querySelector("#manualListName"),
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

async function postJson(path, payload) {
  const response = await fetch(path, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  const parsed = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(parsed.error || parsed.message || JSON.stringify(parsed));
  }
  return parsed;
}

async function refreshState() {
  nodes.refreshButton.disabled = true;
  nodes.refreshButton.textContent = "Refreshing...";

  try {
    const [live, ready, version, startup, runtime, settings, schema, warehouse, studio, campaigns] = await Promise.all([
      fetchJson("/api/v1/health/live"),
      fetchJson("/api/v1/health/ready"),
      fetchJson("/api/v1/version"),
      fetchJson("/api/v1/diagnostics/startup"),
      fetchJson("/api/v1/diagnostics/runtime"),
      fetchJson("/api/v1/settings/effective"),
      fetchJson("/api/v1/settings/schema"),
      fetchJson("/api/v1/warehouse/summary"),
      fetchJson("/api/v1/studio/summary"),
      fetchJson("/api/v1/campaigns/summary"),
    ]);

    const availableCampaignIds = (campaigns.payload?.campaigns || []).map((campaign) => String(campaign.id));
    if (!availableCampaignIds.length) {
      state.selectedCampaignId = null;
    } else if (!state.selectedCampaignId || !availableCampaignIds.includes(String(state.selectedCampaignId))) {
      state.selectedCampaignId = campaigns.payload?.latest_campaign_id || campaigns.payload?.campaigns?.[0]?.id || null;
    }

    const campaignPreview = state.selectedCampaignId
      ? await fetchJson(`/api/v1/campaigns/preview?campaign_id=${encodeURIComponent(state.selectedCampaignId)}`)
      : { ok: true, status: 200, payload: { campaign: null, items: [], queue_counts: {}, launch_ready: false } };

    state.live = live;
    state.ready = ready;
    state.version = version;
    state.startup = startup;
    state.runtime = runtime;
    state.settings = settings;
    state.schema = schema;
    state.warehouse = warehouse;
    state.studio = studio;
    state.campaigns = campaigns;
    state.campaignPreview = campaignPreview;
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

function renderWarehouseSummary(node, warehouse = {}) {
  const cards = [
    { label: "Leads", value: warehouse.lead_count ?? 0 },
    { label: "Companies", value: warehouse.company_count ?? 0 },
    { label: "Lists", value: warehouse.list_count ?? 0 },
    { label: "Saved filters", value: warehouse.saved_filter_count ?? 0 },
  ];
  node.innerHTML = cards
    .map(
      (item) => `
        <article class="check-item">
          <strong>${escapeHtml(item.label)}</strong>
          <span>${escapeHtml(String(item.value))}</span>
        </article>
      `,
    )
    .join("");
}

function renderStudioSummary(node, studio = {}) {
  const cards = [
    { label: "Templates", value: studio.template_count ?? 0 },
    { label: "Offers", value: studio.offer_profile_count ?? 0 },
    { label: "Playbooks", value: studio.vertical_playbook_count ?? 0 },
    { label: "Artifacts", value: studio.artifact_count ?? 0 },
  ];
  node.innerHTML = cards
    .map(
      (item) => `
        <article class="check-item">
          <strong>${escapeHtml(item.label)}</strong>
          <span>${escapeHtml(String(item.value))}</span>
        </article>
      `,
    )
    .join("");
}

function fillSelect(node, items, { valueKey = "id", labelFn, includeBlankLabel = null } = {}) {
  const currentValue = node.value;
  const options = [];
  if (includeBlankLabel !== null) {
    options.push(`<option value="">${escapeHtml(includeBlankLabel)}</option>`);
  }
  for (const item of items) {
    const value = item?.[valueKey];
    const label = labelFn ? labelFn(item) : String(value ?? "");
    options.push(`<option value="${escapeHtml(String(value ?? ""))}">${escapeHtml(label)}</option>`);
  }
  node.innerHTML = options.join("");
  if (currentValue && [...node.options].some((option) => option.value === currentValue)) {
    node.value = currentValue;
    return;
  }
  if ([...node.options].length > 0) {
    node.selectedIndex = 0;
  }
}

function fillMultiSelect(node, items, { valueKey = "id", labelFn } = {}) {
  const selectedValues = new Set([...node.selectedOptions].map((option) => option.value));
  node.innerHTML = items
    .map((item) => {
      const value = String(item?.[valueKey] ?? "");
      const label = labelFn ? labelFn(item) : value;
      return `<option value="${escapeHtml(value)}">${escapeHtml(label)}</option>`;
    })
    .join("");
  for (const option of node.options) {
    option.selected = selectedValues.has(option.value);
  }
  if (!selectedValues.size && node.options.length > 0) {
    node.options[0].selected = true;
  }
}

function splitListInput(value) {
  return String(value || "")
    .split(/[\n,]/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function renderCampaignSummary(node, campaigns = {}) {
  const cards = [
    { label: "Campaigns", value: campaigns.campaign_count ?? 0 },
    { label: "Provider accounts", value: campaigns.provider_account_count ?? 0 },
    { label: "Latest campaign", value: campaigns.latest_campaign_id ?? "None" },
    { label: "Preview rows", value: (campaigns.campaigns || []).reduce((total, item) => total + (item.queued_count || 0), 0) },
  ];
  node.innerHTML = cards
    .map(
      (item) => `
        <article class="check-item">
          <strong>${escapeHtml(item.label)}</strong>
          <span>${escapeHtml(String(item.value))}</span>
        </article>
      `,
    )
    .join("");
}

function renderCampaignPreviewRows(node, items = []) {
  node.innerHTML = items
    .map(
      (item) => `
        <article class="stack-item">
          <strong>${escapeHtml(`${item.lead_name} · step ${item.step_order + 1} · ${item.state}`)}</strong>
          <span>${escapeHtml(`${item.company_name || "Unknown company"} · ${prettyDate(item.scheduled_for)} · ${item.provider_account_name}`)}</span>
          <span>${escapeHtml(`Subject: ${item.subject}`)}</span>
          <span>${escapeHtml(`Body: ${item.body}`)}</span>
          <span>${escapeHtml(`Static sections: ${(item.static_sections || []).map((section) => section.rendered_text).join(" | ") || "None"}`)}</span>
          <span>${escapeHtml(`AI sections: ${(item.ai_sections || []).map((section) => section.rendered_text).join(" | ") || "None"}`)}</span>
          <span>${escapeHtml(`Risk flags: ${(item.risk_flags || []).length}`)}</span>
        </article>
      `,
    )
    .join("");
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
  const warehouse = state.warehouse?.payload ?? {};
  const studio = state.studio?.payload ?? {};
  const campaigns = state.campaigns?.payload ?? {};
  const campaignPreview = state.campaignPreview?.payload ?? {};
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
  renderWarehouseSummary(nodes.warehouseSummary, warehouse);
  renderStudioSummary(nodes.studioSummary, studio);
  renderCampaignSummary(nodes.campaignSummary, campaigns);
  renderSummaryList(
    nodes.importJobs,
    (warehouse.recent_imports || []).map((job) => ({
      label: `${job.source} ${job.import_format} · ${job.status}`,
      value: `rows ${job.total_read} · inserted ${job.inserted_count} · merged ${job.merged_count} · review ${job.manual_review_required_count}`,
    })),
  );
  renderSummaryList(
    nodes.savedFilters,
    (warehouse.saved_filters || []).map((filter) => ({
      label: filter.name,
      value: filter.filter_json,
    })),
  );
  renderSummaryList(
    nodes.warehouseLeads,
    (warehouse.lead_preview || []).map((lead) => ({
      label: `${lead.full_name} · ${lead.company_name || lead.company_name_snapshot || "Unknown company"}`,
      value: `${lead.email || "no email"} · ${lead.source}${lead.tags?.length ? ` · tags: ${lead.tags.join(", ")}` : ""}`,
    })),
  );
  renderSummaryList(
    nodes.studioArtifacts,
    (studio.recent_artifacts || []).map((artifact) => ({
      label: `${artifact.validation_status || "unknown"} · ${artifact.template_name || "Template"} · ${artifact.lead_name || "Lead"}`,
      value: `${artifact.subject || "No subject"}${artifact.risk_flags?.length ? ` · flags ${artifact.risk_flags.length}` : ""}`,
    })),
  );
  renderSummaryList(
    nodes.studioTemplates,
    (studio.templates || []).map((template) => ({
      label: `${template.name} · ${template.variant_count} variants`,
      value: `${template.block_count} blocks · ${template.channel || "email"}`,
    })),
  );
  renderSummaryList(
    nodes.studioOffers,
    (studio.offer_profiles || []).map((offer) => ({
      label: offer.name,
      value: `${offer.value_proposition || "No value proposition"} · ${offer.standard_cta || "No CTA"}`,
    })),
  );
  renderSummaryList(
    nodes.studioPlaybooks,
    (studio.vertical_playbooks || []).map((playbook) => ({
      label: playbook.name,
      value: `${playbook.tone_profile || "No tone"}${playbook.target_pains?.length ? ` · pains: ${playbook.target_pains.join(", ")}` : ""}`,
    })),
  );
  renderSummaryList(
    nodes.campaignList,
    (campaigns.campaigns || []).map((campaign) => ({
      label: `${campaign.name} · ${campaign.status}`,
      value: `audience ${campaign.audience_count} · queued ${campaign.queued_count} · blocked ${campaign.blocked_count} · approved ${campaign.approved_count}`,
    })),
  );
  renderCampaignPreviewRows(nodes.campaignPreviewRows, campaignPreview.items || []);
  nodes.warehouseNotice.textContent = (warehouse.recent_imports || []).length
    ? `Last import updated ${prettyDate((warehouse.recent_imports || [])[0]?.updated_at)}`
    : "No warehouse actions yet.";
  nodes.studioNotice.textContent = (studio.recent_artifacts || []).length
    ? `Last preview updated ${prettyDate((studio.recent_artifacts || [])[0]?.updated_at)}`
    : "No generation artifacts yet.";
  nodes.campaignNotice.textContent = campaignPreview.campaign
    ? `Campaign ${campaignPreview.campaign.name} · ${campaignPreview.launch_ready ? "launch-ready" : "preview only"}`
    : "No campaign previews yet.";

  fillSelect(nodes.renderLeadId, studio.lead_preview || [], {
    valueKey: "id",
    labelFn: (lead) => `${lead.id} · ${lead.full_name} · ${lead.company_name || lead.company_name_snapshot || "Unknown company"}`,
  });
  fillSelect(nodes.renderTemplateId, studio.templates || [], {
    valueKey: "id",
    labelFn: (template) => `${template.id} · ${template.name}`,
  });
  fillSelect(nodes.renderOfferId, studio.offer_profiles || [], {
    valueKey: "id",
    labelFn: (offer) => `${offer.id} · ${offer.name}`,
    includeBlankLabel: "No offer profile",
  });
  fillSelect(nodes.renderPlaybookId, studio.vertical_playbooks || [], {
    valueKey: "id",
    labelFn: (playbook) => `${playbook.id} · ${playbook.name}`,
    includeBlankLabel: "No playbook",
  });
  fillSelect(nodes.regenerateArtifactId, studio.recent_artifacts || [], {
    valueKey: "id",
    labelFn: (artifact) => `${artifact.id} · ${artifact.template_name || "Template"} · ${artifact.subject || "No subject"}`,
  });
  fillSelect(nodes.campaignSavedFilterId, campaigns.saved_filters || [], {
    valueKey: "id",
    labelFn: (filter) => `${filter.id} · ${filter.name}`,
    includeBlankLabel: "All leads",
  });
  fillSelect(nodes.campaignOfferId, campaigns.offer_profiles || [], {
    valueKey: "id",
    labelFn: (offer) => `${offer.id} · ${offer.name}`,
    includeBlankLabel: "No offer profile",
  });
  fillSelect(nodes.campaignPlaybookId, campaigns.vertical_playbooks || [], {
    valueKey: "id",
    labelFn: (playbook) => `${playbook.id} · ${playbook.name}`,
    includeBlankLabel: "No playbook",
  });
  fillMultiSelect(nodes.campaignProviderAccountIds, campaigns.provider_accounts || [], {
    valueKey: "id",
    labelFn: (account) => `${account.id} · ${account.display_name} · ${account.email_address}`,
  });
  fillSelect(nodes.campaignStepOneTemplateId, campaigns.templates || [], {
    valueKey: "id",
    labelFn: (template) => `${template.id} · ${template.name}`,
  });
  fillSelect(nodes.campaignStepTwoTemplateId, campaigns.templates || [], {
    valueKey: "id",
    labelFn: (template) => `${template.id} · ${template.name}`,
    includeBlankLabel: "No second step",
  });
  fillSelect(nodes.campaignPreviewCampaignId, campaigns.campaigns || [], {
    valueKey: "id",
    labelFn: (campaign) => `${campaign.id} · ${campaign.name} · ${campaign.status}`,
  });
  fillSelect(nodes.campaignApproveId, campaigns.campaigns || [], {
    valueKey: "id",
    labelFn: (campaign) => `${campaign.id} · ${campaign.name}`,
  });
  fillSelect(nodes.campaignPreviewMessageId, campaignPreview.items || [], {
    valueKey: "id",
    labelFn: (item) => `${item.id} · ${item.lead_name} · step ${item.step_order + 1}`,
  });
  fillSelect(nodes.campaignRegenerateMessageId, campaignPreview.items || [], {
    valueKey: "id",
    labelFn: (item) => `${item.id} · ${item.lead_name} · step ${item.step_order + 1}`,
  });
  if (state.selectedCampaignId && nodes.campaignPreviewCampaignId.value !== String(state.selectedCampaignId)) {
    nodes.campaignPreviewCampaignId.value = String(state.selectedCampaignId);
  }
  if (state.selectedCampaignId && nodes.campaignApproveId.value !== String(state.selectedCampaignId)) {
    nodes.campaignApproveId.value = String(state.selectedCampaignId);
  }

  if (state.error) {
    nodes.healthSummary.insertAdjacentHTML(
      "afterbegin",
      `<div class="error-banner">Diagnostics refresh failed: ${escapeHtml(state.error)}</div>`,
    );
  }

  renderRoute();
}

async function handleCsvImport(event) {
  event.preventDefault();
  await postJson("/api/v1/warehouse/imports/csv", {
    list_name: nodes.csvListName.value,
    source: nodes.csvSource.value,
    csv_text: nodes.csvImportText.value,
  });
  await refreshState();
  window.location.hash = "#/warehouse";
}

async function handleManualLead(event) {
  event.preventDefault();
  await postJson("/api/v1/warehouse/leads/manual", {
    list_name: nodes.manualListName.value,
    lead: {
      full_name: nodes.manualFullName.value,
      email: nodes.manualEmail.value,
      company: nodes.manualCompany.value,
      domain: nodes.manualDomain.value,
      title: nodes.manualTitle.value,
    },
  });
  await refreshState();
  window.location.hash = "#/warehouse";
}

async function handleOfferProfileSave(event) {
  event.preventDefault();
  await postJson("/api/v1/studio/offers", {
    name: nodes.offerName.value,
    value_proposition: nodes.offerValueProp.value,
    standard_cta: nodes.offerCta.value,
    allowed_claims: splitListInput(nodes.offerAllowedClaims.value),
    disallowed_claims: splitListInput(nodes.offerDisallowedClaims.value),
  });
  await refreshState();
  window.location.hash = "#/studio";
}

async function handlePlaybookSave(event) {
  event.preventDefault();
  await postJson("/api/v1/studio/playbooks", {
    name: nodes.playbookName.value,
    tone_profile: nodes.playbookTone.value,
    target_pains: splitListInput(nodes.playbookPains.value),
    sample_subject_patterns: splitListInput(nodes.playbookSubjects.value),
    disallowed_language: splitListInput(nodes.playbookDisallowedLanguage.value),
  });
  await refreshState();
  window.location.hash = "#/studio";
}

async function handleTemplateSave(event) {
  event.preventDefault();
  const blocks = [
    {
      block_key: "subject_line",
      block_type: "merged",
      section: "subject",
      content: nodes.templateSubject.value,
      is_required: true,
    },
    {
      block_key: "body_intro",
      block_type: "merged",
      section: "body",
      content: nodes.templateIntro.value,
      is_required: true,
    },
    {
      block_key: "body_hook",
      block_type: "ai_generated",
      section: "body",
      content: nodes.templateAiInstruction.value,
      fallback_content: nodes.templateAiFallback.value,
      rules: { max_words: 22 },
      is_required: true,
    },
    {
      block_key: "body_offer",
      block_type: "merged",
      section: "body",
      content: nodes.templateOfferLine.value,
      is_required: false,
    },
    {
      block_key: "body_cta",
      block_type: "merged",
      section: "body",
      content: nodes.templateCtaLine.value,
      is_required: false,
    },
  ].filter((block) => String(block.content || block.fallback_content || "").trim());

  await postJson("/api/v1/studio/templates", {
    name: nodes.templateName.value,
    description: nodes.templateDescription.value,
    variants: [
      {
        name: "default",
        variant_label: "Default",
        is_default: true,
        blocks,
      },
    ],
  });
  await refreshState();
  window.location.hash = "#/studio";
}

async function handleStudioRender(event) {
  event.preventDefault();
  await postJson("/api/v1/studio/render", {
    lead_id: Number(nodes.renderLeadId.value),
    template_id: Number(nodes.renderTemplateId.value),
    offer_profile_id: nodes.renderOfferId.value ? Number(nodes.renderOfferId.value) : null,
    vertical_playbook_id: nodes.renderPlaybookId.value ? Number(nodes.renderPlaybookId.value) : null,
    deterministic_mode: nodes.renderDeterministic.checked,
    disabled_block_keys: splitListInput(nodes.renderDisabledBlocks.value),
  });
  await refreshState();
  window.location.hash = "#/studio";
}

async function handleArtifactRegenerate(event) {
  event.preventDefault();
  await postJson("/api/v1/studio/regenerate", {
    artifact_id: Number(nodes.regenerateArtifactId.value),
    regenerate_block_keys: splitListInput(nodes.regenerateBlockKeys.value),
  });
  await refreshState();
  window.location.hash = "#/studio";
}

async function handleProviderAccountSave(event) {
  event.preventDefault();
  await postJson("/api/v1/campaigns/providers/accounts", {
    display_name: nodes.providerDisplayName.value,
    email_address: nodes.providerEmailAddress.value,
    provider_name: nodes.providerName.value,
    daily_cap: Number(nodes.providerDailyCap.value || 0),
  });
  await refreshState();
  window.location.hash = "#/campaigns";
}

async function handleCampaignBuild(event) {
  event.preventDefault();
  const steps = [
    {
      template_id: Number(nodes.campaignStepOneTemplateId.value),
      delay_days: Number(nodes.campaignStepOneDelayDays.value || 0),
    },
  ];
  if (nodes.campaignStepTwoTemplateId.value) {
    steps.push({
      template_id: Number(nodes.campaignStepTwoTemplateId.value),
      delay_days: Number(nodes.campaignStepTwoDelayDays.value || 0),
    });
  }

  const payload = {
    name: nodes.campaignName.value,
    timezone: nodes.campaignTimezone.value,
    offer_profile_id: nodes.campaignOfferId.value ? Number(nodes.campaignOfferId.value) : null,
    vertical_playbook_id: nodes.campaignPlaybookId.value ? Number(nodes.campaignPlaybookId.value) : null,
    provider_account_ids: [...nodes.campaignProviderAccountIds.selectedOptions].map((option) => Number(option.value)),
    steps,
    start_at: nodes.campaignStartAt.value,
    approval_mode: nodes.campaignApprovalMode.value,
    reply_mode: nodes.campaignReplyMode.value,
    send_window: {
      start_hour: Number(nodes.campaignSendWindowStart.value || 9),
      end_hour: Number(nodes.campaignSendWindowEnd.value || 17),
      interval_minutes: 15,
      timezone: nodes.campaignTimezone.value,
    },
    quiet_hours: {
      start_hour: Number(nodes.campaignQuietHoursStart.value || 20),
      end_hour: Number(nodes.campaignQuietHoursEnd.value || 8),
    },
  };
  if (nodes.campaignSavedFilterId.value) {
    payload.audience = { saved_filter_id: Number(nodes.campaignSavedFilterId.value) };
  }

  const preview = await postJson("/api/v1/campaigns/build", payload);
  state.selectedCampaignId = preview.campaign?.id || null;
  await refreshState();
  window.location.hash = "#/campaigns";
}

async function loadCampaignPreview(campaignId) {
  if (!campaignId) {
    state.selectedCampaignId = null;
    state.campaignPreview = { ok: true, status: 200, payload: { campaign: null, items: [], queue_counts: {}, launch_ready: false } };
    render();
    return;
  }
  state.selectedCampaignId = Number(campaignId);
  state.campaignPreview = await fetchJson(`/api/v1/campaigns/preview?campaign_id=${encodeURIComponent(campaignId)}`);
  render();
}

async function handleCampaignPreviewSelection(event) {
  await loadCampaignPreview(event.target.value);
  window.location.hash = "#/campaigns";
}

async function handleCampaignPreviewEdit(event) {
  event.preventDefault();
  await postJson("/api/v1/campaigns/preview/edit", {
    queued_message_id: Number(nodes.campaignPreviewMessageId.value),
    edited_subject: nodes.campaignEditedSubject.value || null,
    edited_body: nodes.campaignEditedBody.value || null,
  });
  await refreshState();
  window.location.hash = "#/campaigns";
}

async function handleCampaignPreviewRegenerate(event) {
  event.preventDefault();
  await postJson("/api/v1/campaigns/preview/regenerate", {
    queued_message_id: Number(nodes.campaignRegenerateMessageId.value),
    regenerate_block_keys: splitListInput(nodes.campaignRegenerateBlockKeys.value),
  });
  await refreshState();
  window.location.hash = "#/campaigns";
}

async function handleCampaignPreviewApprove(event) {
  event.preventDefault();
  await postJson("/api/v1/campaigns/preview/approve", {
    campaign_id: Number(nodes.campaignApproveId.value),
  });
  state.selectedCampaignId = Number(nodes.campaignApproveId.value);
  await refreshState();
  window.location.hash = "#/campaigns";
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
nodes.csvImportForm.addEventListener("submit", (event) => {
  void handleCsvImport(event);
});
nodes.manualLeadForm.addEventListener("submit", (event) => {
  void handleManualLead(event);
});
nodes.offerProfileForm.addEventListener("submit", (event) => {
  void handleOfferProfileSave(event);
});
nodes.playbookForm.addEventListener("submit", (event) => {
  void handlePlaybookSave(event);
});
nodes.templateForm.addEventListener("submit", (event) => {
  void handleTemplateSave(event);
});
nodes.providerAccountForm.addEventListener("submit", (event) => {
  void handleProviderAccountSave(event);
});
nodes.campaignBuildForm.addEventListener("submit", (event) => {
  void handleCampaignBuild(event);
});
nodes.campaignPreviewCampaignId.addEventListener("change", (event) => {
  void handleCampaignPreviewSelection(event);
});
nodes.campaignPreviewEditForm.addEventListener("submit", (event) => {
  void handleCampaignPreviewEdit(event);
});
nodes.campaignPreviewRegenerateForm.addEventListener("submit", (event) => {
  void handleCampaignPreviewRegenerate(event);
});
nodes.campaignApproveForm.addEventListener("submit", (event) => {
  void handleCampaignPreviewApprove(event);
});
nodes.renderForm.addEventListener("submit", (event) => {
  void handleStudioRender(event);
});
nodes.regenerateForm.addEventListener("submit", (event) => {
  void handleArtifactRegenerate(event);
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
