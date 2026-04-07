"""Versioned schema registry for the Sprint 2 persistence layer."""

from __future__ import annotations

from dataclasses import dataclass

from backend.app.domain.enums import (
    CampaignStatus,
    EntityType,
    ImportJobStatus,
    ImportRowResolution,
    QueuedMessageState,
    ReplyState,
    SuppressionScope,
    TemplateBlockType,
)


def sql_enum(values: list[str]) -> str:
    return ", ".join(f"'{value}'" for value in values)


CAMPAIGN_STATUS_SQL = sql_enum([status.value for status in CampaignStatus])
QUEUED_MESSAGE_STATE_SQL = sql_enum([status.value for status in QueuedMessageState])
REPLY_STATE_SQL = sql_enum([status.value for status in ReplyState])
SUPPRESSION_SCOPE_SQL = sql_enum([scope.value for scope in SuppressionScope])
TEMPLATE_BLOCK_TYPE_SQL = sql_enum([block_type.value for block_type in TemplateBlockType])
ENTITY_TYPE_SQL = sql_enum([entity_type.value for entity_type in EntityType])
IMPORT_JOB_STATUS_SQL = sql_enum([status.value for status in ImportJobStatus])
IMPORT_ROW_RESOLUTION_SQL = sql_enum([resolution.value for resolution in ImportRowResolution])
LEGACY_ENTITY_TYPE_SQL = sql_enum(
    [
        "lead",
        "company",
        "list",
        "campaign",
        "sequence",
        "sequence_step",
        "template",
        "template_variant",
        "template_block",
        "offer_profile",
        "vertical_playbook",
        "queued_message",
        "sent_message",
        "reply",
        "thread",
        "provider_account",
        "provider_mapping",
        "booking_link",
        "suppression_entry",
        "generation_artifact",
        "audit_event",
        "webhook_event",
        "settings_bundle",
        "tag",
    ]
)


@dataclass(frozen=True)
class MigrationDefinition:
    version: str
    description: str
    up_sql: str
    down_sql: str


MIGRATIONS = [
    MigrationDefinition(
        version="0001_sprint_2_domain_model",
        description="Create the Sprint 2 domain model, repositories, and audit schema.",
        up_sql=f"""
CREATE TABLE IF NOT EXISTS companies (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    external_source_id TEXT,
    name TEXT NOT NULL,
    domain TEXT COLLATE NOCASE,
    website TEXT NOT NULL DEFAULT '',
    phone TEXT NOT NULL DEFAULT '',
    vertical TEXT NOT NULL DEFAULT '',
    employee_count_band TEXT NOT NULL DEFAULT '',
    revenue_band TEXT NOT NULL DEFAULT '',
    city TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL DEFAULT '',
    country TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (length(trim(name)) > 0)
);
CREATE UNIQUE INDEX IF NOT EXISTS companies_source_external_id_unique
    ON companies(source, external_source_id)
    WHERE external_source_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS companies_domain_unique
    ON companies(domain)
    WHERE domain IS NOT NULL;

CREATE TABLE IF NOT EXISTS leads (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    external_source_id TEXT,
    first_name TEXT NOT NULL DEFAULT '',
    last_name TEXT NOT NULL DEFAULT '',
    full_name TEXT NOT NULL,
    email TEXT COLLATE NOCASE,
    phone TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    linkedin_url TEXT NOT NULL DEFAULT '',
    company_id INTEGER REFERENCES companies(id) ON DELETE RESTRICT,
    company_name_snapshot TEXT NOT NULL DEFAULT '',
    company_domain_snapshot TEXT NOT NULL DEFAULT '',
    city TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL DEFAULT '',
    country TEXT NOT NULL DEFAULT '',
    enrichment_status TEXT NOT NULL DEFAULT 'pending',
    verification_status TEXT NOT NULL DEFAULT 'unverified',
    suppression_status TEXT NOT NULL DEFAULT 'clear',
    fit_score REAL NOT NULL DEFAULT 0,
    personalization_json TEXT NOT NULL DEFAULT '{{}}',
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (length(trim(full_name)) > 0),
    CHECK (email IS NOT NULL OR phone != '' OR linkedin_url != '')
);
CREATE UNIQUE INDEX IF NOT EXISTS leads_source_external_id_unique
    ON leads(source, external_source_id)
    WHERE external_source_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS leads_email_unique
    ON leads(email)
    WHERE email IS NOT NULL;

CREATE TABLE IF NOT EXISTS lists (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'static',
    description TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(name)
);

CREATE TABLE IF NOT EXISTS list_memberships (
    id INTEGER PRIMARY KEY,
    list_id INTEGER NOT NULL REFERENCES lists(id) ON DELETE CASCADE,
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    added_at TEXT NOT NULL,
    UNIQUE(list_id, lead_id)
);

CREATE TABLE IF NOT EXISTS booking_links (
    id INTEGER PRIMARY KEY,
    label TEXT NOT NULL,
    url TEXT NOT NULL,
    provider_name TEXT NOT NULL DEFAULT 'google_calendar',
    is_default INTEGER NOT NULL DEFAULT 0 CHECK (is_default IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(label),
    UNIQUE(url)
);

CREATE TABLE IF NOT EXISTS offer_profiles (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    target_verticals_json TEXT NOT NULL DEFAULT '[]',
    target_pains_json TEXT NOT NULL DEFAULT '[]',
    value_proposition TEXT NOT NULL DEFAULT '',
    standard_cta TEXT NOT NULL DEFAULT '',
    booking_link_id INTEGER REFERENCES booking_links(id) ON DELETE SET NULL,
    allowed_claims_json TEXT NOT NULL DEFAULT '[]',
    disallowed_claims_json TEXT NOT NULL DEFAULT '[]',
    pricing_framing_snippets_json TEXT NOT NULL DEFAULT '[]',
    objection_handling_snippets_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(name)
);

CREATE TABLE IF NOT EXISTS vertical_playbooks (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    target_pains_json TEXT NOT NULL DEFAULT '[]',
    acceptable_language_json TEXT NOT NULL DEFAULT '[]',
    disallowed_language_json TEXT NOT NULL DEFAULT '[]',
    personalization_strategy TEXT NOT NULL DEFAULT '',
    tone_profile TEXT NOT NULL DEFAULT '',
    sample_subject_patterns_json TEXT NOT NULL DEFAULT '[]',
    standard_objections_json TEXT NOT NULL DEFAULT '[]',
    escalation_rules_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(name)
);

CREATE TABLE IF NOT EXISTS campaigns (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK (status IN ({CAMPAIGN_STATUS_SQL})),
    offer_profile_id INTEGER REFERENCES offer_profiles(id) ON DELETE SET NULL,
    vertical_playbook_id INTEGER REFERENCES vertical_playbooks(id) ON DELETE SET NULL,
    provider_name TEXT NOT NULL,
    approval_mode TEXT NOT NULL,
    reply_mode TEXT NOT NULL,
    send_window_config_json TEXT NOT NULL DEFAULT '{{}}',
    quiet_hours_config_json TEXT NOT NULL DEFAULT '{{}}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    launched_at TEXT,
    paused_at TEXT,
    completed_at TEXT,
    UNIQUE(name)
);

CREATE TABLE IF NOT EXISTS sequences (
    id INTEGER PRIMARY KEY,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    timezone TEXT NOT NULL DEFAULT 'UTC',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(campaign_id, name)
);

CREATE TABLE IF NOT EXISTS templates (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    channel TEXT NOT NULL DEFAULT 'email',
    is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(name)
);

CREATE TABLE IF NOT EXISTS template_variants (
    id INTEGER PRIMARY KEY,
    template_id INTEGER NOT NULL REFERENCES templates(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    variant_label TEXT NOT NULL DEFAULT '',
    is_default INTEGER NOT NULL DEFAULT 0 CHECK (is_default IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(template_id, name)
);

CREATE TABLE IF NOT EXISTS sequence_steps (
    id INTEGER PRIMARY KEY,
    sequence_id INTEGER NOT NULL REFERENCES sequences(id) ON DELETE CASCADE,
    step_order INTEGER NOT NULL CHECK (step_order >= 0),
    step_type TEXT NOT NULL DEFAULT 'email',
    delay_days INTEGER NOT NULL DEFAULT 0 CHECK (delay_days >= 0),
    template_id INTEGER NOT NULL REFERENCES templates(id) ON DELETE RESTRICT,
    template_variant_id INTEGER NOT NULL REFERENCES template_variants(id) ON DELETE RESTRICT,
    subject_override TEXT NOT NULL DEFAULT '',
    body_override TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(sequence_id, step_order)
);

CREATE TABLE IF NOT EXISTS template_blocks (
    id INTEGER PRIMARY KEY,
    template_id INTEGER NOT NULL REFERENCES templates(id) ON DELETE CASCADE,
    template_variant_id INTEGER REFERENCES template_variants(id) ON DELETE CASCADE,
    block_key TEXT NOT NULL,
    block_type TEXT NOT NULL CHECK (block_type IN ({TEMPLATE_BLOCK_TYPE_SQL})),
    content TEXT NOT NULL DEFAULT '',
    position INTEGER NOT NULL DEFAULT 0 CHECK (position >= 0),
    is_required INTEGER NOT NULL DEFAULT 1 CHECK (is_required IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS template_blocks_unique_key
    ON template_blocks(template_id, ifnull(template_variant_id, 0), block_key);

CREATE TABLE IF NOT EXISTS provider_accounts (
    id INTEGER PRIMARY KEY,
    provider_name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    sending_domain TEXT COLLATE NOCASE,
    email_address TEXT COLLATE NOCASE,
    external_account_id TEXT,
    daily_cap INTEGER NOT NULL DEFAULT 0 CHECK (daily_cap >= 0),
    warmup_cap INTEGER NOT NULL DEFAULT 0 CHECK (warmup_cap >= 0),
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(provider_name, display_name)
);
CREATE UNIQUE INDEX IF NOT EXISTS provider_accounts_external_id_unique
    ON provider_accounts(provider_name, external_account_id)
    WHERE external_account_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS generation_artifacts (
    id INTEGER PRIMARY KEY,
    kind TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    prompt_input_json TEXT NOT NULL DEFAULT '{{}}',
    output_text TEXT NOT NULL DEFAULT '',
    output_json TEXT NOT NULL DEFAULT '{{}}',
    validation_status TEXT NOT NULL DEFAULT 'pending',
    risk_flags_json TEXT NOT NULL DEFAULT '[]',
    model_name TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS campaign_audience_snapshots (
    id INTEGER PRIMARY KEY,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE RESTRICT,
    company_id INTEGER REFERENCES companies(id) ON DELETE SET NULL,
    snapshot_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(campaign_id, lead_id)
);

CREATE TABLE IF NOT EXISTS queued_messages (
    id INTEGER PRIMARY KEY,
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE RESTRICT,
    company_id INTEGER REFERENCES companies(id) ON DELETE SET NULL,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    sequence_id INTEGER NOT NULL REFERENCES sequences(id) ON DELETE CASCADE,
    sequence_step_id INTEGER NOT NULL REFERENCES sequence_steps(id) ON DELETE RESTRICT,
    template_id INTEGER NOT NULL REFERENCES templates(id) ON DELETE RESTRICT,
    template_variant_id INTEGER NOT NULL REFERENCES template_variants(id) ON DELETE RESTRICT,
    generation_artifact_id INTEGER NOT NULL REFERENCES generation_artifacts(id) ON DELETE RESTRICT,
    provider_account_id INTEGER NOT NULL REFERENCES provider_accounts(id) ON DELETE RESTRICT,
    scheduled_for TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ({QUEUED_MESSAGE_STATE_SQL})),
    rendered_subject TEXT NOT NULL DEFAULT '',
    rendered_body TEXT NOT NULL DEFAULT '',
    render_hash TEXT NOT NULL DEFAULT '',
    risk_flags_json TEXT NOT NULL DEFAULT '[]',
    failure_reason TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(campaign_id, lead_id, sequence_step_id)
);

CREATE TABLE IF NOT EXISTS threads (
    id INTEGER PRIMARY KEY,
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE RESTRICT,
    campaign_id INTEGER REFERENCES campaigns(id) ON DELETE SET NULL,
    provider_name TEXT NOT NULL DEFAULT '',
    external_thread_id TEXT,
    status TEXT NOT NULL DEFAULT 'open',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS threads_campaign_lead_unique
    ON threads(campaign_id, lead_id)
    WHERE campaign_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS threads_external_id_unique
    ON threads(provider_name, external_thread_id)
    WHERE external_thread_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS sent_messages (
    id INTEGER PRIMARY KEY,
    queued_message_id INTEGER NOT NULL REFERENCES queued_messages(id) ON DELETE RESTRICT,
    thread_id INTEGER REFERENCES threads(id) ON DELETE SET NULL,
    provider_account_id INTEGER REFERENCES provider_accounts(id) ON DELETE SET NULL,
    provider_name TEXT NOT NULL,
    provider_message_id TEXT,
    external_campaign_id TEXT,
    sent_at TEXT NOT NULL,
    delivery_state TEXT NOT NULL DEFAULT 'sent',
    subject TEXT NOT NULL DEFAULT '',
    body TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    UNIQUE(queued_message_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS sent_messages_external_id_unique
    ON sent_messages(provider_name, provider_message_id)
    WHERE provider_message_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS replies (
    id INTEGER PRIMARY KEY,
    thread_id INTEGER NOT NULL REFERENCES threads(id) ON DELETE RESTRICT,
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE RESTRICT,
    campaign_id INTEGER REFERENCES campaigns(id) ON DELETE SET NULL,
    provider_name TEXT NOT NULL DEFAULT '',
    provider_reply_id TEXT,
    state TEXT NOT NULL CHECK (state IN ({REPLY_STATE_SQL})),
    classification TEXT NOT NULL DEFAULT '',
    sentiment TEXT NOT NULL DEFAULT '',
    reply_text TEXT NOT NULL DEFAULT '',
    received_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS replies_external_id_unique
    ON replies(provider_name, provider_reply_id)
    WHERE provider_reply_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS provider_mappings (
    id INTEGER PRIMARY KEY,
    provider_name TEXT NOT NULL,
    entity_type TEXT NOT NULL CHECK (entity_type IN ({ENTITY_TYPE_SQL})),
    internal_entity_id INTEGER NOT NULL,
    external_id TEXT NOT NULL,
    external_parent_id TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{{}}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(provider_name, entity_type, internal_entity_id),
    UNIQUE(provider_name, entity_type, external_id)
);

CREATE TABLE IF NOT EXISTS suppression_entries (
    id INTEGER PRIMARY KEY,
    scope TEXT NOT NULL CHECK (scope IN ({SUPPRESSION_SCOPE_SQL})),
    lead_id INTEGER REFERENCES leads(id) ON DELETE SET NULL,
    company_id INTEGER REFERENCES companies(id) ON DELETE SET NULL,
    email TEXT COLLATE NOCASE,
    domain TEXT COLLATE NOCASE,
    reason TEXT NOT NULL,
    source TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (
        lead_id IS NOT NULL OR
        company_id IS NOT NULL OR
        email IS NOT NULL OR
        domain IS NOT NULL
    )
);
CREATE UNIQUE INDEX IF NOT EXISTS suppression_entries_active_lead_unique
    ON suppression_entries(scope, lead_id)
    WHERE active = 1 AND lead_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS suppression_entries_active_company_unique
    ON suppression_entries(scope, company_id)
    WHERE active = 1 AND company_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS suppression_entries_active_email_unique
    ON suppression_entries(scope, email)
    WHERE active = 1 AND email IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS suppression_entries_active_domain_unique
    ON suppression_entries(scope, domain)
    WHERE active = 1 AND domain IS NOT NULL;

CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY,
    entity_type TEXT NOT NULL CHECK (entity_type IN ({ENTITY_TYPE_SQL})),
    entity_id INTEGER,
    event_type TEXT NOT NULL,
    actor TEXT NOT NULL,
    summary TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{{}}',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS audit_events_entity_lookup
    ON audit_events(entity_type, entity_id, created_at);

CREATE TABLE IF NOT EXISTS webhook_events (
    id INTEGER PRIMARY KEY,
    provider_name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    external_event_id TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{{}}',
    processing_state TEXT NOT NULL DEFAULT 'received',
    processed_at TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(provider_name, external_event_id)
);

CREATE TABLE IF NOT EXISTS tags (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    color TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(name)
);

CREATE TABLE IF NOT EXISTS entity_tags (
    id INTEGER PRIMARY KEY,
    tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    entity_type TEXT NOT NULL CHECK (entity_type IN ({ENTITY_TYPE_SQL})),
    entity_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(tag_id, entity_type, entity_id)
);

CREATE TABLE IF NOT EXISTS settings_bundles (
    id INTEGER PRIMARY KEY,
    bundle_key TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'config',
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(bundle_key)
);
""",
        down_sql="""
DROP TABLE IF EXISTS settings_bundles;
DROP TABLE IF EXISTS entity_tags;
DROP TABLE IF EXISTS tags;
DROP TABLE IF EXISTS webhook_events;
DROP TABLE IF EXISTS audit_events;
DROP TABLE IF EXISTS suppression_entries;
DROP TABLE IF EXISTS provider_mappings;
DROP TABLE IF EXISTS replies;
DROP TABLE IF EXISTS sent_messages;
DROP TABLE IF EXISTS threads;
DROP TABLE IF EXISTS queued_messages;
DROP TABLE IF EXISTS campaign_audience_snapshots;
DROP TABLE IF EXISTS generation_artifacts;
DROP TABLE IF EXISTS provider_accounts;
DROP TABLE IF EXISTS template_blocks;
DROP TABLE IF EXISTS sequence_steps;
DROP TABLE IF EXISTS template_variants;
DROP TABLE IF EXISTS templates;
DROP TABLE IF EXISTS sequences;
DROP TABLE IF EXISTS campaigns;
DROP TABLE IF EXISTS vertical_playbooks;
DROP TABLE IF EXISTS offer_profiles;
DROP TABLE IF EXISTS booking_links;
DROP TABLE IF EXISTS list_memberships;
DROP TABLE IF EXISTS lists;
DROP TABLE IF EXISTS leads;
DROP TABLE IF EXISTS companies;
""",
    )
    ,
    MigrationDefinition(
        version="0002_sprint_3_lead_warehouse",
        description="Add import jobs, import row outcomes, and saved lead filters for Sprint 3.",
        up_sql=f"""
CREATE TABLE IF NOT EXISTS import_jobs (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    import_format TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    list_id INTEGER REFERENCES lists(id) ON DELETE SET NULL,
    status TEXT NOT NULL CHECK (status IN ({IMPORT_JOB_STATUS_SQL})),
    total_read INTEGER NOT NULL DEFAULT 0 CHECK (total_read >= 0),
    inserted_count INTEGER NOT NULL DEFAULT 0 CHECK (inserted_count >= 0),
    merged_count INTEGER NOT NULL DEFAULT 0 CHECK (merged_count >= 0),
    skipped_count INTEGER NOT NULL DEFAULT 0 CHECK (skipped_count >= 0),
    conflicting_count INTEGER NOT NULL DEFAULT 0 CHECK (conflicting_count >= 0),
    manual_review_required_count INTEGER NOT NULL DEFAULT 0 CHECK (manual_review_required_count >= 0),
    summary_json TEXT NOT NULL DEFAULT '{{}}',
    error_message TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS import_job_rows (
    id INTEGER PRIMARY KEY,
    import_job_id INTEGER NOT NULL REFERENCES import_jobs(id) ON DELETE CASCADE,
    row_number INTEGER NOT NULL CHECK (row_number > 0),
    resolution TEXT NOT NULL CHECK (resolution IN ({IMPORT_ROW_RESOLUTION_SQL})),
    dedupe_rule TEXT NOT NULL DEFAULT '',
    lead_id INTEGER REFERENCES leads(id) ON DELETE SET NULL,
    company_id INTEGER REFERENCES companies(id) ON DELETE SET NULL,
    existing_lead_id INTEGER REFERENCES leads(id) ON DELETE SET NULL,
    raw_payload_json TEXT NOT NULL DEFAULT '{{}}',
    normalized_payload_json TEXT NOT NULL DEFAULT '{{}}',
    message TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    UNIQUE(import_job_id, row_number)
);

CREATE TABLE IF NOT EXISTS saved_filters (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('lead')),
    description TEXT NOT NULL DEFAULT '',
    filter_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(name, entity_type)
);
""",
        down_sql="""
DROP TABLE IF EXISTS saved_filters;
DROP TABLE IF EXISTS import_job_rows;
DROP TABLE IF EXISTS import_jobs;
""",
    ),
    MigrationDefinition(
        version="0003_sprint_3_entity_type_constraints",
        description="Refresh entity-type constrained tables so existing Sprint 2 databases accept Sprint 3 entity types.",
        up_sql=f"""
ALTER TABLE audit_events RENAME TO audit_events__old;
CREATE TABLE audit_events (
    id INTEGER PRIMARY KEY,
    entity_type TEXT NOT NULL CHECK (entity_type IN ({ENTITY_TYPE_SQL})),
    entity_id INTEGER,
    event_type TEXT NOT NULL,
    actor TEXT NOT NULL,
    summary TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{{}}',
    created_at TEXT NOT NULL
);
INSERT INTO audit_events (id, entity_type, entity_id, event_type, actor, summary, payload_json, created_at)
SELECT id, entity_type, entity_id, event_type, actor, summary, payload_json, created_at
FROM audit_events__old;
DROP TABLE audit_events__old;
CREATE INDEX IF NOT EXISTS audit_events_entity_lookup
    ON audit_events(entity_type, entity_id, created_at);

ALTER TABLE provider_mappings RENAME TO provider_mappings__old;
CREATE TABLE provider_mappings (
    id INTEGER PRIMARY KEY,
    provider_name TEXT NOT NULL,
    entity_type TEXT NOT NULL CHECK (entity_type IN ({ENTITY_TYPE_SQL})),
    internal_entity_id INTEGER NOT NULL,
    external_id TEXT NOT NULL,
    external_parent_id TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{{}}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(provider_name, entity_type, internal_entity_id),
    UNIQUE(provider_name, entity_type, external_id)
);
INSERT INTO provider_mappings (
    id, provider_name, entity_type, internal_entity_id, external_id, external_parent_id, payload_json, created_at, updated_at
)
SELECT
    id, provider_name, entity_type, internal_entity_id, external_id, external_parent_id, payload_json, created_at, updated_at
FROM provider_mappings__old;
DROP TABLE provider_mappings__old;

ALTER TABLE entity_tags RENAME TO entity_tags__old;
CREATE TABLE entity_tags (
    id INTEGER PRIMARY KEY,
    tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    entity_type TEXT NOT NULL CHECK (entity_type IN ({ENTITY_TYPE_SQL})),
    entity_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(tag_id, entity_type, entity_id)
);
INSERT INTO entity_tags (id, tag_id, entity_type, entity_id, created_at)
SELECT id, tag_id, entity_type, entity_id, created_at
FROM entity_tags__old;
DROP TABLE entity_tags__old;
""",
        down_sql=f"""
ALTER TABLE entity_tags RENAME TO entity_tags__old;
CREATE TABLE entity_tags (
    id INTEGER PRIMARY KEY,
    tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    entity_type TEXT NOT NULL CHECK (entity_type IN ({LEGACY_ENTITY_TYPE_SQL})),
    entity_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(tag_id, entity_type, entity_id)
);
INSERT INTO entity_tags (id, tag_id, entity_type, entity_id, created_at)
SELECT id, tag_id, entity_type, entity_id, created_at
FROM entity_tags__old
WHERE entity_type IN ({LEGACY_ENTITY_TYPE_SQL});
DROP TABLE entity_tags__old;

ALTER TABLE provider_mappings RENAME TO provider_mappings__old;
CREATE TABLE provider_mappings (
    id INTEGER PRIMARY KEY,
    provider_name TEXT NOT NULL,
    entity_type TEXT NOT NULL CHECK (entity_type IN ({LEGACY_ENTITY_TYPE_SQL})),
    internal_entity_id INTEGER NOT NULL,
    external_id TEXT NOT NULL,
    external_parent_id TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{{}}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(provider_name, entity_type, internal_entity_id),
    UNIQUE(provider_name, entity_type, external_id)
);
INSERT INTO provider_mappings (
    id, provider_name, entity_type, internal_entity_id, external_id, external_parent_id, payload_json, created_at, updated_at
)
SELECT
    id, provider_name, entity_type, internal_entity_id, external_id, external_parent_id, payload_json, created_at, updated_at
FROM provider_mappings__old
WHERE entity_type IN ({LEGACY_ENTITY_TYPE_SQL});
DROP TABLE provider_mappings__old;

ALTER TABLE audit_events RENAME TO audit_events__old;
CREATE TABLE audit_events (
    id INTEGER PRIMARY KEY,
    entity_type TEXT NOT NULL CHECK (entity_type IN ({LEGACY_ENTITY_TYPE_SQL})),
    entity_id INTEGER,
    event_type TEXT NOT NULL,
    actor TEXT NOT NULL,
    summary TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{{}}',
    created_at TEXT NOT NULL
);
INSERT INTO audit_events (id, entity_type, entity_id, event_type, actor, summary, payload_json, created_at)
SELECT id, entity_type, entity_id, event_type, actor, summary, payload_json, created_at
FROM audit_events__old
WHERE entity_type IN ({LEGACY_ENTITY_TYPE_SQL});
DROP TABLE audit_events__old;
CREATE INDEX IF NOT EXISTS audit_events_entity_lookup
    ON audit_events(entity_type, entity_id, created_at);
""",
    ),
    MigrationDefinition(
        version="0004_sprint_4_generation_studio",
        description="Add traceable generation metadata and richer template block fields for Sprint 4.",
        up_sql=f"""
ALTER TABLE template_blocks RENAME TO template_blocks__old;
CREATE TABLE template_blocks (
    id INTEGER PRIMARY KEY,
    template_id INTEGER NOT NULL REFERENCES templates(id) ON DELETE CASCADE,
    template_variant_id INTEGER REFERENCES template_variants(id) ON DELETE CASCADE,
    block_key TEXT NOT NULL,
    block_type TEXT NOT NULL CHECK (block_type IN ({TEMPLATE_BLOCK_TYPE_SQL})),
    section TEXT NOT NULL DEFAULT 'body',
    content TEXT NOT NULL DEFAULT '',
    fallback_content TEXT NOT NULL DEFAULT '',
    rules_json TEXT NOT NULL DEFAULT '{{}}',
    position INTEGER NOT NULL DEFAULT 0 CHECK (position >= 0),
    is_required INTEGER NOT NULL DEFAULT 1 CHECK (is_required IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
INSERT INTO template_blocks (
    id,
    template_id,
    template_variant_id,
    block_key,
    block_type,
    section,
    content,
    fallback_content,
    rules_json,
    position,
    is_required,
    created_at,
    updated_at
)
SELECT
    id,
    template_id,
    template_variant_id,
    block_key,
    block_type,
    CASE
        WHEN lower(block_key) LIKE 'subject%' THEN 'subject'
        ELSE 'body'
    END,
    content,
    '',
    '{{}}',
    position,
    is_required,
    created_at,
    updated_at
FROM template_blocks__old;
DROP TABLE template_blocks__old;
CREATE UNIQUE INDEX IF NOT EXISTS template_blocks_unique_key
    ON template_blocks(template_id, ifnull(template_variant_id, 0), block_key);

ALTER TABLE generation_artifacts ADD COLUMN template_id INTEGER;
ALTER TABLE generation_artifacts ADD COLUMN template_variant_id INTEGER;
ALTER TABLE generation_artifacts ADD COLUMN lead_id INTEGER;
ALTER TABLE generation_artifacts ADD COLUMN source_artifact_id INTEGER;
CREATE INDEX IF NOT EXISTS generation_artifacts_recent_lookup
    ON generation_artifacts(updated_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS generation_artifacts_template_lookup
    ON generation_artifacts(template_id, template_variant_id, lead_id);
""",
        down_sql=f"""
DROP INDEX IF EXISTS generation_artifacts_template_lookup;
DROP INDEX IF EXISTS generation_artifacts_recent_lookup;

ALTER TABLE template_blocks RENAME TO template_blocks__old;
CREATE TABLE template_blocks (
    id INTEGER PRIMARY KEY,
    template_id INTEGER NOT NULL REFERENCES templates(id) ON DELETE CASCADE,
    template_variant_id INTEGER REFERENCES template_variants(id) ON DELETE CASCADE,
    block_key TEXT NOT NULL,
    block_type TEXT NOT NULL CHECK (block_type IN ({TEMPLATE_BLOCK_TYPE_SQL})),
    content TEXT NOT NULL DEFAULT '',
    position INTEGER NOT NULL DEFAULT 0 CHECK (position >= 0),
    is_required INTEGER NOT NULL DEFAULT 1 CHECK (is_required IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
INSERT INTO template_blocks (
    id,
    template_id,
    template_variant_id,
    block_key,
    block_type,
    content,
    position,
    is_required,
    created_at,
    updated_at
)
SELECT
    id,
    template_id,
    template_variant_id,
    block_key,
    block_type,
    content,
    position,
    is_required,
    created_at,
    updated_at
FROM template_blocks__old;
DROP TABLE template_blocks__old;
CREATE UNIQUE INDEX IF NOT EXISTS template_blocks_unique_key
    ON template_blocks(template_id, ifnull(template_variant_id, 0), block_key);
""",
    ),
    MigrationDefinition(
        version="0005_sprint_5_campaign_preview_indexes",
        description="Add preview-oriented indexes for campaigns, queued messages, and provider assignment.",
        up_sql="""
CREATE INDEX IF NOT EXISTS queued_messages_campaign_schedule_lookup
    ON queued_messages(campaign_id, state, scheduled_for, id);
CREATE INDEX IF NOT EXISTS campaign_audience_snapshots_campaign_lookup
    ON campaign_audience_snapshots(campaign_id, lead_id);
CREATE INDEX IF NOT EXISTS provider_accounts_status_lookup
    ON provider_accounts(provider_name, status, daily_cap, id);
""",
        down_sql="""
DROP INDEX IF EXISTS provider_accounts_status_lookup;
DROP INDEX IF EXISTS campaign_audience_snapshots_campaign_lookup;
DROP INDEX IF EXISTS queued_messages_campaign_schedule_lookup;
""",
    ),
    MigrationDefinition(
        version="0006_sprint_6_execution_engine",
        description="Add worker claim, retry, and dead-letter persistence for restart-safe outbound execution.",
        up_sql="""
ALTER TABLE queued_messages ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0);
ALTER TABLE queued_messages ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts >= 1);
ALTER TABLE queued_messages ADD COLUMN next_attempt_at TEXT;
ALTER TABLE queued_messages ADD COLUMN claimed_at TEXT;
ALTER TABLE queued_messages ADD COLUMN claimed_by TEXT NOT NULL DEFAULT '';
ALTER TABLE queued_messages ADD COLUMN claim_token TEXT;
ALTER TABLE queued_messages ADD COLUMN claim_expires_at TEXT;
ALTER TABLE queued_messages ADD COLUMN last_attempt_at TEXT;
ALTER TABLE queued_messages ADD COLUMN last_error_code TEXT NOT NULL DEFAULT '';
ALTER TABLE queued_messages ADD COLUMN last_error_detail TEXT NOT NULL DEFAULT '';
ALTER TABLE queued_messages ADD COLUMN dead_lettered_at TEXT;

CREATE TABLE IF NOT EXISTS dead_letter_jobs (
    id INTEGER PRIMARY KEY,
    queued_message_id INTEGER NOT NULL REFERENCES queued_messages(id) ON DELETE CASCADE,
    campaign_id INTEGER REFERENCES campaigns(id) ON DELETE SET NULL,
    provider_account_id INTEGER REFERENCES provider_accounts(id) ON DELETE SET NULL,
    job_kind TEXT NOT NULL DEFAULT 'queued_message_dispatch',
    reason_code TEXT NOT NULL,
    reason_detail TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    resolved_at TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(queued_message_id)
);

CREATE INDEX IF NOT EXISTS queued_messages_due_dispatch_lookup
    ON queued_messages(state, scheduled_for, claim_expires_at, dead_lettered_at, id);
CREATE INDEX IF NOT EXISTS queued_messages_provider_failure_lookup
    ON queued_messages(provider_account_id, state, last_attempt_at, last_error_code);
CREATE INDEX IF NOT EXISTS sent_messages_provider_sent_lookup
    ON sent_messages(provider_account_id, sent_at, id);
CREATE INDEX IF NOT EXISTS replies_campaign_lead_lookup
    ON replies(campaign_id, lead_id, received_at, id);
CREATE INDEX IF NOT EXISTS dead_letter_jobs_recent_lookup
    ON dead_letter_jobs(created_at DESC, id DESC);
""",
        down_sql="""
DROP INDEX IF EXISTS dead_letter_jobs_recent_lookup;
DROP INDEX IF EXISTS replies_campaign_lead_lookup;
DROP INDEX IF EXISTS sent_messages_provider_sent_lookup;
DROP INDEX IF EXISTS queued_messages_provider_failure_lookup;
DROP INDEX IF EXISTS queued_messages_due_dispatch_lookup;
DROP TABLE IF EXISTS dead_letter_jobs;
""",
    ),
]

LATEST_MIGRATION_VERSION = MIGRATIONS[-1].version
