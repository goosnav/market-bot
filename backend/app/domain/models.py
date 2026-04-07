"""Typed creation payloads for repository and service boundaries."""

from __future__ import annotations

from dataclasses import dataclass

from backend.app.domain.enums import (
    CampaignStatus,
    GenerationValidationStatus,
    ImportJobStatus,
    QueuedMessageState,
    ReplyState,
    TemplateBlockType,
)


@dataclass(frozen=True)
class LeadCreate:
    source: str
    full_name: str
    external_source_id: str | None = None
    first_name: str = ""
    last_name: str = ""
    email: str | None = None
    phone: str = ""
    title: str = ""
    linkedin_url: str = ""
    company_id: int | None = None
    company_name_snapshot: str = ""
    company_domain_snapshot: str = ""
    city: str = ""
    state: str = ""
    country: str = ""
    enrichment_status: str = "pending"
    verification_status: str = "unverified"
    suppression_status: str = "clear"
    fit_score: float = 0.0
    personalization_json: str = "{}"
    notes: str = ""


@dataclass(frozen=True)
class CampaignCreate:
    name: str
    description: str = ""
    status: CampaignStatus = CampaignStatus.DRAFT
    offer_profile_id: int | None = None
    vertical_playbook_id: int | None = None
    provider_name: str = "manual"
    approval_mode: str = "manual"
    reply_mode: str = "manual"
    send_window_config_json: str = "{}"
    quiet_hours_config_json: str = "{}"


@dataclass(frozen=True)
class QueuedMessageCreate:
    lead_id: int
    campaign_id: int
    sequence_id: int
    sequence_step_id: int
    template_id: int
    template_variant_id: int
    generation_artifact_id: int
    provider_account_id: int
    scheduled_for: str
    company_id: int | None = None
    state: QueuedMessageState = QueuedMessageState.PENDING_RENDER
    rendered_subject: str = ""
    rendered_body: str = ""
    render_hash: str = ""
    risk_flags_json: str = "[]"
    failure_reason: str = ""


@dataclass(frozen=True)
class ReplyCreate:
    thread_id: int
    lead_id: int
    received_at: str
    campaign_id: int | None = None
    provider_name: str = ""
    provider_reply_id: str | None = None
    state: ReplyState = ReplyState.UNCLASSIFIED
    classification: str = ""
    sentiment: str = ""
    reply_text: str = ""


@dataclass(frozen=True)
class AuditEventCreate:
    entity_type: str
    entity_id: int | None
    event_type: str
    actor: str
    summary: str
    payload_json: str = "{}"


@dataclass(frozen=True)
class ImportJobCreate:
    source: str
    import_format: str
    requested_by: str
    list_id: int | None = None
    status: ImportJobStatus = ImportJobStatus.PENDING
    summary_json: str = "{}"


@dataclass(frozen=True)
class SavedFilterCreate:
    name: str
    filter_json: str
    description: str = ""
    entity_type: str = "lead"


@dataclass(frozen=True)
class OfferProfileCreate:
    name: str
    description: str = ""
    target_verticals_json: str = "[]"
    target_pains_json: str = "[]"
    value_proposition: str = ""
    standard_cta: str = ""
    booking_link_id: int | None = None
    allowed_claims_json: str = "[]"
    disallowed_claims_json: str = "[]"
    pricing_framing_snippets_json: str = "[]"
    objection_handling_snippets_json: str = "[]"


@dataclass(frozen=True)
class VerticalPlaybookCreate:
    name: str
    target_pains_json: str = "[]"
    acceptable_language_json: str = "[]"
    disallowed_language_json: str = "[]"
    personalization_strategy: str = ""
    tone_profile: str = ""
    sample_subject_patterns_json: str = "[]"
    standard_objections_json: str = "[]"
    escalation_rules_json: str = "[]"


@dataclass(frozen=True)
class TemplateCreate:
    name: str
    description: str = ""
    channel: str = "email"
    is_active: bool = True


@dataclass(frozen=True)
class TemplateVariantCreate:
    template_id: int
    name: str
    variant_label: str = ""
    is_default: bool = False


@dataclass(frozen=True)
class TemplateBlockCreate:
    template_id: int
    block_key: str
    block_type: TemplateBlockType
    content: str = ""
    position: int = 0
    is_required: bool = True
    template_variant_id: int | None = None
    section: str = "body"
    fallback_content: str = ""
    rules_json: str = "{}"


@dataclass(frozen=True)
class GenerationArtifactCreate:
    kind: str
    prompt_version: str
    prompt_input_json: str = "{}"
    output_text: str = ""
    output_json: str = "{}"
    validation_status: GenerationValidationStatus = GenerationValidationStatus.PENDING
    risk_flags_json: str = "[]"
    model_name: str = ""


@dataclass(frozen=True)
class ProviderAccountCreate:
    provider_name: str
    display_name: str
    email_address: str
    sending_domain: str | None = None
    external_account_id: str | None = None
    daily_cap: int = 0
    warmup_cap: int = 0
    status: str = "active"


@dataclass(frozen=True)
class SequenceCreate:
    campaign_id: int
    name: str
    description: str = ""
    timezone: str = "UTC"


@dataclass(frozen=True)
class SequenceStepCreate:
    sequence_id: int
    step_order: int
    template_id: int
    template_variant_id: int
    step_type: str = "email"
    delay_days: int = 0
    subject_override: str = ""
    body_override: str = ""


@dataclass(frozen=True)
class CampaignAudienceSnapshotCreate:
    campaign_id: int
    lead_id: int
    snapshot_json: str
    company_id: int | None = None


@dataclass(frozen=True)
class SentMessageCreate:
    queued_message_id: int
    provider_name: str
    sent_at: str
    subject: str
    body: str
    thread_id: int | None = None
    provider_account_id: int | None = None
    provider_message_id: str | None = None
    external_campaign_id: str | None = None
    delivery_state: str = "sent"


@dataclass(frozen=True)
class DeadLetterJobCreate:
    queued_message_id: int
    reason_code: str
    created_at: str
    payload_json: str = "{}"
    campaign_id: int | None = None
    provider_account_id: int | None = None
    job_kind: str = "queued_message_dispatch"
    reason_detail: str = ""
