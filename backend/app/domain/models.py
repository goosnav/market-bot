"""Typed creation payloads for repository and service boundaries."""

from __future__ import annotations

from dataclasses import dataclass

from backend.app.domain.enums import CampaignStatus, QueuedMessageState, ReplyState


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

