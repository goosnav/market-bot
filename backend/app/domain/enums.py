"""Explicit domain enums for Sprint 2 persistence and workflow state."""

from __future__ import annotations

from enum import StrEnum


class CampaignStatus(StrEnum):
    DRAFT = "draft"
    PREVIEW_READY = "preview_ready"
    SCHEDULED = "scheduled"
    LAUNCHING = "launching"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    ARCHIVED = "archived"
    FAILED = "failed"


class QueuedMessageState(StrEnum):
    PENDING_RENDER = "pending_render"
    RENDERED = "rendered"
    APPROVED = "approved"
    SCHEDULED = "scheduled"
    DISPATCHED = "dispatched"
    SENT = "sent"
    FAILED = "failed"
    CANCELED = "canceled"
    SUPPRESSED = "suppressed"
    BLOCKED = "blocked"


class ReplyState(StrEnum):
    UNCLASSIFIED = "unclassified"
    CLASSIFIED = "classified"
    DRAFT_READY = "draft_ready"
    AUTO_SENT = "auto_sent"
    HUMAN_REVIEW_REQUIRED = "human_review_required"
    RESOLVED = "resolved"


class SuppressionScope(StrEnum):
    LEAD = "lead"
    EMAIL = "email"
    DOMAIN = "domain"
    COMPANY = "company"


class TemplateBlockType(StrEnum):
    STATIC = "static"
    MERGED = "merged"
    AI_GENERATED = "ai_generated"
    CONDITIONAL = "conditional"


class EntityType(StrEnum):
    LEAD = "lead"
    COMPANY = "company"
    LIST = "list"
    IMPORT_JOB = "import_job"
    SAVED_FILTER = "saved_filter"
    CAMPAIGN = "campaign"
    SEQUENCE = "sequence"
    SEQUENCE_STEP = "sequence_step"
    TEMPLATE = "template"
    TEMPLATE_VARIANT = "template_variant"
    TEMPLATE_BLOCK = "template_block"
    OFFER_PROFILE = "offer_profile"
    VERTICAL_PLAYBOOK = "vertical_playbook"
    QUEUED_MESSAGE = "queued_message"
    SENT_MESSAGE = "sent_message"
    REPLY = "reply"
    THREAD = "thread"
    PROVIDER_ACCOUNT = "provider_account"
    PROVIDER_MAPPING = "provider_mapping"
    BOOKING_LINK = "booking_link"
    SUPPRESSION_ENTRY = "suppression_entry"
    GENERATION_ARTIFACT = "generation_artifact"
    AUDIT_EVENT = "audit_event"
    WEBHOOK_EVENT = "webhook_event"
    SETTINGS_BUNDLE = "settings_bundle"
    TAG = "tag"


class ImportJobStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class ImportRowResolution(StrEnum):
    INSERTED = "inserted"
    MERGED = "merged"
    SKIPPED = "skipped"
    CONFLICTING = "conflicting"
    MANUAL_REVIEW_REQUIRED = "manual_review_required"


class GenerationValidationStatus(StrEnum):
    PENDING = "pending"
    PASSED = "passed"
    WARNING = "warning"
    BLOCKED = "blocked"
