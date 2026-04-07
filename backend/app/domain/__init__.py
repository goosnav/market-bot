"""Domain-level enums, transitions, and typed creation models."""

from backend.app.domain.enums import CampaignStatus, QueuedMessageState, ReplyState
from backend.app.domain.models import AuditEventCreate, CampaignCreate, LeadCreate, QueuedMessageCreate, ReplyCreate
from backend.app.domain.transitions import (
    InvalidStateTransition,
    assert_valid_campaign_transition,
    assert_valid_queued_message_transition,
    assert_valid_reply_transition,
)

__all__ = [
    "AuditEventCreate",
    "CampaignCreate",
    "CampaignStatus",
    "InvalidStateTransition",
    "LeadCreate",
    "QueuedMessageCreate",
    "QueuedMessageState",
    "ReplyCreate",
    "ReplyState",
    "assert_valid_campaign_transition",
    "assert_valid_queued_message_transition",
    "assert_valid_reply_transition",
]
