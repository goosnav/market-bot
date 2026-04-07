"""Explicit state transition rules for core workflow entities."""

from __future__ import annotations

from collections.abc import Mapping

from backend.app.domain.enums import CampaignStatus, QueuedMessageState, ReplyState


class InvalidStateTransition(ValueError):
    """Raised when a workflow state transition is not allowed."""


CAMPAIGN_TRANSITIONS: dict[CampaignStatus, set[CampaignStatus]] = {
    CampaignStatus.DRAFT: {CampaignStatus.PREVIEW_READY, CampaignStatus.ARCHIVED, CampaignStatus.FAILED},
    CampaignStatus.PREVIEW_READY: {
        CampaignStatus.DRAFT,
        CampaignStatus.SCHEDULED,
        CampaignStatus.LAUNCHING,
        CampaignStatus.ARCHIVED,
        CampaignStatus.FAILED,
    },
    CampaignStatus.SCHEDULED: {
        CampaignStatus.LAUNCHING,
        CampaignStatus.PAUSED,
        CampaignStatus.ARCHIVED,
        CampaignStatus.FAILED,
    },
    CampaignStatus.LAUNCHING: {CampaignStatus.ACTIVE, CampaignStatus.PAUSED, CampaignStatus.FAILED},
    CampaignStatus.ACTIVE: {
        CampaignStatus.PAUSED,
        CampaignStatus.COMPLETED,
        CampaignStatus.ARCHIVED,
        CampaignStatus.FAILED,
    },
    CampaignStatus.PAUSED: {
        CampaignStatus.SCHEDULED,
        CampaignStatus.LAUNCHING,
        CampaignStatus.ACTIVE,
        CampaignStatus.ARCHIVED,
        CampaignStatus.FAILED,
    },
    CampaignStatus.COMPLETED: {CampaignStatus.ARCHIVED},
    CampaignStatus.ARCHIVED: set(),
    CampaignStatus.FAILED: {CampaignStatus.DRAFT, CampaignStatus.ARCHIVED},
}


QUEUED_MESSAGE_TRANSITIONS: dict[QueuedMessageState, set[QueuedMessageState]] = {
    QueuedMessageState.PENDING_RENDER: {
        QueuedMessageState.RENDERED,
        QueuedMessageState.FAILED,
        QueuedMessageState.CANCELED,
        QueuedMessageState.SUPPRESSED,
        QueuedMessageState.BLOCKED,
    },
    QueuedMessageState.RENDERED: {
        QueuedMessageState.APPROVED,
        QueuedMessageState.FAILED,
        QueuedMessageState.CANCELED,
        QueuedMessageState.SUPPRESSED,
        QueuedMessageState.BLOCKED,
    },
    QueuedMessageState.APPROVED: {
        QueuedMessageState.RENDERED,
        QueuedMessageState.SCHEDULED,
        QueuedMessageState.CANCELED,
        QueuedMessageState.SUPPRESSED,
        QueuedMessageState.BLOCKED,
    },
    QueuedMessageState.SCHEDULED: {
        QueuedMessageState.DISPATCHED,
        QueuedMessageState.FAILED,
        QueuedMessageState.CANCELED,
        QueuedMessageState.SUPPRESSED,
        QueuedMessageState.BLOCKED,
    },
    QueuedMessageState.DISPATCHED: {
        QueuedMessageState.SENT,
        QueuedMessageState.FAILED,
        QueuedMessageState.SCHEDULED,
        QueuedMessageState.CANCELED,
        QueuedMessageState.SUPPRESSED,
        QueuedMessageState.BLOCKED,
    },
    QueuedMessageState.SENT: set(),
    QueuedMessageState.FAILED: {
        QueuedMessageState.PENDING_RENDER,
        QueuedMessageState.RENDERED,
        QueuedMessageState.APPROVED,
        QueuedMessageState.SCHEDULED,
        QueuedMessageState.CANCELED,
        QueuedMessageState.BLOCKED,
    },
    QueuedMessageState.CANCELED: set(),
    QueuedMessageState.SUPPRESSED: set(),
    QueuedMessageState.BLOCKED: {
        QueuedMessageState.APPROVED,
        QueuedMessageState.SCHEDULED,
        QueuedMessageState.CANCELED,
        QueuedMessageState.SUPPRESSED,
    },
}


REPLY_TRANSITIONS: dict[ReplyState, set[ReplyState]] = {
    ReplyState.UNCLASSIFIED: {
        ReplyState.CLASSIFIED,
        ReplyState.HUMAN_REVIEW_REQUIRED,
    },
    ReplyState.CLASSIFIED: {
        ReplyState.DRAFT_READY,
        ReplyState.AUTO_SENT,
        ReplyState.HUMAN_REVIEW_REQUIRED,
        ReplyState.RESOLVED,
    },
    ReplyState.DRAFT_READY: {
        ReplyState.AUTO_SENT,
        ReplyState.HUMAN_REVIEW_REQUIRED,
        ReplyState.RESOLVED,
    },
    ReplyState.AUTO_SENT: {ReplyState.RESOLVED},
    ReplyState.HUMAN_REVIEW_REQUIRED: {
        ReplyState.DRAFT_READY,
        ReplyState.RESOLVED,
    },
    ReplyState.RESOLVED: set(),
}


def assert_valid_campaign_transition(current: CampaignStatus, target: CampaignStatus) -> None:
    assert_valid_transition("campaign", current, target, CAMPAIGN_TRANSITIONS)


def assert_valid_queued_message_transition(current: QueuedMessageState, target: QueuedMessageState) -> None:
    assert_valid_transition("queued_message", current, target, QUEUED_MESSAGE_TRANSITIONS)


def assert_valid_reply_transition(current: ReplyState, target: ReplyState) -> None:
    assert_valid_transition("reply", current, target, REPLY_TRANSITIONS)


def assert_valid_transition(entity_name: str, current: str, target: str, rules: Mapping[str, set[str]]) -> None:
    if current == target:
        return
    allowed_targets = rules.get(current, set())
    if target not in allowed_targets:
        allowed = ", ".join(sorted(allowed_targets)) or "none"
        raise InvalidStateTransition(
            f"Invalid {entity_name} transition: {current} -> {target}. Allowed targets: {allowed}."
        )
