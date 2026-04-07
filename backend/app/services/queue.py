"""Queued message services with explicit state transitions and audit visibility."""

from __future__ import annotations

import json

from backend.app.core.logging import utc_now
from backend.app.domain.enums import EntityType, QueuedMessageState
from backend.app.domain.models import AuditEventCreate, QueuedMessageCreate
from backend.app.domain.transitions import assert_valid_queued_message_transition
from backend.app.repositories.audit import AuditEventRepository
from backend.app.repositories.queue import QueuedMessageRepository


class QueuedMessageService:
    """Create and transition queued messages transactionally."""

    def __init__(self, connection) -> None:
        self.queued_messages = QueuedMessageRepository(connection)
        self.audit_events = AuditEventRepository(connection)

    def create_message(self, payload: QueuedMessageCreate, actor: str) -> dict[str, object]:
        queued_message_id = self.queued_messages.create(payload)
        self.audit_events.record(
            AuditEventCreate(
                entity_type=EntityType.QUEUED_MESSAGE.value,
                entity_id=queued_message_id,
                event_type="queued_message.created",
                actor=actor,
                summary=f"Queued message created in state {payload.state.value}.",
                payload_json=json.dumps({"state": payload.state.value}, sort_keys=True),
            )
        )
        queued_message = self.queued_messages.get(queued_message_id)
        if queued_message is None:
            raise LookupError(f"Queued message {queued_message_id} was created but could not be reloaded.")
        return queued_message

    def transition_state(
        self,
        queued_message_id: int,
        target_state: QueuedMessageState,
        actor: str,
        reason: str = "",
    ) -> dict[str, object]:
        queued_message = self.queued_messages.get(queued_message_id)
        if queued_message is None:
            raise LookupError(f"Queued message {queued_message_id} does not exist.")

        current_state = QueuedMessageState(queued_message["state"])
        assert_valid_queued_message_transition(current_state, target_state)
        timestamp = utc_now()
        failure_reason = reason if target_state == QueuedMessageState.FAILED else None

        self.queued_messages.update_state(
            queued_message_id,
            target_state,
            updated_at=timestamp,
            failure_reason=failure_reason,
        )
        self.audit_events.record(
            AuditEventCreate(
                entity_type=EntityType.QUEUED_MESSAGE.value,
                entity_id=queued_message_id,
                event_type="queued_message.state_changed",
                actor=actor,
                summary=f"Queued message state changed from {current_state.value} to {target_state.value}.",
                payload_json=json.dumps(
                    {
                        "from_state": current_state.value,
                        "to_state": target_state.value,
                        "reason": reason,
                    },
                    sort_keys=True,
                ),
            ),
            created_at=timestamp,
        )
        updated = self.queued_messages.get(queued_message_id)
        if updated is None:
            raise LookupError(f"Queued message {queued_message_id} disappeared during transition.")
        return updated
