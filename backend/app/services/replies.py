"""Reply services with explicit state transitions and audit visibility."""

from __future__ import annotations

import json

from backend.app.core.logging import utc_now
from backend.app.domain.enums import EntityType, ReplyState
from backend.app.domain.models import AuditEventCreate, ReplyCreate
from backend.app.domain.transitions import assert_valid_reply_transition
from backend.app.repositories.audit import AuditEventRepository
from backend.app.repositories.queue import ReplyRepository


class ReplyService:
    """Create and transition replies transactionally."""

    def __init__(self, connection) -> None:
        self.replies = ReplyRepository(connection)
        self.audit_events = AuditEventRepository(connection)

    def create_reply(self, payload: ReplyCreate, actor: str) -> dict[str, object]:
        reply_id = self.replies.create(payload)
        self.audit_events.record(
            AuditEventCreate(
                entity_type=EntityType.REPLY.value,
                entity_id=reply_id,
                event_type="reply.created",
                actor=actor,
                summary=f"Reply created in state {payload.state.value}.",
                payload_json=json.dumps({"state": payload.state.value}, sort_keys=True),
            )
        )
        reply = self.replies.get(reply_id)
        if reply is None:
            raise LookupError(f"Reply {reply_id} was created but could not be reloaded.")
        return reply

    def transition_state(
        self,
        reply_id: int,
        target_state: ReplyState,
        actor: str,
        classification: str = "",
        reason: str = "",
    ) -> dict[str, object]:
        reply = self.replies.get(reply_id)
        if reply is None:
            raise LookupError(f"Reply {reply_id} does not exist.")

        current_state = ReplyState(reply["state"])
        assert_valid_reply_transition(current_state, target_state)
        timestamp = utc_now()
        classification_update = classification or None

        self.replies.update_state(
            reply_id,
            target_state,
            updated_at=timestamp,
            classification=classification_update,
        )
        self.audit_events.record(
            AuditEventCreate(
                entity_type=EntityType.REPLY.value,
                entity_id=reply_id,
                event_type="reply.state_changed",
                actor=actor,
                summary=f"Reply state changed from {current_state.value} to {target_state.value}.",
                payload_json=json.dumps(
                    {
                        "from_state": current_state.value,
                        "to_state": target_state.value,
                        "classification": classification,
                        "reason": reason,
                    },
                    sort_keys=True,
                ),
            ),
            created_at=timestamp,
        )
        updated = self.replies.get(reply_id)
        if updated is None:
            raise LookupError(f"Reply {reply_id} disappeared during transition.")
        return updated
