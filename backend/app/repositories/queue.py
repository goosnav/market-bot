"""Queued message and reply repositories."""

from __future__ import annotations

from backend.app.core.logging import utc_now
from backend.app.domain.enums import QueuedMessageState, ReplyState
from backend.app.domain.models import QueuedMessageCreate, ReplyCreate
from backend.app.repositories.base import SqliteRepository


class QueuedMessageRepository(SqliteRepository):
    """Store queued outbound messages."""

    def create(self, payload: QueuedMessageCreate, created_at: str | None = None) -> int:
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO queued_messages (
                lead_id,
                company_id,
                campaign_id,
                sequence_id,
                sequence_step_id,
                template_id,
                template_variant_id,
                generation_artifact_id,
                provider_account_id,
                scheduled_for,
                state,
                rendered_subject,
                rendered_body,
                render_hash,
                risk_flags_json,
                failure_reason,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.lead_id,
                payload.company_id,
                payload.campaign_id,
                payload.sequence_id,
                payload.sequence_step_id,
                payload.template_id,
                payload.template_variant_id,
                payload.generation_artifact_id,
                payload.provider_account_id,
                payload.scheduled_for,
                payload.state.value,
                payload.rendered_subject,
                payload.rendered_body,
                payload.render_hash,
                payload.risk_flags_json,
                payload.failure_reason,
                timestamp,
                timestamp,
            ),
        )
        return int(cursor.lastrowid)

    def get(self, queued_message_id: int) -> dict[str, object] | None:
        return self.fetch_one("SELECT * FROM queued_messages WHERE id = ?", (queued_message_id,))

    def update_state(
        self,
        queued_message_id: int,
        state: QueuedMessageState,
        *,
        updated_at: str | None = None,
        failure_reason: str | None = None,
    ) -> None:
        timestamp = updated_at or utc_now()
        self.execute(
            """
            UPDATE queued_messages
            SET state = ?,
                updated_at = ?,
                failure_reason = CASE WHEN ? IS NULL THEN failure_reason ELSE ? END
            WHERE id = ?
            """,
            (
                state.value,
                timestamp,
                failure_reason,
                failure_reason,
                queued_message_id,
            ),
        )


class ReplyRepository(SqliteRepository):
    """Store inbound replies and reply-state transitions."""

    def create(self, payload: ReplyCreate, created_at: str | None = None) -> int:
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO replies (
                thread_id,
                lead_id,
                campaign_id,
                provider_name,
                provider_reply_id,
                state,
                classification,
                sentiment,
                reply_text,
                received_at,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.thread_id,
                payload.lead_id,
                payload.campaign_id,
                payload.provider_name,
                payload.provider_reply_id,
                payload.state.value,
                payload.classification,
                payload.sentiment,
                payload.reply_text,
                payload.received_at,
                timestamp,
                timestamp,
            ),
        )
        return int(cursor.lastrowid)

    def get(self, reply_id: int) -> dict[str, object] | None:
        return self.fetch_one("SELECT * FROM replies WHERE id = ?", (reply_id,))

    def update_state(
        self,
        reply_id: int,
        state: ReplyState,
        *,
        updated_at: str | None = None,
        classification: str | None = None,
    ) -> None:
        timestamp = updated_at or utc_now()
        self.execute(
            """
            UPDATE replies
            SET state = ?,
                updated_at = ?,
                classification = CASE WHEN ? IS NULL THEN classification ELSE ? END
            WHERE id = ?
            """,
            (
                state.value,
                timestamp,
                classification,
                classification,
                reply_id,
            ),
        )
