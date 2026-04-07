"""Audit repository for append-only state visibility."""

from __future__ import annotations

from backend.app.core.logging import utc_now
from backend.app.domain.models import AuditEventCreate
from backend.app.repositories.base import SqliteRepository


class AuditEventRepository(SqliteRepository):
    """Persist append-only audit events."""

    def record(self, payload: AuditEventCreate, created_at: str | None = None) -> int:
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO audit_events (
                entity_type,
                entity_id,
                event_type,
                actor,
                summary,
                payload_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.entity_type,
                payload.entity_id,
                payload.event_type,
                payload.actor,
                payload.summary,
                payload.payload_json,
                timestamp,
            ),
        )
        return int(cursor.lastrowid)

    def list_for_entity(self, entity_type: str, entity_id: int) -> list[dict[str, object]]:
        return self.fetch_all(
            """
            SELECT *
            FROM audit_events
            WHERE entity_type = ? AND entity_id = ?
            ORDER BY id ASC
            """,
            (entity_type, entity_id),
        )
