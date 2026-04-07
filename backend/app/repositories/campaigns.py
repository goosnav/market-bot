"""Campaign repository with explicit status persistence helpers."""

from __future__ import annotations

from backend.app.core.logging import utc_now
from backend.app.domain.enums import CampaignStatus
from backend.app.domain.models import CampaignCreate
from backend.app.repositories.base import SqliteRepository


class CampaignRepository(SqliteRepository):
    """Store and load campaigns."""

    def create(self, payload: CampaignCreate, created_at: str | None = None) -> int:
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO campaigns (
                name,
                description,
                status,
                offer_profile_id,
                vertical_playbook_id,
                provider_name,
                approval_mode,
                reply_mode,
                send_window_config_json,
                quiet_hours_config_json,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.name,
                payload.description,
                payload.status.value,
                payload.offer_profile_id,
                payload.vertical_playbook_id,
                payload.provider_name,
                payload.approval_mode,
                payload.reply_mode,
                payload.send_window_config_json,
                payload.quiet_hours_config_json,
                timestamp,
                timestamp,
            ),
        )
        return int(cursor.lastrowid)

    def get(self, campaign_id: int) -> dict[str, object] | None:
        return self.fetch_one("SELECT * FROM campaigns WHERE id = ?", (campaign_id,))

    def update_status(
        self,
        campaign_id: int,
        status: CampaignStatus,
        *,
        updated_at: str | None = None,
        launched_at: str | None = None,
        paused_at: str | None = None,
        completed_at: str | None = None,
    ) -> None:
        timestamp = updated_at or utc_now()
        self.execute(
            """
            UPDATE campaigns
            SET status = ?,
                updated_at = ?,
                launched_at = COALESCE(?, launched_at),
                paused_at = COALESCE(?, paused_at),
                completed_at = COALESCE(?, completed_at)
            WHERE id = ?
            """,
            (
                status.value,
                timestamp,
                launched_at,
                paused_at,
                completed_at,
                campaign_id,
            ),
        )
