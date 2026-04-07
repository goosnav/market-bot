"""Campaign services with explicit status transitions and audit emission."""

from __future__ import annotations

import json

from backend.app.core.logging import utc_now
from backend.app.domain.enums import CampaignStatus, EntityType
from backend.app.domain.models import AuditEventCreate, CampaignCreate
from backend.app.domain.transitions import assert_valid_campaign_transition
from backend.app.repositories.audit import AuditEventRepository
from backend.app.repositories.campaigns import CampaignRepository


class CampaignService:
    """Create and transition campaigns transactionally."""

    def __init__(self, connection) -> None:
        self.campaigns = CampaignRepository(connection)
        self.audit_events = AuditEventRepository(connection)

    def create_campaign(self, payload: CampaignCreate, actor: str) -> dict[str, object]:
        campaign_id = self.campaigns.create(payload)
        self.audit_events.record(
            AuditEventCreate(
                entity_type=EntityType.CAMPAIGN.value,
                entity_id=campaign_id,
                event_type="campaign.created",
                actor=actor,
                summary=f"Campaign '{payload.name}' created.",
                payload_json=json.dumps({"status": payload.status.value}, sort_keys=True),
            )
        )
        campaign = self.campaigns.get(campaign_id)
        if campaign is None:
            raise LookupError(f"Campaign {campaign_id} was created but could not be reloaded.")
        return campaign

    def transition_status(
        self,
        campaign_id: int,
        target_status: CampaignStatus,
        actor: str,
        reason: str = "",
    ) -> dict[str, object]:
        campaign = self.campaigns.get(campaign_id)
        if campaign is None:
            raise LookupError(f"Campaign {campaign_id} does not exist.")

        current_status = CampaignStatus(campaign["status"])
        assert_valid_campaign_transition(current_status, target_status)
        timestamp = utc_now()

        launched_at = timestamp if target_status in {CampaignStatus.LAUNCHING, CampaignStatus.ACTIVE} and not campaign["launched_at"] else None
        paused_at = timestamp if target_status == CampaignStatus.PAUSED else None
        completed_at = timestamp if target_status == CampaignStatus.COMPLETED else None

        self.campaigns.update_status(
            campaign_id,
            target_status,
            updated_at=timestamp,
            launched_at=launched_at,
            paused_at=paused_at,
            completed_at=completed_at,
        )
        self.audit_events.record(
            AuditEventCreate(
                entity_type=EntityType.CAMPAIGN.value,
                entity_id=campaign_id,
                event_type="campaign.status_changed",
                actor=actor,
                summary=f"Campaign status changed from {current_status.value} to {target_status.value}.",
                payload_json=json.dumps(
                    {
                        "from_status": current_status.value,
                        "to_status": target_status.value,
                        "reason": reason,
                    },
                    sort_keys=True,
                ),
            ),
            created_at=timestamp,
        )
        updated = self.campaigns.get(campaign_id)
        if updated is None:
            raise LookupError(f"Campaign {campaign_id} disappeared during transition.")
        return updated
