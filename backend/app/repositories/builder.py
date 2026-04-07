"""Repositories for campaign builder, audience snapshots, and preview rows."""

from __future__ import annotations

from backend.app.core.logging import utc_now
from backend.app.domain.models import (
    CampaignAudienceSnapshotCreate,
    ProviderAccountCreate,
    SequenceCreate,
    SequenceStepCreate,
)
from backend.app.repositories.base import SqliteRepository


class ProviderAccountRepository(SqliteRepository):
    """Persist provider accounts used for queue assignment."""

    def create(self, payload: ProviderAccountCreate, created_at: str | None = None) -> int:
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO provider_accounts (
                provider_name,
                display_name,
                sending_domain,
                email_address,
                external_account_id,
                daily_cap,
                warmup_cap,
                status,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.provider_name,
                payload.display_name,
                payload.sending_domain,
                payload.email_address,
                payload.external_account_id,
                payload.daily_cap,
                payload.warmup_cap,
                payload.status,
                timestamp,
                timestamp,
            ),
        )
        return int(cursor.lastrowid)

    def get(self, provider_account_id: int) -> dict[str, object] | None:
        return self.fetch_one("SELECT * FROM provider_accounts WHERE id = ?", (provider_account_id,))

    def list_all(self) -> list[dict[str, object]]:
        return self.fetch_all(
            """
            SELECT *
            FROM provider_accounts
            ORDER BY status ASC, provider_name ASC, display_name ASC, id ASC
            """
        )

    def list_active(self, provider_name: str | None = None) -> list[dict[str, object]]:
        if provider_name:
            return self.fetch_all(
                """
                SELECT *
                FROM provider_accounts
                WHERE status = 'active' AND provider_name = ?
                ORDER BY id ASC
                """,
                (provider_name,),
            )
        return self.fetch_all(
            """
            SELECT *
            FROM provider_accounts
            WHERE status = 'active'
            ORDER BY provider_name ASC, id ASC
            """
        )

    def list_by_ids(self, provider_account_ids: list[int]) -> list[dict[str, object]]:
        if not provider_account_ids:
            return []
        placeholders = ", ".join("?" for _ in provider_account_ids)
        return self.fetch_all(
            f"""
            SELECT *
            FROM provider_accounts
            WHERE id IN ({placeholders})
            ORDER BY id ASC
            """,
            tuple(provider_account_ids),
        )


class SequenceRepository(SqliteRepository):
    """Persist campaign sequences and steps."""

    def create(self, payload: SequenceCreate, created_at: str | None = None) -> int:
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO sequences (
                campaign_id,
                name,
                description,
                timezone,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                payload.campaign_id,
                payload.name,
                payload.description,
                payload.timezone,
                timestamp,
                timestamp,
            ),
        )
        return int(cursor.lastrowid)

    def create_step(self, payload: SequenceStepCreate, created_at: str | None = None) -> int:
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO sequence_steps (
                sequence_id,
                step_order,
                step_type,
                delay_days,
                template_id,
                template_variant_id,
                subject_override,
                body_override,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.sequence_id,
                payload.step_order,
                payload.step_type,
                payload.delay_days,
                payload.template_id,
                payload.template_variant_id,
                payload.subject_override,
                payload.body_override,
                timestamp,
                timestamp,
            ),
        )
        return int(cursor.lastrowid)

    def list_steps(self, sequence_id: int) -> list[dict[str, object]]:
        return self.fetch_all(
            """
            SELECT
                sequence_steps.*,
                templates.name AS template_name,
                template_variants.name AS template_variant_name
            FROM sequence_steps
            INNER JOIN templates ON templates.id = sequence_steps.template_id
            INNER JOIN template_variants ON template_variants.id = sequence_steps.template_variant_id
            WHERE sequence_steps.sequence_id = ?
            ORDER BY sequence_steps.step_order ASC, sequence_steps.id ASC
            """,
            (sequence_id,),
        )


class CampaignAudienceSnapshotRepository(SqliteRepository):
    """Persist frozen audience snapshots for preview reproducibility."""

    def create(self, payload: CampaignAudienceSnapshotCreate, created_at: str | None = None) -> int:
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO campaign_audience_snapshots (
                campaign_id,
                lead_id,
                company_id,
                snapshot_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                payload.campaign_id,
                payload.lead_id,
                payload.company_id,
                payload.snapshot_json,
                timestamp,
            ),
        )
        return int(cursor.lastrowid)

    def list_for_campaign(self, campaign_id: int) -> list[dict[str, object]]:
        return self.fetch_all(
            """
            SELECT *
            FROM campaign_audience_snapshots
            WHERE campaign_id = ?
            ORDER BY id ASC
            """,
            (campaign_id,),
        )


class CampaignPreviewRepository(SqliteRepository):
    """Load preview rows and update queued render outputs."""

    def list_campaigns(self) -> list[dict[str, object]]:
        return self.fetch_all(
            """
            SELECT
                campaigns.*,
                offer_profiles.name AS offer_profile_name,
                vertical_playbooks.name AS vertical_playbook_name,
                COUNT(DISTINCT campaign_audience_snapshots.id) AS audience_count,
                COUNT(DISTINCT queued_messages.id) AS queued_count,
                SUM(CASE WHEN queued_messages.state = 'blocked' THEN 1 ELSE 0 END) AS blocked_count,
                SUM(CASE WHEN queued_messages.state = 'approved' THEN 1 ELSE 0 END) AS approved_count,
                SUM(CASE WHEN queued_messages.state = 'rendered' THEN 1 ELSE 0 END) AS rendered_count
            FROM campaigns
            LEFT JOIN offer_profiles ON offer_profiles.id = campaigns.offer_profile_id
            LEFT JOIN vertical_playbooks ON vertical_playbooks.id = campaigns.vertical_playbook_id
            LEFT JOIN campaign_audience_snapshots ON campaign_audience_snapshots.campaign_id = campaigns.id
            LEFT JOIN queued_messages ON queued_messages.campaign_id = campaigns.id
            GROUP BY campaigns.id
            ORDER BY campaigns.updated_at DESC, campaigns.id DESC
            """
        )

    def get_campaign(self, campaign_id: int) -> dict[str, object] | None:
        campaigns = self.fetch_all(
            """
            SELECT
                campaigns.*,
                offer_profiles.name AS offer_profile_name,
                vertical_playbooks.name AS vertical_playbook_name,
                COUNT(DISTINCT campaign_audience_snapshots.id) AS audience_count,
                COUNT(DISTINCT queued_messages.id) AS queued_count,
                SUM(CASE WHEN queued_messages.state = 'blocked' THEN 1 ELSE 0 END) AS blocked_count,
                SUM(CASE WHEN queued_messages.state = 'approved' THEN 1 ELSE 0 END) AS approved_count,
                SUM(CASE WHEN queued_messages.state = 'rendered' THEN 1 ELSE 0 END) AS rendered_count
            FROM campaigns
            LEFT JOIN offer_profiles ON offer_profiles.id = campaigns.offer_profile_id
            LEFT JOIN vertical_playbooks ON vertical_playbooks.id = campaigns.vertical_playbook_id
            LEFT JOIN campaign_audience_snapshots ON campaign_audience_snapshots.campaign_id = campaigns.id
            LEFT JOIN queued_messages ON queued_messages.campaign_id = campaigns.id
            WHERE campaigns.id = ?
            GROUP BY campaigns.id
            """,
            (campaign_id,),
        )
        return campaigns[0] if campaigns else None

    def list_preview_rows(self, campaign_id: int, limit: int = 500) -> list[dict[str, object]]:
        return self.fetch_all(
            """
            SELECT
                queued_messages.*,
                leads.full_name AS lead_name,
                leads.email AS lead_email,
                companies.name AS company_name,
                companies.domain AS company_domain,
                provider_accounts.display_name AS provider_account_name,
                provider_accounts.email_address AS provider_account_email,
                sequence_steps.step_order,
                sequence_steps.delay_days,
                sequence_steps.step_type,
                sequence_steps.subject_override,
                sequence_steps.body_override,
                templates.name AS template_name,
                template_variants.name AS template_variant_name,
                generation_artifacts.output_json AS artifact_output_json,
                generation_artifacts.prompt_input_json AS artifact_prompt_input_json,
                generation_artifacts.validation_status AS artifact_validation_status,
                generation_artifacts.risk_flags_json AS artifact_risk_flags_json,
                generation_artifacts.source_artifact_id AS source_artifact_id
            FROM queued_messages
            INNER JOIN leads ON leads.id = queued_messages.lead_id
            LEFT JOIN companies ON companies.id = queued_messages.company_id
            INNER JOIN provider_accounts ON provider_accounts.id = queued_messages.provider_account_id
            INNER JOIN sequence_steps ON sequence_steps.id = queued_messages.sequence_step_id
            INNER JOIN templates ON templates.id = queued_messages.template_id
            INNER JOIN template_variants ON template_variants.id = queued_messages.template_variant_id
            INNER JOIN generation_artifacts ON generation_artifacts.id = queued_messages.generation_artifact_id
            WHERE queued_messages.campaign_id = ?
            ORDER BY sequence_steps.step_order ASC, queued_messages.scheduled_for ASC, queued_messages.id ASC
            LIMIT ?
            """,
            (campaign_id, limit),
        )

    def get_preview_row(self, queued_message_id: int) -> dict[str, object] | None:
        rows = self.fetch_all(
            """
            SELECT
                queued_messages.*,
                leads.full_name AS lead_name,
                leads.email AS lead_email,
                companies.name AS company_name,
                companies.domain AS company_domain,
                provider_accounts.display_name AS provider_account_name,
                provider_accounts.email_address AS provider_account_email,
                sequence_steps.step_order,
                sequence_steps.delay_days,
                sequence_steps.step_type,
                sequence_steps.subject_override,
                sequence_steps.body_override,
                templates.name AS template_name,
                template_variants.name AS template_variant_name,
                generation_artifacts.output_json AS artifact_output_json,
                generation_artifacts.prompt_input_json AS artifact_prompt_input_json,
                generation_artifacts.validation_status AS artifact_validation_status,
                generation_artifacts.risk_flags_json AS artifact_risk_flags_json,
                generation_artifacts.source_artifact_id AS source_artifact_id
            FROM queued_messages
            INNER JOIN leads ON leads.id = queued_messages.lead_id
            LEFT JOIN companies ON companies.id = queued_messages.company_id
            INNER JOIN provider_accounts ON provider_accounts.id = queued_messages.provider_account_id
            INNER JOIN sequence_steps ON sequence_steps.id = queued_messages.sequence_step_id
            INNER JOIN templates ON templates.id = queued_messages.template_id
            INNER JOIN template_variants ON template_variants.id = queued_messages.template_variant_id
            INNER JOIN generation_artifacts ON generation_artifacts.id = queued_messages.generation_artifact_id
            WHERE queued_messages.id = ?
            """,
            (queued_message_id,),
        )
        return rows[0] if rows else None

    def update_render_output(
        self,
        queued_message_id: int,
        *,
        generation_artifact_id: int,
        rendered_subject: str,
        rendered_body: str,
        render_hash: str,
        risk_flags_json: str,
        state: str | None = None,
        updated_at: str | None = None,
    ) -> None:
        timestamp = updated_at or utc_now()
        if state is None:
            self.execute(
                """
                UPDATE queued_messages
                SET generation_artifact_id = ?,
                    rendered_subject = ?,
                    rendered_body = ?,
                    render_hash = ?,
                    risk_flags_json = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    generation_artifact_id,
                    rendered_subject,
                    rendered_body,
                    render_hash,
                    risk_flags_json,
                    timestamp,
                    queued_message_id,
                ),
            )
            return
        self.execute(
            """
            UPDATE queued_messages
            SET generation_artifact_id = ?,
                rendered_subject = ?,
                rendered_body = ?,
                render_hash = ?,
                risk_flags_json = ?,
                state = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                generation_artifact_id,
                rendered_subject,
                rendered_body,
                render_hash,
                risk_flags_json,
                state,
                timestamp,
                queued_message_id,
            ),
        )
