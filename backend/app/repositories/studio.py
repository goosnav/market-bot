"""Repositories for templates, playbooks, offer profiles, and generation artifacts."""

from __future__ import annotations

from backend.app.core.logging import utc_now
from backend.app.domain.models import (
    GenerationArtifactCreate,
    OfferProfileCreate,
    TemplateBlockCreate,
    TemplateCreate,
    TemplateVariantCreate,
    VerticalPlaybookCreate,
)
from backend.app.repositories.base import SqliteRepository


class OfferProfileRepository(SqliteRepository):
    """Persist offer profiles used by template rendering."""

    def create(self, payload: OfferProfileCreate, created_at: str | None = None) -> int:
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO offer_profiles (
                name,
                description,
                target_verticals_json,
                target_pains_json,
                value_proposition,
                standard_cta,
                booking_link_id,
                allowed_claims_json,
                disallowed_claims_json,
                pricing_framing_snippets_json,
                objection_handling_snippets_json,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.name,
                payload.description,
                payload.target_verticals_json,
                payload.target_pains_json,
                payload.value_proposition,
                payload.standard_cta,
                payload.booking_link_id,
                payload.allowed_claims_json,
                payload.disallowed_claims_json,
                payload.pricing_framing_snippets_json,
                payload.objection_handling_snippets_json,
                timestamp,
                timestamp,
            ),
        )
        return int(cursor.lastrowid)

    def update(self, offer_profile_id: int, payload: OfferProfileCreate, updated_at: str | None = None) -> None:
        timestamp = updated_at or utc_now()
        self.execute(
            """
            UPDATE offer_profiles
            SET name = ?,
                description = ?,
                target_verticals_json = ?,
                target_pains_json = ?,
                value_proposition = ?,
                standard_cta = ?,
                booking_link_id = ?,
                allowed_claims_json = ?,
                disallowed_claims_json = ?,
                pricing_framing_snippets_json = ?,
                objection_handling_snippets_json = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                payload.name,
                payload.description,
                payload.target_verticals_json,
                payload.target_pains_json,
                payload.value_proposition,
                payload.standard_cta,
                payload.booking_link_id,
                payload.allowed_claims_json,
                payload.disallowed_claims_json,
                payload.pricing_framing_snippets_json,
                payload.objection_handling_snippets_json,
                timestamp,
                offer_profile_id,
            ),
        )

    def get(self, offer_profile_id: int) -> dict[str, object] | None:
        return self.fetch_one("SELECT * FROM offer_profiles WHERE id = ?", (offer_profile_id,))

    def list_all(self) -> list[dict[str, object]]:
        return self.fetch_all("SELECT * FROM offer_profiles ORDER BY updated_at DESC, id DESC")

    def count_all(self) -> int:
        row = self.execute("SELECT COUNT(*) AS total FROM offer_profiles").fetchone()
        return int(row["total"])


class VerticalPlaybookRepository(SqliteRepository):
    """Persist vertical messaging playbooks."""

    def create(self, payload: VerticalPlaybookCreate, created_at: str | None = None) -> int:
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO vertical_playbooks (
                name,
                target_pains_json,
                acceptable_language_json,
                disallowed_language_json,
                personalization_strategy,
                tone_profile,
                sample_subject_patterns_json,
                standard_objections_json,
                escalation_rules_json,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.name,
                payload.target_pains_json,
                payload.acceptable_language_json,
                payload.disallowed_language_json,
                payload.personalization_strategy,
                payload.tone_profile,
                payload.sample_subject_patterns_json,
                payload.standard_objections_json,
                payload.escalation_rules_json,
                timestamp,
                timestamp,
            ),
        )
        return int(cursor.lastrowid)

    def update(self, playbook_id: int, payload: VerticalPlaybookCreate, updated_at: str | None = None) -> None:
        timestamp = updated_at or utc_now()
        self.execute(
            """
            UPDATE vertical_playbooks
            SET name = ?,
                target_pains_json = ?,
                acceptable_language_json = ?,
                disallowed_language_json = ?,
                personalization_strategy = ?,
                tone_profile = ?,
                sample_subject_patterns_json = ?,
                standard_objections_json = ?,
                escalation_rules_json = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                payload.name,
                payload.target_pains_json,
                payload.acceptable_language_json,
                payload.disallowed_language_json,
                payload.personalization_strategy,
                payload.tone_profile,
                payload.sample_subject_patterns_json,
                payload.standard_objections_json,
                payload.escalation_rules_json,
                timestamp,
                playbook_id,
            ),
        )

    def get(self, playbook_id: int) -> dict[str, object] | None:
        return self.fetch_one("SELECT * FROM vertical_playbooks WHERE id = ?", (playbook_id,))

    def list_all(self) -> list[dict[str, object]]:
        return self.fetch_all("SELECT * FROM vertical_playbooks ORDER BY updated_at DESC, id DESC")

    def count_all(self) -> int:
        row = self.execute("SELECT COUNT(*) AS total FROM vertical_playbooks").fetchone()
        return int(row["total"])


class TemplateRepository(SqliteRepository):
    """Persist templates and high-level metadata."""

    def create(self, payload: TemplateCreate, created_at: str | None = None) -> int:
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO templates (
                name,
                description,
                channel,
                is_active,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                payload.name,
                payload.description,
                payload.channel,
                int(payload.is_active),
                timestamp,
                timestamp,
            ),
        )
        return int(cursor.lastrowid)

    def update(self, template_id: int, payload: TemplateCreate, updated_at: str | None = None) -> None:
        timestamp = updated_at or utc_now()
        self.execute(
            """
            UPDATE templates
            SET name = ?,
                description = ?,
                channel = ?,
                is_active = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                payload.name,
                payload.description,
                payload.channel,
                int(payload.is_active),
                timestamp,
                template_id,
            ),
        )

    def get(self, template_id: int) -> dict[str, object] | None:
        return self.fetch_one("SELECT * FROM templates WHERE id = ?", (template_id,))

    def list_all(self) -> list[dict[str, object]]:
        return self.fetch_all(
            """
            SELECT
                templates.*,
                COUNT(DISTINCT template_variants.id) AS variant_count,
                COUNT(DISTINCT template_blocks.id) AS block_count
            FROM templates
            LEFT JOIN template_variants ON template_variants.template_id = templates.id
            LEFT JOIN template_blocks ON template_blocks.template_id = templates.id
            GROUP BY templates.id
            ORDER BY templates.updated_at DESC, templates.id DESC
            """
        )

    def count_all(self) -> int:
        row = self.execute("SELECT COUNT(*) AS total FROM templates").fetchone()
        return int(row["total"])


class TemplateVariantRepository(SqliteRepository):
    """Persist template variants."""

    def create(self, payload: TemplateVariantCreate, created_at: str | None = None) -> int:
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO template_variants (
                template_id,
                name,
                variant_label,
                is_default,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                payload.template_id,
                payload.name,
                payload.variant_label,
                int(payload.is_default),
                timestamp,
                timestamp,
            ),
        )
        return int(cursor.lastrowid)

    def list_for_template(self, template_id: int) -> list[dict[str, object]]:
        return self.fetch_all(
            """
            SELECT *
            FROM template_variants
            WHERE template_id = ?
            ORDER BY is_default DESC, id ASC
            """,
            (template_id,),
        )

    def get(self, template_variant_id: int) -> dict[str, object] | None:
        return self.fetch_one("SELECT * FROM template_variants WHERE id = ?", (template_variant_id,))

    def get_default(self, template_id: int) -> dict[str, object] | None:
        variant = self.fetch_one(
            """
            SELECT *
            FROM template_variants
            WHERE template_id = ? AND is_default = 1
            ORDER BY id ASC
            LIMIT 1
            """,
            (template_id,),
        )
        if variant is not None:
            return variant
        return self.fetch_one(
            """
            SELECT *
            FROM template_variants
            WHERE template_id = ?
            ORDER BY id ASC
            LIMIT 1
            """,
            (template_id,),
        )

    def delete_for_template(self, template_id: int) -> None:
        self.execute("DELETE FROM template_variants WHERE template_id = ?", (template_id,))


class TemplateBlockRepository(SqliteRepository):
    """Persist template blocks for subject/body assembly."""

    def create(self, payload: TemplateBlockCreate, created_at: str | None = None) -> int:
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO template_blocks (
                template_id,
                template_variant_id,
                block_key,
                block_type,
                section,
                content,
                fallback_content,
                rules_json,
                position,
                is_required,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.template_id,
                payload.template_variant_id,
                payload.block_key,
                payload.block_type.value,
                payload.section,
                payload.content,
                payload.fallback_content,
                payload.rules_json,
                payload.position,
                int(payload.is_required),
                timestamp,
                timestamp,
            ),
        )
        return int(cursor.lastrowid)

    def list_for_template(self, template_id: int) -> list[dict[str, object]]:
        return self.fetch_all(
            """
            SELECT *
            FROM template_blocks
            WHERE template_id = ?
            ORDER BY section ASC, position ASC, id ASC
            """,
            (template_id,),
        )

    def list_for_render(self, template_id: int, template_variant_id: int | None) -> list[dict[str, object]]:
        if template_variant_id is None:
            return self.fetch_all(
                """
                SELECT *
                FROM template_blocks
                WHERE template_id = ? AND template_variant_id IS NULL
                ORDER BY section ASC, position ASC, id ASC
                """,
                (template_id,),
            )
        return self.fetch_all(
            """
            SELECT *
            FROM template_blocks
            WHERE template_id = ?
              AND (template_variant_id IS NULL OR template_variant_id = ?)
            ORDER BY section ASC, position ASC, CASE WHEN template_variant_id IS NULL THEN 0 ELSE 1 END ASC, id ASC
            """,
            (template_id, template_variant_id),
        )

    def delete_for_template(self, template_id: int) -> None:
        self.execute("DELETE FROM template_blocks WHERE template_id = ?", (template_id,))


class GenerationArtifactRepository(SqliteRepository):
    """Persist render/generation artifacts."""

    def create(
        self,
        payload: GenerationArtifactCreate,
        *,
        template_id: int | None = None,
        template_variant_id: int | None = None,
        lead_id: int | None = None,
        source_artifact_id: int | None = None,
        created_at: str | None = None,
    ) -> int:
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO generation_artifacts (
                kind,
                prompt_version,
                prompt_input_json,
                output_text,
                output_json,
                validation_status,
                risk_flags_json,
                model_name,
                template_id,
                template_variant_id,
                lead_id,
                source_artifact_id,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.kind,
                payload.prompt_version,
                payload.prompt_input_json,
                payload.output_text,
                payload.output_json,
                payload.validation_status.value,
                payload.risk_flags_json,
                payload.model_name,
                template_id,
                template_variant_id,
                lead_id,
                source_artifact_id,
                timestamp,
                timestamp,
            ),
        )
        return int(cursor.lastrowid)

    def get(self, artifact_id: int) -> dict[str, object] | None:
        return self.fetch_one("SELECT * FROM generation_artifacts WHERE id = ?", (artifact_id,))

    def list_recent(self, limit: int = 20) -> list[dict[str, object]]:
        return self.fetch_all(
            """
            SELECT
                generation_artifacts.*,
                templates.name AS template_name,
                template_variants.name AS template_variant_name,
                leads.full_name AS lead_name
            FROM generation_artifacts
            LEFT JOIN templates ON templates.id = generation_artifacts.template_id
            LEFT JOIN template_variants ON template_variants.id = generation_artifacts.template_variant_id
            LEFT JOIN leads ON leads.id = generation_artifacts.lead_id
            ORDER BY generation_artifacts.updated_at DESC, generation_artifacts.id DESC
            LIMIT ?
            """,
            (limit,),
        )

    def count_all(self) -> int:
        row = self.execute("SELECT COUNT(*) AS total FROM generation_artifacts").fetchone()
        return int(row["total"])
