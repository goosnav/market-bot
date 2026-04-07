"""Sprint 4 template studio tests."""

from __future__ import annotations

from backend.app.core.logging import utc_now
from backend.app.core.database import connect_database
from backend.app.services.template_studio import TemplateStudioService
from backend.app.tests.support import DatabaseTestCase


class TemplateStudioTests(DatabaseTestCase):
    def test_render_with_missing_variables_records_warning_and_artifact(self) -> None:
        with self._service_context() as (connection, service, lead_id):
            template = self._create_template(
                service,
                name="Missing Variable Template",
                blocks=[
                    {
                        "block_key": "subject_line",
                        "block_type": "merged",
                        "section": "subject",
                        "content": "Hi {{ lead.first_name }}",
                    },
                    {
                        "block_key": "body_intro",
                        "block_type": "merged",
                        "section": "body",
                        "content": "Wanted to share one idea for {{ company.name }}.",
                    },
                ],
            )
            artifact = service.render_template(template_id=template["id"], lead_id=lead_id, actor="tester")

            self.assertEqual(artifact["validation_status"], "warning")
            self.assertEqual(artifact["subject"], "Hi")
            self.assertTrue(any(flag["code"] == "missing_variable" for flag in artifact["risk_flags"]))
            count = connection.execute("SELECT COUNT(*) AS total FROM generation_artifacts").fetchone()["total"]
            self.assertEqual(int(count), 1)

    def test_banned_phrase_blocking_marks_artifact_blocked(self) -> None:
        with self._service_context() as (_connection, service, lead_id):
            offer = service.save_offer_profile(
                {
                    "name": "Manufacturing Offer",
                    "value_proposition": "a tighter outreach workflow",
                    "standard_cta": "Open to a quick 15-minute walkthrough?",
                    "disallowed_claims": ["guaranteed results"],
                },
                actor="tester",
            )
            template = self._create_template(
                service,
                name="Blocked Claim Template",
                blocks=[
                    {
                        "block_key": "subject_line",
                        "block_type": "merged",
                        "section": "subject",
                        "content": "Quick idea for {{ company.name }}",
                    },
                    {
                        "block_key": "body_hook",
                        "block_type": "ai_generated",
                        "section": "body",
                        "content": "guaranteed results for {{ company.name }}",
                        "fallback_content": "Wanted to share one idea for {{ company.name }}.",
                    },
                ],
            )
            artifact = service.render_template(
                template_id=template["id"],
                lead_id=lead_id,
                offer_profile_id=offer["id"],
                actor="tester",
            )

            self.assertEqual(artifact["validation_status"], "blocked")
            self.assertTrue(any(flag["code"] == "banned_phrase" for flag in artifact["risk_flags"]))

    def test_deterministic_mode_uses_fallback_for_ai_blocks(self) -> None:
        with self._service_context() as (_connection, service, lead_id):
            template = self._create_template(
                service,
                name="Deterministic Template",
                blocks=[
                    {
                        "block_key": "subject_line",
                        "block_type": "merged",
                        "section": "subject",
                        "content": "Quick note for {{ company.name }}",
                    },
                    {
                        "block_key": "body_hook",
                        "block_type": "ai_generated",
                        "section": "body",
                        "content": "company-specific hook for {{ company.name }}",
                        "fallback_content": "Wanted to share one concrete idea for {{ company.name }}.",
                    },
                ],
            )
            artifact = service.render_template(
                template_id=template["id"],
                lead_id=lead_id,
                actor="tester",
                deterministic_mode=True,
            )

            block = next(block for block in artifact["output"]["blocks"] if block["block_key"] == "body_hook")
            self.assertEqual(block["source"], "deterministic_fallback")
            self.assertIn("Wanted to share one concrete idea", artifact["body"])
            self.assertEqual(artifact["model_name"], "deterministic")

    def test_every_render_is_persisted_as_a_generation_artifact(self) -> None:
        with self._service_context() as (connection, service, lead_id):
            template = self._create_template(
                service,
                name="Artifact Count Template",
                blocks=[
                    {
                        "block_key": "subject_line",
                        "block_type": "merged",
                        "section": "subject",
                        "content": "Quick idea for {{ company.name }}",
                    },
                    {
                        "block_key": "body_hook",
                        "block_type": "ai_generated",
                        "section": "body",
                        "content": "short opener for {{ company.name }}",
                        "fallback_content": "Wanted to share one idea for {{ company.name }}.",
                    },
                ],
            )

            service.render_template(template_id=template["id"], lead_id=lead_id, actor="tester", generation_seed=0)
            service.render_template(template_id=template["id"], lead_id=lead_id, actor="tester", generation_seed=1)

            count = connection.execute("SELECT COUNT(*) AS total FROM generation_artifacts").fetchone()["total"]
            self.assertEqual(int(count), 2)

    def test_unsupported_claims_are_risk_flagged(self) -> None:
        with self._service_context() as (_connection, service, lead_id):
            template = self._create_template(
                service,
                name="Unsupported Claim Template",
                blocks=[
                    {
                        "block_key": "subject_line",
                        "block_type": "merged",
                        "section": "subject",
                        "content": "Question for {{ company.name }}",
                    },
                    {
                        "block_key": "body_hook",
                        "block_type": "ai_generated",
                        "section": "body",
                        "content": "promise a 200% revenue lift for {{ company.name }}",
                        "fallback_content": "Wanted to share one idea for {{ company.name }}.",
                    },
                ],
            )
            artifact = service.render_template(template_id=template["id"], lead_id=lead_id, actor="tester")

            self.assertEqual(artifact["validation_status"], "blocked")
            self.assertTrue(any(flag["code"] == "unsupported_claim" for flag in artifact["risk_flags"]))

    def test_regenerate_only_selected_ai_block_preserves_other_ai_blocks(self) -> None:
        with self._service_context() as (_connection, service, lead_id):
            template = self._create_template(
                service,
                name="Selective Regeneration Template",
                blocks=[
                    {
                        "block_key": "subject_line",
                        "block_type": "merged",
                        "section": "subject",
                        "content": "Quick idea for {{ company.name }}",
                    },
                    {
                        "block_key": "body_hook",
                        "block_type": "ai_generated",
                        "section": "body",
                        "content": "first sentence about {{ company.name }}",
                        "fallback_content": "Wanted to share one idea for {{ company.name }}.",
                    },
                    {
                        "block_key": "body_followup",
                        "block_type": "ai_generated",
                        "section": "body",
                        "content": "company-specific hook for {{ company.name }}",
                        "fallback_content": "A quick note about {{ company.name }}.",
                    },
                ],
            )
            original = service.render_template(template_id=template["id"], lead_id=lead_id, actor="tester", generation_seed=0)
            regenerated = service.regenerate_artifact(original["id"], actor="tester", regenerate_block_keys=["body_hook"])

            original_blocks = {block["block_key"]: block for block in original["output"]["blocks"]}
            regenerated_blocks = {block["block_key"]: block for block in regenerated["output"]["blocks"]}

            self.assertEqual(regenerated["source_artifact_id"], original["id"])
            self.assertEqual(regenerated_blocks["body_followup"]["source"], "preserved_ai")
            self.assertEqual(
                regenerated_blocks["body_followup"]["rendered_text"],
                original_blocks["body_followup"]["rendered_text"],
            )
            self.assertEqual(regenerated_blocks["body_hook"]["source"], "local_ai")

    def _service_context(self):
        class _Context:
            def __init__(self, outer) -> None:
                self.outer = outer

            def __enter__(self):
                connection = connect_database(self.outer.database_path)
                self.connection = connection
                self.service = TemplateStudioService(connection)
                self.lead_id = seed_lead(connection, first_name="", company_name="Forge Works")
                return connection, self.service, self.lead_id

            def __exit__(self, exc_type, exc, tb):
                self.connection.close()
                return False

        return _Context(self)

    def _create_template(self, service: TemplateStudioService, *, name: str, blocks: list[dict[str, object]]) -> dict[str, object]:
        return service.save_template(
            {
                "name": name,
                "description": "Sprint 4 test template",
                "variants": [
                    {
                        "name": "default",
                        "variant_label": "Default",
                        "is_default": True,
                        "blocks": blocks,
                    }
                ],
            },
            actor="tester",
        )


def seed_lead(connection, *, first_name: str, company_name: str) -> int:
    timestamp = utc_now()
    company_id = connection.execute(
        """
        INSERT INTO companies (source, name, domain, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("manual", company_name, "forge.example", timestamp, timestamp),
    ).lastrowid
    lead_id = connection.execute(
        """
        INSERT INTO leads (
            source,
            first_name,
            last_name,
            full_name,
            email,
            company_id,
            company_name_snapshot,
            company_domain_snapshot,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "manual",
            first_name,
            "Stone",
            f"{first_name} Stone".strip(),
            "avery@forge.example",
            company_id,
            company_name,
            "forge.example",
            timestamp,
            timestamp,
        ),
    ).lastrowid
    return int(lead_id)
