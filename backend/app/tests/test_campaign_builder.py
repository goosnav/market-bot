"""Sprint 5 campaign builder and queue preview tests."""

from __future__ import annotations

from backend.app.core.database import connect_database
from backend.app.core.logging import utc_now
from backend.app.services.campaign_builder import CampaignBuilderService
from backend.app.services.template_studio import TemplateStudioService
from backend.app.tests.support import DatabaseTestCase


class CampaignBuilderTests(DatabaseTestCase):
    def test_preview_generation_creates_exact_queue_rows(self) -> None:
        with connect_database(self.database_path) as connection:
            studio = TemplateStudioService(connection)
            builder = CampaignBuilderService(connection)
            lead_id = seed_lead(connection, "Avery Stone", "Forge Works", "avery@forge.example")
            provider = builder.create_provider_account(
                {
                    "display_name": "Primary Pool",
                    "email_address": "pool@forge.example",
                    "provider_name": "manual",
                },
                actor="tester",
            )
            offer = studio.save_offer_profile(
                {
                    "name": "Audit Offer",
                    "value_proposition": "a tighter outbound follow-up workflow",
                    "standard_cta": "Open to a short walkthrough next week?",
                },
                actor="tester",
            )
            playbook = studio.save_vertical_playbook(
                {
                    "name": "Service Firms",
                    "tone_profile": "direct",
                    "target_pains": ["slow follow-up"],
                },
                actor="tester",
            )
            template = create_template(studio, "Preview Accuracy Template")

            preview = builder.build_campaign_preview(
                {
                    "name": "Preview Accuracy Campaign",
                    "offer_profile_id": offer["id"],
                    "vertical_playbook_id": playbook["id"],
                    "provider_account_ids": [provider["id"]],
                    "steps": [{"template_id": template["id"], "delay_days": 0}],
                    "start_at": "2026-04-06T09:00:00+00:00",
                    "timezone": "UTC",
                },
                actor="tester",
            )

        self.assertEqual(preview["campaign"]["status"], "preview_ready")
        self.assertEqual(len(preview["items"]), 1)
        item = preview["items"][0]
        self.assertEqual(item["lead_id"], lead_id)
        self.assertEqual(item["subject"], "Quick idea for Forge Works")
        self.assertIn("Hi Avery Stone", item["body"])
        self.assertEqual(item["provider_account_name"], "Primary Pool")
        self.assertEqual(len(item["static_sections"]), 3)
        self.assertEqual(len(item["ai_sections"]), 1)

    def test_queue_generation_supports_multiple_sequence_steps(self) -> None:
        with connect_database(self.database_path) as connection:
            studio = TemplateStudioService(connection)
            builder = CampaignBuilderService(connection)
            seed_lead(connection, "Avery Stone", "Forge Works", "avery@forge.example")
            seed_lead(connection, "Jordan Ames", "Mill Co", "jordan@mill.example")
            provider = builder.create_provider_account(
                {"display_name": "Primary Pool", "email_address": "pool@example.com"},
                actor="tester",
            )
            template_a = create_template(studio, "Step One Template")
            template_b = create_template(studio, "Step Two Template")

            preview = builder.build_campaign_preview(
                {
                    "name": "Multi Step Campaign",
                    "provider_account_ids": [provider["id"]],
                    "steps": [
                        {"template_id": template_a["id"], "delay_days": 0},
                        {"template_id": template_b["id"], "delay_days": 2},
                    ],
                    "start_at": "2026-04-06T09:00:00+00:00",
                    "timezone": "UTC",
                },
                actor="tester",
            )

        self.assertEqual(len(preview["items"]), 4)
        self.assertEqual([item["step_order"] for item in preview["items"]], [0, 0, 1, 1])

    def test_quiet_hour_adjustments_move_preview_into_allowed_window(self) -> None:
        with connect_database(self.database_path) as connection:
            studio = TemplateStudioService(connection)
            builder = CampaignBuilderService(connection)
            seed_lead(connection, "Avery Stone", "Forge Works", "avery@forge.example")
            provider = builder.create_provider_account(
                {"display_name": "Primary Pool", "email_address": "pool@example.com"},
                actor="tester",
            )
            template = create_template(studio, "Quiet Hours Template")

            preview = builder.build_campaign_preview(
                {
                    "name": "Quiet Hours Campaign",
                    "provider_account_ids": [provider["id"]],
                    "steps": [{"template_id": template["id"], "delay_days": 0}],
                    "start_at": "2026-04-06T21:30:00+00:00",
                    "timezone": "UTC",
                    "send_window": {"start_hour": 9, "end_hour": 17, "interval_minutes": 15, "timezone": "UTC"},
                    "quiet_hours": {"start_hour": 20, "end_hour": 8},
                },
                actor="tester",
            )

        self.assertEqual(preview["items"][0]["scheduled_for"], "2026-04-07T09:00:00+00:00")

    def test_provider_account_assignment_rotates_across_pool(self) -> None:
        with connect_database(self.database_path) as connection:
            studio = TemplateStudioService(connection)
            builder = CampaignBuilderService(connection)
            seed_lead(connection, "Lead One", "Acme One", "one@acme.example")
            seed_lead(connection, "Lead Two", "Acme Two", "two@acme.example")
            seed_lead(connection, "Lead Three", "Acme Three", "three@acme.example")
            first = builder.create_provider_account(
                {"display_name": "Pool A", "email_address": "a@example.com"},
                actor="tester",
            )
            second = builder.create_provider_account(
                {"display_name": "Pool B", "email_address": "b@example.com"},
                actor="tester",
            )
            template = create_template(studio, "Rotation Template")

            preview = builder.build_campaign_preview(
                {
                    "name": "Rotation Campaign",
                    "provider_account_ids": [first["id"], second["id"]],
                    "steps": [{"template_id": template["id"], "delay_days": 0}],
                    "start_at": "2026-04-06T09:00:00+00:00",
                    "timezone": "UTC",
                },
                actor="tester",
            )

        self.assertEqual(
            [item["provider_account_name"] for item in preview["items"]],
            ["Pool A", "Pool B", "Pool A"],
        )

    def test_manual_preview_edits_are_preserved(self) -> None:
        with connect_database(self.database_path) as connection:
            studio = TemplateStudioService(connection)
            builder = CampaignBuilderService(connection)
            seed_lead(connection, "Avery Stone", "Forge Works", "avery@forge.example")
            provider = builder.create_provider_account(
                {"display_name": "Primary Pool", "email_address": "pool@example.com"},
                actor="tester",
            )
            template = create_template(studio, "Manual Edit Template")
            preview = builder.build_campaign_preview(
                {
                    "name": "Manual Edit Campaign",
                    "provider_account_ids": [provider["id"]],
                    "steps": [{"template_id": template["id"], "delay_days": 0}],
                    "start_at": "2026-04-06T09:00:00+00:00",
                    "timezone": "UTC",
                },
                actor="tester",
            )
            queued_message_id = preview["items"][0]["id"]
            edited = builder.manual_edit_preview(
                queued_message_id,
                actor="tester",
                edited_body="Manual body override for preview.",
            )
            refreshed = builder.get_preview(preview["campaign"]["id"])

        self.assertEqual(edited["body"], "Manual body override for preview.")
        self.assertEqual(refreshed["items"][0]["body"], "Manual body override for preview.")
        self.assertIsNotNone(edited["source_artifact_id"])

    def test_regenerate_only_ai_block_preserves_subject_and_static_sections(self) -> None:
        with connect_database(self.database_path) as connection:
            studio = TemplateStudioService(connection)
            builder = CampaignBuilderService(connection)
            seed_lead(connection, "Avery Stone", "Forge Works", "avery@forge.example")
            provider = builder.create_provider_account(
                {"display_name": "Primary Pool", "email_address": "pool@example.com"},
                actor="tester",
            )
            template = create_template(studio, "Selective Regen Template")
            preview = builder.build_campaign_preview(
                {
                    "name": "Selective Regen Campaign",
                    "provider_account_ids": [provider["id"]],
                    "steps": [{"template_id": template["id"], "delay_days": 0}],
                    "start_at": "2026-04-06T09:00:00+00:00",
                    "timezone": "UTC",
                },
                actor="tester",
            )
            original = preview["items"][0]
            regenerated = builder.regenerate_preview(
                original["id"],
                actor="tester",
                regenerate_block_keys=["body_hook"],
            )

        self.assertEqual(regenerated["subject"], original["subject"])
        self.assertEqual(regenerated["static_sections"], original["static_sections"])
        self.assertIsNotNone(regenerated["source_artifact_id"])

    def test_preview_approval_makes_campaign_launch_ready(self) -> None:
        with connect_database(self.database_path) as connection:
            studio = TemplateStudioService(connection)
            builder = CampaignBuilderService(connection)
            seed_lead(connection, "Avery Stone", "Forge Works", "avery@forge.example")
            provider = builder.create_provider_account(
                {"display_name": "Primary Pool", "email_address": "pool@example.com"},
                actor="tester",
            )
            template = create_template(studio, "Approval Template")
            preview = builder.build_campaign_preview(
                {
                    "name": "Approval Campaign",
                    "provider_account_ids": [provider["id"]],
                    "steps": [{"template_id": template["id"], "delay_days": 0}],
                    "start_at": "2026-04-06T09:00:00+00:00",
                    "timezone": "UTC",
                },
                actor="tester",
            )
            approved = builder.approve_preview(preview["campaign"]["id"], actor="tester")

        self.assertTrue(approved["launch_ready"])
        self.assertEqual(approved["campaign"]["status"], "scheduled")
        self.assertEqual({item["state"] for item in approved["items"]}, {"approved"})


def create_template(studio: TemplateStudioService, name: str) -> dict[str, object]:
    return studio.save_template(
        {
            "name": name,
            "variants": [
                {
                    "name": "default",
                    "variant_label": "Default",
                    "is_default": True,
                    "blocks": [
                        {
                            "block_key": "subject_line",
                            "block_type": "merged",
                            "section": "subject",
                            "content": "Quick idea for {{ company.name }}",
                        },
                        {
                            "block_key": "body_intro",
                            "block_type": "merged",
                            "section": "body",
                            "content": "Hi {{ lead.full_name }}, I noticed {{ company.name }} and wanted to share one idea.",
                        },
                        {
                            "block_key": "body_hook",
                            "block_type": "ai_generated",
                            "section": "body",
                            "content": "short company-specific hook for {{ company.name }}",
                            "fallback_content": "Wanted to share one concrete idea for {{ company.name }}.",
                            "rules": {"max_words": 22},
                        },
                        {
                            "block_key": "body_cta",
                            "block_type": "merged",
                            "section": "body",
                            "content": "Open to a short walkthrough next week?",
                        },
                    ],
                }
            ],
        },
        actor="tester",
    )


def seed_lead(connection, full_name: str, company_name: str, email: str) -> int:
    timestamp = utc_now()
    company_domain = f"{company_name.lower().replace(' ', '-')}.example"
    company_id = connection.execute(
        """
        INSERT INTO companies (source, name, domain, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("manual", company_name, company_domain, timestamp, timestamp),
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
            full_name.split()[0],
            full_name.split()[-1],
            full_name,
            email,
            company_id,
            company_name,
            company_domain,
            timestamp,
            timestamp,
        ),
    ).lastrowid
    return int(lead_id)
