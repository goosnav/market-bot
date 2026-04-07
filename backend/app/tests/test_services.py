"""Repository and service behavior tests for Sprint 2."""

from __future__ import annotations

import sqlite3

from backend.app.domain.enums import CampaignStatus, QueuedMessageState, ReplyState
from backend.app.domain.models import CampaignCreate, LeadCreate, QueuedMessageCreate, ReplyCreate
from backend.app.domain.transitions import InvalidStateTransition
from backend.app.repositories import AuditEventRepository, CompanyRepository, LeadRepository, read_session, write_session
from backend.app.services import CampaignService, QueuedMessageService, ReplyService
from backend.app.tests.support import DatabaseTestCase, seed_campaign_graph


class ServiceTests(DatabaseTestCase):
    def test_campaign_status_transition_emits_audit_event(self) -> None:
        with write_session(self.database_path) as connection:
            service = CampaignService(connection)
            campaign = service.create_campaign(CampaignCreate(name="Spring Campaign"), actor="operator")
            updated = service.transition_status(campaign["id"], CampaignStatus.PREVIEW_READY, actor="operator", reason="Preview complete")
            audit_events = AuditEventRepository(connection).list_for_entity("campaign", campaign["id"])

        self.assertEqual(updated["status"], CampaignStatus.PREVIEW_READY.value)
        self.assertEqual(len(audit_events), 2)
        self.assertEqual(audit_events[-1]["event_type"], "campaign.status_changed")

    def test_invalid_campaign_transition_is_rejected_without_side_effects(self) -> None:
        with write_session(self.database_path) as connection:
            campaign = CampaignService(connection).create_campaign(CampaignCreate(name="Blocked Campaign"), actor="operator")

        with self.assertRaises(InvalidStateTransition):
            with write_session(self.database_path) as connection:
                CampaignService(connection).transition_status(campaign["id"], CampaignStatus.ACTIVE, actor="operator")

        with read_session(self.database_path) as connection:
            persisted = connection.execute("SELECT status FROM campaigns WHERE id = ?", (campaign["id"],)).fetchone()
            audit_count = connection.execute(
                "SELECT COUNT(*) FROM audit_events WHERE entity_type = 'campaign' AND entity_id = ?",
                (campaign["id"],),
            ).fetchone()[0]

        self.assertEqual(persisted["status"], CampaignStatus.DRAFT.value)
        self.assertEqual(audit_count, 1)

    def test_write_session_rolls_back_on_integrity_error(self) -> None:
        with self.assertRaises(sqlite3.IntegrityError):
            with write_session(self.database_path) as connection:
                companies = CompanyRepository(connection)
                companies.create(source="csv", name="Acme 1", domain="acme.example")
                companies.create(source="csv", name="Acme 2", domain="acme.example")

        with read_session(self.database_path) as connection:
            company_count = connection.execute("SELECT COUNT(*) FROM companies").fetchone()[0]

        self.assertEqual(company_count, 0)

    def test_lead_email_uniqueness_is_case_insensitive(self) -> None:
        with self.assertRaises(sqlite3.IntegrityError):
            with write_session(self.database_path) as connection:
                leads = LeadRepository(connection)
                leads.create(LeadCreate(source="csv", full_name="Jordan Smith", email="Jordan@Example.com"))
                leads.create(LeadCreate(source="csv", full_name="Jordan Smith Two", email="jordan@example.com"))

    def test_company_delete_is_restricted_when_leads_exist(self) -> None:
        with write_session(self.database_path) as connection:
            companies = CompanyRepository(connection)
            company_id = companies.create(source="csv", name="Restrict Co", domain="restrict.example")
            LeadRepository(connection).create(
                LeadCreate(
                    source="csv",
                    full_name="Taylor Ames",
                    email="taylor@restrict.example",
                    company_id=company_id,
                    company_name_snapshot="Restrict Co",
                    company_domain_snapshot="restrict.example",
                )
            )

        with self.assertRaises(sqlite3.IntegrityError):
            with write_session(self.database_path) as connection:
                connection.execute("DELETE FROM companies WHERE id = ?", (company_id,))

    def test_queued_message_state_transition_emits_audit_event(self) -> None:
        with write_session(self.database_path) as connection:
            ids = seed_campaign_graph(connection)
            service = QueuedMessageService(connection)
            queued_message = service.create_message(
                QueuedMessageCreate(
                    lead_id=ids["lead_id"],
                    company_id=ids["company_id"],
                    campaign_id=ids["campaign_id"],
                    sequence_id=ids["sequence_id"],
                    sequence_step_id=ids["sequence_step_id"],
                    template_id=ids["template_id"],
                    template_variant_id=ids["template_variant_id"],
                    generation_artifact_id=ids["generation_artifact_id"],
                    provider_account_id=ids["provider_account_id"],
                    scheduled_for="2026-04-06T09:00:00+00:00",
                    state=QueuedMessageState.RENDERED,
                    rendered_subject="Hello",
                    rendered_body="World",
                    render_hash="hash-1",
                ),
                actor="scheduler",
            )
            updated = service.transition_state(
                queued_message["id"],
                QueuedMessageState.APPROVED,
                actor="scheduler",
                reason="Validation passed",
            )
            audit_events = AuditEventRepository(connection).list_for_entity("queued_message", queued_message["id"])

        self.assertEqual(updated["state"], QueuedMessageState.APPROVED.value)
        self.assertEqual(len(audit_events), 2)

    def test_reply_state_transition_emits_audit_event(self) -> None:
        with write_session(self.database_path) as connection:
            ids = seed_campaign_graph(connection)
            service = ReplyService(connection)
            reply = service.create_reply(
                ReplyCreate(
                    thread_id=ids["thread_id"],
                    lead_id=ids["lead_id"],
                    campaign_id=ids["campaign_id"],
                    provider_name="smartlead",
                    received_at="2026-04-06T10:00:00+00:00",
                    reply_text="Interested, send details.",
                ),
                actor="webhook",
            )
            updated = service.transition_state(
                reply["id"],
                ReplyState.CLASSIFIED,
                actor="classifier",
                classification="interested",
                reason="Rule match",
            )
            audit_events = AuditEventRepository(connection).list_for_entity("reply", reply["id"])

        self.assertEqual(updated["state"], ReplyState.CLASSIFIED.value)
        self.assertEqual(updated["classification"], "interested")
        self.assertEqual(len(audit_events), 2)
