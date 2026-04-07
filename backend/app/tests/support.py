"""Shared fixtures and seed helpers for backend persistence tests."""

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from backend.app.core.database import bootstrap_database
from backend.app.core.logging import utc_now


class DatabaseTestCase(unittest.TestCase):
    """Create and bootstrap an isolated SQLite database per test."""

    def setUp(self) -> None:
        super().setUp()
        self._tempdir = tempfile.TemporaryDirectory()
        self.database_path = Path(self._tempdir.name) / "test.db"
        bootstrap_database(self.database_path, "0.5.0")

    def tearDown(self) -> None:
        self._tempdir.cleanup()
        super().tearDown()


def seed_campaign_graph(connection) -> dict[str, int]:
    """Insert the minimum campaign graph required for queued-message tests."""
    timestamp = utc_now()
    company_id = connection.execute(
        """
        INSERT INTO companies (source, name, domain, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("csv", "Acme Manufacturing", "acme.example", timestamp, timestamp),
    ).lastrowid
    lead_id = connection.execute(
        """
        INSERT INTO leads (
            source,
            full_name,
            email,
            company_id,
            company_name_snapshot,
            company_domain_snapshot,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("csv", "Jordan Smith", "jordan@acme.example", company_id, "Acme Manufacturing", "acme.example", timestamp, timestamp),
    ).lastrowid
    campaign_id = connection.execute(
        """
        INSERT INTO campaigns (
            name,
            status,
            provider_name,
            approval_mode,
            reply_mode,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("Manufacturing Outreach", "draft", "manual", "manual", "manual", timestamp, timestamp),
    ).lastrowid
    sequence_id = connection.execute(
        """
        INSERT INTO sequences (campaign_id, name, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        """,
        (campaign_id, "Default Sequence", timestamp, timestamp),
    ).lastrowid
    template_id = connection.execute(
        """
        INSERT INTO templates (name, created_at, updated_at)
        VALUES (?, ?, ?)
        """,
        ("Initial Outreach", timestamp, timestamp),
    ).lastrowid
    template_variant_id = connection.execute(
        """
        INSERT INTO template_variants (template_id, name, variant_label, is_default, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (template_id, "default", "Default", 1, timestamp, timestamp),
    ).lastrowid
    sequence_step_id = connection.execute(
        """
        INSERT INTO sequence_steps (
            sequence_id,
            step_order,
            template_id,
            template_variant_id,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (sequence_id, 0, template_id, template_variant_id, timestamp, timestamp),
    ).lastrowid
    provider_account_id = connection.execute(
        """
        INSERT INTO provider_accounts (provider_name, display_name, email_address, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("smartlead", "Primary Pool", "pool@example.com", timestamp, timestamp),
    ).lastrowid
    generation_artifact_id = connection.execute(
        """
        INSERT INTO generation_artifacts (kind, prompt_version, output_text, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("email_render", "v1", "Hello from Market Bot", timestamp, timestamp),
    ).lastrowid
    thread_id = connection.execute(
        """
        INSERT INTO threads (lead_id, campaign_id, provider_name, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (lead_id, campaign_id, "smartlead", timestamp, timestamp),
    ).lastrowid
    return {
        "company_id": int(company_id),
        "lead_id": int(lead_id),
        "campaign_id": int(campaign_id),
        "sequence_id": int(sequence_id),
        "template_id": int(template_id),
        "template_variant_id": int(template_variant_id),
        "sequence_step_id": int(sequence_step_id),
        "provider_account_id": int(provider_account_id),
        "generation_artifact_id": int(generation_artifact_id),
        "thread_id": int(thread_id),
    }
