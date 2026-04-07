"""Migration coverage for the Sprint 2 schema."""

from __future__ import annotations

import sqlite3

from backend.app.core.database import (
    bootstrap_database,
    check_database,
    connect_database,
    get_current_schema_version,
    inspect_table_names,
    migrate_database,
)
from backend.app.migrations import LATEST_MIGRATION_VERSION
from backend.app.tests.support import DatabaseTestCase


class MigrationTests(DatabaseTestCase):
    def test_bootstrap_applies_latest_domain_schema(self) -> None:
        summary = bootstrap_database(self.database_path, "0.2.0")
        self.assertEqual(summary["schema_version"], LATEST_MIGRATION_VERSION)

        with connect_database(self.database_path) as connection:
            table_names = inspect_table_names(connection)

        self.assertIn("campaigns", table_names)
        self.assertIn("queued_messages", table_names)
        self.assertIn("audit_events", table_names)

        database_status = check_database(self.database_path)
        self.assertTrue(database_status["ok"])
        self.assertEqual(database_status["schema_version"], LATEST_MIGRATION_VERSION)

    def test_migrations_can_downgrade_to_base_and_reapply(self) -> None:
        migrate_database(self.database_path, target_version="base")
        with connect_database(self.database_path) as connection:
            self.assertEqual(get_current_schema_version(connection), "base")
            self.assertNotIn("campaigns", inspect_table_names(connection))

        migrate_database(self.database_path, target_version=LATEST_MIGRATION_VERSION)
        with connect_database(self.database_path) as connection:
            self.assertEqual(get_current_schema_version(connection), LATEST_MIGRATION_VERSION)
            self.assertIn("campaigns", inspect_table_names(connection))

    def test_unique_step_order_constraint_is_enforced(self) -> None:
        with connect_database(self.database_path) as connection:
            timestamp = "2026-04-06T00:00:00+00:00"
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
                ("Duplicate Step Campaign", "draft", "manual", "manual", "manual", timestamp, timestamp),
            ).lastrowid
            sequence_id = connection.execute(
                "INSERT INTO sequences (campaign_id, name, created_at, updated_at) VALUES (?, ?, ?, ?)",
                (campaign_id, "Sequence A", timestamp, timestamp),
            ).lastrowid
            template_id = connection.execute(
                "INSERT INTO templates (name, created_at, updated_at) VALUES (?, ?, ?)",
                ("Template A", timestamp, timestamp),
            ).lastrowid
            template_variant_id = connection.execute(
                """
                INSERT INTO template_variants (template_id, name, variant_label, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (template_id, "default", "Default", timestamp, timestamp),
            ).lastrowid
            connection.execute(
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
            )

            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
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
                )
