"""Migration registry exports."""

from backend.app.migrations.registry import LATEST_MIGRATION_VERSION, MIGRATIONS, MigrationDefinition

__all__ = ["LATEST_MIGRATION_VERSION", "MIGRATIONS", "MigrationDefinition"]
