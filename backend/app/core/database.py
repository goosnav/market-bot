"""Versioned SQLite bootstrap and migration helpers for Sprint 2."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import sqlite3

from backend.app.migrations import LATEST_MIGRATION_VERSION, MIGRATIONS, MigrationDefinition


class DatabaseBootstrapError(RuntimeError):
    """Raised when the local runtime database cannot be initialized."""


class MigrationError(DatabaseBootstrapError):
    """Raised when a migration cannot be applied or rolled back cleanly."""


def utc_now() -> str:
    return datetime.now(tz=UTC).isoformat()


def connect_database(database_path: Path) -> sqlite3.Connection:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON;")
    connection.execute("PRAGMA journal_mode = WAL;")
    connection.execute("PRAGMA synchronous = NORMAL;")
    connection.execute("PRAGMA busy_timeout = 5000;")
    return connection


def bootstrap_database(database_path: Path, app_version: str) -> dict[str, str | int]:
    now = utc_now()
    try:
        connection = connect_database(database_path)
        ensure_bootstrap_tables(connection)
        apply_migrations(connection)
        schema_version = get_current_schema_version(connection)
        upsert_runtime_metadata(connection, "app_version", app_version, now)
        upsert_runtime_metadata(connection, "schema_version", schema_version, now)
        upsert_runtime_metadata(connection, "schema_updated_at", now, now)
        integrity = connection.execute("PRAGMA integrity_check;").fetchone()
        if not integrity or integrity[0] != "ok":
            raise DatabaseBootstrapError(f"Database integrity check failed: {integrity!r}")
        applied_migrations = len(fetch_applied_migrations(connection))
        connection.commit()
        connection.close()
    except sqlite3.DatabaseError as exc:
        raise DatabaseBootstrapError(f"Unable to initialize database at {database_path}: {exc}") from exc

    return {
        "database_path": str(database_path),
        "journal_mode": "wal",
        "integrity_check": "ok",
        "initialized_at": now,
        "schema_version": schema_version,
        "applied_migrations": applied_migrations,
    }


def migrate_database(database_path: Path, target_version: str | None = None) -> dict[str, str | int]:
    now = utc_now()
    try:
        connection = connect_database(database_path)
        ensure_bootstrap_tables(connection)
        apply_migrations(connection, target_version=target_version)
        schema_version = get_current_schema_version(connection)
        upsert_runtime_metadata(connection, "schema_version", schema_version, now)
        upsert_runtime_metadata(connection, "schema_updated_at", now, now)
        applied_migrations = len(fetch_applied_migrations(connection))
        connection.commit()
        connection.close()
    except sqlite3.DatabaseError as exc:
        raise MigrationError(f"Unable to migrate database at {database_path}: {exc}") from exc

    return {
        "database_path": str(database_path),
        "schema_version": schema_version,
        "applied_migrations": applied_migrations,
        "migrated_at": now,
    }


def ensure_bootstrap_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS app_runtime_metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            description TEXT NOT NULL,
            applied_at TEXT NOT NULL
        )
        """
    )


def upsert_runtime_metadata(connection: sqlite3.Connection, key: str, value: str, updated_at: str) -> None:
    connection.execute(
        """
        INSERT INTO app_runtime_metadata (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at
        """,
        (key, value, updated_at),
    )


def apply_migrations(connection: sqlite3.Connection, target_version: str | None = None) -> None:
    ensure_bootstrap_tables(connection)
    known_versions = [migration.version for migration in MIGRATIONS]
    normalized_target = normalize_target_version(target_version, known_versions)
    current_index = current_migration_index(connection, known_versions)
    target_index = -1 if normalized_target == "base" else known_versions.index(normalized_target)

    if current_index < target_index:
        for migration in MIGRATIONS[current_index + 1 : target_index + 1]:
            run_up_migration(connection, migration)
    elif current_index > target_index:
        for migration in reversed(MIGRATIONS[target_index + 1 : current_index + 1]):
            run_down_migration(connection, migration)


def normalize_target_version(target_version: str | None, known_versions: list[str]) -> str:
    if target_version is None:
        return LATEST_MIGRATION_VERSION if known_versions else "base"
    if target_version == "base":
        return "base"
    if target_version not in known_versions:
        available = ", ".join(known_versions) or "base"
        raise MigrationError(f"Unknown migration target '{target_version}'. Available targets: {available}.")
    return target_version


def current_migration_index(connection: sqlite3.Connection, known_versions: list[str]) -> int:
    current_version = get_current_schema_version(connection)
    if current_version == "base":
        return -1
    try:
        return known_versions.index(current_version)
    except ValueError as exc:
        raise MigrationError(f"Database has unknown applied schema version '{current_version}'.") from exc


def fetch_applied_migrations(connection: sqlite3.Connection) -> list[str]:
    if "schema_migrations" not in inspect_table_names(connection):
        return []
    return [
        row["version"]
        for row in connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version ASC"
        ).fetchall()
    ]


def get_current_schema_version(connection: sqlite3.Connection) -> str:
    applied = fetch_applied_migrations(connection)
    return applied[-1] if applied else "base"


def inspect_table_names(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
        """
    ).fetchall()
    return {row["name"] for row in rows}


def run_up_migration(connection: sqlite3.Connection, migration: MigrationDefinition) -> None:
    applied_at = utc_now()
    script = f"""
BEGIN IMMEDIATE;
{migration.up_sql}
INSERT INTO schema_migrations (version, description, applied_at)
VALUES ('{sql_literal(migration.version)}', '{sql_literal(migration.description)}', '{sql_literal(applied_at)}');
COMMIT;
"""
    try:
        connection.executescript(script)
    except sqlite3.DatabaseError as exc:
        connection.rollback()
        raise MigrationError(f"Failed to apply migration {migration.version}: {exc}") from exc


def run_down_migration(connection: sqlite3.Connection, migration: MigrationDefinition) -> None:
    script = f"""
BEGIN IMMEDIATE;
{migration.down_sql}
DELETE FROM schema_migrations
WHERE version = '{sql_literal(migration.version)}';
COMMIT;
"""
    try:
        connection.executescript(script)
    except sqlite3.DatabaseError as exc:
        connection.rollback()
        raise MigrationError(f"Failed to roll back migration {migration.version}: {exc}") from exc


def sql_literal(value: str) -> str:
    return value.replace("'", "''")


def check_database(database_path: Path) -> dict[str, str | bool | int]:
    if not database_path.exists():
        return {
            "ok": False,
            "message": "Database file is missing.",
            "schema_version": "missing",
            "applied_migrations": 0,
        }
    try:
        connection = connect_database(database_path)
        ensure_bootstrap_tables(connection)
        integrity = connection.execute("PRAGMA integrity_check;").fetchone()
        schema_version = get_current_schema_version(connection)
        applied_migrations = len(fetch_applied_migrations(connection))
        connection.close()
    except sqlite3.DatabaseError as exc:
        return {
            "ok": False,
            "message": str(exc),
            "schema_version": "unknown",
            "applied_migrations": 0,
        }

    return {
        "ok": bool(integrity and integrity[0] == "ok"),
        "message": integrity[0] if integrity else "unknown",
        "schema_version": schema_version,
        "applied_migrations": applied_migrations,
    }
