"""Bootstrap-level SQLite helpers for Sprint 1."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import sqlite3


class DatabaseBootstrapError(RuntimeError):
    """Raised when the local runtime database cannot be initialized."""


def bootstrap_database(database_path: Path, app_version: str) -> dict[str, str]:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(tz=UTC).isoformat()
    try:
        connection = sqlite3.connect(database_path)
        connection.execute("PRAGMA journal_mode=WAL;")
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
            INSERT INTO app_runtime_metadata (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            ("app_version", app_version, now),
        )
        integrity = connection.execute("PRAGMA integrity_check;").fetchone()
        if not integrity or integrity[0] != "ok":
            raise DatabaseBootstrapError(f"Database integrity check failed: {integrity!r}")
        connection.commit()
        connection.close()
    except sqlite3.DatabaseError as exc:
        raise DatabaseBootstrapError(f"Unable to initialize database at {database_path}: {exc}") from exc

    return {
        "database_path": str(database_path),
        "journal_mode": "wal",
        "integrity_check": "ok",
        "initialized_at": now,
    }


def check_database(database_path: Path) -> dict[str, str | bool]:
    if not database_path.exists():
        return {"ok": False, "message": "Database file is missing."}
    try:
        connection = sqlite3.connect(database_path)
        integrity = connection.execute("PRAGMA integrity_check;").fetchone()
        connection.close()
    except sqlite3.DatabaseError as exc:
        return {"ok": False, "message": str(exc)}

    return {"ok": bool(integrity and integrity[0] == "ok"), "message": integrity[0] if integrity else "unknown"}

