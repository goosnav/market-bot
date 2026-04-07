"""Repositories for import jobs, lists, saved filters, tags, and warehouse summaries."""

from __future__ import annotations

from backend.app.core.logging import utc_now
from backend.app.domain.enums import ImportJobStatus, ImportRowResolution
from backend.app.domain.models import ImportJobCreate, SavedFilterCreate
from backend.app.repositories.base import SqliteRepository


class ListRepository(SqliteRepository):
    """Persist audience and import lists."""

    def get_or_create(self, name: str, *, kind: str = "static", description: str = "", created_at: str | None = None) -> int:
        existing = self.fetch_one("SELECT id FROM lists WHERE name = ?", (name,))
        if existing:
            return int(existing["id"])
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO lists (name, kind, description, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (name, kind, description, timestamp, timestamp),
        )
        return int(cursor.lastrowid)

    def add_membership(self, list_id: int, lead_id: int, added_at: str | None = None) -> None:
        timestamp = added_at or utc_now()
        self.execute(
            """
            INSERT INTO list_memberships (list_id, lead_id, added_at)
            VALUES (?, ?, ?)
            ON CONFLICT(list_id, lead_id) DO NOTHING
            """,
            (list_id, lead_id, timestamp),
        )

    def list_all(self) -> list[dict[str, object]]:
        return self.fetch_all(
            """
            SELECT lists.*, COUNT(list_memberships.id) AS member_count
            FROM lists
            LEFT JOIN list_memberships ON list_memberships.list_id = lists.id
            GROUP BY lists.id
            ORDER BY lists.updated_at DESC, lists.id DESC
            """
        )

    def count_all(self) -> int:
        row = self.execute("SELECT COUNT(*) AS total FROM lists").fetchone()
        return int(row["total"])


class ImportJobRepository(SqliteRepository):
    """Persist import jobs and row-level outcomes."""

    def create(self, payload: ImportJobCreate, created_at: str | None = None) -> int:
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO import_jobs (
                source,
                import_format,
                requested_by,
                list_id,
                status,
                summary_json,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.source,
                payload.import_format,
                payload.requested_by,
                payload.list_id,
                payload.status.value,
                payload.summary_json,
                timestamp,
                timestamp,
            ),
        )
        return int(cursor.lastrowid)

    def update_job(
        self,
        import_job_id: int,
        *,
        status: ImportJobStatus,
        total_read: int,
        inserted_count: int,
        merged_count: int,
        skipped_count: int,
        conflicting_count: int,
        manual_review_required_count: int,
        summary_json: str,
        error_message: str = "",
        completed_at: str | None = None,
        updated_at: str | None = None,
    ) -> None:
        timestamp = updated_at or utc_now()
        self.execute(
            """
            UPDATE import_jobs
            SET status = ?,
                total_read = ?,
                inserted_count = ?,
                merged_count = ?,
                skipped_count = ?,
                conflicting_count = ?,
                manual_review_required_count = ?,
                summary_json = ?,
                error_message = ?,
                updated_at = ?,
                completed_at = ?
            WHERE id = ?
            """,
            (
                status.value,
                total_read,
                inserted_count,
                merged_count,
                skipped_count,
                conflicting_count,
                manual_review_required_count,
                summary_json,
                error_message,
                timestamp,
                completed_at,
                import_job_id,
            ),
        )

    def record_row(
        self,
        import_job_id: int,
        *,
        row_number: int,
        resolution: ImportRowResolution,
        dedupe_rule: str,
        raw_payload_json: str,
        normalized_payload_json: str,
        message: str,
        lead_id: int | None = None,
        company_id: int | None = None,
        existing_lead_id: int | None = None,
        created_at: str | None = None,
    ) -> int:
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO import_job_rows (
                import_job_id,
                row_number,
                resolution,
                dedupe_rule,
                lead_id,
                company_id,
                existing_lead_id,
                raw_payload_json,
                normalized_payload_json,
                message,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                import_job_id,
                row_number,
                resolution.value,
                dedupe_rule,
                lead_id,
                company_id,
                existing_lead_id,
                raw_payload_json,
                normalized_payload_json,
                message,
                timestamp,
            ),
        )
        return int(cursor.lastrowid)

    def list_recent(self, limit: int = 20) -> list[dict[str, object]]:
        return self.fetch_all(
            """
            SELECT import_jobs.*, lists.name AS list_name
            FROM import_jobs
            LEFT JOIN lists ON lists.id = import_jobs.list_id
            ORDER BY import_jobs.created_at DESC, import_jobs.id DESC
            LIMIT ?
            """,
            (limit,),
        )


class SavedFilterRepository(SqliteRepository):
    """Persist saved lead filters."""

    def create(self, payload: SavedFilterCreate, created_at: str | None = None) -> int:
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO saved_filters (
                name,
                entity_type,
                description,
                filter_json,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                payload.name,
                payload.entity_type,
                payload.description,
                payload.filter_json,
                timestamp,
                timestamp,
            ),
        )
        return int(cursor.lastrowid)

    def list_all(self) -> list[dict[str, object]]:
        return self.fetch_all(
            "SELECT * FROM saved_filters ORDER BY updated_at DESC, id DESC"
        )

    def get(self, saved_filter_id: int) -> dict[str, object] | None:
        return self.fetch_one("SELECT * FROM saved_filters WHERE id = ?", (saved_filter_id,))


class TagRepository(SqliteRepository):
    """Persist tags and entity tag mappings."""

    def get_or_create(self, name: str, color: str = "", created_at: str | None = None) -> int:
        existing = self.fetch_one("SELECT id FROM tags WHERE name = ? COLLATE NOCASE", (name,))
        if existing:
            return int(existing["id"])
        timestamp = created_at or utc_now()
        cursor = self.execute(
            """
            INSERT INTO tags (name, color, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (name, color, timestamp, timestamp),
        )
        return int(cursor.lastrowid)

    def assign(self, tag_id: int, entity_type: str, entity_id: int, created_at: str | None = None) -> None:
        timestamp = created_at or utc_now()
        self.execute(
            """
            INSERT INTO entity_tags (tag_id, entity_type, entity_id, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(tag_id, entity_type, entity_id) DO NOTHING
            """,
            (tag_id, entity_type, entity_id, timestamp),
        )

    def list_for_entity(self, entity_type: str, entity_id: int) -> list[dict[str, object]]:
        return self.fetch_all(
            """
            SELECT tags.*
            FROM entity_tags
            INNER JOIN tags ON tags.id = entity_tags.tag_id
            WHERE entity_tags.entity_type = ? AND entity_tags.entity_id = ?
            ORDER BY tags.name ASC
            """,
            (entity_type, entity_id),
        )
