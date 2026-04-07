"""Repositories for queued-message execution, recovery, and dead-letter visibility."""

from __future__ import annotations

import sqlite3

from backend.app.core.logging import utc_now
from backend.app.domain.enums import CampaignStatus, QueuedMessageState
from backend.app.domain.models import DeadLetterJobCreate, SentMessageCreate
from backend.app.repositories.base import SqliteRepository

DISPATCHABLE_CAMPAIGN_STATUSES = (
    CampaignStatus.SCHEDULED.value,
    CampaignStatus.LAUNCHING.value,
    CampaignStatus.ACTIVE.value,
)

CLAIMED_MESSAGE_SELECT = """
SELECT
    queued_messages.*,
    campaigns.name AS campaign_name,
    campaigns.status AS campaign_status,
    campaigns.reply_mode AS campaign_reply_mode,
    campaigns.send_window_config_json,
    campaigns.quiet_hours_config_json,
    leads.full_name AS lead_name,
    leads.email AS lead_email,
    leads.company_name_snapshot,
    leads.company_domain_snapshot,
    leads.suppression_status AS lead_suppression_status,
    companies.name AS company_name,
    companies.domain AS company_domain,
    provider_accounts.provider_name,
    provider_accounts.display_name AS provider_account_name,
    provider_accounts.email_address AS provider_account_email,
    provider_accounts.daily_cap AS provider_account_daily_cap,
    provider_accounts.status AS provider_account_status
FROM queued_messages
INNER JOIN campaigns ON campaigns.id = queued_messages.campaign_id
INNER JOIN leads ON leads.id = queued_messages.lead_id
LEFT JOIN companies ON companies.id = queued_messages.company_id
INNER JOIN provider_accounts ON provider_accounts.id = queued_messages.provider_account_id
"""


class ExecutionRepository(SqliteRepository):
    """Persist worker claims, retries, sent messages, and dead-letter jobs."""

    def stage_approved_campaign_rows(self, staged_at: str) -> int:
        self.execute(
            f"""
            UPDATE queued_messages
            SET state = ?,
                updated_at = ?
            WHERE id IN (
                SELECT queued_messages.id
                FROM queued_messages
                INNER JOIN campaigns ON campaigns.id = queued_messages.campaign_id
                WHERE queued_messages.state = ?
                  AND queued_messages.dead_lettered_at IS NULL
                  AND campaigns.status IN ({", ".join("?" for _ in DISPATCHABLE_CAMPAIGN_STATUSES)})
            )
            """,
            (
                QueuedMessageState.SCHEDULED.value,
                staged_at,
                QueuedMessageState.APPROVED.value,
                *DISPATCHABLE_CAMPAIGN_STATUSES,
            ),
        )
        return self._changes()

    def stage_retryable_failed_rows(self, staged_at: str) -> int:
        self.execute(
            f"""
            UPDATE queued_messages
            SET state = ?,
                claimed_at = NULL,
                claimed_by = '',
                claim_token = NULL,
                claim_expires_at = NULL,
                updated_at = ?
            WHERE id IN (
                SELECT queued_messages.id
                FROM queued_messages
                INNER JOIN campaigns ON campaigns.id = queued_messages.campaign_id
                WHERE queued_messages.state = ?
                  AND queued_messages.dead_lettered_at IS NULL
                  AND queued_messages.next_attempt_at IS NOT NULL
                  AND queued_messages.next_attempt_at <= ?
                  AND campaigns.status IN ({", ".join("?" for _ in DISPATCHABLE_CAMPAIGN_STATUSES)})
            )
            """,
            (
                QueuedMessageState.SCHEDULED.value,
                staged_at,
                QueuedMessageState.FAILED.value,
                staged_at,
                *DISPATCHABLE_CAMPAIGN_STATUSES,
            ),
        )
        return self._changes()

    def list_expired_claims(self, now: str) -> list[dict[str, object]]:
        return self.fetch_all(
            """
            SELECT id, campaign_id, provider_account_id, claim_token
            FROM queued_messages
            WHERE state = ?
              AND claim_expires_at IS NOT NULL
              AND claim_expires_at <= ?
            ORDER BY claim_expires_at ASC, id ASC
            """,
            (QueuedMessageState.DISPATCHED.value, now),
        )

    def release_expired_claims(self, released_at: str) -> int:
        self.execute(
            """
            UPDATE queued_messages
            SET state = ?,
                claimed_at = NULL,
                claimed_by = '',
                claim_token = NULL,
                claim_expires_at = NULL,
                updated_at = ?
            WHERE state = ?
              AND claim_expires_at IS NOT NULL
              AND claim_expires_at <= ?
            """,
            (
                QueuedMessageState.SCHEDULED.value,
                released_at,
                QueuedMessageState.DISPATCHED.value,
                released_at,
            ),
        )
        return self._changes()

    def claim_due_messages(
        self,
        *,
        now: str,
        worker_id: str,
        claim_token: str,
        claim_expires_at: str,
        batch_size: int,
    ) -> list[dict[str, object]]:
        self.execute(
            f"""
            UPDATE queued_messages
            SET state = ?,
                claimed_at = ?,
                claimed_by = ?,
                claim_token = ?,
                claim_expires_at = ?,
                updated_at = ?
            WHERE id IN (
                SELECT queued_messages.id
                FROM queued_messages
                INNER JOIN campaigns ON campaigns.id = queued_messages.campaign_id
                WHERE queued_messages.state = ?
                  AND queued_messages.dead_lettered_at IS NULL
                  AND queued_messages.scheduled_for <= ?
                  AND (
                        queued_messages.claim_expires_at IS NULL
                        OR queued_messages.claim_expires_at <= ?
                        OR queued_messages.claimed_by = ''
                      )
                  AND campaigns.status IN ({", ".join("?" for _ in DISPATCHABLE_CAMPAIGN_STATUSES)})
                ORDER BY queued_messages.scheduled_for ASC, queued_messages.id ASC
                LIMIT ?
            )
            """,
            (
                QueuedMessageState.DISPATCHED.value,
                now,
                worker_id,
                claim_token,
                claim_expires_at,
                now,
                QueuedMessageState.SCHEDULED.value,
                now,
                now,
                *DISPATCHABLE_CAMPAIGN_STATUSES,
                batch_size,
            ),
        )
        return self.list_claimed_messages(worker_id, claim_token)

    def list_claimed_messages(self, worker_id: str, claim_token: str) -> list[dict[str, object]]:
        return self.fetch_all(
            f"""
            {CLAIMED_MESSAGE_SELECT}
            WHERE queued_messages.state = ?
              AND queued_messages.claimed_by = ?
              AND queued_messages.claim_token = ?
            ORDER BY queued_messages.scheduled_for ASC, queued_messages.id ASC
            """,
            (QueuedMessageState.DISPATCHED.value, worker_id, claim_token),
        )

    def get_claimed_message(self, queued_message_id: int, worker_id: str, claim_token: str) -> dict[str, object] | None:
        return self.fetch_one(
            f"""
            {CLAIMED_MESSAGE_SELECT}
            WHERE queued_messages.id = ?
              AND queued_messages.state = ?
              AND queued_messages.claimed_by = ?
              AND queued_messages.claim_token = ?
            """,
            (queued_message_id, QueuedMessageState.DISPATCHED.value, worker_id, claim_token),
        )

    def find_active_suppression(
        self,
        *,
        lead_id: int,
        company_id: int | None,
        email: str | None,
        domain: str | None,
    ) -> dict[str, object] | None:
        if lead_id:
            row = self.fetch_one(
                """
                SELECT *
                FROM suppression_entries
                WHERE active = 1 AND scope = 'lead' AND lead_id = ?
                """,
                (lead_id,),
            )
            if row is not None:
                return row
        if email:
            row = self.fetch_one(
                """
                SELECT *
                FROM suppression_entries
                WHERE active = 1 AND scope = 'email' AND email = ? COLLATE NOCASE
                """,
                (email,),
            )
            if row is not None:
                return row
        if domain:
            row = self.fetch_one(
                """
                SELECT *
                FROM suppression_entries
                WHERE active = 1 AND scope = 'domain' AND domain = ? COLLATE NOCASE
                """,
                (domain,),
            )
            if row is not None:
                return row
        if company_id:
            return self.fetch_one(
                """
                SELECT *
                FROM suppression_entries
                WHERE active = 1 AND scope = 'company' AND company_id = ?
                """,
                (company_id,),
            )
        return None

    def has_reply_for_campaign_lead(self, campaign_id: int, lead_id: int) -> bool:
        row = self.execute(
            """
            SELECT COUNT(*) AS total
            FROM replies
            WHERE campaign_id = ? AND lead_id = ?
            """,
            (campaign_id, lead_id),
        ).fetchone()
        return int(row["total"]) > 0

    def count_sent_for_provider_window(self, provider_account_id: int, start_at: str, end_at: str) -> int:
        row = self.execute(
            """
            SELECT COUNT(*) AS total
            FROM sent_messages
            WHERE provider_account_id = ?
              AND sent_at >= ?
              AND sent_at < ?
            """,
            (provider_account_id, start_at, end_at),
        ).fetchone()
        return int(row["total"])

    def get_provider_failure_window(self, provider_account_id: int, since: str) -> dict[str, object]:
        row = self.execute(
            """
            SELECT
                COUNT(*) AS failure_count,
                MAX(last_attempt_at) AS latest_failure_at
            FROM queued_messages
            WHERE provider_account_id = ?
              AND state = ?
              AND last_attempt_at IS NOT NULL
              AND last_attempt_at >= ?
              AND last_error_code NOT IN ('', 'circuit_open')
            """,
            (provider_account_id, QueuedMessageState.FAILED.value, since),
        ).fetchone()
        return {
            "failure_count": int(row["failure_count"] or 0),
            "latest_failure_at": row["latest_failure_at"],
        }

    def get_sent_message(self, queued_message_id: int) -> dict[str, object] | None:
        return self.fetch_one("SELECT * FROM sent_messages WHERE queued_message_id = ?", (queued_message_id,))

    def get_or_create_thread(
        self,
        *,
        lead_id: int,
        campaign_id: int,
        provider_name: str,
        created_at: str | None = None,
    ) -> int:
        existing = self.fetch_one(
            """
            SELECT id
            FROM threads
            WHERE lead_id = ? AND campaign_id = ?
            """,
            (lead_id, campaign_id),
        )
        if existing is not None:
            return int(existing["id"])

        timestamp = created_at or utc_now()
        try:
            cursor = self.execute(
                """
                INSERT INTO threads (
                    lead_id,
                    campaign_id,
                    provider_name,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (lead_id, campaign_id, provider_name, timestamp, timestamp),
            )
            return int(cursor.lastrowid)
        except sqlite3.IntegrityError:
            existing = self.fetch_one(
                """
                SELECT id
                FROM threads
                WHERE lead_id = ? AND campaign_id = ?
                """,
                (lead_id, campaign_id),
            )
            if existing is None:
                raise
            return int(existing["id"])

    def create_sent_message(self, payload: SentMessageCreate, created_at: str | None = None) -> int:
        timestamp = created_at or payload.sent_at
        cursor = self.execute(
            """
            INSERT INTO sent_messages (
                queued_message_id,
                thread_id,
                provider_account_id,
                provider_name,
                provider_message_id,
                external_campaign_id,
                sent_at,
                delivery_state,
                subject,
                body,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.queued_message_id,
                payload.thread_id,
                payload.provider_account_id,
                payload.provider_name,
                payload.provider_message_id,
                payload.external_campaign_id,
                payload.sent_at,
                payload.delivery_state,
                payload.subject,
                payload.body,
                timestamp,
            ),
        )
        return int(cursor.lastrowid)

    def record_dead_letter(self, payload: DeadLetterJobCreate) -> int:
        cursor = self.execute(
            """
            INSERT INTO dead_letter_jobs (
                queued_message_id,
                campaign_id,
                provider_account_id,
                job_kind,
                reason_code,
                reason_detail,
                payload_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(queued_message_id) DO UPDATE SET
                campaign_id = excluded.campaign_id,
                provider_account_id = excluded.provider_account_id,
                job_kind = excluded.job_kind,
                reason_code = excluded.reason_code,
                reason_detail = excluded.reason_detail,
                payload_json = excluded.payload_json,
                resolved_at = NULL,
                created_at = excluded.created_at
            """,
            (
                payload.queued_message_id,
                payload.campaign_id,
                payload.provider_account_id,
                payload.job_kind,
                payload.reason_code,
                payload.reason_detail,
                payload.payload_json,
                payload.created_at,
            ),
        )
        return int(cursor.lastrowid)

    def list_recent_dead_letters(self, limit: int = 20) -> list[dict[str, object]]:
        return self.fetch_all(
            """
            SELECT *
            FROM dead_letter_jobs
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )

    def mark_sent(
        self,
        queued_message_id: int,
        *,
        sent_at: str,
        attempt_count: int,
    ) -> None:
        self.execute(
            """
            UPDATE queued_messages
            SET state = ?,
                attempt_count = ?,
                last_attempt_at = ?,
                next_attempt_at = NULL,
                claimed_at = NULL,
                claimed_by = '',
                claim_token = NULL,
                claim_expires_at = NULL,
                last_error_code = '',
                last_error_detail = '',
                failure_reason = '',
                updated_at = ?
            WHERE id = ?
            """,
            (
                QueuedMessageState.SENT.value,
                attempt_count,
                sent_at,
                sent_at,
                queued_message_id,
            ),
        )

    def mark_failed_for_retry(
        self,
        queued_message_id: int,
        *,
        failed_at: str,
        attempt_count: int,
        next_attempt_at: str,
        error_code: str,
        error_detail: str,
    ) -> None:
        self.execute(
            """
            UPDATE queued_messages
            SET state = ?,
                attempt_count = ?,
                last_attempt_at = ?,
                next_attempt_at = ?,
                claimed_at = NULL,
                claimed_by = '',
                claim_token = NULL,
                claim_expires_at = NULL,
                last_error_code = ?,
                last_error_detail = ?,
                failure_reason = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                QueuedMessageState.FAILED.value,
                attempt_count,
                failed_at,
                next_attempt_at,
                error_code,
                error_detail,
                error_detail,
                failed_at,
                queued_message_id,
            ),
        )

    def mark_dead_letter(
        self,
        queued_message_id: int,
        *,
        failed_at: str,
        attempt_count: int,
        error_code: str,
        error_detail: str,
    ) -> None:
        self.execute(
            """
            UPDATE queued_messages
            SET state = ?,
                attempt_count = ?,
                last_attempt_at = ?,
                next_attempt_at = NULL,
                claimed_at = NULL,
                claimed_by = '',
                claim_token = NULL,
                claim_expires_at = NULL,
                last_error_code = ?,
                last_error_detail = ?,
                failure_reason = ?,
                dead_lettered_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                QueuedMessageState.FAILED.value,
                attempt_count,
                failed_at,
                error_code,
                error_detail,
                error_detail,
                failed_at,
                failed_at,
                queued_message_id,
            ),
        )

    def mark_suppressed(self, queued_message_id: int, *, suppressed_at: str, reason: str) -> None:
        self._mark_terminal(
            queued_message_id,
            state=QueuedMessageState.SUPPRESSED.value,
            updated_at=suppressed_at,
            reason_code="suppressed",
            reason_detail=reason,
        )

    def mark_blocked(self, queued_message_id: int, *, blocked_at: str, reason_code: str, reason_detail: str) -> None:
        self._mark_terminal(
            queued_message_id,
            state=QueuedMessageState.BLOCKED.value,
            updated_at=blocked_at,
            reason_code=reason_code,
            reason_detail=reason_detail,
        )

    def reschedule_message(
        self,
        queued_message_id: int,
        *,
        scheduled_for: str,
        updated_at: str,
        reason_code: str = "",
        reason_detail: str = "",
    ) -> None:
        self.execute(
            """
            UPDATE queued_messages
            SET state = ?,
                scheduled_for = ?,
                claimed_at = NULL,
                claimed_by = '',
                claim_token = NULL,
                claim_expires_at = NULL,
                last_error_code = ?,
                last_error_detail = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                QueuedMessageState.SCHEDULED.value,
                scheduled_for,
                reason_code,
                reason_detail,
                updated_at,
                queued_message_id,
            ),
        )

    def count_open_campaign_messages(self, campaign_id: int) -> int:
        row = self.execute(
            """
            SELECT COUNT(*) AS total
            FROM queued_messages
            WHERE campaign_id = ?
              AND (
                    state IN ('approved', 'scheduled', 'dispatched')
                    OR (state = 'failed' AND dead_lettered_at IS NULL)
                  )
            """,
            (campaign_id,),
        ).fetchone()
        return int(row["total"])

    def _mark_terminal(
        self,
        queued_message_id: int,
        *,
        state: str,
        updated_at: str,
        reason_code: str,
        reason_detail: str,
    ) -> None:
        self.execute(
            """
            UPDATE queued_messages
            SET state = ?,
                next_attempt_at = NULL,
                claimed_at = NULL,
                claimed_by = '',
                claim_token = NULL,
                claim_expires_at = NULL,
                last_error_code = ?,
                last_error_detail = ?,
                failure_reason = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                state,
                reason_code,
                reason_detail,
                reason_detail,
                updated_at,
                queued_message_id,
            ),
        )

    def _changes(self) -> int:
        row = self.connection.execute("SELECT changes() AS total").fetchone()
        return int(row["total"] or 0)
