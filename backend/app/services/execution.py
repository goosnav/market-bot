"""Restart-safe queued-message execution and retry services for Sprint 6."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
import sqlite3

from backend.app.core.logging import utc_now
from backend.app.domain.enums import CampaignStatus, EntityType
from backend.app.domain.models import AuditEventCreate, DeadLetterJobCreate, SentMessageCreate
from backend.app.repositories.audit import AuditEventRepository
from backend.app.repositories.execution import DISPATCHABLE_CAMPAIGN_STATUSES, ExecutionRepository
from backend.app.services.campaign_builder import (
    WEEKDAY_NAMES,
    align_to_window,
    normalize_quiet_hours,
    normalize_send_window,
    parse_json_object,
    resolve_zone,
    within_quiet_hours,
)
from backend.app.services.campaigns import CampaignService


class DispatchAttemptError(RuntimeError):
    """Raised when a provider dispatch attempt fails."""

    def __init__(self, code: str, detail: str, *, retryable: bool = True) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.retryable = retryable


@dataclass(frozen=True)
class ExecutionCycleSummary:
    recovered_claims: int = 0
    newly_scheduled: int = 0
    retry_released: int = 0
    claimed: int = 0
    sent: int = 0
    rescheduled: int = 0
    blocked: int = 0
    suppressed: int = 0
    retried: int = 0
    dead_lettered: int = 0
    already_sent: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "recovered_claims": self.recovered_claims,
            "newly_scheduled": self.newly_scheduled,
            "retry_released": self.retry_released,
            "claimed": self.claimed,
            "sent": self.sent,
            "rescheduled": self.rescheduled,
            "blocked": self.blocked,
            "suppressed": self.suppressed,
            "retried": self.retried,
            "dead_lettered": self.dead_lettered,
            "already_sent": self.already_sent,
        }


class ExecutionEngineService:
    """Own claim/retry/dispatch behavior for queued outbound work."""

    def __init__(self, connection) -> None:
        self.audit_events = AuditEventRepository(connection)
        self.campaign_service = CampaignService(connection)
        self.execution = ExecutionRepository(connection)

    def release_expired_claims(self, *, now: str, actor: str) -> int:
        expired = self.execution.list_expired_claims(now)
        count = self.execution.release_expired_claims(now)
        for row in expired:
            self._record_event(
                entity_type=EntityType.QUEUED_MESSAGE.value,
                entity_id=int(row["id"]),
                event_type="dispatch.claim_released",
                actor=actor,
                summary=f"Recovered expired claim for queued message {row['id']}.",
                payload={
                    "claim_token": text_value(row.get("claim_token")),
                    "recovered_to_state": "scheduled",
                },
                created_at=now,
            )
        return count

    def stage_dispatchable_messages(self, *, now: str, actor: str) -> dict[str, int]:
        newly_scheduled = self.execution.stage_approved_campaign_rows(now)
        retry_released = self.execution.stage_retryable_failed_rows(now)
        if newly_scheduled:
            self._record_event(
                entity_type=EntityType.CAMPAIGN.value,
                entity_id=None,
                event_type="dispatch.scheduler_promoted",
                actor=actor,
                summary=f"Scheduler promoted {newly_scheduled} approved queued messages.",
                payload={"count": newly_scheduled},
                created_at=now,
            )
        if retry_released:
            self._record_event(
                entity_type=EntityType.CAMPAIGN.value,
                entity_id=None,
                event_type="dispatch.retry_released",
                actor=actor,
                summary=f"Scheduler released {retry_released} retry-ready queued messages.",
                payload={"count": retry_released},
                created_at=now,
            )
        return {"newly_scheduled": newly_scheduled, "retry_released": retry_released}

    def claim_due_messages(
        self,
        *,
        now: str,
        worker_id: str,
        claim_token: str,
        claim_ttl_seconds: int,
        batch_size: int,
    ) -> list[dict[str, object]]:
        claim_expires_at = (ensure_utc_datetime(now) + timedelta(seconds=claim_ttl_seconds)).isoformat()
        claimed = self.execution.claim_due_messages(
            now=now,
            worker_id=worker_id,
            claim_token=claim_token,
            claim_expires_at=claim_expires_at,
            batch_size=batch_size,
        )
        for row in claimed:
            self._record_event(
                entity_type=EntityType.QUEUED_MESSAGE.value,
                entity_id=int(row["id"]),
                event_type="dispatch.claimed",
                actor=worker_id,
                summary=f"Worker claimed queued message {row['id']} for dispatch.",
                payload={"claim_token": claim_token, "claim_expires_at": claim_expires_at},
                created_at=now,
            )
        return claimed

    def process_claimed_message(
        self,
        *,
        queued_message_id: int,
        claim_token: str,
        worker_id: str,
        now: str,
        retry_backoff_seconds: int,
        circuit_breaker_threshold: int,
        circuit_breaker_cooldown_seconds: int,
    ) -> dict[str, object]:
        row = self.execution.get_claimed_message(queued_message_id, worker_id, claim_token)
        if row is None:
            return {"queued_message_id": queued_message_id, "outcome": "stale_claim"}

        now_dt = ensure_utc_datetime(now)
        send_window = normalize_send_window(parse_json_object(row.get("send_window_config_json")))
        quiet_hours = normalize_quiet_hours(parse_json_object(row.get("quiet_hours_config_json")))

        campaign_status = text_value(row.get("campaign_status"))
        if campaign_status not in DISPATCHABLE_CAMPAIGN_STATUSES:
            self.execution.reschedule_message(
                queued_message_id,
                scheduled_for=text_value(row.get("scheduled_for")),
                updated_at=now,
                reason_code="campaign_paused",
                reason_detail=f"Campaign is {campaign_status or 'unknown'} and cannot dispatch.",
            )
            self._record_dispatch_result(
                queued_message_id=queued_message_id,
                actor=worker_id,
                event_type="dispatch.released",
                summary=f"Queued message {queued_message_id} released because the campaign is not dispatchable.",
                payload={"campaign_status": campaign_status},
                created_at=now,
            )
            return {"queued_message_id": queued_message_id, "outcome": "released"}

        lead_email = nullable_text(row.get("lead_email"))
        company_domain = nullable_text(row.get("company_domain")) or nullable_text(row.get("company_domain_snapshot"))
        suppression = self.execution.find_active_suppression(
            lead_id=int(row["lead_id"]),
            company_id=int(row["company_id"]) if row.get("company_id") is not None else None,
            email=lead_email,
            domain=company_domain,
        )
        if suppression is not None:
            reason = f"{suppression['scope']} suppression from {suppression['source']}: {suppression['reason']}"
            self.execution.mark_suppressed(queued_message_id, suppressed_at=now, reason=reason)
            self._record_dispatch_result(
                queued_message_id=queued_message_id,
                actor=worker_id,
                event_type="dispatch.suppressed",
                summary=f"Queued message {queued_message_id} suppressed at dispatch time.",
                payload={"reason": reason},
                created_at=now,
            )
            self._complete_campaign_if_finished(int(row["campaign_id"]), worker_id)
            return {"queued_message_id": queued_message_id, "outcome": "suppressed"}

        if not lead_email:
            self.execution.mark_blocked(
                queued_message_id,
                blocked_at=now,
                reason_code="missing_email",
                reason_detail="Lead email is missing at dispatch time.",
            )
            self._record_dispatch_result(
                queued_message_id=queued_message_id,
                actor=worker_id,
                event_type="dispatch.blocked",
                summary=f"Queued message {queued_message_id} blocked because the lead email is missing.",
                payload={"reason_code": "missing_email"},
                created_at=now,
            )
            self._complete_campaign_if_finished(int(row["campaign_id"]), worker_id)
            return {"queued_message_id": queued_message_id, "outcome": "blocked"}

        if text_value(row.get("provider_account_status"), "active") != "active":
            self.execution.mark_blocked(
                queued_message_id,
                blocked_at=now,
                reason_code="provider_unavailable",
                reason_detail="Provider account is no longer active.",
            )
            self._record_dispatch_result(
                queued_message_id=queued_message_id,
                actor=worker_id,
                event_type="dispatch.blocked",
                summary=f"Queued message {queued_message_id} blocked because the provider account is unavailable.",
                payload={"reason_code": "provider_unavailable"},
                created_at=now,
            )
            self._complete_campaign_if_finished(int(row["campaign_id"]), worker_id)
            return {"queued_message_id": queued_message_id, "outcome": "blocked"}

        if should_stop_on_reply(text_value(row.get("campaign_reply_mode"), "manual")) and self.execution.has_reply_for_campaign_lead(
            int(row["campaign_id"]),
            int(row["lead_id"]),
        ):
            self.execution.mark_blocked(
                queued_message_id,
                blocked_at=now,
                reason_code="reply_received",
                reason_detail="A reply already exists for this lead and campaign.",
            )
            self._record_dispatch_result(
                queued_message_id=queued_message_id,
                actor=worker_id,
                event_type="dispatch.reply_blocked",
                summary=f"Queued message {queued_message_id} blocked because the lead already replied.",
                payload={"campaign_id": int(row["campaign_id"]), "lead_id": int(row["lead_id"])},
                created_at=now,
            )
            self._complete_campaign_if_finished(int(row["campaign_id"]), worker_id)
            return {"queued_message_id": queued_message_id, "outcome": "blocked"}

        if not dispatch_window_allows(now_dt, send_window, quiet_hours):
            rescheduled_for = next_allowed_dispatch_time(now_dt, send_window, quiet_hours)
            self.execution.reschedule_message(
                queued_message_id,
                scheduled_for=rescheduled_for,
                updated_at=now,
                reason_code="window_deferred",
                reason_detail="Dispatch moved to the next allowed send window.",
            )
            self._record_dispatch_result(
                queued_message_id=queued_message_id,
                actor=worker_id,
                event_type="dispatch.rescheduled",
                summary=f"Queued message {queued_message_id} deferred to the next allowed send window.",
                payload={"scheduled_for": rescheduled_for, "reason_code": "window_deferred"},
                created_at=now,
            )
            return {"queued_message_id": queued_message_id, "outcome": "rescheduled", "scheduled_for": rescheduled_for}

        daily_cap = int(row.get("provider_account_daily_cap") or 0)
        if daily_cap > 0:
            start_at, end_at = local_day_window(now_dt, text_value(send_window.get("timezone"), "UTC"))
            current_sent = self.execution.count_sent_for_provider_window(int(row["provider_account_id"]), start_at, end_at)
            if current_sent >= daily_cap:
                rescheduled_for = next_provider_day_open(now_dt, send_window, quiet_hours)
                self.execution.reschedule_message(
                    queued_message_id,
                    scheduled_for=rescheduled_for,
                    updated_at=now,
                    reason_code="daily_cap_reached",
                    reason_detail="Provider account daily cap is already exhausted.",
                )
                self._record_dispatch_result(
                    queued_message_id=queued_message_id,
                    actor=worker_id,
                    event_type="dispatch.rescheduled",
                    summary=f"Queued message {queued_message_id} deferred because the provider daily cap was reached.",
                    payload={"scheduled_for": rescheduled_for, "reason_code": "daily_cap_reached"},
                    created_at=now,
                )
                return {"queued_message_id": queued_message_id, "outcome": "rescheduled", "scheduled_for": rescheduled_for}

        circuit_window_start = (now_dt - timedelta(seconds=circuit_breaker_cooldown_seconds)).isoformat()
        failure_window = self.execution.get_provider_failure_window(int(row["provider_account_id"]), circuit_window_start)
        latest_failure_at = nullable_text(failure_window.get("latest_failure_at"))
        if failure_window["failure_count"] >= circuit_breaker_threshold and latest_failure_at:
            resume_at = ensure_utc_datetime(latest_failure_at) + timedelta(seconds=circuit_breaker_cooldown_seconds)
            if resume_at > now_dt:
                rescheduled_for = next_allowed_dispatch_time(resume_at, send_window, quiet_hours)
                self.execution.reschedule_message(
                    queued_message_id,
                    scheduled_for=rescheduled_for,
                    updated_at=now,
                    reason_code="circuit_open",
                    reason_detail=f"Provider circuit is open until {resume_at.isoformat()}.",
                )
                self._record_dispatch_result(
                    queued_message_id=queued_message_id,
                    actor=worker_id,
                    event_type="dispatch.circuit_open",
                    summary=f"Queued message {queued_message_id} deferred because the provider circuit is open.",
                    payload={"scheduled_for": rescheduled_for, "failure_count": failure_window["failure_count"]},
                    created_at=now,
                )
                return {"queued_message_id": queued_message_id, "outcome": "rescheduled", "scheduled_for": rescheduled_for}

        existing_sent = self.execution.get_sent_message(queued_message_id)
        if existing_sent is not None:
            self.execution.mark_sent(
                queued_message_id,
                sent_at=text_value(existing_sent.get("sent_at"), now),
                attempt_count=max(int(row.get("attempt_count") or 0), 1),
            )
            self._activate_campaign_if_needed(int(row["campaign_id"]), worker_id)
            self._complete_campaign_if_finished(int(row["campaign_id"]), worker_id)
            self._record_dispatch_result(
                queued_message_id=queued_message_id,
                actor=worker_id,
                event_type="dispatch.idempotent_skip",
                summary=f"Queued message {queued_message_id} already has a sent-message record and was not resent.",
                payload={"sent_message_id": int(existing_sent["id"])},
                created_at=now,
            )
            return {"queued_message_id": queued_message_id, "outcome": "already_sent"}

        attempt_count = int(row.get("attempt_count") or 0) + 1
        provider_name = text_value(row.get("provider_name"), "manual")
        try:
            dispatch_result = dispatch_provider_message(row, attempt_count=attempt_count, sent_at=now)
        except DispatchAttemptError as exc:
            max_attempts = max(int(row.get("max_attempts") or 3), 1)
            if exc.retryable and attempt_count < max_attempts:
                retry_at = next_allowed_dispatch_time(
                    now_dt + timedelta(seconds=retry_backoff_seconds * attempt_count),
                    send_window,
                    quiet_hours,
                )
                self.execution.mark_failed_for_retry(
                    queued_message_id,
                    failed_at=now,
                    attempt_count=attempt_count,
                    next_attempt_at=retry_at,
                    error_code=exc.code,
                    error_detail=exc.detail,
                )
                self._record_dispatch_result(
                    queued_message_id=queued_message_id,
                    actor=worker_id,
                    event_type="dispatch.retry_scheduled",
                    summary=f"Queued message {queued_message_id} failed and was scheduled for retry.",
                    payload={"reason_code": exc.code, "next_attempt_at": retry_at, "attempt_count": attempt_count},
                    created_at=now,
                )
                return {"queued_message_id": queued_message_id, "outcome": "retry_scheduled", "next_attempt_at": retry_at}

            self.execution.mark_dead_letter(
                queued_message_id,
                failed_at=now,
                attempt_count=attempt_count,
                error_code=exc.code,
                error_detail=exc.detail,
            )
            self.execution.record_dead_letter(
                DeadLetterJobCreate(
                    queued_message_id=queued_message_id,
                    campaign_id=int(row["campaign_id"]),
                    provider_account_id=int(row["provider_account_id"]),
                    reason_code=exc.code,
                    reason_detail=exc.detail,
                    payload_json=json.dumps(
                        {
                            "queued_message_id": queued_message_id,
                            "campaign_id": int(row["campaign_id"]),
                            "provider_name": provider_name,
                            "attempt_count": attempt_count,
                        },
                        sort_keys=True,
                    ),
                    created_at=now,
                )
            )
            self._record_dispatch_result(
                queued_message_id=queued_message_id,
                actor=worker_id,
                event_type="dispatch.dead_lettered",
                summary=f"Queued message {queued_message_id} exhausted retries and was dead-lettered.",
                payload={"reason_code": exc.code, "attempt_count": attempt_count},
                created_at=now,
            )
            self._complete_campaign_if_finished(int(row["campaign_id"]), worker_id)
            return {"queued_message_id": queued_message_id, "outcome": "dead_lettered"}

        thread_id = self.execution.get_or_create_thread(
            lead_id=int(row["lead_id"]),
            campaign_id=int(row["campaign_id"]),
            provider_name=provider_name,
            created_at=now,
        )
        try:
            self.execution.create_sent_message(
                SentMessageCreate(
                    queued_message_id=queued_message_id,
                    thread_id=thread_id,
                    provider_account_id=int(row["provider_account_id"]),
                    provider_name=provider_name,
                    provider_message_id=text_value(dispatch_result.get("provider_message_id")),
                    external_campaign_id=nullable_text(dispatch_result.get("external_campaign_id")),
                    sent_at=now,
                    subject=text_value(row.get("rendered_subject")),
                    body=text_value(row.get("rendered_body")),
                    delivery_state=text_value(dispatch_result.get("delivery_state"), "sent"),
                )
            )
        except sqlite3.IntegrityError:
            existing_sent = self.execution.get_sent_message(queued_message_id)
            if existing_sent is None:
                raise

        self.execution.mark_sent(queued_message_id, sent_at=now, attempt_count=attempt_count)
        self._activate_campaign_if_needed(int(row["campaign_id"]), worker_id)
        self._complete_campaign_if_finished(int(row["campaign_id"]), worker_id)
        self._record_dispatch_result(
            queued_message_id=queued_message_id,
            actor=worker_id,
            event_type="dispatch.sent",
            summary=f"Queued message {queued_message_id} sent successfully.",
            payload={
                "provider_name": provider_name,
                "provider_message_id": text_value(dispatch_result.get("provider_message_id")),
                "attempt_count": attempt_count,
            },
            created_at=now,
        )
        return {"queued_message_id": queued_message_id, "outcome": "sent"}

    def get_execution_summary(self) -> dict[str, object]:
        state_rows = self.execution.fetch_all(
            """
            SELECT state, COUNT(*) AS total
            FROM queued_messages
            GROUP BY state
            ORDER BY state ASC
            """
        )
        state_counts = {row["state"]: int(row["total"]) for row in state_rows}
        sent_count = int(
            self.execution.execute("SELECT COUNT(*) AS total FROM sent_messages").fetchone()["total"]
        )
        dead_letter_count = int(
            self.execution.execute("SELECT COUNT(*) AS total FROM dead_letter_jobs").fetchone()["total"]
        )
        return {
            "queue_state_counts": state_counts,
            "sent_count": sent_count,
            "dead_letter_count": dead_letter_count,
            "recent_dead_letters": self.execution.list_recent_dead_letters(limit=5),
        }

    def _activate_campaign_if_needed(self, campaign_id: int, actor: str) -> None:
        campaign = self.campaign_service.campaigns.get(campaign_id)
        if campaign is None:
            return
        status = CampaignStatus(campaign["status"])
        if status == CampaignStatus.SCHEDULED:
            self.campaign_service.transition_status(
                campaign_id,
                CampaignStatus.LAUNCHING,
                actor=actor,
                reason="Worker started outbound execution.",
            )
            self.campaign_service.transition_status(
                campaign_id,
                CampaignStatus.ACTIVE,
                actor=actor,
                reason="First queued message was dispatched successfully.",
            )
            return
        if status == CampaignStatus.LAUNCHING:
            self.campaign_service.transition_status(
                campaign_id,
                CampaignStatus.ACTIVE,
                actor=actor,
                reason="Queued execution is active.",
            )

    def _complete_campaign_if_finished(self, campaign_id: int, actor: str) -> None:
        if self.execution.count_open_campaign_messages(campaign_id) > 0:
            return
        campaign = self.campaign_service.campaigns.get(campaign_id)
        if campaign is None:
            return
        status = CampaignStatus(campaign["status"])
        if status == CampaignStatus.LAUNCHING:
            self.campaign_service.transition_status(
                campaign_id,
                CampaignStatus.ACTIVE,
                actor=actor,
                reason="Execution reached terminal queue states.",
            )
            status = CampaignStatus.ACTIVE
        if status == CampaignStatus.ACTIVE:
            self.campaign_service.transition_status(
                campaign_id,
                CampaignStatus.COMPLETED,
                actor=actor,
                reason="All queued rows reached terminal states.",
            )

    def _record_dispatch_result(
        self,
        *,
        queued_message_id: int,
        actor: str,
        event_type: str,
        summary: str,
        payload: dict[str, object],
        created_at: str,
    ) -> None:
        self._record_event(
            entity_type=EntityType.QUEUED_MESSAGE.value,
            entity_id=queued_message_id,
            event_type=event_type,
            actor=actor,
            summary=summary,
            payload=payload,
            created_at=created_at,
        )

    def _record_event(
        self,
        *,
        entity_type: str,
        entity_id: int | None,
        event_type: str,
        actor: str,
        summary: str,
        payload: dict[str, object],
        created_at: str,
    ) -> None:
        self.audit_events.record(
            AuditEventCreate(
                entity_type=entity_type,
                entity_id=entity_id,
                event_type=event_type,
                actor=actor,
                summary=summary,
                payload_json=json.dumps(payload, sort_keys=True),
            ),
            created_at=created_at,
        )


def dispatch_provider_message(row: dict[str, object], *, attempt_count: int, sent_at: str) -> dict[str, str]:
    provider_name = text_value(row.get("provider_name"), "manual")
    if provider_name == "mock_fail_once" and attempt_count == 1:
        raise DispatchAttemptError("provider_transient", "Mock provider failed the first attempt.", retryable=True)
    if provider_name == "mock_fail_always":
        raise DispatchAttemptError("provider_transient", "Mock provider failed every attempt.", retryable=True)
    if provider_name == "mock_permanent_fail":
        raise DispatchAttemptError("provider_permanent", "Mock provider rejected the message permanently.", retryable=False)
    return {
        "provider_message_id": f"{provider_name}-{row['id']}-{attempt_count}",
        "external_campaign_id": f"campaign-{row['campaign_id']}",
        "delivery_state": "sent",
        "sent_at": sent_at,
    }


def dispatch_window_allows(now: datetime, send_window: dict[str, object], quiet_hours: dict[str, object]) -> bool:
    zone = resolve_zone(text_value(send_window.get("timezone"), "UTC"))
    local = now.astimezone(zone)
    weekday = WEEKDAY_NAMES[local.weekday()]
    allowed_days = {str(day).lower()[:3] for day in send_window.get("allowed_weekdays", WEEKDAY_NAMES[:5])}
    if weekday not in allowed_days:
        return False
    if within_quiet_hours(local, quiet_hours):
        return False
    start_hour = int(send_window["start_hour"])
    end_hour = int(send_window["end_hour"])
    return start_hour <= local.hour < end_hour


def next_allowed_dispatch_time(reference: datetime, send_window: dict[str, object], quiet_hours: dict[str, object]) -> str:
    zone = resolve_zone(text_value(send_window.get("timezone"), "UTC"))
    local = reference.astimezone(zone).replace(second=0, microsecond=0)
    local = align_to_window(local, send_window, quiet_hours)
    return local.astimezone(UTC).isoformat()


def next_provider_day_open(reference: datetime, send_window: dict[str, object], quiet_hours: dict[str, object]) -> str:
    zone = resolve_zone(text_value(send_window.get("timezone"), "UTC"))
    local = reference.astimezone(zone).replace(second=0, microsecond=0) + timedelta(days=1)
    local = local.replace(hour=int(send_window["start_hour"]), minute=0, second=0, microsecond=0)
    local = align_to_window(local, send_window, quiet_hours)
    return local.astimezone(UTC).isoformat()


def local_day_window(reference: datetime, timezone_name: str) -> tuple[str, str]:
    zone = resolve_zone(timezone_name)
    local = reference.astimezone(zone)
    start = local.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start.astimezone(UTC).isoformat(), end.astimezone(UTC).isoformat()


def should_stop_on_reply(reply_mode: str) -> bool:
    normalized = reply_mode.strip().lower()
    return normalized not in {"continue", "continue_sequence", "ignore"}


def ensure_utc_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def text_value(value: object, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def nullable_text(value: object) -> str | None:
    text = text_value(value).strip()
    return text or None
