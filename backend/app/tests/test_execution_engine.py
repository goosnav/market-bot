"""Sprint 6 execution-engine and restart-safety tests."""

from __future__ import annotations

from backend.app.core.database import connect_database
from backend.app.domain.enums import CampaignStatus
from backend.app.domain.models import ReplyCreate
from backend.app.repositories import read_session, write_session
from backend.app.services import CampaignBuilderService, CampaignService, ExecutionEngineService, ReplyService, TemplateStudioService
from backend.app.tests.support import DatabaseTestCase
from backend.app.tests.test_campaign_builder import create_template, seed_lead


class ExecutionEngineTests(DatabaseTestCase):
    def test_dispatch_cycle_sends_due_rows_and_keeps_future_rows_active(self) -> None:
        with write_session(self.database_path) as connection:
            launch = seed_launchable_campaign(
                connection,
                name="Active Dispatch Campaign",
                step_delays=[0, 2],
                provider_name="manual",
            )

        summary = run_dispatch_cycle(self.database_path, now="2026-04-06T09:00:00+00:00")

        with read_session(self.database_path) as connection:
            queue_rows = list_queue_rows(connection, launch["campaign_id"])
            sent_count = connection.execute("SELECT COUNT(*) AS total FROM sent_messages").fetchone()["total"]
            campaign_status = connection.execute(
                "SELECT status FROM campaigns WHERE id = ?",
                (launch["campaign_id"],),
            ).fetchone()["status"]

        self.assertEqual(summary["sent"], 1)
        self.assertEqual(summary["claimed"], 1)
        self.assertEqual(sent_count, 1)
        self.assertEqual([row["state"] for row in queue_rows], ["sent", "scheduled"])
        self.assertEqual(campaign_status, CampaignStatus.ACTIVE.value)

    def test_claim_race_only_allows_one_worker_to_claim_due_message(self) -> None:
        with write_session(self.database_path) as connection:
            seed_launchable_campaign(connection, name="Claim Race Campaign", provider_name="manual")
            engine = ExecutionEngineService(connection)
            engine.stage_dispatchable_messages(now="2026-04-06T09:00:00+00:00", actor="worker-a")
            first_claim = engine.claim_due_messages(
                now="2026-04-06T09:00:00+00:00",
                worker_id="worker-a",
                claim_token="claim-a",
                claim_ttl_seconds=30,
                batch_size=1,
            )

        with write_session(self.database_path) as connection:
            second_claim = ExecutionEngineService(connection).claim_due_messages(
                now="2026-04-06T09:00:00+00:00",
                worker_id="worker-b",
                claim_token="claim-b",
                claim_ttl_seconds=30,
                batch_size=1,
            )

        self.assertEqual(len(first_claim), 1)
        self.assertEqual(second_claim, [])

    def test_expired_claim_is_recovered_and_dispatched_once(self) -> None:
        with write_session(self.database_path) as connection:
            launch = seed_launchable_campaign(connection, name="Recovery Campaign", provider_name="manual")
            engine = ExecutionEngineService(connection)
            engine.stage_dispatchable_messages(now="2026-04-06T09:00:00+00:00", actor="worker-a")
            claimed = engine.claim_due_messages(
                now="2026-04-06T09:00:00+00:00",
                worker_id="worker-a",
                claim_token="stale-claim",
                claim_ttl_seconds=1,
                batch_size=1,
            )

        self.assertEqual(len(claimed), 1)

        summary = run_dispatch_cycle(self.database_path, now="2026-04-06T09:00:05+00:00", worker_id="worker-b")

        with read_session(self.database_path) as connection:
            row = list_queue_rows(connection, launch["campaign_id"])[0]
            sent_count = connection.execute("SELECT COUNT(*) AS total FROM sent_messages").fetchone()["total"]

        self.assertEqual(summary["recovered_claims"], 1)
        self.assertEqual(summary["sent"], 1)
        self.assertEqual(row["state"], "sent")
        self.assertEqual(sent_count, 1)

    def test_transient_failure_retries_and_then_succeeds(self) -> None:
        with write_session(self.database_path) as connection:
            launch = seed_launchable_campaign(connection, name="Retry Campaign", provider_name="mock_fail_once")

        first = run_dispatch_cycle(
            self.database_path,
            now="2026-04-06T09:00:00+00:00",
            retry_backoff_seconds=60,
        )
        second = run_dispatch_cycle(
            self.database_path,
            now="2026-04-06T09:15:00+00:00",
            retry_backoff_seconds=60,
        )

        with read_session(self.database_path) as connection:
            row = list_queue_rows(connection, launch["campaign_id"])[0]
            sent_count = connection.execute("SELECT COUNT(*) AS total FROM sent_messages").fetchone()["total"]

        self.assertEqual(first["retried"], 1)
        self.assertEqual(second["sent"], 1)
        self.assertEqual(row["state"], "sent")
        self.assertEqual(row["attempt_count"], 2)
        self.assertEqual(sent_count, 1)

    def test_retry_exhaustion_dead_letters_failed_message(self) -> None:
        with write_session(self.database_path) as connection:
            launch = seed_launchable_campaign(connection, name="Dead Letter Campaign", provider_name="mock_fail_always")

        run_dispatch_cycle(self.database_path, now="2026-04-06T09:00:00+00:00", retry_backoff_seconds=60)
        run_dispatch_cycle(self.database_path, now="2026-04-06T09:15:00+00:00", retry_backoff_seconds=60)
        final = run_dispatch_cycle(self.database_path, now="2026-04-06T09:30:00+00:00", retry_backoff_seconds=60)

        with read_session(self.database_path) as connection:
            row = list_queue_rows(connection, launch["campaign_id"])[0]
            dead_letter_count = connection.execute("SELECT COUNT(*) AS total FROM dead_letter_jobs").fetchone()["total"]

        self.assertEqual(final["dead_lettered"], 1)
        self.assertEqual(row["state"], "failed")
        self.assertEqual(row["attempt_count"], 3)
        self.assertTrue(row["dead_lettered_at"])
        self.assertEqual(dead_letter_count, 1)

    def test_dispatch_rechecks_suppression_before_send(self) -> None:
        with write_session(self.database_path) as connection:
            launch = seed_launchable_campaign(connection, name="Suppression Campaign", provider_name="manual")
            connection.execute(
                """
                INSERT INTO suppression_entries (scope, lead_id, reason, source, active, created_at, updated_at)
                VALUES ('lead', ?, 'manual suppression', 'operator', 1, ?, ?)
                """,
                (launch["lead_ids"][0], "2026-04-06T08:59:00+00:00", "2026-04-06T08:59:00+00:00"),
            )

        summary = run_dispatch_cycle(self.database_path, now="2026-04-06T09:00:00+00:00")

        with read_session(self.database_path) as connection:
            row = list_queue_rows(connection, launch["campaign_id"])[0]
            sent_count = connection.execute("SELECT COUNT(*) AS total FROM sent_messages").fetchone()["total"]

        self.assertEqual(summary["suppressed"], 1)
        self.assertEqual(row["state"], "suppressed")
        self.assertEqual(sent_count, 0)

    def test_reply_gating_blocks_follow_up_after_reply(self) -> None:
        with write_session(self.database_path) as connection:
            launch = seed_launchable_campaign(
                connection,
                name="Reply Gate Campaign",
                provider_name="manual",
                step_delays=[0, 2],
                reply_mode="manual",
            )

        run_dispatch_cycle(self.database_path, now="2026-04-06T09:00:00+00:00")

        with read_session(self.database_path) as connection:
            thread_id = connection.execute(
                "SELECT id FROM threads WHERE campaign_id = ? AND lead_id = ?",
                (launch["campaign_id"], launch["lead_ids"][0]),
            ).fetchone()["id"]

        with write_session(self.database_path) as connection:
            ReplyService(connection).create_reply(
                ReplyCreate(
                    thread_id=int(thread_id),
                    lead_id=launch["lead_ids"][0],
                    campaign_id=launch["campaign_id"],
                    provider_name="manual",
                    received_at="2026-04-07T12:00:00+00:00",
                    reply_text="Interested. Please stop the sequence.",
                ),
                actor="webhook",
            )

        summary = run_dispatch_cycle(self.database_path, now="2026-04-08T09:15:00+00:00")

        with read_session(self.database_path) as connection:
            queue_rows = list_queue_rows(connection, launch["campaign_id"])

        self.assertEqual(summary["blocked"], 1)
        self.assertEqual([row["state"] for row in queue_rows], ["sent", "blocked"])

    def test_pause_and_resume_prevents_new_claims_under_load(self) -> None:
        with write_session(self.database_path) as connection:
            launch = seed_launchable_campaign(
                connection,
                name="Pause Resume Campaign",
                provider_name="manual",
                lead_count=2,
            )

        first = run_dispatch_cycle(self.database_path, now="2026-04-06T09:00:00+00:00", batch_size=1)

        with write_session(self.database_path) as connection:
            CampaignService(connection).transition_status(
                launch["campaign_id"],
                CampaignStatus.PAUSED,
                actor="operator",
                reason="Pause test",
            )

        paused = run_dispatch_cycle(self.database_path, now="2026-04-06T09:15:00+00:00", batch_size=1)

        with write_session(self.database_path) as connection:
            CampaignService(connection).transition_status(
                launch["campaign_id"],
                CampaignStatus.SCHEDULED,
                actor="operator",
                reason="Resume test",
            )

        resumed = run_dispatch_cycle(self.database_path, now="2026-04-06T09:16:00+00:00", batch_size=1)

        with read_session(self.database_path) as connection:
            queue_rows = list_queue_rows(connection, launch["campaign_id"])

        self.assertEqual(first["sent"], 1)
        self.assertEqual(paused["claimed"], 0)
        self.assertEqual(resumed["sent"], 1)
        self.assertEqual([row["state"] for row in queue_rows], ["sent", "sent"])


def seed_launchable_campaign(
    connection,
    *,
    name: str,
    provider_name: str,
    lead_count: int = 1,
    step_delays: list[int] | None = None,
    reply_mode: str = "manual",
) -> dict[str, object]:
    studio = TemplateStudioService(connection)
    builder = CampaignBuilderService(connection)
    lead_ids = [
        seed_lead(
            connection,
            full_name=f"Lead {index + 1}",
            company_name=f"Company {index + 1}",
            email=f"lead{index + 1}@company{index + 1}.example",
        )
        for index in range(lead_count)
    ]
    provider = builder.create_provider_account(
        {
            "display_name": f"{name} Pool",
            "email_address": f"{provider_name}@example.com",
            "provider_name": provider_name,
        },
        actor="tester",
    )
    template = create_template(studio, f"{name} Template")
    steps = [{"template_id": template["id"], "delay_days": delay} for delay in (step_delays or [0])]
    preview = builder.build_campaign_preview(
        {
            "name": name,
            "provider_account_ids": [provider["id"]],
            "reply_mode": reply_mode,
            "steps": steps,
            "start_at": "2026-04-06T09:00:00+00:00",
            "timezone": "UTC",
        },
        actor="tester",
    )
    approved = builder.approve_preview(preview["campaign"]["id"], actor="tester")
    return {
        "campaign_id": int(approved["campaign"]["id"]),
        "provider_account_id": int(provider["id"]),
        "lead_ids": lead_ids,
    }


def run_dispatch_cycle(
    database_path,
    *,
    now: str,
    worker_id: str = "worker-test",
    claim_ttl_seconds: int = 30,
    batch_size: int = 8,
    retry_backoff_seconds: int = 300,
    circuit_breaker_threshold: int = 3,
    circuit_breaker_cooldown_seconds: int = 900,
) -> dict[str, int]:
    with write_session(database_path) as connection:
        engine = ExecutionEngineService(connection)
        recovered_claims = engine.release_expired_claims(now=now, actor=worker_id)
        stage_counts = engine.stage_dispatchable_messages(now=now, actor=worker_id)
        claimed_rows = engine.claim_due_messages(
            now=now,
            worker_id=worker_id,
            claim_token=f"{worker_id}-{now}",
            claim_ttl_seconds=claim_ttl_seconds,
            batch_size=batch_size,
        )

    summary = {
        "recovered_claims": recovered_claims,
        "newly_scheduled": stage_counts["newly_scheduled"],
        "retry_released": stage_counts["retry_released"],
        "claimed": len(claimed_rows),
        "sent": 0,
        "rescheduled": 0,
        "blocked": 0,
        "suppressed": 0,
        "retried": 0,
        "dead_lettered": 0,
        "already_sent": 0,
    }
    for row in claimed_rows:
        with write_session(database_path) as connection:
            result = ExecutionEngineService(connection).process_claimed_message(
                queued_message_id=int(row["id"]),
                claim_token=str(row["claim_token"]),
                worker_id=worker_id,
                now=now,
                retry_backoff_seconds=retry_backoff_seconds,
                circuit_breaker_threshold=circuit_breaker_threshold,
                circuit_breaker_cooldown_seconds=circuit_breaker_cooldown_seconds,
            )
        outcome = str(result.get("outcome"))
        if outcome == "sent":
            summary["sent"] += 1
        elif outcome == "rescheduled":
            summary["rescheduled"] += 1
        elif outcome == "blocked":
            summary["blocked"] += 1
        elif outcome == "suppressed":
            summary["suppressed"] += 1
        elif outcome == "retry_scheduled":
            summary["retried"] += 1
        elif outcome == "dead_lettered":
            summary["dead_lettered"] += 1
        elif outcome == "already_sent":
            summary["already_sent"] += 1
    return summary


def list_queue_rows(connection, campaign_id: int) -> list[dict[str, object]]:
    return [
        dict(row)
        for row in connection.execute(
            """
            SELECT *
            FROM queued_messages
            WHERE campaign_id = ?
            ORDER BY sequence_step_id ASC, id ASC
            """,
            (campaign_id,),
        ).fetchall()
    ]
