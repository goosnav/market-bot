"""Dedicated worker loop for the Sprint 6 execution engine."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import os
from pathlib import Path
import secrets
import signal
import threading
import time

from backend.app.core.database import check_database
from backend.app.core.diagnostics import atomic_write_json
from backend.app.core.logging import StructuredLogger
from backend.app.core.paths import build_runtime_paths, ensure_runtime_directories, resolve_app_root
from backend.app.core.settings import load_settings
from backend.app.repositories import read_session, write_session
from backend.app.services import ExecutionEngineService


def utc_now() -> str:
    return datetime.now(tz=UTC).isoformat()


def build_claim_token(worker_id: str) -> str:
    return f"{worker_id}:{int(time.time() * 1000)}:{secrets.token_hex(4)}"


def run_worker_cycle(database_path: Path, settings, worker_id: str) -> dict[str, object]:
    cycle_started_at = utc_now()
    with write_session(database_path) as connection:
        engine = ExecutionEngineService(connection)
        recovered_claims = engine.release_expired_claims(now=cycle_started_at, actor=worker_id)
        stage_counts = engine.stage_dispatchable_messages(now=cycle_started_at, actor=worker_id)
        claimed_rows = engine.claim_due_messages(
            now=cycle_started_at,
            worker_id=worker_id,
            claim_token=build_claim_token(worker_id),
            claim_ttl_seconds=settings.worker.claim_ttl_seconds,
            batch_size=settings.worker.batch_size,
        )

    outcome_counts = {
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
                now=utc_now(),
                retry_backoff_seconds=settings.worker.retry_backoff_seconds,
                circuit_breaker_threshold=settings.worker.circuit_breaker_threshold,
                circuit_breaker_cooldown_seconds=settings.worker.circuit_breaker_cooldown_seconds,
            )
        outcome = str(result.get("outcome", ""))
        if outcome == "sent":
            outcome_counts["sent"] += 1
        elif outcome == "rescheduled":
            outcome_counts["rescheduled"] += 1
        elif outcome == "blocked":
            outcome_counts["blocked"] += 1
        elif outcome == "suppressed":
            outcome_counts["suppressed"] += 1
        elif outcome == "retry_scheduled":
            outcome_counts["retried"] += 1
        elif outcome == "dead_lettered":
            outcome_counts["dead_lettered"] += 1
        elif outcome == "already_sent":
            outcome_counts["already_sent"] += 1

    with read_session(database_path) as connection:
        execution_summary = ExecutionEngineService(connection).get_execution_summary()

    return {
        "started_at": cycle_started_at,
        "completed_at": utc_now(),
        "recovered_claims": recovered_claims,
        "newly_scheduled": stage_counts["newly_scheduled"],
        "retry_released": stage_counts["retry_released"],
        "claimed": len(claimed_rows),
        **outcome_counts,
        "execution": execution_summary,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Market Bot Sprint 6 execution worker.")
    parser.add_argument("--app-root", default=None)
    parser.add_argument("--config", default=None)
    args = parser.parse_args(argv)

    app_root = resolve_app_root(args.app_root)
    settings, _ = load_settings(app_root, args.config)
    paths = build_runtime_paths(app_root, settings)
    ensure_runtime_directories(paths)
    logger = StructuredLogger("worker", paths.worker_log_path, settings.logging.redact_keys)
    stop_event = threading.Event()
    started_at = utc_now()
    worker_id = f"worker-{os.getpid()}"

    def request_stop(signum: int, _frame: object) -> None:
        logger.log(
            stage="shutdown",
            level="INFO",
            event="signal_received",
            message="Worker shutdown requested.",
            context={"signal": signum},
        )
        stop_event.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    logger.log(
        stage="startup",
        level="INFO",
        event="worker_boot",
        message="Starting the Sprint 6 execution worker.",
        context={"pid": os.getpid(), "worker_id": worker_id},
    )

    last_cycle: dict[str, object] = {}
    last_error = ""
    next_cycle_at = 0.0

    while not stop_event.is_set():
        loop_started = time.monotonic()
        db_status = check_database(paths.database_path)
        if loop_started >= next_cycle_at and db_status["ok"]:
            try:
                last_cycle = run_worker_cycle(paths.database_path, settings, worker_id)
                last_error = ""
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                logger.log(
                    stage="worker",
                    level="ERROR",
                    event="cycle_failed",
                    message="Worker execution cycle failed.",
                    context={"worker_id": worker_id},
                    error=exc,
                )
            next_cycle_at = time.monotonic() + settings.worker.poll_interval_seconds

        heartbeat_at = utc_now()
        atomic_write_json(
            paths.worker_status_path,
            {
                "pid": os.getpid(),
                "worker_id": worker_id,
                "state": "running",
                "started_at": started_at,
                "last_heartbeat_at": heartbeat_at,
                "database_ok": db_status["ok"],
                "database_message": db_status["message"],
                "schema_version": db_status["schema_version"],
                "applied_migrations": db_status["applied_migrations"],
                "last_cycle": last_cycle,
                "last_error": last_error,
            },
        )

        sleep_for = min(
            settings.worker.heartbeat_interval_seconds,
            max(0.1, next_cycle_at - time.monotonic()) if next_cycle_at else settings.worker.heartbeat_interval_seconds,
        )
        stop_event.wait(sleep_for)

    atomic_write_json(
        paths.worker_status_path,
        {
            "pid": os.getpid(),
            "worker_id": worker_id,
            "state": "stopped",
            "started_at": started_at,
            "stopped_at": utc_now(),
            "last_cycle": last_cycle,
            "last_error": last_error,
        },
    )
    logger.log(stage="shutdown", level="INFO", event="worker_stopped", message="Worker stopped cleanly.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
