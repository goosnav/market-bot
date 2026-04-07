"""Dedicated worker shell for the Sprint 1 runtime."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import os
from pathlib import Path
import signal
import threading
import time

from backend.app.core.database import check_database
from backend.app.core.diagnostics import atomic_write_json
from backend.app.core.logging import StructuredLogger
from backend.app.core.paths import build_runtime_paths, ensure_runtime_directories, resolve_app_root
from backend.app.core.settings import load_settings


def utc_now() -> str:
    return datetime.now(tz=UTC).isoformat()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Market Bot Sprint 1 worker shell.")
    parser.add_argument("--app-root", default=None)
    parser.add_argument("--config", default=None)
    args = parser.parse_args(argv)

    app_root = resolve_app_root(args.app_root)
    settings, _ = load_settings(app_root, args.config)
    paths = build_runtime_paths(app_root, settings)
    ensure_runtime_directories(paths)
    logger = StructuredLogger("worker", paths.worker_log_path, settings.logging.redact_keys)
    stop_event = threading.Event()

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
        message="Starting worker heartbeat loop.",
        context={"pid": os.getpid()},
    )

    while not stop_event.is_set():
        db_status = check_database(paths.database_path)
        atomic_write_json(
            paths.worker_status_path,
            {
                "pid": os.getpid(),
                "state": "running",
                "started_at": utc_now(),
                "last_heartbeat_at": utc_now(),
                "database_ok": db_status["ok"],
                "database_message": db_status["message"],
            },
        )
        time.sleep(settings.worker.heartbeat_interval_seconds)

    atomic_write_json(
        paths.worker_status_path,
        {
            "pid": os.getpid(),
            "state": "stopped",
            "stopped_at": utc_now(),
        },
    )
    logger.log(stage="shutdown", level="INFO", event="worker_stopped", message="Worker stopped cleanly.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

