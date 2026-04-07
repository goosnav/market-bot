"""Runtime state inspection used by the backend health endpoints."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app import APP_NAME, APP_VERSION, SPRINT_VERSION
from backend.app.core.database import check_database
from backend.app.core.diagnostics import read_json
from backend.app.core.paths import RuntimePaths
from backend.app.core.settings import SettingsBundle


@dataclass(frozen=True)
class RuntimeContext:
    settings: SettingsBundle
    paths: RuntimePaths
    config_path: Path
    host: str
    port: int
    started_at: str
    backend_pid: int


def collect_runtime_report(context: RuntimeContext) -> dict[str, Any]:
    worker_status = read_json(context.paths.worker_status_path, default={})
    startup_diagnostics = read_json(context.paths.startup_diagnostics_path, default={})
    backend_status = read_json(context.paths.backend_status_path, default={})
    db_status = check_database(context.paths.database_path)
    worker_ok = is_worker_heartbeat_fresh(worker_status, context.settings.worker.stale_after_seconds)
    frontend_ok = (context.paths.frontend_dir / "index.html").exists()

    checks = {
        "config_loaded": True,
        "database_ready": db_status["ok"],
        "worker_heartbeat_fresh": worker_ok,
        "frontend_bundle_present": frontend_ok,
    }

    return {
        "app": {
            "name": APP_NAME,
            "version": APP_VERSION,
            "sprint": SPRINT_VERSION,
        },
        "server": {
            "host": context.host,
            "port": context.port,
            "started_at": context.started_at,
            "pid": context.backend_pid,
        },
        "checks": checks,
        "database": db_status,
        "paths": context.paths.as_dict(),
        "config_path": str(context.config_path),
        "worker_status": worker_status,
        "startup_diagnostics": startup_diagnostics,
        "backend_status": backend_status,
    }


def readiness_from_report(report: dict[str, Any]) -> tuple[bool, list[str]]:
    failed_checks = [name for name, ok in report["checks"].items() if not ok]
    return (len(failed_checks) == 0, failed_checks)


def is_worker_heartbeat_fresh(worker_status: dict[str, Any], stale_after_seconds: int) -> bool:
    last_heartbeat = worker_status.get("last_heartbeat_at")
    if not last_heartbeat:
        return False
    try:
        heartbeat_at = datetime.fromisoformat(last_heartbeat)
    except ValueError:
        return False
    age_seconds = (datetime.now(tz=UTC) - heartbeat_at).total_seconds()
    return age_seconds <= stale_after_seconds and worker_status.get("state") == "running"

