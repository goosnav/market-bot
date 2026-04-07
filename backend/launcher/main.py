"""Supervisor launcher for the portable local runtime shell.

Safe edit zone:
- Keep the launcher focused on orchestration, diagnostics, and portability.
- Do not move domain or provider logic into this file.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import webbrowser

from backend.app import APP_NAME, APP_VERSION, SPRINT_VERSION
from backend.app.core.database import DatabaseBootstrapError, bootstrap_database
from backend.app.core.diagnostics import (
    StartupDiagnosticsRecorder,
    atomic_write_json,
    read_json,
    render_startup_error_page,
)
from backend.app.core.logging import StructuredLogger
from backend.app.core.paths import build_runtime_paths, ensure_runtime_directories, resolve_app_root
from backend.app.core.settings import SettingsError, load_settings


@dataclass
class ChildProcesses:
    backend: subprocess.Popen[str]
    worker: subprocess.Popen[str]


def utc_now() -> str:
    return datetime.now(tz=UTC).isoformat()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Start the Market Bot Sprint 6 local shell.")
    parser.add_argument("command", nargs="?", default="start", choices=["start"])
    parser.add_argument("--app-root", default=None)
    parser.add_argument("--config", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args(argv)

    app_root = resolve_app_root(args.app_root)
    settings = None
    paths = None
    logger = None
    recorder = None
    children = None

    try:
        settings, config_path = load_settings(app_root, args.config)
        paths = build_runtime_paths(app_root, settings)
        ensure_runtime_directories(paths)
        logger = StructuredLogger("launcher", paths.launcher_log_path, settings.logging.redact_keys)
        recorder = StartupDiagnosticsRecorder(
            paths.startup_diagnostics_path,
            {
                "app": {
                    "name": APP_NAME,
                    "version": APP_VERSION,
                    "sprint": SPRINT_VERSION,
                },
                "started_at": utc_now(),
                "app_root": str(app_root),
                "config_path": str(config_path),
                "log_paths": {
                    "launcher": str(paths.launcher_log_path),
                    "backend": str(paths.backend_log_path),
                    "worker": str(paths.worker_log_path),
                },
                "resolved_paths": paths.as_dict(),
            },
        )
        recorder.record_stage("config", "ok", "Configuration loaded and validated.")
        logger.log(stage="startup", level="INFO", event="config_loaded", message="Settings loaded.", context={"config_path": str(config_path)})

        db_summary = bootstrap_database(paths.database_path, APP_VERSION)
        recorder.record_stage("database", "ok", "Runtime database bootstrap completed.", db_summary)
        logger.log(stage="startup", level="INFO", event="database_ready", message="Runtime database ready.", context=db_summary)

        preferred_port = args.port if args.port is not None else settings.server.preferred_port
        selected_port = choose_port(settings.server.host, preferred_port, settings.server.port_search_limit)
        recorder.record_stage("port", "ok", "Selected localhost port.", {"requested_port": preferred_port, "selected_port": selected_port})
        logger.log(stage="startup", level="INFO", event="port_selected", message="Port selected.", context={"selected_port": selected_port})

        children = start_children(app_root, config_path, settings.server.host, selected_port, logger)
        recorder.record_stage(
            "processes",
            "ok",
            "Backend and worker processes started.",
            {"backend_pid": children.backend.pid, "worker_pid": children.worker.pid},
        )

        wait_for_readiness(
            settings.server.host,
            selected_port,
            children,
            logger,
        )
        launcher_url = f"http://{settings.server.host}:{selected_port}/{settings.ui.default_route.lstrip('/')}"
        recorder.finalize(
            "ready",
            {
                "selected_port": selected_port,
                "launcher_url": launcher_url,
                "processes": {"backend_pid": children.backend.pid, "worker_pid": children.worker.pid},
            },
        )
        logger.log(stage="startup", level="INFO", event="runtime_ready", message="Local shell is ready.", context={"url": launcher_url})

        if settings.server.open_browser and not args.no_browser:
            webbrowser.open(launcher_url)

        return supervise(children, settings.worker.shutdown_grace_seconds, logger, recorder)
    except (SettingsError, DatabaseBootstrapError, RuntimeError, OSError) as exc:
        return fail_startup(exc, app_root, settings, paths, logger, recorder)
    finally:
        if children and children.backend.poll() is not None and children.worker.poll() is None:
            terminate_process(children.worker, grace_seconds=settings.worker.shutdown_grace_seconds if settings else 5)


def choose_port(host: str, preferred_port: int, search_limit: int) -> int:
    if preferred_port == 0:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind((host, 0))
            return int(probe.getsockname()[1])
    for port in range(preferred_port, preferred_port + search_limit + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind((host, port))
            except OSError:
                continue
            return port
    raise RuntimeError(f"No open localhost port found from {preferred_port} to {preferred_port + search_limit}.")


def start_children(
    app_root: Path,
    config_path: Path,
    host: str,
    port: int,
    logger: StructuredLogger,
) -> ChildProcesses:
    shared_env = os.environ.copy()
    backend_command = [
        sys.executable,
        "-m",
        "backend.app.api.server",
        "--app-root",
        str(app_root),
        "--config",
        str(config_path),
        "--host",
        host,
        "--port",
        str(port),
    ]
    worker_command = [
        sys.executable,
        "-m",
        "backend.app.workers.main",
        "--app-root",
        str(app_root),
        "--config",
        str(config_path),
    ]
    logger.log(stage="startup", level="INFO", event="spawn_backend", message="Starting backend child.", context={"command": backend_command})
    backend_process = subprocess.Popen(backend_command, cwd=app_root, env=shared_env, text=True)
    logger.log(stage="startup", level="INFO", event="spawn_worker", message="Starting worker child.", context={"command": worker_command})
    worker_process = subprocess.Popen(worker_command, cwd=app_root, env=shared_env, text=True)
    return ChildProcesses(backend=backend_process, worker=worker_process)


def wait_for_readiness(
    host: str,
    port: int,
    children: ChildProcesses,
    logger: StructuredLogger,
    timeout_seconds: int = 20,
) -> None:
    deadline = time.time() + timeout_seconds
    health_url = f"http://{host}:{port}/api/v1/health/ready"
    last_failed_checks: list[str] = []
    while time.time() < deadline:
        if not child_processes_healthy(children):
            raise RuntimeError("A child process exited before the runtime became ready.")
        backend_ready = False
        try:
            with urllib.request.urlopen(health_url, timeout=1) as response:
                payload = read_response_json(response)
                backend_ready = response.status == 200 and payload.get("ok") is True
                last_failed_checks = list(payload.get("failed_checks", []))
        except urllib.error.HTTPError as exc:
            payload = read_response_json(exc)
            backend_ready = False
            last_failed_checks = list(payload.get("failed_checks", []))
        except (urllib.error.URLError, TimeoutError):
            backend_ready = False
        if backend_ready:
            logger.log(stage="startup", level="INFO", event="readiness_ok", message="Backend and worker are responding.")
            return
        time.sleep(0.5)
    failed_checks_suffix = f" Failed checks: {', '.join(last_failed_checks)}." if last_failed_checks else ""
    raise RuntimeError(f"Timed out waiting for runtime readiness on {health_url}.{failed_checks_suffix}")


def read_response_json(response) -> dict[str, object]:
    try:
        return read_json_payload(response.read().decode("utf-8"))
    except UnicodeDecodeError:
        return {}


def read_json_payload(payload: str) -> dict[str, object]:
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return {}


def child_processes_healthy(children: ChildProcesses) -> bool:
    return children.backend.poll() is None and children.worker.poll() is None


def supervise(
    children: ChildProcesses,
    shutdown_grace_seconds: int,
    logger: StructuredLogger,
    recorder: StartupDiagnosticsRecorder,
) -> int:
    stop_requested = False

    def request_stop(signum: int, _frame: object) -> None:
        nonlocal stop_requested
        logger.log(stage="shutdown", level="INFO", event="signal_received", message="Launcher shutdown requested.", context={"signal": signum})
        stop_requested = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    exit_code = 0
    while True:
        if stop_requested:
            break
        if children.backend.poll() is not None:
            exit_code = children.backend.returncode or 1
            logger.log(stage="supervisor", level="ERROR", event="backend_exit", message="Backend exited unexpectedly.", context={"returncode": exit_code})
            recorder.record_stage("supervisor", "error", "Backend exited unexpectedly.", {"returncode": exit_code})
            break
        if children.worker.poll() is not None:
            exit_code = children.worker.returncode or 1
            logger.log(stage="supervisor", level="ERROR", event="worker_exit", message="Worker exited unexpectedly.", context={"returncode": exit_code})
            recorder.record_stage("supervisor", "error", "Worker exited unexpectedly.", {"returncode": exit_code})
            break
        time.sleep(0.5)

    terminate_process(children.backend, shutdown_grace_seconds)
    terminate_process(children.worker, shutdown_grace_seconds)
    recorder.finalize("stopped" if exit_code == 0 else "degraded")
    logger.log(stage="shutdown", level="INFO", event="launcher_exit", message="Launcher stopped.", context={"exit_code": exit_code})
    return exit_code


def terminate_process(process: subprocess.Popen[str], grace_seconds: int) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    deadline = time.time() + grace_seconds
    while time.time() < deadline:
        if process.poll() is not None:
            return
        time.sleep(0.1)
    process.kill()


def fail_startup(
    exc: BaseException,
    app_root: Path,
    settings,
    paths,
    logger: StructuredLogger | None,
    recorder: StartupDiagnosticsRecorder | None,
) -> int:
    if settings and paths is None:
        paths = build_runtime_paths(app_root, settings)
        ensure_runtime_directories(paths)

    if logger is None and settings and paths:
        logger = StructuredLogger("launcher", paths.launcher_log_path, settings.logging.redact_keys)

    if logger:
        logger.log(stage="startup", level="ERROR", event="startup_failed", message="Launcher failed during startup.", error=exc)

    if recorder:
        recorder.record_stage("failure", "error", str(exc), {"exception_type": exc.__class__.__name__})
        recorder.finalize("failed")

    diagnostics = read_json(paths.startup_diagnostics_path, default={}) if paths else {"stages": []}
    diagnostics.setdefault("log_paths", {})
    title = "Market Bot Startup Failed"
    if paths:
        html = render_startup_error_page(title, diagnostics)
        paths.startup_error_page_path.write_text(html, encoding="utf-8")
        try:
            webbrowser.open(paths.startup_error_page_path.as_uri())
        except Exception:
            pass
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
