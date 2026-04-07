"""HTTP server for the local application shell.

Safe edit zone:
- Keep API routes explicit; do not hide business logic in the request handler.
- Sprint 1 intentionally exposes only runtime and diagnostics endpoints.
"""

from __future__ import annotations

import argparse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import mimetypes
import os
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import unquote, urlparse

from backend.app import APP_NAME, APP_VERSION, SPRINT_VERSION
from backend.app.core.diagnostics import atomic_write_json, read_json
from backend.app.core.logging import StructuredLogger, utc_now
from backend.app.core.paths import build_runtime_paths, ensure_runtime_directories, resolve_app_root
from backend.app.core.runtime import RuntimeContext, collect_runtime_report, readiness_from_report
from backend.app.core.settings import SETTINGS_SCHEMA, load_settings


class MarketBotHttpServer(ThreadingHTTPServer):
    """Typed HTTP server wrapper for runtime context access."""

    def __init__(self, server_address: tuple[str, int], context: SimpleNamespace) -> None:
        super().__init__(server_address, MarketBotRequestHandler)
        self.context = context
        self.timeout = 0.5


class MarketBotRequestHandler(BaseHTTPRequestHandler):
    """Serve both JSON diagnostics endpoints and the static shell."""

    server: MarketBotHttpServer

    def do_GET(self) -> None:  # noqa: N802
        self.handle_request(include_body=True)

    def do_HEAD(self) -> None:  # noqa: N802
        self.handle_request(include_body=False)

    def handle_request(self, include_body: bool) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        if path.startswith("/api/v1/"):
            self.handle_api(path, include_body)
            return
        self.handle_static(path, include_body)

    def log_message(self, format_string: str, *args: object) -> None:
        self.server.context.logger.log(
            stage="http",
            level="INFO",
            event="access",
            message=format_string % args,
            context={"client_address": self.client_address[0]},
        )

    def handle_api(self, path: str, include_body: bool) -> None:
        routes = {
            "/api/v1/health/live": self.api_live,
            "/api/v1/health/ready": self.api_ready,
            "/api/v1/version": self.api_version,
            "/api/v1/diagnostics/startup": self.api_startup_diagnostics,
            "/api/v1/diagnostics/runtime": self.api_runtime_diagnostics,
            "/api/v1/settings/schema": self.api_settings_schema,
            "/api/v1/settings/effective": self.api_settings_effective,
        }
        handler = routes.get(path)
        if not handler:
            self.send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": f"Unknown endpoint: {path}"}, include_body)
            return
        handler(include_body)

    def api_live(self, include_body: bool) -> None:
        self.send_json(
            HTTPStatus.OK,
            {
                "ok": True,
                "service": "backend",
                "name": APP_NAME,
                "version": APP_VERSION,
                "sprint": SPRINT_VERSION,
                "timestamp": utc_now(),
            },
            include_body,
        )

    def api_ready(self, include_body: bool) -> None:
        report = collect_runtime_report(self.server.context.runtime_context)
        ready, failed_checks = readiness_from_report(report)
        payload = {
            "ok": ready,
            "failed_checks": failed_checks,
            "checks": report["checks"],
            "timestamp": utc_now(),
        }
        self.send_json(HTTPStatus.OK if ready else HTTPStatus.SERVICE_UNAVAILABLE, payload, include_body)

    def api_version(self, include_body: bool) -> None:
        payload = {
            "name": APP_NAME,
            "version": APP_VERSION,
            "sprint": SPRINT_VERSION,
            "started_at": self.server.context.runtime_context.started_at,
        }
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_startup_diagnostics(self, include_body: bool) -> None:
        payload = read_json(self.server.context.paths.startup_diagnostics_path, default={})
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_runtime_diagnostics(self, include_body: bool) -> None:
        payload = collect_runtime_report(self.server.context.runtime_context)
        ready, failed_checks = readiness_from_report(payload)
        payload["ready"] = ready
        payload["failed_checks"] = failed_checks
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_settings_schema(self, include_body: bool) -> None:
        self.send_json(HTTPStatus.OK, SETTINGS_SCHEMA, include_body)

    def api_settings_effective(self, include_body: bool) -> None:
        self.send_json(HTTPStatus.OK, self.server.context.settings.to_public_dict(), include_body)

    def handle_static(self, path: str, include_body: bool) -> None:
        if path == "/":
            self.serve_file(self.server.context.paths.frontend_dir / "index.html", include_body)
            return

        relative_path = unquote(path.lstrip("/"))
        candidate = (self.server.context.paths.frontend_dir / relative_path).resolve()
        if is_safe_child(candidate, self.server.context.paths.frontend_dir) and candidate.is_file():
            self.serve_file(candidate, include_body)
            return

        self.serve_file(self.server.context.paths.frontend_dir / "index.html", include_body)

    def serve_file(self, file_path: Path, include_body: bool) -> None:
        if not file_path.exists():
            self.send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": f"Missing asset: {file_path.name}"}, include_body)
            return
        mime_type, _ = mimetypes.guess_type(file_path.name)
        payload = file_path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", mime_type or "application/octet-stream")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if include_body:
            self.wfile.write(payload)

    def send_json(self, status: HTTPStatus, payload: dict[str, object], include_body: bool) -> None:
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if include_body:
            self.wfile.write(body)


def is_safe_child(candidate: Path, parent: Path) -> bool:
    try:
        candidate.relative_to(parent)
    except ValueError:
        return False
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Market Bot Sprint 1 backend shell.")
    parser.add_argument("--app-root", default=None)
    parser.add_argument("--config", default=None)
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    args = parser.parse_args(argv)

    app_root = resolve_app_root(args.app_root)
    settings, config_path = load_settings(app_root, args.config)
    paths = build_runtime_paths(app_root, settings)
    ensure_runtime_directories(paths)

    logger = StructuredLogger("backend", paths.backend_log_path, settings.logging.redact_keys)
    host = args.host or settings.server.host
    port = args.port or settings.server.preferred_port

    runtime_context = RuntimeContext(
        settings=settings,
        paths=paths,
        config_path=config_path,
        host=host,
        port=port,
        started_at=utc_now(),
        backend_pid=os.getpid(),
    )
    context = SimpleNamespace(
        logger=logger,
        settings=settings,
        paths=paths,
        runtime_context=runtime_context,
    )

    atomic_write_json(
        paths.backend_status_path,
        {
            "pid": os.getpid(),
            "state": "running",
            "started_at": runtime_context.started_at,
            "host": host,
            "port": port,
        },
    )

    logger.log(
        stage="startup",
        level="INFO",
        event="backend_boot",
        message="Starting backend server.",
        context={"host": host, "port": port, "frontend_dir": str(paths.frontend_dir)},
    )

    server = MarketBotHttpServer((host, port), context)
    try:
        while True:
            server.handle_request()
    except KeyboardInterrupt:
        logger.log(stage="shutdown", level="INFO", event="interrupt", message="Backend interrupted.")
    finally:
        atomic_write_json(
            paths.backend_status_path,
            {
                "pid": os.getpid(),
                "state": "stopped",
                "stopped_at": utc_now(),
                "host": host,
                "port": port,
            },
        )
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
