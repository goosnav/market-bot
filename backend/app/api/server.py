"""HTTP server for the local application shell.

Safe edit zone:
- Keep API routes explicit; do not hide business logic in the request handler.
- Expose only explicit runtime, warehouse, studio, and campaign-preview endpoints backed by service modules.
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
from urllib.parse import parse_qs, unquote, urlparse

from backend.app import APP_NAME, APP_VERSION, SPRINT_VERSION
from backend.app.core.diagnostics import atomic_write_json, read_json
from backend.app.core.logging import StructuredLogger, utc_now
from backend.app.core.paths import build_runtime_paths, ensure_runtime_directories, resolve_app_root
from backend.app.core.runtime import RuntimeContext, collect_runtime_report, readiness_from_report
from backend.app.core.settings import SETTINGS_SCHEMA, load_settings
from backend.app.repositories import read_session, write_session
from backend.app.services import CampaignBuilderService, LeadWarehouseService, TemplateStudioService


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

    def do_POST(self) -> None:  # noqa: N802
        self.handle_request(include_body=True)

    def handle_request(self, include_body: bool) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        if path.startswith("/api/v1/"):
            self.handle_api(self.command, parsed, include_body)
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

    def handle_api(self, method: str, parsed, include_body: bool) -> None:
        get_routes = {
            "/api/v1/health/live": self.api_live,
            "/api/v1/health/ready": self.api_ready,
            "/api/v1/version": self.api_version,
            "/api/v1/diagnostics/startup": self.api_startup_diagnostics,
            "/api/v1/diagnostics/runtime": self.api_runtime_diagnostics,
            "/api/v1/settings/schema": self.api_settings_schema,
            "/api/v1/settings/effective": self.api_settings_effective,
            "/api/v1/warehouse/summary": self.api_warehouse_summary,
            "/api/v1/warehouse/leads": self.api_warehouse_leads,
            "/api/v1/warehouse/imports": self.api_warehouse_imports,
            "/api/v1/warehouse/filters": self.api_warehouse_filters,
            "/api/v1/studio/summary": self.api_studio_summary,
            "/api/v1/studio/templates": self.api_studio_templates,
            "/api/v1/studio/offers": self.api_studio_offers,
            "/api/v1/studio/playbooks": self.api_studio_playbooks,
            "/api/v1/studio/artifacts": self.api_studio_artifacts,
            "/api/v1/campaigns/summary": self.api_campaigns_summary,
            "/api/v1/campaigns/preview": self.api_campaigns_preview,
        }
        post_routes = {
            "/api/v1/warehouse/imports/csv": self.api_warehouse_import_csv,
            "/api/v1/warehouse/leads/manual": self.api_warehouse_manual_lead,
            "/api/v1/warehouse/filters": self.api_warehouse_create_filter,
            "/api/v1/warehouse/tags/assign": self.api_warehouse_assign_tag,
            "/api/v1/studio/templates": self.api_studio_create_template,
            "/api/v1/studio/offers": self.api_studio_create_offer,
            "/api/v1/studio/playbooks": self.api_studio_create_playbook,
            "/api/v1/studio/render": self.api_studio_render,
            "/api/v1/studio/regenerate": self.api_studio_regenerate,
            "/api/v1/campaigns/providers/accounts": self.api_campaigns_create_provider_account,
            "/api/v1/campaigns/build": self.api_campaigns_build,
            "/api/v1/campaigns/preview/edit": self.api_campaigns_preview_edit,
            "/api/v1/campaigns/preview/regenerate": self.api_campaigns_preview_regenerate,
            "/api/v1/campaigns/preview/approve": self.api_campaigns_preview_approve,
        }
        routes = get_routes if method in {"GET", "HEAD"} else post_routes if method == "POST" else {}
        handler = routes.get(parsed.path)
        if not handler:
            self.send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": f"Unknown endpoint: {parsed.path}"}, include_body)
            return
        try:
            handler(parsed, include_body)
        except Exception as exc:
            self.server.context.logger.log(
                stage="http",
                level="ERROR",
                event="handler_failed",
                message="API request failed.",
                context={"path": parsed.path, "method": method},
                error=exc,
            )
            self.send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)}, include_body)

    def api_live(self, _parsed, include_body: bool) -> None:
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

    def api_ready(self, _parsed, include_body: bool) -> None:
        report = collect_runtime_report(self.server.context.runtime_context)
        ready, failed_checks = readiness_from_report(report)
        payload = {
            "ok": ready,
            "failed_checks": failed_checks,
            "checks": report["checks"],
            "timestamp": utc_now(),
        }
        self.send_json(HTTPStatus.OK if ready else HTTPStatus.SERVICE_UNAVAILABLE, payload, include_body)

    def api_version(self, _parsed, include_body: bool) -> None:
        payload = {
            "name": APP_NAME,
            "version": APP_VERSION,
            "sprint": SPRINT_VERSION,
            "started_at": self.server.context.runtime_context.started_at,
        }
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_startup_diagnostics(self, _parsed, include_body: bool) -> None:
        payload = read_json(self.server.context.paths.startup_diagnostics_path, default={})
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_runtime_diagnostics(self, _parsed, include_body: bool) -> None:
        payload = collect_runtime_report(self.server.context.runtime_context)
        ready, failed_checks = readiness_from_report(payload)
        payload["ready"] = ready
        payload["failed_checks"] = failed_checks
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_settings_schema(self, _parsed, include_body: bool) -> None:
        self.send_json(HTTPStatus.OK, SETTINGS_SCHEMA, include_body)

    def api_settings_effective(self, _parsed, include_body: bool) -> None:
        self.send_json(HTTPStatus.OK, self.server.context.settings.to_public_dict(), include_body)

    def api_warehouse_summary(self, _parsed, include_body: bool) -> None:
        with read_session(self.server.context.paths.database_path) as connection:
            payload = LeadWarehouseService(connection).get_summary()
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_warehouse_leads(self, parsed, include_body: bool) -> None:
        filters = self.parse_query_params(parsed.query)
        limit = int(filters.pop("limit", 50))
        with read_session(self.server.context.paths.database_path) as connection:
            payload = {
                "items": LeadWarehouseService(connection).list_leads(filters, limit=limit),
                "filters": filters,
            }
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_warehouse_imports(self, parsed, include_body: bool) -> None:
        query = self.parse_query_params(parsed.query)
        limit = int(query.get("limit", 20))
        with read_session(self.server.context.paths.database_path) as connection:
            payload = {"items": LeadWarehouseService(connection).import_jobs.list_recent(limit=limit)}
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_warehouse_filters(self, _parsed, include_body: bool) -> None:
        with read_session(self.server.context.paths.database_path) as connection:
            payload = {"items": LeadWarehouseService(connection).list_saved_filters()}
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_warehouse_import_csv(self, _parsed, include_body: bool) -> None:
        body = self.read_json_body()
        with write_session(self.server.context.paths.database_path) as connection:
            payload = LeadWarehouseService(connection).import_csv_text(
                str(body.get("csv_text", "")),
                actor=str(body.get("actor", "operator")),
                list_name=str(body.get("list_name", "Imported CSV")),
                source=str(body.get("source", "csv")),
            )
        status = HTTPStatus.OK if payload.get("status") == "completed" else HTTPStatus.BAD_REQUEST
        self.send_json(status, payload, include_body)

    def api_warehouse_manual_lead(self, _parsed, include_body: bool) -> None:
        body = self.read_json_body()
        lead_payload = body.get("lead") if isinstance(body.get("lead"), dict) else body
        with write_session(self.server.context.paths.database_path) as connection:
            payload = LeadWarehouseService(connection).create_manual_lead(
                dict(lead_payload),
                actor=str(body.get("actor", "operator")),
                list_name=str(body.get("list_name", "Manual Leads")),
            )
        status = HTTPStatus.OK if payload.get("status") == "completed" else HTTPStatus.BAD_REQUEST
        self.send_json(status, payload, include_body)

    def api_warehouse_create_filter(self, _parsed, include_body: bool) -> None:
        body = self.read_json_body()
        with write_session(self.server.context.paths.database_path) as connection:
            payload = LeadWarehouseService(connection).save_filter(
                name=str(body.get("name", "")),
                filters=dict(body.get("filters", {})),
                description=str(body.get("description", "")),
            )
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_warehouse_assign_tag(self, _parsed, include_body: bool) -> None:
        body = self.read_json_body()
        with write_session(self.server.context.paths.database_path) as connection:
            payload = {
                "items": LeadWarehouseService(connection).assign_tag(
                    int(body.get("lead_id", 0)),
                    str(body.get("tag_name", "")),
                    color=str(body.get("color", "")),
                )
            }
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_studio_summary(self, _parsed, include_body: bool) -> None:
        with read_session(self.server.context.paths.database_path) as connection:
            payload = TemplateStudioService(connection).get_summary()
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_studio_templates(self, _parsed, include_body: bool) -> None:
        with read_session(self.server.context.paths.database_path) as connection:
            payload = {"items": TemplateStudioService(connection).list_templates()}
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_studio_offers(self, _parsed, include_body: bool) -> None:
        with read_session(self.server.context.paths.database_path) as connection:
            payload = {"items": TemplateStudioService(connection).list_offer_profiles()}
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_studio_playbooks(self, _parsed, include_body: bool) -> None:
        with read_session(self.server.context.paths.database_path) as connection:
            payload = {"items": TemplateStudioService(connection).list_vertical_playbooks()}
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_studio_artifacts(self, parsed, include_body: bool) -> None:
        query = self.parse_query_params(parsed.query)
        limit = int(query.get("limit", 20))
        with read_session(self.server.context.paths.database_path) as connection:
            payload = {"items": TemplateStudioService(connection).list_artifacts(limit=limit)}
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_studio_create_template(self, _parsed, include_body: bool) -> None:
        body = self.read_json_body()
        with write_session(self.server.context.paths.database_path) as connection:
            payload = TemplateStudioService(connection).save_template(body, actor=str(body.get("actor", "operator")))
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_studio_create_offer(self, _parsed, include_body: bool) -> None:
        body = self.read_json_body()
        with write_session(self.server.context.paths.database_path) as connection:
            payload = TemplateStudioService(connection).save_offer_profile(body, actor=str(body.get("actor", "operator")))
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_studio_create_playbook(self, _parsed, include_body: bool) -> None:
        body = self.read_json_body()
        with write_session(self.server.context.paths.database_path) as connection:
            payload = TemplateStudioService(connection).save_vertical_playbook(body, actor=str(body.get("actor", "operator")))
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_studio_render(self, _parsed, include_body: bool) -> None:
        body = self.read_json_body()
        with write_session(self.server.context.paths.database_path) as connection:
            payload = TemplateStudioService(connection).render_template(
                template_id=int(body.get("template_id", 0)),
                template_variant_id=int(body["template_variant_id"]) if body.get("template_variant_id") else None,
                lead_id=int(body.get("lead_id", 0)),
                offer_profile_id=int(body["offer_profile_id"]) if body.get("offer_profile_id") else None,
                vertical_playbook_id=int(body["vertical_playbook_id"]) if body.get("vertical_playbook_id") else None,
                actor=str(body.get("actor", "operator")),
                deterministic_mode=bool(body.get("deterministic_mode", False)),
                disabled_block_keys=list(body.get("disabled_block_keys", [])) if isinstance(body.get("disabled_block_keys"), list) else [],
                generation_seed=int(body.get("generation_seed", 0)),
            )
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_studio_regenerate(self, _parsed, include_body: bool) -> None:
        body = self.read_json_body()
        with write_session(self.server.context.paths.database_path) as connection:
            payload = TemplateStudioService(connection).regenerate_artifact(
                int(body.get("artifact_id", 0)),
                actor=str(body.get("actor", "operator")),
                regenerate_block_keys=list(body.get("regenerate_block_keys", []))
                if isinstance(body.get("regenerate_block_keys"), list)
                else [],
                deterministic_mode=body.get("deterministic_mode")
                if isinstance(body.get("deterministic_mode"), bool)
                else None,
            )
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_campaigns_summary(self, _parsed, include_body: bool) -> None:
        with read_session(self.server.context.paths.database_path) as connection:
            payload = CampaignBuilderService(connection).get_summary()
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_campaigns_preview(self, parsed, include_body: bool) -> None:
        query = self.parse_query_params(parsed.query)
        campaign_id = int(query.get("campaign_id", 0))
        limit = int(query.get("limit", 500))
        with read_session(self.server.context.paths.database_path) as connection:
            payload = CampaignBuilderService(connection).get_preview(campaign_id, limit=limit)
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_campaigns_create_provider_account(self, _parsed, include_body: bool) -> None:
        body = self.read_json_body()
        with write_session(self.server.context.paths.database_path) as connection:
            payload = CampaignBuilderService(connection).create_provider_account(body, actor=str(body.get("actor", "operator")))
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_campaigns_build(self, _parsed, include_body: bool) -> None:
        body = self.read_json_body()
        with write_session(self.server.context.paths.database_path) as connection:
            payload = CampaignBuilderService(connection).build_campaign_preview(body, actor=str(body.get("actor", "operator")))
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_campaigns_preview_edit(self, _parsed, include_body: bool) -> None:
        body = self.read_json_body()
        with write_session(self.server.context.paths.database_path) as connection:
            payload = CampaignBuilderService(connection).manual_edit_preview(
                int(body.get("queued_message_id", 0)),
                actor=str(body.get("actor", "operator")),
                edited_subject=str(body["edited_subject"]) if body.get("edited_subject") is not None else None,
                edited_body=str(body["edited_body"]) if body.get("edited_body") is not None else None,
            )
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_campaigns_preview_regenerate(self, _parsed, include_body: bool) -> None:
        body = self.read_json_body()
        with write_session(self.server.context.paths.database_path) as connection:
            payload = CampaignBuilderService(connection).regenerate_preview(
                int(body.get("queued_message_id", 0)),
                actor=str(body.get("actor", "operator")),
                regenerate_block_keys=list(body.get("regenerate_block_keys", []))
                if isinstance(body.get("regenerate_block_keys"), list)
                else [],
            )
        self.send_json(HTTPStatus.OK, payload, include_body)

    def api_campaigns_preview_approve(self, _parsed, include_body: bool) -> None:
        body = self.read_json_body()
        with write_session(self.server.context.paths.database_path) as connection:
            payload = CampaignBuilderService(connection).approve_preview(
                int(body.get("campaign_id", 0)),
                actor=str(body.get("actor", "operator")),
            )
        self.send_json(HTTPStatus.OK, payload, include_body)

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

    def send_json(self, status: HTTPStatus, payload: object, include_body: bool) -> None:
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if include_body:
            self.wfile.write(body)

    def read_json_body(self) -> dict[str, object]:
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length <= 0:
            return {}
        raw_body = self.rfile.read(content_length)
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}

    def parse_query_params(self, query_string: str) -> dict[str, object]:
        parsed = parse_qs(query_string, keep_blank_values=False)
        result: dict[str, object] = {}
        for key, values in parsed.items():
            if not values:
                continue
            raw_value = values[-1]
            if raw_value.lower() in {"true", "false"}:
                result[key] = raw_value.lower() == "true"
            else:
                result[key] = raw_value
        return result


def is_safe_child(candidate: Path, parent: Path) -> bool:
    try:
        candidate.relative_to(parent)
    except ValueError:
        return False
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Market Bot Sprint 6 backend shell.")
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
