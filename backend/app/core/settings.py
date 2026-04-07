"""Typed config loader for the local runtime shell.

Safe edit zone:
- Add new settings sections as dataclasses rather than opaque dicts.
- Keep validation conservative and portability-focused.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
from typing import Any


class SettingsError(RuntimeError):
    """Raised when the runtime configuration is invalid."""


@dataclass(frozen=True)
class AppConfig:
    name: str
    version: str
    environment: str
    support_email: str


@dataclass(frozen=True)
class ServerConfig:
    host: str
    preferred_port: int
    port_search_limit: int
    open_browser: bool


@dataclass(frozen=True)
class PathsConfig:
    data_dir: str
    logs_dir: str
    exports_dir: str
    cache_dir: str
    runtime_dir: str
    database_path: str
    frontend_dir: str


@dataclass(frozen=True)
class LoggingConfig:
    level: str
    redact_keys: list[str]


@dataclass(frozen=True)
class UiConfig:
    default_route: str
    startup_failure_page: str


@dataclass(frozen=True)
class ProviderCredentialConfig:
    enabled: bool
    api_key: str


@dataclass(frozen=True)
class ProvidersConfig:
    apollo: ProviderCredentialConfig
    sending: ProviderCredentialConfig
    llm: ProviderCredentialConfig


@dataclass(frozen=True)
class BookingConfig:
    default_link: str


@dataclass(frozen=True)
class WorkerConfig:
    heartbeat_interval_seconds: int
    stale_after_seconds: int
    shutdown_grace_seconds: int
    poll_interval_seconds: int
    claim_ttl_seconds: int
    batch_size: int
    retry_backoff_seconds: int
    circuit_breaker_threshold: int
    circuit_breaker_cooldown_seconds: int


@dataclass(frozen=True)
class SettingsBundle:
    app: AppConfig
    server: ServerConfig
    paths: PathsConfig
    logging: LoggingConfig
    ui: UiConfig
    providers: ProvidersConfig
    booking: BookingConfig
    worker: WorkerConfig

    def to_public_dict(self) -> dict[str, Any]:
        """Return redacted settings for diagnostics and UI."""
        payload = asdict(self)
        for provider_name, provider_data in payload["providers"].items():
            if provider_data.get("api_key"):
                provider_data["api_key"] = redact_secret(provider_data["api_key"])
        return payload


SETTINGS_SCHEMA: dict[str, Any] = {
    "app": {
        "required": True,
        "fields": {
            "name": "Display name shown in the shell UI and diagnostics.",
            "version": "App version string.",
            "environment": "Runtime environment label. Sprint 1 expects local.",
            "support_email": "Operator-facing support contact."
        }
    },
    "server": {
        "required": True,
        "fields": {
            "host": "Loopback host. Use 127.0.0.1 for portable local launch.",
            "preferred_port": "Primary localhost port to try first.",
            "port_search_limit": "How many fallback ports to scan if preferred_port is unavailable.",
            "open_browser": "Whether the launcher should open the local app automatically."
        }
    },
    "paths": {
        "required": True,
        "fields": {
            "data_dir": "Relative app data root.",
            "logs_dir": "Relative log directory under the data root.",
            "exports_dir": "Relative export directory under the data root.",
            "cache_dir": "Relative cache directory under the data root.",
            "runtime_dir": "Relative runtime directory under the data root.",
            "database_path": "Relative SQLite file path under the data root.",
            "frontend_dir": "Relative path to the static frontend bundle."
        }
    },
    "logging": {
        "required": True,
        "fields": {
            "level": "Structured log level.",
            "redact_keys": "Keys that must be redacted before logs or diagnostics are written."
        }
    },
    "ui": {
        "required": True,
        "fields": {
            "default_route": "Hash route opened after successful launch.",
            "startup_failure_page": "Runtime-generated HTML page shown if startup fails."
        }
    },
    "providers": {
        "required": True,
        "fields": {
            "apollo": "Sprint 1 placeholder provider config.",
            "sending": "Sprint 1 placeholder provider config.",
            "llm": "Sprint 1 placeholder provider config."
        }
    },
    "booking": {
        "required": True,
        "fields": {
            "default_link": "Default Google Calendar booking link placeholder."
        }
    },
    "worker": {
        "required": True,
        "fields": {
            "heartbeat_interval_seconds": "Worker heartbeat cadence.",
            "stale_after_seconds": "Maximum age for a healthy worker heartbeat.",
            "shutdown_grace_seconds": "Grace period before the launcher force-stops a child process.",
            "poll_interval_seconds": "How often the worker polls for due queue work.",
            "claim_ttl_seconds": "How long a claimed dispatch job stays owned before recovery releases it.",
            "batch_size": "Maximum number of queued rows the worker claims in one cycle.",
            "retry_backoff_seconds": "Base backoff used for transient dispatch retries.",
            "circuit_breaker_threshold": "Recent provider failure count that opens the local dispatch circuit.",
            "circuit_breaker_cooldown_seconds": "Cooldown before a tripped provider circuit is retried."
        }
    }
}


def load_settings(app_root: Path, config_override: str | None = None) -> tuple[SettingsBundle, Path]:
    """Load and validate the runtime configuration."""
    config_path = resolve_settings_path(app_root, config_override)
    try:
        raw_data = json.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise SettingsError(
            f"Config file not found: {config_path}. Copy config/settings.example.json or restore config/settings.json."
        ) from exc
    except json.JSONDecodeError as exc:
        raise SettingsError(f"Config file is not valid JSON: {config_path} ({exc})") from exc

    apply_secret_env_overrides(raw_data)
    settings = parse_settings(raw_data)
    validate_settings(settings)
    return settings, config_path


def resolve_settings_path(app_root: Path, config_override: str | None = None) -> Path:
    if config_override:
        candidate = Path(config_override)
        return candidate if candidate.is_absolute() else (app_root / candidate).resolve()
    local_override = app_root / "config/settings.local.json"
    if local_override.exists():
        return local_override.resolve()
    return (app_root / "config/settings.json").resolve()


def parse_settings(raw_data: dict[str, Any]) -> SettingsBundle:
    return SettingsBundle(
        app=AppConfig(**required_section(raw_data, "app")),
        server=ServerConfig(**required_section(raw_data, "server")),
        paths=PathsConfig(**required_section(raw_data, "paths")),
        logging=LoggingConfig(**required_section(raw_data, "logging")),
        ui=UiConfig(**required_section(raw_data, "ui")),
        providers=ProvidersConfig(
            apollo=ProviderCredentialConfig(**required_provider(raw_data, "apollo")),
            sending=ProviderCredentialConfig(**required_provider(raw_data, "sending")),
            llm=ProviderCredentialConfig(**required_provider(raw_data, "llm")),
        ),
        booking=BookingConfig(**required_section(raw_data, "booking")),
        worker=WorkerConfig(**required_section(raw_data, "worker")),
    )


def required_section(raw_data: dict[str, Any], section_name: str) -> dict[str, Any]:
    section = raw_data.get(section_name)
    if not isinstance(section, dict):
        raise SettingsError(f"Missing required settings section: {section_name}")
    return section


def required_provider(raw_data: dict[str, Any], provider_name: str) -> dict[str, Any]:
    providers = required_section(raw_data, "providers")
    provider_data = providers.get(provider_name)
    if not isinstance(provider_data, dict):
        raise SettingsError(f"Missing required provider settings section: providers.{provider_name}")
    return provider_data


def apply_secret_env_overrides(raw_data: dict[str, Any]) -> None:
    overrides = {
        "apollo": os.environ.get("MARKET_BOT_APOLLO_API_KEY"),
        "sending": os.environ.get("MARKET_BOT_SENDING_API_KEY"),
        "llm": os.environ.get("MARKET_BOT_LLM_API_KEY"),
    }
    providers = raw_data.setdefault("providers", {})
    for provider_name, secret in overrides.items():
        if not secret:
            continue
        provider_data = providers.setdefault(provider_name, {})
        provider_data["api_key"] = secret


def validate_settings(settings: SettingsBundle) -> None:
    if settings.server.host not in {"127.0.0.1", "localhost"}:
        raise SettingsError("server.host must stay on localhost for Sprint 1.")
    if not 1 <= settings.server.preferred_port <= 65535:
        raise SettingsError("server.preferred_port must be between 1 and 65535.")
    if settings.server.port_search_limit < 0:
        raise SettingsError("server.port_search_limit must be zero or greater.")
    if settings.logging.level not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
        raise SettingsError("logging.level must be one of DEBUG, INFO, WARNING, ERROR.")
    if settings.worker.heartbeat_interval_seconds <= 0:
        raise SettingsError("worker.heartbeat_interval_seconds must be positive.")
    if settings.worker.stale_after_seconds <= settings.worker.heartbeat_interval_seconds:
        raise SettingsError("worker.stale_after_seconds must be greater than the heartbeat interval.")
    if settings.worker.poll_interval_seconds <= 0:
        raise SettingsError("worker.poll_interval_seconds must be positive.")
    if settings.worker.claim_ttl_seconds <= settings.worker.poll_interval_seconds:
        raise SettingsError("worker.claim_ttl_seconds must be greater than worker.poll_interval_seconds.")
    if settings.worker.batch_size <= 0:
        raise SettingsError("worker.batch_size must be positive.")
    if settings.worker.retry_backoff_seconds <= 0:
        raise SettingsError("worker.retry_backoff_seconds must be positive.")
    if settings.worker.circuit_breaker_threshold <= 0:
        raise SettingsError("worker.circuit_breaker_threshold must be positive.")
    if settings.worker.circuit_breaker_cooldown_seconds <= 0:
        raise SettingsError("worker.circuit_breaker_cooldown_seconds must be positive.")

    for field_name, relative_value in asdict(settings.paths).items():
        if field_name == "frontend_dir":
            ensure_relative_path(relative_value, f"paths.{field_name}")
            continue
        ensure_relative_path(relative_value, f"paths.{field_name}")


def ensure_relative_path(value: str, label: str) -> None:
    path = Path(value)
    if path.is_absolute():
        raise SettingsError(f"{label} must be relative for portable runtime resolution.")
    if ".." in path.parts:
        raise SettingsError(f"{label} must not escape the app root or data directory.")


def redact_secret(value: str) -> str:
    if not value:
        return value
    if len(value) <= 4:
        return "****"
    return f"{value[:2]}***{value[-2:]}"
