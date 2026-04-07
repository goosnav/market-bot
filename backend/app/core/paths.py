"""Runtime path resolution helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from backend.app.core.settings import SettingsBundle


@dataclass(frozen=True)
class RuntimePaths:
    app_root: Path
    config_dir: Path
    data_dir: Path
    logs_dir: Path
    exports_dir: Path
    cache_dir: Path
    runtime_dir: Path
    database_path: Path
    frontend_dir: Path
    startup_diagnostics_path: Path
    startup_error_page_path: Path
    worker_status_path: Path
    backend_status_path: Path
    launcher_log_path: Path
    backend_log_path: Path
    worker_log_path: Path

    def as_dict(self) -> dict[str, str]:
        return {
            "app_root": str(self.app_root),
            "config_dir": str(self.config_dir),
            "data_dir": str(self.data_dir),
            "logs_dir": str(self.logs_dir),
            "exports_dir": str(self.exports_dir),
            "cache_dir": str(self.cache_dir),
            "runtime_dir": str(self.runtime_dir),
            "database_path": str(self.database_path),
            "frontend_dir": str(self.frontend_dir),
            "startup_diagnostics_path": str(self.startup_diagnostics_path),
            "startup_error_page_path": str(self.startup_error_page_path),
            "worker_status_path": str(self.worker_status_path),
            "backend_status_path": str(self.backend_status_path),
            "launcher_log_path": str(self.launcher_log_path),
            "backend_log_path": str(self.backend_log_path),
            "worker_log_path": str(self.worker_log_path),
        }


def resolve_app_root(explicit_root: str | None = None) -> Path:
    if explicit_root:
        return Path(explicit_root).resolve()
    return Path(__file__).resolve().parents[3]


def build_runtime_paths(app_root: Path, settings: SettingsBundle) -> RuntimePaths:
    data_dir = (app_root / settings.paths.data_dir).resolve()
    logs_dir = data_dir / settings.paths.logs_dir
    exports_dir = data_dir / settings.paths.exports_dir
    cache_dir = data_dir / settings.paths.cache_dir
    runtime_dir = data_dir / settings.paths.runtime_dir
    frontend_dir = (app_root / settings.paths.frontend_dir).resolve()

    return RuntimePaths(
        app_root=app_root,
        config_dir=(app_root / "config").resolve(),
        data_dir=data_dir,
        logs_dir=logs_dir,
        exports_dir=exports_dir,
        cache_dir=cache_dir,
        runtime_dir=runtime_dir,
        database_path=data_dir / settings.paths.database_path,
        frontend_dir=frontend_dir,
        startup_diagnostics_path=runtime_dir / "startup-diagnostics.json",
        startup_error_page_path=runtime_dir / settings.ui.startup_failure_page,
        worker_status_path=runtime_dir / "worker-status.json",
        backend_status_path=runtime_dir / "backend-status.json",
        launcher_log_path=logs_dir / "launcher.jsonl",
        backend_log_path=logs_dir / "backend.jsonl",
        worker_log_path=logs_dir / "worker.jsonl",
    )


def ensure_runtime_directories(paths: RuntimePaths) -> None:
    for directory in (
        paths.data_dir,
        paths.logs_dir,
        paths.exports_dir,
        paths.cache_dir,
        paths.runtime_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)

