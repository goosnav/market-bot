"""Startup and runtime diagnostics helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import json
from pathlib import Path
import tempfile
from typing import Any


def utc_now() -> str:
    return datetime.now(tz=UTC).isoformat()


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=path.parent) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.flush()
        temp_path = Path(handle.name)
    temp_path.replace(path)


def read_json(path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
    if not path.exists():
        return default or {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default or {}


@dataclass
class StartupDiagnosticsRecorder:
    output_path: Path
    base_payload: dict[str, Any]
    stages: list[dict[str, Any]] = field(default_factory=list)

    def record_stage(self, stage: str, status: str, message: str, details: dict[str, Any] | None = None) -> None:
        self.stages.append(
            {
                "timestamp": utc_now(),
                "stage": stage,
                "status": status,
                "message": message,
                "details": details or {},
            }
        )
        self.write()

    def write(self, status: str = "starting", details: dict[str, Any] | None = None) -> None:
        payload = dict(self.base_payload)
        payload["status"] = status
        payload["updated_at"] = utc_now()
        payload["stages"] = self.stages
        if details:
            payload.update(details)
        atomic_write_json(self.output_path, payload)

    def finalize(self, status: str, details: dict[str, Any] | None = None) -> None:
        self.write(status=status, details=details)


def render_startup_error_page(title: str, diagnostics: dict[str, Any]) -> str:
    stages_html = "".join(
        (
            f"<li><strong>{item['stage']}</strong> "
            f"<span>{item['status']}</span> "
            f"<p>{item['message']}</p></li>"
        )
        for item in diagnostics.get("stages", [])
    )
    log_paths = diagnostics.get("log_paths", {})
    log_html = "".join(f"<li><code>{name}</code>: {value}</li>" for name, value in log_paths.items())
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #111826;
      --panel: #162237;
      --border: #2a3a56;
      --text: #e7edf7;
      --muted: #99a9c2;
      --danger: #ff8d8d;
    }}
    body {{
      margin: 0;
      font-family: "Segoe UI", Arial, sans-serif;
      background: radial-gradient(circle at top, #192844, var(--bg) 60%);
      color: var(--text);
    }}
    main {{
      max-width: 920px;
      margin: 0 auto;
      padding: 48px 24px 72px;
    }}
    .panel {{
      background: rgba(22, 34, 55, 0.92);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 24px;
      margin-top: 24px;
    }}
    h1 {{ margin: 0 0 12px; }}
    p, li {{ color: var(--muted); line-height: 1.6; }}
    strong {{ color: var(--text); }}
    code {{
      color: var(--text);
      font-family: "SFMono-Regular", Consolas, monospace;
    }}
    .status {{
      color: var(--danger);
      font-weight: 700;
    }}
  </style>
</head>
<body>
  <main>
    <h1>{title}</h1>
    <p class="status">The launcher hit a critical startup failure. Review the stages and logs below.</p>
    <div class="panel">
      <h2>Startup Stages</h2>
      <ol>{stages_html}</ol>
    </div>
    <div class="panel">
      <h2>Log Paths</h2>
      <ul>{log_html}</ul>
    </div>
  </main>
</body>
</html>"""

