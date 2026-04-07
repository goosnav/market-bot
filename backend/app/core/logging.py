"""Small structured logger used by the launcher, backend, and worker."""

from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
import threading
import traceback
from typing import Any


def utc_now() -> str:
    return datetime.now(tz=UTC).isoformat()


def redact_value(value: Any, redact_keys: set[str], current_key: str | None = None) -> Any:
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            if key.lower() in redact_keys:
                redacted[key] = "***REDACTED***"
            else:
                redacted[key] = redact_value(item, redact_keys, key)
        return redacted
    if isinstance(value, list):
        return [redact_value(item, redact_keys, current_key) for item in value]
    if current_key and current_key.lower() in redact_keys and value:
        return "***REDACTED***"
    return value


class StructuredLogger:
    """Write append-only JSONL events with lightweight console echoes."""

    def __init__(self, service: str, log_path: Path, redact_keys: list[str]) -> None:
        self.service = service
        self.log_path = log_path
        self.redact_keys = {key.lower() for key in redact_keys}
        self._lock = threading.Lock()

    def log(
        self,
        stage: str,
        level: str,
        event: str,
        message: str,
        context: dict[str, Any] | None = None,
        error: BaseException | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "timestamp": utc_now(),
            "service": self.service,
            "stage": stage,
            "level": level,
            "event": event,
            "message": message,
            "context": redact_value(context or {}, self.redact_keys),
        }
        if error is not None:
            payload["error"] = {
                "type": error.__class__.__name__,
                "message": str(error),
                "traceback": traceback.format_exc(),
            }

        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            with self.log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, sort_keys=True) + "\n")

        console_message = f"[{payload['service']}] {payload['stage']} {payload['level']} {payload['event']}: {payload['message']}"
        print(console_message, flush=True)

