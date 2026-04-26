"""V0 alert sinks."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from pitlake.utils import isoformat


def write_local_alert(logs_dir: Path, message: str, payload: dict[str, Any] | None = None) -> Path:
    logs_dir.mkdir(parents=True, exist_ok=True)
    path = logs_dir / "alerts.jsonl"
    line = (
        json.dumps(
            {"time": isoformat(), "message": message, "payload": payload or {}},
            ensure_ascii=False,
        )
        + "\n"
    )
    with path.open("a", encoding="utf-8") as fh:
        fh.write(line)
    return path


def send_webhook_alert(
    *,
    webhook_url: str,
    message: str,
    payload: dict[str, Any] | None = None,
    timeout_seconds: int = 10,
) -> dict[str, Any]:
    requests = __import__("requests")
    body = {"time": isoformat(), "message": message, "payload": payload or {}}
    response = requests.post(webhook_url, json=body, timeout=timeout_seconds)
    return {
        "status_code": response.status_code,
        "ok": 200 <= response.status_code < 300,
        "response_text": response.text[:1000],
    }


def dispatch_alert(
    *,
    logs_dir: Path,
    message: str,
    payload: dict[str, Any] | None = None,
    webhook_url: str | None = None,
) -> dict[str, Any]:
    local_path = write_local_alert(logs_dir, message, payload)
    result: dict[str, Any] = {
        "local_alert_path": str(local_path),
        "webhook_attempted": False,
        "webhook_result": None,
    }
    url = webhook_url or os.environ.get("PITLAKE_ALERT_WEBHOOK_URL")
    if not url:
        return result
    result["webhook_attempted"] = True
    try:
        result["webhook_result"] = send_webhook_alert(
            webhook_url=url,
            message=message,
            payload=payload,
        )
    except Exception as exc:
        result["webhook_result"] = {"ok": False, "error": str(exc)[:1000]}
    return result
