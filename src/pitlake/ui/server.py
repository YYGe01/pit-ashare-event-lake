"""Standard-library HTTP server for the local PitLake console."""

from __future__ import annotations

import json
import mimetypes
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from pitlake.settings import ProjectSettings
from pitlake.storage.layout import LakeLayout
from pitlake.storage.metadata_store import MetadataStore
from pitlake.ui.console_data import PitLakeConsoleData

STATIC_ROOT = Path(__file__).parent / "static"


def serve_console(settings: ProjectSettings, *, host: str = "127.0.0.1", port: int = 8765) -> None:
    """Serve the local read-only console until interrupted."""

    LakeLayout(settings).create()
    MetadataStore(settings).init_schema()
    data = PitLakeConsoleData(settings)
    handler_class = _handler_factory(data)
    server = ThreadingHTTPServer((host, port), handler_class)
    url = f"http://{server.server_address[0]}:{server.server_address[1]}"
    print(f"PitLake Console: {url}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nPitLake Console stopped")
    finally:
        server.server_close()


def _handler_factory(data: PitLakeConsoleData) -> type[BaseHTTPRequestHandler]:
    class PitLakeConsoleHandler(BaseHTTPRequestHandler):
        server_version = "PitLakeConsole/0.1"

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path.startswith("/api/"):
                self._serve_api(parsed.path, parse_qs(parsed.query))
                return
            self._serve_static(parsed.path)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            return

        def _serve_api(self, path: str, query: dict[str, list[str]]) -> None:
            try:
                payload = _dispatch_api(data, path, query)
            except ValueError as exc:
                self._write_json({"error": str(exc)}, status=HTTPStatus.NOT_FOUND)
                return
            except Exception as exc:  # pragma: no cover - defensive HTTP boundary
                self._write_json(
                    {"error": f"{type(exc).__name__}: {exc}"},
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )
                return
            self._write_json(payload)

        def _serve_static(self, path: str) -> None:
            relative = "index.html" if path in {"", "/"} else unquote(path).lstrip("/")
            target = (STATIC_ROOT / relative).resolve()
            try:
                target.relative_to(STATIC_ROOT.resolve())
            except ValueError:
                self.send_error(HTTPStatus.FORBIDDEN)
                return
            if not target.exists() or not target.is_file():
                target = STATIC_ROOT / "index.html"
            content_type = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
            body = target.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _write_json(self, payload: Any, *, status: HTTPStatus = HTTPStatus.OK) -> None:
            body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

    return PitLakeConsoleHandler


def _dispatch_api(
    data: PitLakeConsoleData,
    path: str,
    query: dict[str, list[str]],
) -> dict[str, Any]:
    parts = [unquote(part) for part in path.removeprefix("/api/").split("/") if part]
    date = _single(query, "date")
    limit = int(_single(query, "limit", "100") or "100")
    if not parts:
        return {"status": "ok", "service": "pitlake-console"}
    if parts == ["overview"]:
        return data.overview(date=date)
    if parts == ["days"] and date:
        return data.overview(date=date)
    if len(parts) == 2 and parts[0] == "days":
        return data.overview(date=parts[1])
    if parts == ["datasets"]:
        return data.datasets(date=date)
    if len(parts) == 2 and parts[0] == "datasets":
        return data.dataset_detail(parts[1], date=date, limit=limit)
    if len(parts) == 3 and parts[0] == "datasets" and parts[2] == "items":
        return data.dataset_items(parts[1], date=date, limit=limit)
    if len(parts) == 3 and parts[0] == "datasets" and parts[2] == "coverage":
        return data.dataset_coverage(parts[1], date=date, limit=limit)
    if len(parts) == 3 and parts[0] == "datasets" and parts[2] == "quality":
        return data.dataset_quality(parts[1], date=date)
    if len(parts) == 3 and parts[0] == "datasets" and parts[2] == "reconciliation":
        return data.dataset_reconciliation(parts[1], date=date)
    if parts == ["sources"]:
        return data.sources(date=date)
    if len(parts) == 2 and parts[0] == "sources":
        return data.source_detail(parts[1], date=date)
    if parts == ["symbols"]:
        return data.symbols(date=date, limit=limit)
    if len(parts) == 2 and parts[0] == "symbols":
        return data.symbol_detail(parts[1], date=date, limit=limit)
    if parts == ["runs"]:
        return data.runs(
            date=date,
            source_id=_single(query, "source_id"),
            logical_dataset=_single(query, "logical_dataset"),
            limit=limit,
        )
    if len(parts) == 2 and parts[0] == "runs":
        return data.run_detail(parts[1])
    if parts == ["quality", "findings"] or parts == ["quality"]:
        return data.quality_findings(date=date)
    if parts == ["reconciliation"]:
        return data.reconciliation(date=date)
    if parts == ["governance"]:
        return data.governance(date=date)
    if parts == ["manifests"]:
        return data.manifests(limit=limit)
    if len(parts) == 2 and parts[0] == "manifests":
        return data.manifest_detail(parts[1])
    if parts == ["raw"]:
        return data.raw_objects(
            date=date,
            source_id=_single(query, "source_id"),
            logical_dataset=_single(query, "logical_dataset"),
            limit=limit,
        )
    if len(parts) == 2 and parts[0] == "raw":
        return data.raw_detail(parts[1])
    if parts == ["search"]:
        return data.search(_single(query, "q", "") or "", limit=limit)
    raise ValueError(f"unknown API path: {path}")


def _single(query: dict[str, list[str]], key: str, default: str | None = None) -> str | None:
    values = query.get(key)
    if not values:
        return default
    return values[0]
