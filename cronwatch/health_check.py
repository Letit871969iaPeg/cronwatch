"""Health check endpoint exposing a simple HTTP server for liveness probes."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Callable

from cronwatch.tracker import JobTracker


def _utcnow_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


class _HealthHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler that serves /health and /ready endpoints."""

    get_status: Callable[[], dict]  # injected by HealthCheckServer

    def do_GET(self) -> None:  # noqa: N802
        if self.path in ("/health", "/ready"):
            payload = self.get_status()
            body = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt: str, *args: object) -> None:  # silence access log
        pass


class HealthCheckServer:
    """Runs a background HTTP server exposing job-tracker health status."""

    def __init__(self, tracker: JobTracker, host: str = "127.0.0.1", port: int = 8765) -> None:
        self._tracker = tracker
        self._host = host
        self._port = port
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    def _build_status(self) -> dict:
        jobs = list(self._tracker._records.keys())  # noqa: SLF001
        failing = [
            name for name in jobs
            if self._tracker.last_status(name) == "failure"
        ]
        return {
            "status": "degraded" if failing else "ok",
            "timestamp": _utcnow_iso(),
            "total_jobs": len(jobs),
            "failing_jobs": failing,
        }

    # ------------------------------------------------------------------
    def start(self) -> None:
        """Start the health-check server in a daemon thread."""
        handler_cls = type(
            "_BoundHandler",
            (_HealthHandler,),
            {"get_status": lambda _self: self._build_status()},
        )
        self._server = HTTPServer((self._host, self._port), handler_cls)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            daemon=True,
            name="cronwatch-health",
        )
        self._thread.start()

    def stop(self) -> None:
        """Shut down the server gracefully."""
        if self._server is not None:
            self._server.shutdown()
            self._server = None
