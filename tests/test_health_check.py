"""Tests for cronwatch.health_check."""

from __future__ import annotations

import json
import threading
import time
import urllib.request
from unittest.mock import MagicMock

import pytest

from cronwatch.health_check import HealthCheckServer
from cronwatch.tracker import JobTracker, JobRecord


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _free_port() -> int:
    import socket
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture()
def tracker():
    return JobTracker()


@pytest.fixture()
def server(tracker):
    port = _free_port()
    srv = HealthCheckServer(tracker, host="127.0.0.1", port=port)
    srv.start()
    # give the thread a moment to bind
    time.sleep(0.05)
    yield srv, port
    srv.stop()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_health_ok_no_jobs(server):
    srv, port = server
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/health") as resp:
        data = json.loads(resp.read())
    assert data["status"] == "ok"
    assert data["total_jobs"] == 0
    assert data["failing_jobs"] == []


def test_health_ok_with_successful_job(server, tracker):
    srv, port = server
    rec = JobRecord()
    rec.last_exit_code = 0
    tracker._records["backup"] = rec

    with urllib.request.urlopen(f"http://127.0.0.1:{port}/health") as resp:
        data = json.loads(resp.read())
    assert data["status"] == "ok"
    assert data["total_jobs"] == 1
    assert "backup" not in data["failing_jobs"]


def test_health_degraded_with_failed_job(server, tracker):
    srv, port = server
    rec = JobRecord()
    rec.last_exit_code = 1
    tracker._records["sync"] = rec

    with urllib.request.urlopen(f"http://127.0.0.1:{port}/health") as resp:
        data = json.loads(resp.read())
    assert data["status"] == "degraded"
    assert "sync" in data["failing_jobs"]


def test_ready_endpoint_same_as_health(server, tracker):
    srv, port = server
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/ready") as resp:
        data = json.loads(resp.read())
    assert "status" in data
    assert "timestamp" in data


def test_unknown_path_returns_404(server):
    srv, port = server
    import urllib.error
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(f"http://127.0.0.1:{port}/unknown")
    assert exc_info.value.code == 404


def test_stop_is_idempotent(server):
    srv, port = server
    srv.stop()
    srv.stop()  # should not raise
