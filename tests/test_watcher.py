"""Tests for cronwatch.watcher.JobWatcher."""

import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from cronwatch.watcher import JobWatcher


@pytest.fixture()
def tracker():
    t = MagicMock()
    return t


@pytest.fixture()
def history():
    h = MagicMock()
    return h


@pytest.fixture()
def watcher(tracker, history):
    return JobWatcher("backup", tracker, history, timeout=30.0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_completed_process(returncode: int):
    cp = MagicMock()
    cp.returncode = returncode
    cp.stdout = b""
    cp.stderr = b""
    return cp


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_successful_run_returns_zero(watcher, tracker, history):
    with patch("cronwatch.watcher.subprocess.run", return_value=_make_completed_process(0)) as mock_run:
        code = watcher.run(["echo", "hello"])

    assert code == 0
    mock_run.assert_called_once_with(["echo", "hello"], capture_output=True, timeout=30.0)
    tracker.mark_start.assert_called_once()
    tracker.mark_end.assert_called_once()
    _, end_kwargs = tracker.mark_end.call_args
    assert end_kwargs["success"] is True
    history.record.assert_called_once()


def test_failed_run_returns_nonzero(watcher, tracker, history):
    with patch("cronwatch.watcher.subprocess.run", return_value=_make_completed_process(1)):
        code = watcher.run(["false"])

    assert code == 1
    _, end_kwargs = tracker.mark_end.call_args
    assert end_kwargs["success"] is False


def test_timeout_returns_minus_one(watcher, tracker, history):
    import subprocess
    with patch("cronwatch.watcher.subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="sleep", timeout=30)):
        code = watcher.run(["sleep", "999"])

    assert code == -1
    _, end_kwargs = tracker.mark_end.call_args
    assert end_kwargs["success"] is False


def test_unexpected_exception_returns_minus_two(watcher, tracker, history):
    with patch("cronwatch.watcher.subprocess.run", side_effect=OSError("no such file")):
        code = watcher.run(["nonexistent_binary"])

    assert code == -2
    history.record.assert_called_once()


def test_history_record_called_with_job_name(watcher, tracker, history):
    with patch("cronwatch.watcher.subprocess.run", return_value=_make_completed_process(0)):
        watcher.run(["true"])

    args, _ = history.record.call_args
    assert args[0] == "backup"
