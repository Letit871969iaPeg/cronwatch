"""Tests for job_blackout and blackout_guard modules."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from cronwatch.job_blackout import BlackoutStore, BlackoutWindow
from cronwatch.blackout_guard import BlackoutGuard


TZ = timezone.utc


@pytest.fixture()
def store() -> BlackoutStore:
    return BlackoutStore()  # in-memory


@pytest.fixture()
def guard(store: BlackoutStore) -> BlackoutGuard:
    return BlackoutGuard(store)


def _ts(offset_minutes: int = 0) -> datetime:
    return datetime.now(TZ) + timedelta(minutes=offset_minutes)


# ---------------------------------------------------------------------------
# BlackoutWindow.is_active
# ---------------------------------------------------------------------------

def test_window_active_within_range():
    w = BlackoutWindow(
        job_name="j",
        start_iso=_ts(-10).isoformat(),
        end_iso=_ts(10).isoformat(),
    )
    assert w.is_active() is True


def test_window_inactive_before_start():
    w = BlackoutWindow(
        job_name="j",
        start_iso=_ts(5).isoformat(),
        end_iso=_ts(15).isoformat(),
    )
    assert w.is_active() is False


def test_window_inactive_after_end():
    w = BlackoutWindow(
        job_name="j",
        start_iso=_ts(-20).isoformat(),
        end_iso=_ts(-5).isoformat(),
    )
    assert w.is_active() is False


# ---------------------------------------------------------------------------
# BlackoutStore
# ---------------------------------------------------------------------------

def test_fetch_empty_returns_empty(store: BlackoutStore):
    assert store.fetch("nojob") == []


def test_add_and_fetch(store: BlackoutStore):
    w = BlackoutWindow("backup", _ts(-5).isoformat(), _ts(5).isoformat(), "maintenance")
    store.add(w)
    results = store.fetch("backup")
    assert len(results) == 1
    assert results[0].reason == "maintenance"


def test_is_blacked_out_true(store: BlackoutStore):
    store.add(BlackoutWindow("j", _ts(-1).isoformat(), _ts(1).isoformat()))
    assert store.is_blacked_out("j") is True


def test_is_blacked_out_false_when_no_window(store: BlackoutStore):
    assert store.is_blacked_out("j") is False


def test_remove_expired_cleans_old_windows(store: BlackoutStore):
    store.add(BlackoutWindow("j", _ts(-20).isoformat(), _ts(-10).isoformat()))
    removed = store.remove_expired()
    assert removed == 1
    assert store.fetch("j") == []


# ---------------------------------------------------------------------------
# BlackoutGuard
# ---------------------------------------------------------------------------

def test_should_skip_returns_true_during_blackout(store: BlackoutStore, guard: BlackoutGuard):
    guard.add_window("deploy", _ts(-5).isoformat(), _ts(5).isoformat(), "deploy freeze")
    assert guard.should_skip("deploy") is True


def test_should_skip_returns_false_outside_blackout(guard: BlackoutGuard):
    assert guard.should_skip("deploy") is False


def test_should_skip_sends_alert_when_alerter_provided(store: BlackoutStore):
    alerter = MagicMock()
    g = BlackoutGuard(store, alerter=alerter)
    g.add_window("nightly", _ts(-1).isoformat(), _ts(1).isoformat(), "nightly maintenance")
    result = g.should_skip("nightly")
    assert result is True
    alerter.send.assert_called_once()
    event = alerter.send.call_args[0][0]
    assert event.event_type == "blackout_skip"
    assert "nightly" in event.message


def test_no_alert_when_not_blacked_out(store: BlackoutStore):
    alerter = MagicMock()
    g = BlackoutGuard(store, alerter=alerter)
    g.should_skip("clean_job")
    alerter.send.assert_not_called()
