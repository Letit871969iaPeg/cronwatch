"""Tests for job_fingerprint and fingerprint_checker."""

from __future__ import annotations

import types
from unittest.mock import MagicMock, call

import pytest

from cronwatch.job_fingerprint import FingerprintStore
from cronwatch.fingerprint_checker import FingerprintChecker


@pytest.fixture()
def store(tmp_path):
    return FingerprintStore(str(tmp_path / "fp.db"))


def _job(name="backup", command="/usr/bin/backup.sh", schedule="0 2 * * *"):
    j = types.SimpleNamespace(name=name, command=command, schedule=schedule)
    return j


# ---------------------------------------------------------------------------
# FingerprintStore unit tests
# ---------------------------------------------------------------------------

def test_compute_is_deterministic(store):
    a = store.compute("/bin/foo", "0 * * * *")
    b = store.compute("/bin/foo", "0 * * * *")
    assert a == b


def test_compute_differs_on_command_change(store):
    a = store.compute("/bin/foo", "0 * * * *")
    b = store.compute("/bin/bar", "0 * * * *")
    assert a != b


def test_compute_differs_on_schedule_change(store):
    a = store.compute("/bin/foo", "0 * * * *")
    b = store.compute("/bin/foo", "5 * * * *")
    assert a != b


def test_get_unknown_returns_none(store):
    assert store.get("no-such-job") is None


def test_upsert_and_get(store):
    store.upsert("myjob", "/bin/myjob", "@daily", "2024-01-01T00:00:00+00:00")
    entry = store.get("myjob")
    assert entry is not None
    assert entry.job_name == "myjob"
    assert entry.command == "/bin/myjob"
    assert entry.schedule == "@daily"
    assert len(entry.fingerprint) == 16


def test_upsert_overwrites_on_change(store):
    store.upsert("myjob", "/bin/old", "@daily", "2024-01-01T00:00:00+00:00")
    store.upsert("myjob", "/bin/new", "@hourly", "2024-06-01T00:00:00+00:00")
    entry = store.get("myjob")
    assert entry.command == "/bin/new"
    assert entry.schedule == "@hourly"


def test_has_changed_true_when_unknown(store):
    assert store.has_changed("unknown", "/bin/x", "@daily") is True


def test_has_changed_false_when_same(store):
    store.upsert("j", "/bin/x", "@daily", "2024-01-01T00:00:00+00:00")
    assert store.has_changed("j", "/bin/x", "@daily") is False


def test_has_changed_true_when_different(store):
    store.upsert("j", "/bin/x", "@daily", "2024-01-01T00:00:00+00:00")
    assert store.has_changed("j", "/bin/x", "@hourly") is True


# ---------------------------------------------------------------------------
# FingerprintChecker integration tests
# ---------------------------------------------------------------------------

@pytest.fixture()
def alerter():
    return MagicMock()


@pytest.fixture()
def checker(store, alerter):
    return FingerprintChecker(store, alerter)


def test_first_check_emits_alert_and_records(checker, alerter, store):
    checker.check([_job()])
    alerter.send.assert_called_once()
    event = alerter.send.call_args[0][0]
    assert event.event_type == "fingerprint_changed"
    assert "first seen" in event.message
    assert store.get("backup") is not None


def test_second_check_no_change_no_alert(checker, alerter):
    checker.check([_job()])
    alerter.reset_mock()
    checker.check([_job()])
    alerter.send.assert_not_called()


def test_changed_command_emits_alert(checker, alerter):
    checker.check([_job(command="/bin/old")])
    alerter.reset_mock()
    checker.check([_job(command="/bin/new")])
    alerter.send.assert_called_once()
    msg = alerter.send.call_args[0][0].message
    assert "/bin/new" in msg


def test_multiple_jobs_each_checked(checker, alerter):
    jobs = [_job("a", "/bin/a", "@daily"), _job("b", "/bin/b", "@hourly")]
    checker.check(jobs)
    assert alerter.send.call_count == 2
