"""Tests for SLA tracking: SLAStore, SLAChecker, and sla_config loader."""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest

from cronwatch.alerter import AlertEvent
from cronwatch.job_sla import SLAPolicy, SLAStore
from cronwatch.sla_checker import SLAChecker
from cronwatch.sla_config import load_sla_policies


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def store():
    return SLAStore(db_path=":memory:")


@pytest.fixture()
def alerter():
    return MagicMock()


@pytest.fixture()
def tracker():
    return MagicMock()


@pytest.fixture()
def checker(tracker, alerter, store):
    return SLAChecker(tracker=tracker, alerter=alerter, store=store)


# ---------------------------------------------------------------------------
# SLAStore
# ---------------------------------------------------------------------------

def test_record_and_fetch(store):
    store.record_breach("backup", "ran 120s, limit 60s")
    rows = store.fetch_breaches()
    assert len(rows) == 1
    assert rows[0]["job_name"] == "backup"
    assert "120s" in rows[0]["reason"]


def test_fetch_by_job_filters(store):
    store.record_breach("job_a", "too slow")
    store.record_breach("job_b", "too slow")
    rows = store.fetch_breaches(job_name="job_a")
    assert all(r["job_name"] == "job_a" for r in rows)


def test_fetch_empty_returns_empty(store):
    assert store.fetch_breaches() == []


# ---------------------------------------------------------------------------
# SLAChecker — duration breach
# ---------------------------------------------------------------------------

def test_duration_breach_fires_alert(checker, tracker, alerter, store):
    checker.set_policy(SLAPolicy(job_name="etl", max_duration_seconds=30.0))
    record = MagicMock(last_duration_seconds=45.0, last_end_time=None)
    tracker.get.return_value = record

    checker.check_job("etl")

    alerter.send.assert_called_once()
    event: AlertEvent = alerter.send.call_args[0][0]
    assert event.event_type == "sla_breach"
    assert "etl" in event.message
    assert len(store.fetch_breaches()) == 1


def test_within_sla_no_alert(checker, tracker, alerter):
    checker.set_policy(SLAPolicy(job_name="etl", max_duration_seconds=60.0))
    record = MagicMock(last_duration_seconds=30.0, last_end_time=None)
    tracker.get.return_value = record

    checker.check_job("etl")
    alerter.send.assert_not_called()


def test_no_policy_no_alert(checker, tracker, alerter):
    tracker.get.return_value = MagicMock(last_duration_seconds=999.0)
    checker.check_job("unknown_job")
    alerter.send.assert_not_called()


def test_no_record_no_alert(checker, tracker, alerter):
    checker.set_policy(SLAPolicy(job_name="etl", max_duration_seconds=10.0))
    tracker.get.return_value = None
    checker.check_job("etl")
    alerter.send.assert_not_called()


# ---------------------------------------------------------------------------
# SLAChecker — deadline breach
# ---------------------------------------------------------------------------

def test_deadline_breach_fires_alert(checker, tracker, alerter, store):
    checker.set_policy(
        SLAPolicy(job_name="report", max_duration_seconds=9999.0, deadline_time="02:00")
    )
    late_end = datetime(2024, 1, 1, 2, 30, 0, tzinfo=timezone.utc)
    record = MagicMock(last_duration_seconds=10.0, last_end_time=late_end)
    tracker.get.return_value = record

    checker.check_job("report")

    assert alerter.send.call_count == 1
    event: AlertEvent = alerter.send.call_args[0][0]
    assert event.event_type == "sla_deadline_breach"


# ---------------------------------------------------------------------------
# sla_config loader
# ---------------------------------------------------------------------------

def test_load_sla_policies_from_config():
    job = SimpleNamespace(
        name="cleanup",
        sla={"max_duration_seconds": 120, "deadline_time": "03:00"},
    )
    cfg = SimpleNamespace(jobs=[job])
    policies = load_sla_policies(cfg)
    assert len(policies) == 1
    assert policies[0].job_name == "cleanup"
    assert policies[0].max_duration_seconds == 120.0
    assert policies[0].deadline_time == "03:00"


def test_load_sla_policies_skips_missing_sla():
    job = SimpleNamespace(name="noop", sla=None)
    cfg = SimpleNamespace(jobs=[job])
    assert load_sla_policies(cfg) == []


def test_load_sla_policies_empty_jobs():
    cfg = SimpleNamespace(jobs=[])
    assert load_sla_policies(cfg) == []
