"""Tests for cronwatch.dependency_checker."""

from __future__ import annotations

from unittest.mock import MagicMock, call
import pytest

from cronwatch.dependency_checker import DependencyChecker, DependencyPolicy
from cronwatch.tracker import JobRecord


@pytest.fixture()
def alerter():
    return MagicMock()


@pytest.fixture()
def tracker():
    return MagicMock()


@pytest.fixture()
def checker(tracker, alerter):
    return DependencyChecker(tracker=tracker, alerter=alerter)


def _record(status: str, last_end: float | None = 1000.0) -> JobRecord:
    r = MagicMock(spec=JobRecord)
    r.last_status = status
    r.last_end = last_end
    return r


def test_no_policy_returns_true(checker, tracker, alerter):
    assert checker.check("job_a", now=2000.0) is True
    alerter.send.assert_not_called()


def test_empty_depends_on_returns_true(checker, tracker, alerter):
    checker.set_policy(DependencyPolicy(job_name="job_a", depends_on=[]))
    assert checker.check("job_a", now=2000.0) is True
    alerter.send.assert_not_called()


def test_dependency_missing_emits_alert(checker, tracker, alerter):
    checker.set_policy(DependencyPolicy(job_name="job_a", depends_on=["job_b"]))
    tracker.get.return_value = None

    result = checker.check("job_a", now=2000.0)

    assert result is False
    alerter.send.assert_called_once()
    event = alerter.send.call_args[0][0]
    assert event.kind == "dependency_missing"
    assert "job_b" in event.message


def test_dependency_failed_emits_alert(checker, tracker, alerter):
    checker.set_policy(DependencyPolicy(job_name="job_a", depends_on=["job_b"]))
    tracker.get.return_value = _record(status="failure")

    result = checker.check("job_a", now=2000.0)

    assert result is False
    event = alerter.send.call_args[0][0]
    assert event.kind == "dependency_failed"
    assert "failure" in event.message


def test_dependency_stale_emits_alert(checker, tracker, alerter):
    checker.set_policy(
        DependencyPolicy(job_name="job_a", depends_on=["job_b"], max_age_seconds=300)
    )
    tracker.get.return_value = _record(status="success", last_end=1000.0)

    result = checker.check("job_a", now=2000.0)  # age = 1000s > 300s

    assert result is False
    event = alerter.send.call_args[0][0]
    assert event.kind == "dependency_stale"
    assert "1000" in event.message


def test_fresh_success_returns_true(checker, tracker, alerter):
    checker.set_policy(
        DependencyPolicy(job_name="job_a", depends_on=["job_b"], max_age_seconds=3600)
    )
    tracker.get.return_value = _record(status="success", last_end=1900.0)

    result = checker.check("job_a", now=2000.0)  # age = 100s < 3600s

    assert result is True
    alerter.send.assert_not_called()


def test_multiple_deps_all_must_pass(checker, tracker, alerter):
    checker.set_policy(
        DependencyPolicy(job_name="job_a", depends_on=["job_b", "job_c"])
    )
    tracker.get.side_effect = [
        _record(status="success"),
        _record(status="failure"),
    ]

    result = checker.check("job_a", now=2000.0)

    assert result is False
    assert alerter.send.call_count == 1
