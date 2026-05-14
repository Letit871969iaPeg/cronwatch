"""Tests for cronwatch.job_priority and cronwatch.priority_config."""
import pytest

from cronwatch.job_priority import Priority, PriorityManager, PriorityPolicy
from cronwatch.priority_config import load_priority_manager


# ---------------------------------------------------------------------------
# Priority.from_str
# ---------------------------------------------------------------------------

def test_from_str_known_values():
    assert Priority.from_str("low") == Priority.LOW
    assert Priority.from_str("CRITICAL") == Priority.CRITICAL
    assert Priority.from_str(" High ") == Priority.HIGH


def test_from_str_unknown_raises():
    with pytest.raises(ValueError, match="Unknown priority"):
        Priority.from_str("urgent")


# ---------------------------------------------------------------------------
# PriorityManager
# ---------------------------------------------------------------------------

@pytest.fixture()
def manager() -> PriorityManager:
    m = PriorityManager()
    m.set_policy(PriorityPolicy("backup", Priority.HIGH, Priority.HIGH))
    m.set_policy(PriorityPolicy("cleanup", Priority.LOW, Priority.NORMAL))
    return m


def test_get_priority_returns_configured(manager):
    assert manager.get_priority("backup") == Priority.HIGH


def test_get_priority_unknown_job_returns_normal(manager):
    assert manager.get_priority("unknown") == Priority.NORMAL


def test_should_alert_meets_threshold(manager):
    assert manager.should_alert("backup", Priority.HIGH) is True
    assert manager.should_alert("backup", Priority.CRITICAL) is True


def test_should_alert_below_threshold(manager):
    assert manager.should_alert("backup", Priority.NORMAL) is False
    assert manager.should_alert("backup", Priority.LOW) is False


def test_should_alert_no_policy_always_true(manager):
    assert manager.should_alert("nonexistent") is True


def test_should_alert_uses_job_priority_when_event_priority_omitted():
    m = PriorityManager()
    m.set_policy(PriorityPolicy("job", Priority.CRITICAL, Priority.HIGH))
    # job priority (CRITICAL) >= threshold (HIGH) → True
    assert m.should_alert("job") is True


def test_jobs_at_or_above(manager):
    result = manager.jobs_at_or_above(Priority.HIGH)
    assert "backup" in result
    assert "cleanup" not in result


# ---------------------------------------------------------------------------
# load_priority_manager
# ---------------------------------------------------------------------------

def test_load_priority_manager_full_block():
    raw = [
        {
            "name": "report",
            "priority": {"level": "critical", "alert_threshold": "high"},
        }
    ]
    m = load_priority_manager(raw)
    assert m.get_priority("report") == Priority.CRITICAL
    assert m.should_alert("report", Priority.HIGH) is True
    assert m.should_alert("report", Priority.NORMAL) is False


def test_load_priority_manager_defaults_when_no_priority_block():
    raw = [{"name": "sync"}]
    m = load_priority_manager(raw)
    assert m.get_priority("sync") == Priority.NORMAL
    assert m.should_alert("sync") is True


def test_load_priority_manager_skips_nameless_jobs():
    raw = [{"priority": {"level": "high"}}]
    m = load_priority_manager(raw)
    assert m.jobs_at_or_above(Priority.LOW) == []


def test_load_priority_manager_invalid_level_falls_back_to_normal():
    raw = [{"name": "job", "priority": {"level": "super_urgent"}}]
    m = load_priority_manager(raw)
    assert m.get_priority("job") == Priority.NORMAL
