"""Tests for cronwatch.job_cooldown and cronwatch.cooldown_config."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from types import SimpleNamespace

import pytest

from cronwatch.job_cooldown import CooldownPolicy, JobCooldown
from cronwatch.cooldown_config import load_cooldown_manager


@pytest.fixture()
def cooldown() -> JobCooldown:
    return JobCooldown(db_path=":memory:")


def _ts(offset_seconds: float = 0.0) -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=offset_seconds)


# ---------------------------------------------------------------------------
# CooldownPolicy / JobCooldown
# ---------------------------------------------------------------------------

def test_no_policy_never_cooling_down(cooldown: JobCooldown) -> None:
    cooldown.record_completion("backup", completed_at=_ts())
    assert cooldown.is_cooling_down("backup", now=_ts(10)) is False


def test_within_cooldown_returns_true(cooldown: JobCooldown) -> None:
    cooldown.set_policy(CooldownPolicy(job_name="backup", min_interval_seconds=300))
    cooldown.record_completion("backup", completed_at=_ts())
    assert cooldown.is_cooling_down("backup", now=_ts(100)) is True


def test_after_cooldown_returns_false(cooldown: JobCooldown) -> None:
    cooldown.set_policy(CooldownPolicy(job_name="backup", min_interval_seconds=300))
    cooldown.record_completion("backup", completed_at=_ts())
    assert cooldown.is_cooling_down("backup", now=_ts(301)) is False


def test_exactly_at_boundary_not_cooling(cooldown: JobCooldown) -> None:
    cooldown.set_policy(CooldownPolicy(job_name="backup", min_interval_seconds=300))
    cooldown.record_completion("backup", completed_at=_ts())
    # elapsed == min_interval_seconds → not cooling
    assert cooldown.is_cooling_down("backup", now=_ts(300)) is False


def test_no_prior_completion_not_cooling(cooldown: JobCooldown) -> None:
    cooldown.set_policy(CooldownPolicy(job_name="backup", min_interval_seconds=300))
    assert cooldown.is_cooling_down("backup", now=_ts()) is False


def test_seconds_remaining_within_window(cooldown: JobCooldown) -> None:
    cooldown.set_policy(CooldownPolicy(job_name="sync", min_interval_seconds=60))
    cooldown.record_completion("sync", completed_at=_ts())
    remaining = cooldown.seconds_remaining("sync", now=_ts(20))
    assert abs(remaining - 40.0) < 0.01


def test_seconds_remaining_after_window_is_zero(cooldown: JobCooldown) -> None:
    cooldown.set_policy(CooldownPolicy(job_name="sync", min_interval_seconds=60))
    cooldown.record_completion("sync", completed_at=_ts())
    assert cooldown.seconds_remaining("sync", now=_ts(120)) == 0.0


def test_seconds_remaining_no_policy_is_zero(cooldown: JobCooldown) -> None:
    cooldown.record_completion("sync", completed_at=_ts())
    assert cooldown.seconds_remaining("sync", now=_ts(5)) == 0.0


def test_multiple_completions_uses_latest(cooldown: JobCooldown) -> None:
    cooldown.set_policy(CooldownPolicy(job_name="report", min_interval_seconds=120))
    cooldown.record_completion("report", completed_at=_ts(0))
    cooldown.record_completion("report", completed_at=_ts(200))  # more recent
    # 50 s after the latest completion → still cooling
    assert cooldown.is_cooling_down("report", now=_ts(250)) is True
    # 130 s after the latest completion → done
    assert cooldown.is_cooling_down("report", now=_ts(330)) is False


# ---------------------------------------------------------------------------
# load_cooldown_manager
# ---------------------------------------------------------------------------

def test_load_cooldown_manager_from_namespace() -> None:
    job = SimpleNamespace(name="etl", cooldown_seconds=600)
    cfg = SimpleNamespace(jobs=[job])
    mgr = load_cooldown_manager(cfg)
    mgr.record_completion("etl", completed_at=_ts())
    assert mgr.is_cooling_down("etl", now=_ts(100)) is True


def test_load_cooldown_manager_from_dict() -> None:
    cfg = SimpleNamespace(jobs=[{"name": "cleanup", "cooldown_seconds": 30}])
    mgr = load_cooldown_manager(cfg)
    mgr.record_completion("cleanup", completed_at=_ts())
    assert mgr.is_cooling_down("cleanup", now=_ts(10)) is True


def test_load_skips_job_without_cooldown() -> None:
    cfg = SimpleNamespace(jobs=[SimpleNamespace(name="ping")])
    mgr = load_cooldown_manager(cfg)
    mgr.record_completion("ping", completed_at=_ts())
    assert mgr.is_cooling_down("ping", now=_ts(5)) is False


def test_load_skips_job_without_name() -> None:
    cfg = SimpleNamespace(jobs=[{"cooldown_seconds": 60}])
    mgr = load_cooldown_manager(cfg)  # should not raise
    assert mgr.seconds_remaining("unknown") == 0.0
