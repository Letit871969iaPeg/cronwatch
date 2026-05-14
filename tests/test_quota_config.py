"""Tests for cronwatch.quota_config.load_quota_manager."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from cronwatch.job_quota import QuotaPolicy
from cronwatch.quota_config import load_quota_manager


def _cfg(*jobs):
    """Build a minimal config-like namespace."""
    return SimpleNamespace(jobs=list(jobs))


def _job(name: str, quota=None):
    return SimpleNamespace(name=name, quota=quota)


# ---------------------------------------------------------------------------

def test_no_quota_field_skipped() -> None:
    cfg = _cfg(_job("daily_backup"))
    mgr = load_quota_manager(cfg)
    assert mgr.is_quota_exceeded("daily_backup") is False


def test_quota_as_dict_parsed() -> None:
    job = _job("sync", quota={"max_runs": 4, "window_seconds": 1800})
    mgr = load_quota_manager(_cfg(job))
    for _ in range(4):
        mgr.record_run("sync")
    assert mgr.is_quota_exceeded("sync") is True


def test_quota_as_namespace_parsed() -> None:
    q = SimpleNamespace(max_runs=2, window_seconds=600)
    job = _job("report", quota=q)
    mgr = load_quota_manager(_cfg(job))
    mgr.record_run("report")
    assert mgr.is_quota_exceeded("report") is False
    mgr.record_run("report")
    assert mgr.is_quota_exceeded("report") is True


def test_zero_max_runs_skips_policy() -> None:
    """A max_runs of 0 means no policy — quota never exceeded."""
    job = _job("noop", quota={"max_runs": 0, "window_seconds": 3600})
    mgr = load_quota_manager(_cfg(job))
    for _ in range(10):
        mgr.record_run("noop")
    assert mgr.is_quota_exceeded("noop") is False


def test_multiple_jobs_loaded_independently() -> None:
    jobs = [
        _job("a", quota={"max_runs": 1, "window_seconds": 3600}),
        _job("b", quota={"max_runs": 5, "window_seconds": 3600}),
    ]
    mgr = load_quota_manager(_cfg(*jobs))
    mgr.record_run("a")
    mgr.record_run("b")
    assert mgr.is_quota_exceeded("a") is True
    assert mgr.is_quota_exceeded("b") is False


def test_empty_jobs_list() -> None:
    mgr = load_quota_manager(_cfg())
    assert mgr.is_quota_exceeded("anything") is False
