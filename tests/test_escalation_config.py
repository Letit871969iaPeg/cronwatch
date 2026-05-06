"""Tests for load_escalation_policies."""
from __future__ import annotations

import pytest

from cronwatch.escalation import EscalationPolicy
from cronwatch.escalation_config import load_escalation_policies


RAW_JOBS = [
    {
        "name": "backup",
        "schedule": "0 2 * * *",
        "command": "backup.sh",
        "escalation": {"threshold": 4, "repeat_every": 3},
    },
    {
        "name": "cleanup",
        "schedule": "0 3 * * *",
        "command": "cleanup.sh",
        # no escalation block
    },
    {
        "name": "report",
        "schedule": "0 6 * * 1",
        "command": "report.sh",
        "escalation": {"threshold": 2},
    },
]


def test_parses_full_escalation_block():
    policies = load_escalation_policies(RAW_JOBS)
    assert "backup" in policies
    p = policies["backup"]
    assert p.threshold == 4
    assert p.repeat_every == 3


def test_job_without_escalation_not_in_result():
    policies = load_escalation_policies(RAW_JOBS)
    assert "cleanup" not in policies


def test_partial_escalation_uses_defaults():
    policies = load_escalation_policies(RAW_JOBS)
    p = policies["report"]
    assert p.threshold == 2
    assert p.repeat_every == 1  # default


def test_empty_jobs_list():
    assert load_escalation_policies([]) == {}


def test_job_missing_name_is_skipped():
    jobs = [{"schedule": "* * * * *", "escalation": {"threshold": 1}}]
    policies = load_escalation_policies(jobs)
    assert policies == {}
