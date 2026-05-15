"""Tests for cronwatch.circuit_breaker_config."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from cronwatch.circuit_breaker_config import load_circuit_breaker
from cronwatch.job_circuit_breaker import CircuitBreakerPolicy, CircuitState


def _cfg(*jobs):
    cfg = MagicMock()
    cfg.jobs = list(jobs)
    return cfg


def _job(name, circuit_breaker=None):
    j = SimpleNamespace(name=name, circuit_breaker=circuit_breaker)
    return j


def test_no_circuit_breaker_field_skipped():
    cfg = _cfg(_job("nightly"))
    cb = load_circuit_breaker(cfg)
    assert cb.is_open("nightly") is False


def test_circuit_breaker_as_dict_parsed():
    cfg = _cfg(_job("backup", circuit_breaker={"failure_threshold": 2, "recovery_timeout": 120}))
    cb = load_circuit_breaker(cfg)
    policy = cb._policies.get("backup")
    assert policy is not None
    assert policy.failure_threshold == 2
    assert policy.recovery_timeout == 120


def test_circuit_breaker_as_namespace_parsed():
    raw = SimpleNamespace(failure_threshold=5, recovery_timeout=600)
    cfg = _cfg(_job("deploy", circuit_breaker=raw))
    cb = load_circuit_breaker(cfg)
    policy = cb._policies.get("deploy")
    assert policy.failure_threshold == 5
    assert policy.recovery_timeout == 600


def test_defaults_applied_when_keys_missing():
    cfg = _cfg(_job("sync", circuit_breaker={}))
    cb = load_circuit_breaker(cfg)
    policy = cb._policies["sync"]
    assert policy.failure_threshold == 3
    assert policy.recovery_timeout == 300


def test_multiple_jobs_each_get_policy():
    cfg = _cfg(
        _job("job_a", circuit_breaker={"failure_threshold": 1}),
        _job("job_b", circuit_breaker={"failure_threshold": 4}),
        _job("job_c"),  # no policy
    )
    cb = load_circuit_breaker(cfg)
    assert "job_a" in cb._policies
    assert "job_b" in cb._policies
    assert "job_c" not in cb._policies


def test_empty_jobs_list():
    cfg = _cfg()
    cb = load_circuit_breaker(cfg)
    assert cb._policies == {}
