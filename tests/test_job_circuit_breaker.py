"""Tests for cronwatch.job_circuit_breaker."""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from cronwatch.job_circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerPolicy,
    CircuitState,
)


@pytest.fixture()
def cb() -> CircuitBreaker:
    breaker = CircuitBreaker()
    breaker.set_policy("backup", CircuitBreakerPolicy(failure_threshold=3, recovery_timeout=60))
    return breaker


def test_no_policy_never_open():
    cb = CircuitBreaker()
    assert cb.is_open("unknown_job") is False


def test_initial_state_is_closed(cb):
    assert cb.get_state("backup") == CircuitState.CLOSED


def test_failures_below_threshold_keep_closed(cb):
    cb.record_failure("backup")
    cb.record_failure("backup")
    assert cb.get_state("backup") == CircuitState.CLOSED
    assert cb.is_open("backup") is False


def test_failures_at_threshold_open_circuit(cb):
    cb.record_failure("backup")
    cb.record_failure("backup")
    state = cb.record_failure("backup")
    assert state == CircuitState.OPEN
    assert cb.is_open("backup") is True


def test_success_resets_to_closed(cb):
    for _ in range(3):
        cb.record_failure("backup")
    assert cb.is_open("backup") is True
    cb.record_success("backup")
    assert cb.get_state("backup") == CircuitState.CLOSED
    assert cb.is_open("backup") is False


def test_circuit_transitions_to_half_open_after_timeout(cb):
    for _ in range(3):
        cb.record_failure("backup")
    assert cb.is_open("backup") is True

    future = time.time() + 61
    with patch("cronwatch.job_circuit_breaker._utcnow", return_value=future):
        assert cb.is_open("backup") is False
        assert cb.get_state("backup") == CircuitState.HALF_OPEN


def test_still_open_before_recovery_timeout(cb):
    for _ in range(3):
        cb.record_failure("backup")

    future = time.time() + 30  # only half the timeout
    with patch("cronwatch.job_circuit_breaker._utcnow", return_value=future):
        assert cb.is_open("backup") is True


def test_additional_failure_keeps_open(cb):
    for _ in range(3):
        cb.record_failure("backup")
    cb.record_failure("backup")  # 4th failure
    assert cb.is_open("backup") is True


def test_multiple_jobs_independent():
    cb = CircuitBreaker()
    cb.set_policy("job_a", CircuitBreakerPolicy(failure_threshold=2, recovery_timeout=60))
    cb.set_policy("job_b", CircuitBreakerPolicy(failure_threshold=2, recovery_timeout=60))

    cb.record_failure("job_a")
    cb.record_failure("job_a")

    assert cb.is_open("job_a") is True
    assert cb.is_open("job_b") is False
