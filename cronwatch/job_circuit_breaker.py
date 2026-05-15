"""Circuit breaker for cron jobs: open the circuit after N consecutive
failures and keep it open until a configurable cool-down has elapsed."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional


class CircuitState(str, Enum):
    CLOSED = "closed"      # normal operation
    OPEN = "open"          # tripped; job should be skipped
    HALF_OPEN = "half_open"  # one probe attempt allowed


@dataclass
class CircuitBreakerPolicy:
    failure_threshold: int = 3   # consecutive failures before opening
    recovery_timeout: int = 300  # seconds before moving to HALF_OPEN


@dataclass
class _CircuitState:
    state: CircuitState = CircuitState.CLOSED
    consecutive_failures: int = 0
    opened_at: Optional[float] = None


def _utcnow() -> float:  # pragma: no cover
    return time.time()


class CircuitBreaker:
    """Per-job circuit breaker."""

    def __init__(self) -> None:
        self._policies: Dict[str, CircuitBreakerPolicy] = {}
        self._states: Dict[str, _CircuitState] = {}

    def set_policy(self, job_name: str, policy: CircuitBreakerPolicy) -> None:
        self._policies[job_name] = policy
        if job_name not in self._states:
            self._states[job_name] = _CircuitState()

    def _state(self, job_name: str) -> _CircuitState:
        if job_name not in self._states:
            self._states[job_name] = _CircuitState()
        return self._states[job_name]

    def is_open(self, job_name: str) -> bool:
        """Return True if the circuit is OPEN (job should be skipped)."""
        policy = self._policies.get(job_name)
        if policy is None:
            return False
        st = self._state(job_name)
        if st.state == CircuitState.OPEN:
            elapsed = _utcnow() - (st.opened_at or 0.0)
            if elapsed >= policy.recovery_timeout:
                st.state = CircuitState.HALF_OPEN
                return False
            return True
        return False

    def record_success(self, job_name: str) -> None:
        st = self._state(job_name)
        st.consecutive_failures = 0
        st.state = CircuitState.CLOSED
        st.opened_at = None

    def record_failure(self, job_name: str) -> CircuitState:
        policy = self._policies.get(job_name)
        st = self._state(job_name)
        st.consecutive_failures += 1
        if policy and st.consecutive_failures >= policy.failure_threshold:
            if st.state != CircuitState.OPEN:
                st.opened_at = _utcnow()
            st.state = CircuitState.OPEN
        return st.state

    def get_state(self, job_name: str) -> CircuitState:
        self.is_open(job_name)  # trigger transition check
        return self._state(job_name).state
