"""Job retry policy: track consecutive failures and emit retry-exhausted alerts."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

from cronwatch.alerter import AlertEvent, Alerter
from cronwatch.tracker import JobTracker


@dataclass
class RetryPolicy:
    """Configuration for how many times a job may be retried before alerting."""
    max_retries: int = 3
    alert_on_exhaustion: bool = True


@dataclass
class _RetryState:
    consecutive_failures: int = 0
    exhausted_alerted: bool = False


class RetryManager:
    """Tracks consecutive job failures and fires alerts when retries are exhausted."""

    def __init__(self, alerter: Alerter, tracker: JobTracker) -> None:
        self._alerter = alerter
        self._tracker = tracker
        self._policies: Dict[str, RetryPolicy] = {}
        self._states: Dict[str, _RetryState] = {}

    def set_policy(self, job_name: str, policy: RetryPolicy) -> None:
        self._policies[job_name] = policy
        self._states.setdefault(job_name, _RetryState())

    def _state(self, job_name: str) -> _RetryState:
        return self._states.setdefault(job_name, _RetryState())

    def record_outcome(self, job_name: str, success: bool) -> None:
        """Call after each job run. Tracks failures and emits alert when exhausted."""
        state = self._state(job_name)
        policy = self._policies.get(job_name)

        if success:
            state.consecutive_failures = 0
            state.exhausted_alerted = False
            return

        state.consecutive_failures += 1

        if policy is None:
            return

        if (
            policy.alert_on_exhaustion
            and state.consecutive_failures >= policy.max_retries
            and not state.exhausted_alerted
        ):
            event = AlertEvent(
                job_name=job_name,
                event_type="retry_exhausted",
                message=(
                    f"Job '{job_name}' has failed {state.consecutive_failures} "
                    f"consecutive time(s) (max_retries={policy.max_retries})."
                ),
            )
            self._alerter.send(event)
            state.exhausted_alerted = True

    def failure_count(self, job_name: str) -> int:
        return self._state(job_name).consecutive_failures

    def is_exhausted(self, job_name: str) -> bool:
        policy = self._policies.get(job_name)
        if policy is None:
            return False
        return self._state(job_name).consecutive_failures >= policy.max_retries
