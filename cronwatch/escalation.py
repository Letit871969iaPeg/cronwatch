"""Escalation policy: track consecutive failures and emit escalated alerts."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

from cronwatch.alerter import AlertEvent, Alerter


@dataclass
class EscalationPolicy:
    """Configuration for escalation behaviour for a single job."""
    threshold: int = 3          # consecutive failures before escalating
    repeat_every: int = 1       # re-alert every N failures after threshold


@dataclass
class _JobState:
    consecutive_failures: int = 0
    last_escalated_at: int = 0  # failure count at which we last escalated


class EscalationManager:
    """Tracks consecutive failures per job and triggers escalated alerts."""

    def __init__(self, alerter: Alerter) -> None:
        self._alerter = alerter
        self._states: Dict[str, _JobState] = {}
        self._policies: Dict[str, EscalationPolicy] = {}

    def set_policy(self, job_name: str, policy: EscalationPolicy) -> None:
        self._policies[job_name] = policy

    def _state(self, job_name: str) -> _JobState:
        if job_name not in self._states:
            self._states[job_name] = _JobState()
        return self._states[job_name]

    def record_failure(self, job_name: str, event: AlertEvent) -> bool:
        """Record a failure; returns True if an escalated alert was sent."""
        policy = self._policies.get(job_name, EscalationPolicy())
        state = self._state(job_name)
        state.consecutive_failures += 1

        count = state.consecutive_failures
        if count < policy.threshold:
            return False

        since_last = count - state.last_escalated_at
        if state.last_escalated_at == 0 or since_last >= policy.repeat_every:
            escalated = AlertEvent(
                job_name=event.job_name,
                kind=f"ESCALATED:{event.kind}",
                message=(
                    f"[ESCALATED x{count}] {event.message}"
                ),
                timestamp=event.timestamp,
                duration_seconds=event.duration_seconds,
            )
            self._alerter.send(escalated)
            state.last_escalated_at = count
            return True
        return False

    def record_success(self, job_name: str) -> None:
        """Reset failure streak on success."""
        if job_name in self._states:
            self._states[job_name] = _JobState()

    def consecutive_failures(self, job_name: str) -> int:
        return self._state(job_name).consecutive_failures
