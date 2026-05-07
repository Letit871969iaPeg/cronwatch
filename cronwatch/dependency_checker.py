"""Check that cron job dependencies (other jobs) completed successfully before running."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from cronwatch.tracker import JobTracker
from cronwatch.alerter import Alerter, AlertEvent


@dataclass
class DependencyPolicy:
    """Defines which jobs must have succeeded before this job is allowed to run."""
    job_name: str
    depends_on: List[str] = field(default_factory=list)
    max_age_seconds: Optional[int] = None  # how fresh the dependency run must be


class DependencyChecker:
    """Validates that upstream job dependencies are satisfied."""

    def __init__(self, tracker: JobTracker, alerter: Alerter) -> None:
        self._tracker = tracker
        self._alerter = alerter
        self._policies: dict[str, DependencyPolicy] = {}

    def set_policy(self, policy: DependencyPolicy) -> None:
        self._policies[policy.job_name] = policy

    def check(self, job_name: str, now: Optional[float] = None) -> bool:
        """Return True if all dependencies are satisfied, False otherwise.

        Emits an alert for each unsatisfied dependency.
        """
        import time
        if now is None:
            now = time.time()

        policy = self._policies.get(job_name)
        if policy is None or not policy.depends_on:
            return True

        all_ok = True
        for dep in policy.depends_on:
            record = self._tracker.get(dep)
            if record is None:
                self._alerter.send(AlertEvent(
                    job_name=job_name,
                    kind="dependency_missing",
                    message=f"Dependency '{dep}' has no recorded runs.",
                ))
                all_ok = False
                continue

            if record.last_status != "success":
                self._alerter.send(AlertEvent(
                    job_name=job_name,
                    kind="dependency_failed",
                    message=(
                        f"Dependency '{dep}' last status is "
                        f"'{record.last_status}', expected 'success'."
                    ),
                ))
                all_ok = False
                continue

            if policy.max_age_seconds is not None and record.last_end is not None:
                age = now - record.last_end
                if age > policy.max_age_seconds:
                    self._alerter.send(AlertEvent(
                        job_name=job_name,
                        kind="dependency_stale",
                        message=(
                            f"Dependency '{dep}' last success was {age:.0f}s ago, "
                            f"max allowed is {policy.max_age_seconds}s."
                        ),
                    ))
                    all_ok = False

        return all_ok
