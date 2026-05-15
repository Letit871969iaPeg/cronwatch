"""Checks job records against SLA policies and fires alerts on breach."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional

from cronwatch.alerter import AlertEvent, Alerter
from cronwatch.job_sla import SLAPolicy, SLAStore
from cronwatch.tracker import JobTracker


class SLAChecker:
    """Evaluate SLA policies for all tracked jobs."""

    def __init__(
        self,
        tracker: JobTracker,
        alerter: Alerter,
        store: SLAStore,
    ) -> None:
        self._tracker = tracker
        self._alerter = alerter
        self._store = store
        self._policies: Dict[str, SLAPolicy] = {}

    def set_policy(self, policy: SLAPolicy) -> None:
        self._policies[policy.job_name] = policy

    def check_job(self, job_name: str) -> None:
        policy = self._policies.get(job_name)
        if policy is None:
            return

        record = self._tracker.get(job_name)
        if record is None or record.last_duration_seconds is None:
            return

        # Duration breach
        if record.last_duration_seconds > policy.max_duration_seconds:
            reason = (
                f"duration {record.last_duration_seconds:.1f}s exceeded "
                f"SLA of {policy.max_duration_seconds:.1f}s"
            )
            self._store.record_breach(job_name, reason)
            self._alerter.send(
                AlertEvent(
                    job_name=job_name,
                    event_type="sla_breach",
                    message=f"SLA breach for '{job_name}': {reason}",
                )
            )

        # Deadline breach
        if policy.deadline_time and record.last_end_time:
            hh, mm = (int(x) for x in policy.deadline_time.split(":"))
            end: datetime = record.last_end_time
            deadline = end.replace(hour=hh, minute=mm, second=0, microsecond=0,
                                   tzinfo=timezone.utc)
            if end > deadline:
                reason = (
                    f"finished at {end.strftime('%H:%M:%S')} UTC, "
                    f"past deadline {policy.deadline_time} UTC"
                )
                self._store.record_breach(job_name, reason)
                self._alerter.send(
                    AlertEvent(
                        job_name=job_name,
                        event_type="sla_deadline_breach",
                        message=f"SLA deadline breach for '{job_name}': {reason}",
                    )
                )

    def check_all(self) -> None:
        for job_name in list(self._policies):
            self.check_job(job_name)
