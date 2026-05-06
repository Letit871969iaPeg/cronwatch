"""Checks job records for drift and failure conditions, emitting AlertEvents."""

import logging
from datetime import datetime, timezone
from typing import List

from cronwatch.alerter import AlertEvent, Alerter
from cronwatch.config import JobConfig
from cronwatch.tracker import JobRecord, JobTracker

logger = logging.getLogger(__name__)


class DriftChecker:
    """Compares recent job execution data against configured thresholds."""

    def __init__(self, tracker: JobTracker, alerter: Alerter):
        self.tracker = tracker
        self.alerter = alerter

    def check_job(self, job_cfg: JobConfig) -> List[AlertEvent]:
        """Run all checks for a single job and return any triggered events."""
        events: List[AlertEvent] = []
        record: JobRecord = self.tracker.get(job_cfg.name)

        if record is None:
            events.append(
                AlertEvent(
                    job_name=job_cfg.name,
                    reason="missing",
                    details="No execution record found for this job.",
                    severity="warning",
                )
            )
            return events

        # Failure check
        if record.last_status == "failure":
            events.append(
                AlertEvent(
                    job_name=job_cfg.name,
                    reason="failure",
                    details=f"Last run at {record.last_start} exited with failure.",
                    severity="critical",
                )
            )

        # Duration drift check
        if (
            job_cfg.expected_duration_seconds is not None
            and record.last_duration_seconds is not None
        ):
            tolerance = job_cfg.drift_tolerance_pct / 100.0
            threshold = job_cfg.expected_duration_seconds * (1 + tolerance)
            if record.last_duration_seconds > threshold:
                events.append(
                    AlertEvent(
                        job_name=job_cfg.name,
                        reason="drift",
                        details=(
                            f"Duration {record.last_duration_seconds:.1f}s exceeds "
                            f"expected {job_cfg.expected_duration_seconds}s "
                            f"by >{job_cfg.drift_tolerance_pct}%."
                        ),
                        severity="warning",
                    )
                )

        # Overdue check
        if job_cfg.max_interval_seconds is not None and record.last_start is not None:
            now = datetime.now(tz=timezone.utc)
            elapsed = (now - record.last_start).total_seconds()
            if elapsed > job_cfg.max_interval_seconds:
                events.append(
                    AlertEvent(
                        job_name=job_cfg.name,
                        reason="missing",
                        details=(
                            f"Job has not run in {elapsed:.0f}s; "
                            f"max interval is {job_cfg.max_interval_seconds}s."
                        ),
                        severity="critical",
                    )
                )

        for event in events:
            self.alerter.send(event)

        return events
