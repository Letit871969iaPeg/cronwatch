"""Checks job fingerprints on each scheduler tick and emits alerts on change."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from cronwatch.alerter import AlertEvent, Alerter
from cronwatch.config import JobConfig
from cronwatch.job_fingerprint import FingerprintStore


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class FingerprintChecker:
    """Compares current job config against stored fingerprints and alerts on drift."""

    def __init__(self, store: FingerprintStore, alerter: Alerter) -> None:
        self._store = store
        self._alerter = alerter

    def check(self, jobs: List[JobConfig]) -> None:
        """Iterate over all configured jobs and emit an alert for any that changed."""
        now = _utcnow_iso()
        for job in jobs:
            command = job.command
            schedule = job.schedule
            if self._store.has_changed(job.name, command, schedule):
                old = self._store.get(job.name)
                self._emit_alert(job.name, old, command, schedule)
            # Always upsert so the current definition is authoritative.
            self._store.upsert(job.name, command, schedule, now)

    def _emit_alert(
        self,
        job_name: str,
        old: object,
        new_command: str,
        new_schedule: str,
    ) -> None:
        if old is None:
            detail = "first seen — fingerprint recorded"
        else:
            detail = (
                f"command or schedule changed "
                f"(was cmd={old.command!r} sched={old.schedule!r}, "
                f"now cmd={new_command!r} sched={new_schedule!r})"
            )
        event = AlertEvent(
            job_name=job_name,
            event_type="fingerprint_changed",
            message=f"Job definition changed for '{job_name}': {detail}",
            timestamp=_utcnow_iso(),
        )
        self._alerter.send(event)
