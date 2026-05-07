"""Job timeout enforcement: tracks running jobs and emits alerts when they exceed their configured max duration."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from cronwatch.alerter import AlertEvent, Alerter
from cronwatch.config import JobConfig
from cronwatch.tracker import JobTracker

log = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class TimeoutChecker:
    """Checks running jobs against their max_duration_seconds and fires alerts on breach."""

    def __init__(self, tracker: JobTracker, alerter: Alerter) -> None:
        self._tracker = tracker
        self._alerter = alerter
        # Track which jobs have already had a timeout alert fired this run
        # so we don't spam; key = job_name, value = start_time of the run
        self._alerted: dict[str, datetime] = {}

    def check_job(self, cfg: JobConfig) -> None:
        """Inspect *cfg* and alert if the job is running beyond its allowed duration."""
        max_dur: Optional[int] = getattr(cfg, "max_duration_seconds", None)
        if max_dur is None:
            return

        record = self._tracker.get(cfg.name)
        if record is None or not record.is_running():
            # Job finished or never started — clear any stale alert state
            self._alerted.pop(cfg.name, None)
            return

        start: datetime = record.started_at  # type: ignore[attr-defined]
        elapsed = (_utcnow() - start).total_seconds()

        if elapsed <= max_dur:
            return

        # Only alert once per start-time to avoid flooding
        if self._alerted.get(cfg.name) == start:
            return

        self._alerted[cfg.name] = start
        overtime = elapsed - max_dur
        event = AlertEvent(
            job_name=cfg.name,
            event_type="timeout",
            message=(
                f"Job '{cfg.name}' has been running for {elapsed:.0f}s, "
                f"exceeding max_duration_seconds={max_dur} by {overtime:.0f}s."
            ),
        )
        log.warning(event.message)
        self._alerter.send(event)
