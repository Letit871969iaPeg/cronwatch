"""PauseGuard: thin helper used by the Scheduler and JobWatcher to skip
paused jobs and emit an audit-log entry when a run is suppressed.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from cronwatch.audit_log import AuditLog
from cronwatch.job_pause import PauseStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PauseGuard:
    """Decide whether a job should be skipped due to an active pause.

    Parameters
    ----------
    pause_store:
        Persistent pause state.
    audit_log:
        Optional audit log; when provided a ``job_skipped_paused`` event is
        recorded each time a run is suppressed.
    """

    def __init__(
        self,
        pause_store: PauseStore,
        audit_log: Optional[AuditLog] = None,
    ) -> None:
        self._store = pause_store
        self._audit = audit_log

    def should_skip(self, job_name: str) -> bool:
        """Return True (and optionally audit) if the job is currently paused."""
        if not self._store.is_paused(job_name):
            return False

        if self._audit is not None:
            entry = self._store.get(job_name)
            reason = entry.reason if entry else ""
            self._audit.record(
                job_name=job_name,
                event_type="job_skipped_paused",
                detail=f"run suppressed — pause reason: {reason or '(none)'}" ,
            )
        return True

    def pause(self, job_name: str, reason: str = "") -> None:
        """Convenience wrapper: pause a job and audit the action."""
        self._store.pause(job_name, reason=reason)
        if self._audit is not None:
            self._audit.record(
                job_name=job_name,
                event_type="job_paused",
                detail=reason or "(no reason given)",
            )

    def resume(self, job_name: str) -> None:
        """Convenience wrapper: resume a job and audit the action."""
        self._store.resume(job_name)
        if self._audit is not None:
            self._audit.record(
                job_name=job_name,
                event_type="job_resumed",
                detail="",
            )
