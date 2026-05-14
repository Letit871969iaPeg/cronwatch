"""Guard that checks blackout windows before allowing a job to run."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from cronwatch.job_blackout import BlackoutStore
from cronwatch.alerter import Alerter, AlertEvent


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class BlackoutGuard:
    """Decide whether a job should be skipped due to an active blackout window.

    Parameters
    ----------
    store:
        Backing :class:`BlackoutStore` instance.
    alerter:
        Optional alerter; when provided an ``INFO``-level alert is sent on skip.
    """

    def __init__(self, store: BlackoutStore, alerter: Optional[Alerter] = None) -> None:
        self._store = store
        self._alerter = alerter

    def should_skip(self, job_name: str, at: Optional[datetime] = None) -> bool:
        """Return *True* when the job is currently blacked out."""
        now = at or _utcnow()
        blacked_out = self._store.is_blacked_out(job_name, at=now)
        if blacked_out and self._alerter is not None:
            windows = self._store.fetch(job_name)
            active = next((w for w in windows if w.is_active(at=now)), None)
            reason = active.reason if active else ""
            event = AlertEvent(
                job_name=job_name,
                event_type="blackout_skip",
                message=(
                    f"Job '{job_name}' skipped: active blackout window"
                    + (f" ({reason})" if reason else "")
                ),
                severity="info",
            )
            self._alerter.send(event)
        return blacked_out

    def add_window(
        self,
        job_name: str,
        start_iso: str,
        end_iso: str,
        reason: str = "",
    ) -> None:
        """Convenience wrapper to add a blackout window for *job_name*."""
        from cronwatch.job_blackout import BlackoutWindow

        self._store.add(
            BlackoutWindow(
                job_name=job_name,
                start_iso=start_iso,
                end_iso=end_iso,
                reason=reason,
            )
        )
