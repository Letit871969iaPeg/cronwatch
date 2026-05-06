"""Silence window support: suppress alerts during planned maintenance."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, time
from typing import List, Optional

log = logging.getLogger(__name__)


@dataclass
class SilenceWindow:
    """A recurring daily window during which alerts are suppressed."""

    job_name: str
    start: time  # UTC
    end: time    # UTC
    reason: str = ""

    def is_active(self, now: Optional[datetime] = None) -> bool:
        """Return True if *now* (UTC) falls within this silence window."""
        if now is None:
            now = datetime.utcnow()
        current = now.time().replace(second=0, microsecond=0)
        if self.start <= self.end:
            return self.start <= current <= self.end
        # Overnight window, e.g. 23:00 – 01:00
        return current >= self.start or current <= self.end


@dataclass
class Silencer:
    """Holds all silence windows and answers suppression queries."""

    windows: List[SilenceWindow] = field(default_factory=list)

    def add_window(self, window: SilenceWindow) -> None:
        self.windows.append(window)
        log.debug(
            "Registered silence window for '%s': %s–%s",
            window.job_name,
            window.start,
            window.end,
        )

    def is_silenced(self, job_name: str, now: Optional[datetime] = None) -> bool:
        """Return True if *job_name* should have alerts suppressed right now."""
        for w in self.windows:
            if w.job_name == job_name and w.is_active(now):
                log.info(
                    "Alert for '%s' suppressed by silence window (%s): %s",
                    job_name,
                    w.reason or "no reason given",
                    w,
                )
                return True
        return False


def load_silencer(raw_jobs: list) -> Silencer:
    """Build a Silencer from the 'silence_windows' list in each job config dict."""
    silencer = Silencer()
    for job in raw_jobs:
        name = job.get("name", "")
        for entry in job.get("silence_windows", []):
            start = time.fromisoformat(entry["start"])
            end = time.fromisoformat(entry["end"])
            reason = entry.get("reason", "")
            silencer.add_window(SilenceWindow(job_name=name, start=start, end=end, reason=reason))
    return silencer
