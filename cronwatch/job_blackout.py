"""Blackout windows: prevent jobs from running during scheduled maintenance periods."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class BlackoutWindow:
    job_name: str
    start_iso: str   # ISO-8601 UTC
    end_iso: str     # ISO-8601 UTC
    reason: str = ""

    @property
    def start(self) -> datetime:
        return datetime.fromisoformat(self.start_iso)

    @property
    def end(self) -> datetime:
        return datetime.fromisoformat(self.end_iso)

    def is_active(self, at: Optional[datetime] = None) -> bool:
        now = at or _utcnow()
        return self.start <= now <= self.end


class BlackoutStore:
    """Persist blackout windows in SQLite."""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS blackout_windows (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                job     TEXT NOT NULL,
                start   TEXT NOT NULL,
                end     TEXT NOT NULL,
                reason  TEXT NOT NULL DEFAULT ''
            )
            """
        )
        self._conn.commit()

    def add(self, window: BlackoutWindow) -> None:
        self._conn.execute(
            "INSERT INTO blackout_windows (job, start, end, reason) VALUES (?, ?, ?, ?)",
            (window.job_name, window.start_iso, window.end_iso, window.reason),
        )
        self._conn.commit()

    def remove_expired(self, before: Optional[datetime] = None) -> int:
        cutoff = (before or _utcnow()).isoformat()
        cur = self._conn.execute(
            "DELETE FROM blackout_windows WHERE end < ?", (cutoff,)
        )
        self._conn.commit()
        return cur.rowcount

    def fetch(self, job_name: str) -> List[BlackoutWindow]:
        rows = self._conn.execute(
            "SELECT job, start, end, reason FROM blackout_windows WHERE job = ?",
            (job_name,),
        ).fetchall()
        return [BlackoutWindow(job_name=r[0], start_iso=r[1], end_iso=r[2], reason=r[3]) for r in rows]

    def is_blacked_out(self, job_name: str, at: Optional[datetime] = None) -> bool:
        now = at or _utcnow()
        windows = self.fetch(job_name)
        return any(w.is_active(at=now) for w in windows)
