"""Pause/resume support for individual cron jobs.

A paused job is skipped by the scheduler and watcher without
raising alerts.  Pause state is persisted in SQLite so it
survives daemon restarts.
"""
from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class PauseEntry:
    job_name: str
    paused_at: datetime
    reason: str = ""
    paused_until: Optional[datetime] = None


class PauseStore:
    """Persist pause state for jobs in a SQLite database."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._lock = threading.Lock()
        with self._connect() as conn:
            self._init_schema(conn)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS job_pauses (
                job_name     TEXT PRIMARY KEY,
                paused_at    TEXT NOT NULL,
                reason       TEXT NOT NULL DEFAULT '',
                paused_until TEXT
            )
            """
        )
        conn.commit()

    def pause(self, job_name: str, reason: str = "",
              paused_until: Optional[datetime] = None) -> None:
        now = _utcnow().isoformat()
        until = paused_until.isoformat() if paused_until else None
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO job_pauses (job_name, paused_at, reason, paused_until)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(job_name) DO UPDATE SET
                        paused_at=excluded.paused_at,
                        reason=excluded.reason,
                        paused_until=excluded.paused_until
                    """,
                    (job_name, now, reason, until),
                )
                conn.commit()

    def resume(self, job_name: str) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "DELETE FROM job_pauses WHERE job_name = ?", (job_name,)
                )
                conn.commit()

    def is_paused(self, job_name: str) -> bool:
        """Return True if the job is currently paused (respects paused_until)."""
        entry = self.get(job_name)
        if entry is None:
            return False
        if entry.paused_until is not None and _utcnow() >= entry.paused_until:
            self.resume(job_name)  # auto-expire
            return False
        return True

    def get(self, job_name: str) -> Optional[PauseEntry]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM job_pauses WHERE job_name = ?", (job_name,)
            ).fetchone()
        if row is None:
            return None
        return PauseEntry(
            job_name=row["job_name"],
            paused_at=datetime.fromisoformat(row["paused_at"]),
            reason=row["reason"] or "",
            paused_until=(
                datetime.fromisoformat(row["paused_until"])
                if row["paused_until"]
                else None
            ),
        )

    def list_paused(self) -> list[PauseEntry]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM job_pauses ORDER BY paused_at"
            ).fetchall()
        entries = []
        for row in rows:
            entries.append(
                PauseEntry(
                    job_name=row["job_name"],
                    paused_at=datetime.fromisoformat(row["paused_at"]),
                    reason=row["reason"] or "",
                    paused_until=(
                        datetime.fromisoformat(row["paused_until"])
                        if row["paused_until"]
                        else None
                    ),
                )
            )
        return entries
