"""SLA tracking: detect when a job fails to complete within a deadline."""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class SLAPolicy:
    """Per-job SLA configuration."""
    job_name: str
    max_duration_seconds: float  # breach if runtime exceeds this
    deadline_time: Optional[str] = None  # HH:MM UTC — job must *finish* by this time


class SLAStore:
    """Persists SLA breach events in SQLite."""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sla_breaches (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                job_name TEXT    NOT NULL,
                reason   TEXT    NOT NULL,
                ts       TEXT    NOT NULL
            )
            """
        )
        self._conn.commit()

    def record_breach(self, job_name: str, reason: str) -> None:
        ts = _utcnow().isoformat()
        self._conn.execute(
            "INSERT INTO sla_breaches (job_name, reason, ts) VALUES (?, ?, ?)",
            (job_name, reason, ts),
        )
        self._conn.commit()

    def fetch_breaches(self, job_name: Optional[str] = None, limit: int = 50):
        if job_name:
            cur = self._conn.execute(
                "SELECT job_name, reason, ts FROM sla_breaches WHERE job_name=? ORDER BY id DESC LIMIT ?",
                (job_name, limit),
            )
        else:
            cur = self._conn.execute(
                "SELECT job_name, reason, ts FROM sla_breaches ORDER BY id DESC LIMIT ?",
                (limit,),
            )
        return [{"job_name": r[0], "reason": r[1], "ts": r[2]} for r in cur.fetchall()]
