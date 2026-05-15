"""Incident tracking: open, update, and resolve incidents tied to cron job failures."""

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Incident:
    incident_id: str
    job_name: str
    opened_at: str
    reason: str
    status: str          # "open" | "resolved"
    resolved_at: Optional[str] = None
    notes: Optional[str] = None


class IncidentStore:
    def __init__(self, db_path: str = ":memory:") -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS incidents (
                incident_id TEXT PRIMARY KEY,
                job_name    TEXT NOT NULL,
                opened_at   TEXT NOT NULL,
                reason      TEXT NOT NULL,
                status      TEXT NOT NULL DEFAULT 'open',
                resolved_at TEXT,
                notes       TEXT
            )
            """
        )
        self._conn.commit()

    def open(self, job_name: str, reason: str, notes: Optional[str] = None) -> Incident:
        inc = Incident(
            incident_id=str(uuid.uuid4()),
            job_name=job_name,
            opened_at=_utcnow(),
            reason=reason,
            status="open",
            notes=notes,
        )
        self._conn.execute(
            "INSERT INTO incidents VALUES (?,?,?,?,?,?,?)",
            (inc.incident_id, inc.job_name, inc.opened_at, inc.reason,
             inc.status, inc.resolved_at, inc.notes),
        )
        self._conn.commit()
        return inc

    def resolve(self, incident_id: str, notes: Optional[str] = None) -> bool:
        resolved_at = _utcnow()
        cur = self._conn.execute(
            "UPDATE incidents SET status='resolved', resolved_at=?, notes=COALESCE(?,notes)"
            " WHERE incident_id=? AND status='open'",
            (resolved_at, notes, incident_id),
        )
        self._conn.commit()
        return cur.rowcount > 0

    def fetch(self, job_name: Optional[str] = None, status: Optional[str] = None) -> List[Incident]:
        query = "SELECT * FROM incidents WHERE 1=1"
        params: list = []
        if job_name:
            query += " AND job_name=?"
            params.append(job_name)
        if status:
            query += " AND status=?"
            params.append(status)
        query += " ORDER BY opened_at DESC"
        rows = self._conn.execute(query, params).fetchall()
        return [Incident(**dict(r)) for r in rows]

    def get(self, incident_id: str) -> Optional[Incident]:
        row = self._conn.execute(
            "SELECT * FROM incidents WHERE incident_id=?", (incident_id,)
        ).fetchone()
        return Incident(**dict(row)) if row else None
