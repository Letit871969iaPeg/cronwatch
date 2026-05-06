"""Append-only audit log for cronwatch alert and escalation events."""

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


@dataclass
class AuditEntry:
    id: int
    event_type: str
    job_name: str
    message: str
    occurred_at: datetime
    extra: Optional[dict] = None


class AuditLog:
    def __init__(self, db_path: str = "cronwatch_audit.db") -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type  TEXT    NOT NULL,
                job_name    TEXT    NOT NULL,
                message     TEXT    NOT NULL,
                occurred_at TEXT    NOT NULL,
                extra       TEXT
            )
            """
        )
        self._conn.commit()

    def record(self, event_type: str, job_name: str, message: str,
               extra: Optional[dict] = None,
               occurred_at: Optional[datetime] = None) -> None:
        ts = (occurred_at or datetime.now(timezone.utc)).isoformat()
        self._conn.execute(
            "INSERT INTO audit_log (event_type, job_name, message, occurred_at, extra) "
            "VALUES (?, ?, ?, ?, ?)",
            (event_type, job_name, message, ts,
             json.dumps(extra) if extra else None),
        )
        self._conn.commit()

    def fetch(self, job_name: Optional[str] = None,
              event_type: Optional[str] = None,
              limit: int = 100) -> List[AuditEntry]:
        query = "SELECT * FROM audit_log WHERE 1=1"
        params: list = []
        if job_name:
            query += " AND job_name = ?"
            params.append(job_name)
        if event_type:
            query += " AND event_type = ?"
            params.append(event_type)
        query += " ORDER BY occurred_at DESC LIMIT ?"
        params.append(limit)
        rows = self._conn.execute(query, params).fetchall()
        return [
            AuditEntry(
                id=r["id"],
                event_type=r["event_type"],
                job_name=r["job_name"],
                message=r["message"],
                occurred_at=datetime.fromisoformat(r["occurred_at"]),
                extra=json.loads(r["extra"]) if r["extra"] else None,
            )
            for r in rows
        ]

    def close(self) -> None:
        self._conn.close()
