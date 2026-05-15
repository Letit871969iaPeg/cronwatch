"""Job fingerprinting: detect when a job's command or schedule changes."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from typing import Optional


@dataclass
class FingerprintEntry:
    job_name: str
    fingerprint: str
    command: str
    schedule: str
    recorded_at: str


class FingerprintStore:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS job_fingerprints (
                    job_name  TEXT PRIMARY KEY,
                    fingerprint TEXT NOT NULL,
                    command   TEXT NOT NULL,
                    schedule  TEXT NOT NULL,
                    recorded_at TEXT NOT NULL
                )
                """
            )

    @staticmethod
    def compute(command: str, schedule: str) -> str:
        payload = json.dumps({"command": command, "schedule": schedule}, sort_keys=True)
        return hashlib.sha256(payload.encode()).hexdigest()[:16]

    def get(self, job_name: str) -> Optional[FingerprintEntry]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM job_fingerprints WHERE job_name = ?", (job_name,)
            ).fetchone()
        if row is None:
            return None
        return FingerprintEntry(**dict(row))

    def upsert(self, job_name: str, command: str, schedule: str, recorded_at: str) -> None:
        fp = self.compute(command, schedule)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO job_fingerprints (job_name, fingerprint, command, schedule, recorded_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(job_name) DO UPDATE SET
                    fingerprint  = excluded.fingerprint,
                    command      = excluded.command,
                    schedule     = excluded.schedule,
                    recorded_at  = excluded.recorded_at
                """,
                (job_name, fp, command, schedule, recorded_at),
            )

    def has_changed(self, job_name: str, command: str, schedule: str) -> bool:
        """Return True if the job definition differs from the stored fingerprint."""
        entry = self.get(job_name)
        if entry is None:
            return True
        return entry.fingerprint != self.compute(command, schedule)
