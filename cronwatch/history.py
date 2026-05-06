"""Persistent job execution history using a simple SQLite backend."""

import sqlite3
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from cronwatch.tracker import JobRecord

log = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path("~/.cronwatch/history.db").expanduser()

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS job_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_name    TEXT    NOT NULL,
    started_at  REAL,
    finished_at REAL,
    exit_code   INTEGER,
    duration_s  REAL
);
"""


class HistoryStore:
    """Read/write job execution records to SQLite."""

    def __init__(self, db_path: Path = DEFAULT_DB_PATH) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(CREATE_TABLE_SQL)
        self._conn.commit()

    def record(self, job_name: str, rec: JobRecord) -> None:
        """Persist a finished JobRecord."""
        duration = (
            (rec.finished_at - rec.started_at).total_seconds()
            if rec.started_at and rec.finished_at
            else None
        )
        self._conn.execute(
            "INSERT INTO job_history (job_name, started_at, finished_at, exit_code, duration_s)"
            " VALUES (?, ?, ?, ?, ?)",
            (
                job_name,
                rec.started_at.timestamp() if rec.started_at else None,
                rec.finished_at.timestamp() if rec.finished_at else None,
                rec.exit_code,
                duration,
            ),
        )
        self._conn.commit()
        log.debug("Recorded history for job %s (exit=%s)", job_name, rec.exit_code)

    def fetch(self, job_name: str, limit: int = 50) -> List[sqlite3.Row]:
        """Return the most recent *limit* rows for *job_name*."""
        cur = self._conn.execute(
            "SELECT * FROM job_history WHERE job_name = ?"
            " ORDER BY id DESC LIMIT ?",
            (job_name, limit),
        )
        return cur.fetchall()

    def average_duration(self, job_name: str, window: int = 10) -> Optional[float]:
        """Return mean duration (seconds) over the last *window* successful runs."""
        cur = self._conn.execute(
            "SELECT AVG(duration_s) FROM ("
            "  SELECT duration_s FROM job_history"
            "  WHERE job_name = ? AND exit_code = 0 AND duration_s IS NOT NULL"
            "  ORDER BY id DESC LIMIT ?"
            ")",
            (job_name, window),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def close(self) -> None:
        self._conn.close()
