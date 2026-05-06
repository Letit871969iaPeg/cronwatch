"""Prune old history records based on a configurable retention policy."""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from cronwatch.history import HistoryStore

log = logging.getLogger(__name__)


@dataclass
class RetentionPolicy:
    """How long to keep history records."""

    max_age_days: int = 30
    max_records_per_job: Optional[int] = 500


class RetentionManager:
    """Applies a :class:`RetentionPolicy` to a :class:`HistoryStore`."""

    def __init__(self, store: HistoryStore, policy: RetentionPolicy) -> None:
        self._store = store
        self._policy = policy

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def prune(self) -> dict[str, int]:
        """Run both pruning passes and return counts of deleted rows."""
        deleted_age = self._prune_by_age()
        deleted_count = self._prune_by_count()
        total = deleted_age + deleted_count
        log.info(
            "retention prune complete: %d by age, %d by count, %d total",
            deleted_age,
            deleted_count,
            total,
        )
        return {"by_age": deleted_age, "by_count": deleted_count, "total": total}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _prune_by_age(self) -> int:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(
            days=self._policy.max_age_days
        )
        cutoff_ts = cutoff.timestamp()
        with sqlite3.connect(self._store.db_path) as conn:
            cur = conn.execute(
                "DELETE FROM history WHERE started_at < ?", (cutoff_ts,)
            )
            return cur.rowcount

    def _prune_by_count(self) -> int:
        if self._policy.max_records_per_job is None:
            return 0

        limit = self._policy.max_records_per_job
        deleted = 0
        with sqlite3.connect(self._store.db_path) as conn:
            job_names = [
                row[0]
                for row in conn.execute(
                    "SELECT DISTINCT job_name FROM history"
                ).fetchall()
            ]
            for job in job_names:
                cur = conn.execute(
                    """
                    DELETE FROM history
                    WHERE job_name = ?
                      AND rowid NOT IN (
                          SELECT rowid FROM history
                          WHERE job_name = ?
                          ORDER BY started_at DESC
                          LIMIT ?
                      )
                    """,
                    (job, job, limit),
                )
                deleted += cur.rowcount
        return deleted
