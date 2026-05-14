"""Collect completed-run durations and feed them into the BaselineStore."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from cronwatch.job_baseline import BaselineStore
from cronwatch.tracker import JobRecord, JobTracker


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class BaselineCollector:
    """Reads finished job records from *tracker* and stores duration samples.

    Designed to be called periodically (e.g. from the scheduler tick) so
    that the baseline database stays up to date without a separate process.
    """

    def __init__(self, tracker: JobTracker, store: BaselineStore) -> None:
        self._tracker = tracker
        self._store = store
        # Track which run_ids we have already ingested to avoid duplicates.
        self._seen: set[str] = set()

    def collect(self, job_names: List[str] | None = None) -> int:
        """Ingest new completed runs.

        Args:
            job_names: Optional list of job names to restrict collection to.
                       If None, all jobs in the tracker are processed.

        Returns:
            Number of new samples recorded.
        """
        names = job_names if job_names is not None else list(self._tracker._records.keys())
        ingested = 0
        for name in names:
            record: JobRecord | None = self._tracker._records.get(name)
            if record is None:
                continue
            if record.run_id in self._seen:
                continue
            if record.started_at is None or record.finished_at is None:
                continue
            duration = (record.finished_at - record.started_at).total_seconds()
            if duration < 0:
                continue
            self._store.add_sample(
                job_name=name,
                duration_seconds=duration,
                recorded_at=_utcnow_iso(),
            )
            self._seen.add(record.run_id)
            ingested += 1
        return ingested
