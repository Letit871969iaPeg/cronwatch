"""Collects live job state from the tracker and writes snapshots."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from cronwatch.snapshot import JobSnapshot, SnapshotStore, _utcnow
from cronwatch.tracker import JobTracker


class SnapshotCollector:
    """Reads current state from *tracker* and persists a snapshot for every
    known job.

    Intended to be called periodically (e.g., by the Scheduler or a dedicated
    thread) so that operators can inspect historical trends without waiting for
    a run to complete.
    """

    def __init__(self, tracker: JobTracker, store: SnapshotStore) -> None:
        self._tracker = tracker
        self._store = store

    def collect(self, job_names: List[str]) -> List[JobSnapshot]:
        """Capture snapshots for *job_names* and persist them.

        Returns the list of snapshots that were saved.
        """
        captured_at = _utcnow().isoformat()
        snapshots: List[JobSnapshot] = []

        for name in job_names:
            record = self._tracker.get(name)
            if record is None:
                snap = JobSnapshot(
                    job_name=name,
                    captured_at=captured_at,
                    last_status=None,
                    last_run_ts=None,
                    last_duration_s=None,
                    consecutive_failures=0,
                )
            else:
                last_run_ts = (
                    record.last_start.isoformat() if record.last_start else None
                )
                snap = JobSnapshot(
                    job_name=name,
                    captured_at=captured_at,
                    last_status=record.last_status(),
                    last_run_ts=last_run_ts,
                    last_duration_s=record.last_duration_s,
                    consecutive_failures=record.consecutive_failures,
                )

            self._store.save(snap)
            snapshots.append(snap)

        return snapshots
