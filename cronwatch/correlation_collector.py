"""Middleware that auto-links a job run to a correlation ID at watcher time."""

from __future__ import annotations

from typing import Optional

from cronwatch.job_correlation import CorrelationStore


class CorrelationCollector:
    """Attach correlation IDs to job runs recorded by the watcher.

    Usage::

        collector = CorrelationCollector(store)
        cid = collector.start_group()          # one ID for a batch of jobs
        collector.record(cid, "backup", run_id)
        collector.record(cid, "cleanup", run_id2)
    """

    def __init__(self, store: CorrelationStore) -> None:
        self._store = store
        self._active: dict[str, str] = {}  # job_name -> current correlation_id

    def start_group(self) -> str:
        """Create a new correlation ID for a logical group of jobs."""
        return self._store.new_correlation_id()

    def record(self, correlation_id: str, job_name: str, run_id: str) -> None:
        """Link *run_id* for *job_name* under *correlation_id*."""
        self._store.link(correlation_id, job_name, run_id)
        self._active[job_name] = correlation_id

    def active_correlation(self, job_name: str) -> Optional[str]:
        """Return the most recently used correlation ID for *job_name*, if any."""
        return self._active.get(job_name)

    def clear_active(self, job_name: str) -> None:
        """Remove the tracked active correlation for *job_name*."""
        self._active.pop(job_name, None)

    def fetch_group(self, correlation_id: str):
        """Convenience passthrough to the underlying store."""
        return self._store.fetch(correlation_id)
