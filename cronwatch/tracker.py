"""Job execution tracker for cronwatch.

Tracks the last run time, duration, and status of monitored cron jobs.
Persists state to a JSON file so the daemon can survive restarts.
"""

import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from typing import Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class JobRecord:
    """Stores execution history for a single cron job."""

    job_name: str
    last_start: Optional[float] = None       # Unix timestamp
    last_finish: Optional[float] = None      # Unix timestamp
    last_duration: Optional[float] = None    # Seconds
    last_exit_code: Optional[int] = None
    consecutive_failures: int = 0
    total_runs: int = 0
    total_failures: int = 0

    @property
    def last_status(self) -> Optional[str]:
        """Return 'ok', 'failed', or None if never run."""
        if self.last_exit_code is None:
            return None
        return "ok" if self.last_exit_code == 0 else "failed"

    @property
    def is_running(self) -> bool:
        """True if a start has been recorded but no finish yet."""
        return self.last_start is not None and (
            self.last_finish is None or self.last_start > self.last_finish
        )


class JobTracker:
    """Manages in-memory job state and persists it to disk."""

    def __init__(self, state_path: str):
        """
        Args:
            state_path: Path to the JSON file used for state persistence.
        """
        self.state_path = state_path
        self._records: Dict[str, JobRecord] = {}
        self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record_start(self, job_name: str) -> None:
        """Mark a job as started right now."""
        record = self._get_or_create(job_name)
        record.last_start = time.time()
        logger.debug("Job '%s' started at %s", job_name, record.last_start)
        self._save()

    def record_finish(self, job_name: str, exit_code: int) -> JobRecord:
        """Mark a job as finished and update statistics.

        Args:
            job_name: Identifier matching the configured job name.
            exit_code: Process exit code (0 = success).

        Returns:
            The updated JobRecord.
        """
        record = self._get_or_create(job_name)
        now = time.time()
        record.last_finish = now
        record.last_exit_code = exit_code
        record.total_runs += 1

        if record.last_start is not None:
            record.last_duration = now - record.last_start

        if exit_code != 0:
            record.total_failures += 1
            record.consecutive_failures += 1
            logger.warning(
                "Job '%s' finished with exit code %d (consecutive failures: %d)",
                job_name,
                exit_code,
                record.consecutive_failures,
            )
        else:
            record.consecutive_failures = 0
            logger.debug(
                "Job '%s' finished OK in %.2fs", job_name, record.last_duration or 0
            )

        self._save()
        return record

    def get(self, job_name: str) -> Optional[JobRecord]:
        """Return the JobRecord for *job_name*, or None if unseen."""
        return self._records.get(job_name)

    def all_records(self) -> Dict[str, JobRecord]:
        """Return a copy of all tracked job records."""
        return dict(self._records)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_or_create(self, job_name: str) -> JobRecord:
        if job_name not in self._records:
            self._records[job_name] = JobRecord(job_name=job_name)
        return self._records[job_name]

    def _load(self) -> None:
        """Load persisted state from disk, silently ignoring missing files."""
        if not os.path.exists(self.state_path):
            logger.debug("No state file found at '%s'; starting fresh.", self.state_path)
            return
        try:
            with open(self.state_path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            for name, data in raw.items():
                self._records[name] = JobRecord(**data)
            logger.info("Loaded state for %d job(s) from '%s'.", len(self._records), self.state_path)
        except (json.JSONDecodeError, TypeError, KeyError) as exc:
            logger.error("Failed to load state file '%s': %s", self.state_path, exc)

    def _save(self) -> None:
        """Persist current state to disk atomically."""
        tmp_path = self.state_path + ".tmp"
        try:
            os.makedirs(os.path.dirname(self.state_path) or ".", exist_ok=True)
            with open(tmp_path, "w", encoding="utf-8") as fh:
                json.dump(
                    {name: asdict(rec) for name, rec in self._records.items()},
                    fh,
                    indent=2,
                )
            os.replace(tmp_path, self.state_path)
        except OSError as exc:
            logger.error("Failed to save state to '%s': %s", self.state_path, exc)
