"""Watcher: wraps a cron job subprocess and records start/end in the tracker."""

import subprocess
import time
import logging
from datetime import datetime, timezone
from typing import Optional, List

from cronwatch.tracker import JobTracker
from cronwatch.history import HistoryStore

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


class JobWatcher:
    """Executes a shell command on behalf of a named cron job and persists the result."""

    def __init__(
        self,
        job_name: str,
        tracker: JobTracker,
        history: HistoryStore,
        timeout: Optional[float] = None,
    ) -> None:
        self.job_name = job_name
        self.tracker = tracker
        self.history = history
        self.timeout = timeout

    def run(self, command: List[str]) -> int:
        """Run *command*, record outcome, return exit code."""
        started_at = _utcnow()
        logger.info("[%s] starting: %s", self.job_name, command)
        self.tracker.mark_start(self.job_name, started_at)

        exit_code: int
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                timeout=self.timeout,
            )
            exit_code = result.returncode
            if result.stdout:
                logger.debug("[%s] stdout: %s", self.job_name, result.stdout.decode(errors="replace"))
            if result.stderr:
                logger.debug("[%s] stderr: %s", self.job_name, result.stderr.decode(errors="replace"))
        except subprocess.TimeoutExpired:
            logger.error("[%s] timed out after %s seconds", self.job_name, self.timeout)
            exit_code = -1
        except Exception as exc:  # noqa: BLE001
            logger.error("[%s] unexpected error: %s", self.job_name, exc)
            exit_code = -2

        finished_at = _utcnow()
        duration = (finished_at - started_at).total_seconds()
        success = exit_code == 0

        self.tracker.mark_end(self.job_name, finished_at, success=success)
        self.history.record(self.job_name, started_at, finished_at, exit_code)

        logger.info(
            "[%s] finished in %.2fs — exit_code=%d",
            self.job_name,
            duration,
            exit_code,
        )
        return exit_code
