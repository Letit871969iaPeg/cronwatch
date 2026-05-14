"""Scheduler that periodically runs drift checks for all configured jobs."""

import logging
import threading
import time
from typing import Optional

from cronwatch.alerter import Alerter
from cronwatch.config import CronwatchConfig
from cronwatch.drift_checker import DriftChecker
from cronwatch.tracker import JobTracker

logger = logging.getLogger(__name__)


class Scheduler:
    """Runs drift checks on a fixed interval in a background thread."""

    def __init__(
        self,
        config: CronwatchConfig,
        tracker: JobTracker,
        alerter: Alerter,
        interval: int = 60,
    ) -> None:
        self.config = config
        self.tracker = tracker
        self.alerter = alerter
        self.interval = interval
        self._checker = DriftChecker(tracker, alerter)
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_tick: Optional[float] = None

    def _run(self) -> None:
        logger.info("Scheduler started (interval=%ds)", self.interval)
        while not self._stop_event.is_set():
            self._tick()
            self._stop_event.wait(timeout=self.interval)
        logger.info("Scheduler stopped")

    def _tick(self) -> None:
        self._last_tick = time.monotonic()
        logger.debug("Running drift checks for %d jobs", len(self.config.jobs))
        for job_cfg in self.config.jobs:
            try:
                self._checker.check_job(job_cfg)
            except Exception:
                logger.exception("Unexpected error checking job '%s'", job_cfg.name)

    def start(self) -> None:
        """Start the background scheduler thread."""
        if self._thread and self._thread.is_alive():
            raise RuntimeError("Scheduler is already running")
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="cronwatch-scheduler")
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the scheduler to stop and wait for the thread to finish."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=timeout)
            self._thread = None

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    @property
    def seconds_since_last_tick(self) -> Optional[float]:
        """Return the number of seconds elapsed since the last tick, or None if no tick has occurred."""
        if self._last_tick is None:
            return None
        return time.monotonic() - self._last_tick
