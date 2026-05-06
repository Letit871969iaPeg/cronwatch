"""Rate limiter to suppress repeated alerts for the same job/event within a cooldown window."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class RateLimitPolicy:
    """Defines cooldown rules for a job's alert type."""
    cooldown_seconds: int = 300  # default 5 minutes
    max_alerts_per_hour: int = 10


@dataclass
class _AlertBucket:
    last_sent: Optional[datetime] = None
    sent_this_hour: int = 0
    hour_window_start: Optional[datetime] = None


class RateLimiter:
    """Tracks alert emission and suppresses duplicates based on cooldown and hourly caps."""

    def __init__(self) -> None:
        # key: (job_name, event_type) -> _AlertBucket
        self._buckets: Dict[Tuple[str, str], _AlertBucket] = {}
        self._policies: Dict[str, RateLimitPolicy] = {}

    def set_policy(self, job_name: str, policy: RateLimitPolicy) -> None:
        self._policies[job_name] = policy

    def _get_policy(self, job_name: str) -> RateLimitPolicy:
        return self._policies.get(job_name, RateLimitPolicy())

    def _get_bucket(self, job_name: str, event_type: str) -> _AlertBucket:
        key = (job_name, event_type)
        if key not in self._buckets:
            self._buckets[key] = _AlertBucket()
        return self._buckets[key]

    def is_allowed(self, job_name: str, event_type: str) -> bool:
        """Return True if the alert should be emitted; False if it should be suppressed."""
        policy = self._get_policy(job_name)
        bucket = self._get_bucket(job_name, event_type)
        now = _utcnow()

        # Reset hourly window if needed
        if bucket.hour_window_start is None or (
            now - bucket.hour_window_start >= timedelta(hours=1)
        ):
            bucket.hour_window_start = now
            bucket.sent_this_hour = 0

        # Check hourly cap
        if bucket.sent_this_hour >= policy.max_alerts_per_hour:
            return False

        # Check cooldown
        if bucket.last_sent is not None:
            elapsed = (now - bucket.last_sent).total_seconds()
            if elapsed < policy.cooldown_seconds:
                return False

        return True

    def record_sent(self, job_name: str, event_type: str) -> None:
        """Mark that an alert was emitted for this job/event."""
        bucket = self._get_bucket(job_name, event_type)
        now = _utcnow()
        bucket.last_sent = now
        bucket.sent_this_hour += 1
        if bucket.hour_window_start is None:
            bucket.hour_window_start = now
