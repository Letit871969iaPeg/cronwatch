"""Tests for cronwatch.rate_limiter."""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from cronwatch.rate_limiter import RateLimiter, RateLimitPolicy

JOB = "backup"
EVENT = "drift"
_BASE = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def limiter() -> RateLimiter:
    return RateLimiter()


def _now(offset_seconds: float = 0):
    return _BASE + timedelta(seconds=offset_seconds)


def test_first_alert_always_allowed(limiter):
    with patch("cronwatch.rate_limiter._utcnow", return_value=_now()):
        assert limiter.is_allowed(JOB, EVENT) is True


def test_alert_suppressed_within_cooldown(limiter):
    limiter.set_policy(JOB, RateLimitPolicy(cooldown_seconds=300))
    with patch("cronwatch.rate_limiter._utcnow", return_value=_now(0)):
        limiter.record_sent(JOB, EVENT)

    # 100 seconds later — still in cooldown
    with patch("cronwatch.rate_limiter._utcnow", return_value=_now(100)):
        assert limiter.is_allowed(JOB, EVENT) is False


def test_alert_allowed_after_cooldown(limiter):
    limiter.set_policy(JOB, RateLimitPolicy(cooldown_seconds=300))
    with patch("cronwatch.rate_limiter._utcnow", return_value=_now(0)):
        limiter.record_sent(JOB, EVENT)

    # 301 seconds later — cooldown expired
    with patch("cronwatch.rate_limiter._utcnow", return_value=_now(301)):
        assert limiter.is_allowed(JOB, EVENT) is True


def test_hourly_cap_suppresses_excess(limiter):
    limiter.set_policy(JOB, RateLimitPolicy(cooldown_seconds=0, max_alerts_per_hour=3))
    for i in range(3):
        with patch("cronwatch.rate_limiter._utcnow", return_value=_now(i * 10)):
            assert limiter.is_allowed(JOB, EVENT) is True
            limiter.record_sent(JOB, EVENT)

    # 4th alert in same hour window — should be suppressed
    with patch("cronwatch.rate_limiter._utcnow", return_value=_now(35)):
        assert limiter.is_allowed(JOB, EVENT) is False


def test_hourly_cap_resets_after_one_hour(limiter):
    limiter.set_policy(JOB, RateLimitPolicy(cooldown_seconds=0, max_alerts_per_hour=2))
    for i in range(2):
        with patch("cronwatch.rate_limiter._utcnow", return_value=_now(i)):
            limiter.record_sent(JOB, EVENT)

    # 61 minutes later — new hour window
    with patch("cronwatch.rate_limiter._utcnow", return_value=_now(3660)):
        assert limiter.is_allowed(JOB, EVENT) is True


def test_different_event_types_tracked_independently(limiter):
    limiter.set_policy(JOB, RateLimitPolicy(cooldown_seconds=300))
    with patch("cronwatch.rate_limiter._utcnow", return_value=_now(0)):
        limiter.record_sent(JOB, "drift")

    with patch("cronwatch.rate_limiter._utcnow", return_value=_now(10)):
        # "failure" event not yet sent — should be allowed
        assert limiter.is_allowed(JOB, "failure") is True


def test_default_policy_applied_when_no_policy_set(limiter):
    # Default cooldown is 300s; no policy registered
    with patch("cronwatch.rate_limiter._utcnow", return_value=_now(0)):
        limiter.record_sent(JOB, EVENT)

    with patch("cronwatch.rate_limiter._utcnow", return_value=_now(100)):
        assert limiter.is_allowed(JOB, EVENT) is False

    with patch("cronwatch.rate_limiter._utcnow", return_value=_now(301)):
        assert limiter.is_allowed(JOB, EVENT) is True
