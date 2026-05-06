"""Tag-based filtering for cron jobs.

Allows selecting subsets of jobs by tag labels defined in config,
enabling targeted reporting, silencing, or escalation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional

from cronwatch.config import JobConfig


@dataclass
class TagFilter:
    """Matches jobs against a set of required tags."""

    tags: List[str] = field(default_factory=list)

    def matches(self, job: JobConfig) -> bool:
        """Return True if the job carries ALL tags in this filter."""
        if not self.tags:
            return True
        job_tags: List[str] = getattr(job, "tags", []) or []
        return all(t in job_tags for t in self.tags)

    def filter_jobs(
        self, jobs: Iterable[JobConfig]
    ) -> List[JobConfig]:
        """Return only those jobs that match every tag in this filter."""
        return [j for j in jobs if self.matches(j)]


def parse_tags(raw: Optional[str]) -> List[str]:
    """Parse a comma-separated tag string into a list of stripped tags.

    >>> parse_tags("nightly, backup , db")
    ['nightly', 'backup', 'db']
    >>> parse_tags(None)
    []
    """
    if not raw:
        return []
    return [t.strip() for t in raw.split(",") if t.strip()]
