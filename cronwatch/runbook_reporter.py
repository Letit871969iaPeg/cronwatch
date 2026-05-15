"""Embed runbook links into alert / summary output."""

from __future__ import annotations

from typing import Optional

from cronwatch.job_runbook import RunbookStore


class RunbookReporter:
    """Enriches alert messages with runbook context when available."""

    def __init__(self, store: RunbookStore) -> None:
        self._store = store

    def enrich(self, job_name: str, message: str) -> str:
        """Return *message* with runbook URL and notes appended if present."""
        entry = self._store.get(job_name)
        if entry is None:
            return message
        parts = [message]
        if entry.url:
            parts.append(f"Runbook: {entry.url}")
        if entry.notes:
            parts.append(f"Notes: {entry.notes}")
        return "\n".join(parts)

    def format_entry(self, job_name: str) -> Optional[str]:
        """Return a formatted runbook block for *job_name*, or None."""
        entry = self._store.get(job_name)
        if entry is None:
            return None
        lines = [f"[Runbook: {job_name}]"]
        if entry.url:
            lines.append(f"  URL:   {entry.url}")
        if entry.notes:
            lines.append(f"  Notes: {entry.notes}")
        return "\n".join(lines)

    def print_runbook(self, job_name: str) -> bool:
        """Print runbook info to stdout; return False if no entry exists."""
        block = self.format_entry(job_name)
        if block is None:
            return False
        print(block)
        return True
