"""CLI: pause / resume / list paused cron jobs.

Usage examples
--------------
  cronwatch-pause pause  backup --reason "maintenance window"
  cronwatch-pause pause  backup --until 2024-12-01T06:00:00+00:00
  cronwatch-pause resume backup
  cronwatch-pause list
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from cronwatch.job_pause import PauseStore


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cronwatch-pause",
        description="Pause, resume, or list paused cron jobs.",
    )
    p.add_argument(
        "--db",
        default="cronwatch.db",
        metavar="PATH",
        help="Path to the cronwatch SQLite database (default: cronwatch.db)",
    )
    sub = p.add_subparsers(dest="command", required=True)

    # pause
    sp = sub.add_parser("pause", help="Pause a job")
    sp.add_argument("job", help="Job name")
    sp.add_argument("--reason", default="", help="Human-readable reason")
    sp.add_argument(
        "--until",
        default=None,
        metavar="ISO8601",
        help="Auto-resume after this UTC datetime (ISO-8601)",
    )

    # resume
    sr = sub.add_parser("resume", help="Resume a paused job")
    sr.add_argument("job", help="Job name")

    # list
    sub.add_parser("list", help="List all currently paused jobs")

    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    store = PauseStore(args.db)

    if args.command == "pause":
        until: datetime | None = None
        if args.until:
            try:
                until = datetime.fromisoformat(args.until)
                if until.tzinfo is None:
                    until = until.replace(tzinfo=timezone.utc)
            except ValueError:
                print(f"ERROR: --until value is not valid ISO-8601: {args.until}",
                      file=sys.stderr)
                return 2
        store.pause(args.job, reason=args.reason, paused_until=until)
        msg = f"Job '{args.job}' paused."
        if until:
            msg += f" Auto-resumes at {until.isoformat()}."
        print(msg)
        return 0

    if args.command == "resume":
        if not store.is_paused(args.job):
            print(f"Job '{args.job}' is not currently paused.")
            return 1
        store.resume(args.job)
        print(f"Job '{args.job}' resumed.")
        return 0

    if args.command == "list":
        entries = store.list_paused()
        if not entries:
            print("No jobs are currently paused.")
            return 0
        print(f"{'JOB':<30} {'PAUSED AT':<27} {'UNTIL':<27} REASON")
        print("-" * 90)
        for e in entries:
            until_str = e.paused_until.isoformat() if e.paused_until else "indefinite"
            print(f"{e.job_name:<30} {e.paused_at.isoformat():<27} {until_str:<27} {e.reason}")
        return 0

    return 1  # unreachable


if __name__ == "__main__":
    sys.exit(main())
