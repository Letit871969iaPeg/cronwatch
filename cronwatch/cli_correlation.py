"""CLI entry point for inspecting job correlation groups."""

from __future__ import annotations

import argparse
import sys

from cronwatch.job_correlation import CorrelationStore


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cronwatch-correlation",
        description="Inspect correlated cron job runs.",
    )
    p.add_argument("--db", default="cronwatch.db", help="Path to SQLite database")
    sub = p.add_subparsers(dest="cmd", required=True)

    show = sub.add_parser("show", help="Show all jobs linked to a correlation ID")
    show.add_argument("correlation_id", help="Correlation ID to look up")

    by_job = sub.add_parser("by-job", help="Show all correlations for a job")
    by_job.add_argument("job_name", help="Job name to look up")

    rm = sub.add_parser("delete", help="Delete all entries for a correlation ID")
    rm.add_argument("correlation_id", help="Correlation ID to delete")

    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    store = CorrelationStore(db_path=args.db)

    if args.cmd == "show":
        entries = store.fetch(args.correlation_id)
        if not entries:
            print(f"No entries found for correlation ID: {args.correlation_id}")
            return 1
        print(f"{'JOB':<30} {'RUN ID':<38} CREATED AT")
        print("-" * 85)
        for e in entries:
            print(f"{e.job_name:<30} {e.run_id:<38} {e.created_at}")
        return 0

    if args.cmd == "by-job":
        entries = store.fetch_by_job(args.job_name)
        if not entries:
            print(f"No correlations found for job: {args.job_name}")
            return 1
        print(f"{'CORRELATION ID':<38} {'RUN ID':<38} CREATED AT")
        print("-" * 90)
        for e in entries:
            print(f"{e.correlation_id:<38} {e.run_id:<38} {e.created_at}")
        return 0

    if args.cmd == "delete":
        n = store.delete(args.correlation_id)
        if n == 0:
            print(f"No entries found for correlation ID: {args.correlation_id}")
            return 1
        print(f"Deleted {n} entr{'y' if n == 1 else 'ies'}.")
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
