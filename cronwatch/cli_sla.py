"""CLI: query recorded SLA breaches."""
from __future__ import annotations

import argparse
import sys

from cronwatch.job_sla import SLAStore


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cronwatch-sla",
        description="Show SLA breach history.",
    )
    p.add_argument("--db", default="cronwatch.db", help="Path to SQLite database")
    p.add_argument("--job", default=None, help="Filter by job name")
    p.add_argument("--limit", type=int, default=20, help="Max rows to show")
    return p


def main(argv=None) -> int:
    args = _build_parser().parse_args(argv)
    store = SLAStore(db_path=args.db)
    breaches = store.fetch_breaches(job_name=args.job, limit=args.limit)

    if not breaches:
        print("No SLA breaches recorded.")
        return 0

    header = f"{'JOB':<30} {'REASON':<55} {'TIMESTAMP'}"
    print(header)
    print("-" * len(header))
    for b in breaches:
        print(f"{b['job_name']:<30} {b['reason']:<55} {b['ts']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
