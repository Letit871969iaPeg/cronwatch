"""CLI entry-point: cronwatch-audit — query the audit log."""

import argparse
import sys
from typing import Optional

from cronwatch.audit_log import AuditLog


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cronwatch-audit",
        description="Query the cronwatch audit log.",
    )
    p.add_argument("--db", default="cronwatch_audit.db",
                   help="Path to audit SQLite database (default: cronwatch_audit.db)")
    p.add_argument("--job", default=None, metavar="JOB_NAME",
                   help="Filter by job name")
    p.add_argument("--event-type", default=None, dest="event_type",
                   metavar="TYPE",
                   help="Filter by event type (e.g. alert, escalation)")
    p.add_argument("--limit", type=int, default=50,
                   help="Maximum number of entries to show (default: 50)")
    return p


def main(argv: Optional[list] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    log = AuditLog(db_path=args.db)
    entries = log.fetch(
        job_name=args.job,
        event_type=args.event_type,
        limit=args.limit,
    )
    log.close()

    if not entries:
        print("No audit entries found.")
        return 0

    header = f"{'ID':>6}  {'OCCURRED_AT':25}  {'TYPE':12}  {'JOB':20}  MESSAGE"
    print(header)
    print("-" * len(header))
    for e in entries:
        ts = e.occurred_at.strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"{e.id:>6}  {ts:25}  {e.event_type:12}  {e.job_name:20}  {e.message}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
