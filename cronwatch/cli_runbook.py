"""CLI for managing per-job runbook entries."""

from __future__ import annotations

import argparse
import sys

from cronwatch.job_runbook import RunbookStore


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Manage job runbook links and notes")
    p.add_argument("--db", default="cronwatch.db", help="Path to SQLite database")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("set", help="Set runbook URL / notes for a job")
    s.add_argument("job_name")
    s.add_argument("--url", default=None)
    s.add_argument("--notes", default=None)

    g = sub.add_parser("get", help="Show runbook entry for a job")
    g.add_argument("job_name")

    d = sub.add_parser("delete", help="Remove runbook entry for a job")
    d.add_argument("job_name")

    sub.add_parser("list", help="List all runbook entries")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    store = RunbookStore(args.db)

    if args.command == "set":
        store.set(args.job_name, url=args.url, notes=args.notes)
        print(f"Runbook updated for '{args.job_name}'")
        return 0

    if args.command == "get":
        entry = store.get(args.job_name)
        if entry is None:
            print(f"No runbook entry for '{args.job_name}'", file=sys.stderr)
            return 1
        print(f"job:   {entry.job_name}")
        print(f"url:   {entry.url or '—'}")
        print(f"notes: {entry.notes or '—'}")
        return 0

    if args.command == "delete":
        removed = store.delete(args.job_name)
        if not removed:
            print(f"No runbook entry found for '{args.job_name}'", file=sys.stderr)
            return 1
        print(f"Runbook entry deleted for '{args.job_name}'")
        return 0

    if args.command == "list":
        entries = store.all()
        if not entries:
            print("No runbook entries found.")
            return 0
        print(f"{'JOB':<30}  {'URL':<40}  NOTES")
        print("-" * 80)
        for e in entries:
            print(f"{e.job_name:<30}  {(e.url or '—'):<40}  {e.notes or '—'}")
        return 0

    return 0  # unreachable


if __name__ == "__main__":
    sys.exit(main())
