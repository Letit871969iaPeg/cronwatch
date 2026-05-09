"""CLI for managing job labels: set, get, delete, and search."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from cronwatch.job_labels import LabelStore


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cronwatch-labels",
        description="Manage key-value labels attached to cron jobs.",
    )
    p.add_argument("--db", default="cronwatch.db", help="Path to the SQLite database.")
    sub = p.add_subparsers(dest="command", required=True)

    # set
    s = sub.add_parser("set", help="Attach or update a label on a job.")
    s.add_argument("job", help="Job name")
    s.add_argument("key", help="Label key")
    s.add_argument("value", help="Label value")

    # get
    g = sub.add_parser("get", help="Show all labels for a job.")
    g.add_argument("job", help="Job name")

    # delete
    d = sub.add_parser("delete", help="Remove a label from a job.")
    d.add_argument("job", help="Job name")
    d.add_argument("key", help="Label key to remove")

    # find
    f = sub.add_parser("find", help="List jobs matching a key=value label.")
    f.add_argument("key", help="Label key")
    f.add_argument("value", help="Label value")

    # list
    sub.add_parser("list", help="Show all labels across all jobs.")

    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    store = LabelStore(args.db)

    if args.command == "set":
        store.set(args.job, args.key, args.value)
        print(f"Set {args.job}[{args.key}] = {args.value}")

    elif args.command == "get":
        labels = store.get(args.job)
        if not labels:
            print(f"No labels for job '{args.job}'.")
            return 1
        for k, v in sorted(labels.items()):
            print(f"  {k}: {v}")

    elif args.command == "delete":
        store.delete(args.job, args.key)
        print(f"Deleted {args.job}[{args.key}]")

    elif args.command == "find":
        jobs = store.find_by_label(args.key, args.value)
        if not jobs:
            print(f"No jobs found with {args.key}={args.value}.")
            return 1
        for name in jobs:
            print(name)

    elif args.command == "list":
        entries = store.all_labels()
        if not entries:
            print("No labels stored.")
            return 0
        for e in entries:
            print(f"  {e.job_name}  {e.key}={e.value}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
