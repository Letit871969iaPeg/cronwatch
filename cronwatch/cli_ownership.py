"""CLI for managing job ownership records."""

from __future__ import annotations

import argparse
import sys

from cronwatch.job_ownership import OwnerEntry, OwnershipStore


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cronwatch-ownership",
        description="Manage job ownership records.",
    )
    p.add_argument("--db", default="cronwatch.db", help="Path to SQLite database.")
    sub = p.add_subparsers(dest="command", required=True)

    # set
    s = sub.add_parser("set", help="Set or update ownership for a job.")
    s.add_argument("job_name")
    s.add_argument("--owner", required=True)
    s.add_argument("--team", default=None)
    s.add_argument("--email", default=None)
    s.add_argument("--slack", dest="slack_channel", default=None)

    # get
    g = sub.add_parser("get", help="Show ownership for a job.")
    g.add_argument("job_name")

    # delete
    d = sub.add_parser("delete", help="Remove ownership record for a job.")
    d.add_argument("job_name")

    # list
    sub.add_parser("list", help="List all ownership records.")

    return p


def _print_entry(entry: OwnerEntry) -> None:
    print(f"job:     {entry.job_name}")
    print(f"owner:   {entry.owner}")
    if entry.team:
        print(f"team:    {entry.team}")
    if entry.email:
        print(f"email:   {entry.email}")
    if entry.slack_channel:
        print(f"slack:   {entry.slack_channel}")


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    store = OwnershipStore(db_path=args.db)

    if args.command == "set":
        store.set(
            OwnerEntry(
                job_name=args.job_name,
                owner=args.owner,
                team=args.team,
                email=args.email,
                slack_channel=args.slack_channel,
            )
        )
        print(f"Ownership set for '{args.job_name}'.")
        return 0

    if args.command == "get":
        entry = store.get(args.job_name)
        if entry is None:
            print(f"No ownership record for '{args.job_name}'.", file=sys.stderr)
            return 1
        _print_entry(entry)
        return 0

    if args.command == "delete":
        removed = store.delete(args.job_name)
        if not removed:
            print(f"No record found for '{args.job_name}'.", file=sys.stderr)
            return 1
        print(f"Ownership record for '{args.job_name}' deleted.")
        return 0

    if args.command == "list":
        entries = store.all()
        if not entries:
            print("No ownership records found.")
            return 0
        for i, entry in enumerate(entries):
            if i:
                print()
            _print_entry(entry)
        return 0

    return 1  # unreachable


if __name__ == "__main__":
    sys.exit(main())
