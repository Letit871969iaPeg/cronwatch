"""CLI for managing job groups."""
from __future__ import annotations

import argparse
import sys

from cronwatch.job_grouping import GroupStore


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cronwatch-groups",
        description="Manage job groups.",
    )
    p.add_argument("--db", default="cronwatch.db", help="Path to SQLite database.")
    sub = p.add_subparsers(dest="cmd", required=True)

    add_p = sub.add_parser("add", help="Add a job to a group.")
    add_p.add_argument("group", help="Group name.")
    add_p.add_argument("job", help="Job name.")
    add_p.add_argument("--description", default="", help="Optional group description.")

    rm_p = sub.add_parser("remove", help="Remove a job from a group.")
    rm_p.add_argument("group", help="Group name.")
    rm_p.add_argument("job", help="Job name.")

    show_p = sub.add_parser("show", help="Show members of a group.")
    show_p.add_argument("group", help="Group name.")

    sub.add_parser("list", help="List all group names.")

    jobs_p = sub.add_parser("jobs", help="List groups that contain a job.")
    jobs_p.add_argument("job", help="Job name.")

    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    store = GroupStore(args.db)

    if args.cmd == "add":
        store.add(args.group, args.job, args.description)
        print(f"Added '{args.job}' to group '{args.group}'.")
        return 0

    if args.cmd == "remove":
        store.remove(args.group, args.job)
        print(f"Removed '{args.job}' from group '{args.group}'.")
        return 0

    if args.cmd == "show":
        group = store.get_group(args.group)
        if group is None:
            print(f"Group '{args.group}' not found.", file=sys.stderr)
            return 1
        print(f"Group : {group.name}")
        if group.description:
            print(f"Desc  : {group.description}")
        print(f"Jobs  : {', '.join(group.jobs) if group.jobs else '(none)'}")
        return 0

    if args.cmd == "list":
        names = store.list_groups()
        if not names:
            print("No groups defined.")
        else:
            for name in names:
                print(name)
        return 0

    if args.cmd == "jobs":
        groups = store.groups_for_job(args.job)
        if not groups:
            print(f"Job '{args.job}' belongs to no groups.")
        else:
            for g in groups:
                print(g)
        return 0

    return 0  # unreachable


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
