from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .store import Compactor


COMMANDS = {"commit", "read", "compact", "vacuum"}


def _canonical_output(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _extract_root(argv: list[str]) -> tuple[str, list[str]]:
    root = "."
    remaining: list[str] = []
    index = 0
    while index < len(argv):
        token = argv[index]
        if token == "--root" or token == "-r":
            if index + 1 >= len(argv):
                raise ValueError("--root requires a value")
            root = argv[index + 1]
            index += 2
            continue
        if token.startswith("--root="):
            root = token.split("=", 1)[1]
            index += 1
            continue
        remaining.append(token)
        index += 1
    if (
        remaining
        and remaining[0] not in COMMANDS
        and not remaining[0].startswith("-")
        and len(remaining) > 1
        and remaining[1] in COMMANDS
    ):
        root = remaining.pop(0)
    return root, remaining


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="icecrate")
    commands = parser.add_subparsers(dest="command", required=True)

    commit = commands.add_parser("commit")
    commit.add_argument("snapshot_id", nargs="?")
    commit.add_argument("--snapshot", "--snapshot-id", dest="snapshot_option")
    commit.add_argument("segments", nargs="*")
    commit.add_argument("--parent", default=None)
    commit.add_argument("--fault", default=None)
    commit.add_argument("--segment", action="append", default=[])
    commit.add_argument("--segments", dest="segment_options", nargs="*", default=[])

    read = commands.add_parser("read")
    read.add_argument("snapshot_id", nargs="?")
    read.add_argument("--snapshot", "--snapshot-id", dest="snapshot_option")

    compact = commands.add_parser("compact")
    compact.add_argument("snapshot_id", nargs="?")
    compact.add_argument("--snapshot", "--snapshot-id", dest="snapshot_option")

    vacuum = commands.add_parser("vacuum")
    vacuum.add_argument("retain", nargs="?", type=int)
    vacuum.add_argument("grace_before", nargs="?")
    vacuum.add_argument("--retain", dest="retain_option", type=int)
    vacuum.add_argument("--grace-before", dest="grace_option")
    return parser


def _snapshot_argument(args: argparse.Namespace) -> str:
    value = getattr(args, "snapshot_id", None) or getattr(
        args, "snapshot_option", None
    )
    if value is None:
        raise ValueError("snapshot id is required")
    return value


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    try:
        root, arguments = _extract_root(list(argv))
        args = _build_parser().parse_args(arguments)
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    try:
        compactor = Compactor(root)
        if args.command == "commit":
            snapshot_id = _snapshot_argument(args)
            parent = None if args.parent in {None, "", "-", "null", "none"} else args.parent
            segments = (
                list(args.segments)
                + list(args.segment)
                + list(args.segment_options)
            )
            if (
                args.parent is None
                and segments
                and not Path(segments[0]).exists()
                and (
                    segments[0] in {"", "-", "null", "none"}
                    or compactor._find_manifest(segments[0]) is not None
                )
            ):
                token = segments.pop(0)
                parent = None if token in {"", "-", "null", "none"} else token
            result = compactor.commit(
                snapshot_id, parent, segments, fault=args.fault
            )
        elif args.command == "read":
            snapshot_id = args.snapshot_id or args.snapshot_option
            result = compactor.read(snapshot_id)
        elif args.command == "compact":
            result = compactor.compact(_snapshot_argument(args))
        elif args.command == "vacuum":
            retain = args.retain if args.retain is not None else args.retain_option
            grace = (
                args.grace_before
                if args.grace_before is not None
                else args.grace_option
            )
            if retain is None or grace is None:
                raise ValueError("vacuum requires retain and grace_before")
            result = compactor.vacuum(retain, grace)
        else:
            raise ValueError(f"unknown command: {args.command}")
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    print(_canonical_output(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
