"""Command line interface for the scheduler.

The CLI only manipulates scheduling state.  It never runs a plan command.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

try:
    from .scheduler import Scheduler
except ImportError:  # direct ``python backfillloom/__main__.py`` execution
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from backfillloom.scheduler import Scheduler


def _read_json(value: str):
    if value == "-":
        return json.loads(sys.stdin.read())
    stripped = value.lstrip()
    if stripped.startswith("{") or stripped.startswith("["):
        return json.loads(value)
    path = Path(value)
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return json.loads(value)


def _add_common(parser):
    parser.add_argument(
        "--db", default=argparse.SUPPRESS, help="SQLite database path"
    )


def _build_parser():
    parser = argparse.ArgumentParser(prog="backfillloom")
    parser.add_argument("--db", help="SQLite database path")
    commands = parser.add_subparsers(dest="command", required=True)

    load = commands.add_parser("load", help="load or recover a plan")
    _add_common(load)
    load.add_argument("plan", nargs="?")
    load.add_argument("--plan", "--plan-file", dest="plan_opt")
    load.add_argument("--fault", choices=("after_lease", "before_commit"))

    extend = commands.add_parser("extend", help="append later partitions")
    _add_common(extend)
    extend.add_argument("plan", nargs="?")
    extend.add_argument("--plan", "--plan-file", dest="plan_opt")
    extend.add_argument("--fault", choices=("after_lease", "before_commit"))

    claim = commands.add_parser("claim", help="claim ready instances")
    _add_common(claim)
    claim.add_argument("--worker", required=True)
    claim.add_argument("--now", type=float)
    claim.add_argument(
        "--lease-seconds", "--lease_seconds", "--lease", dest="lease_seconds",
        type=float, default=300.0,
    )
    claim.add_argument("--limit", type=int, default=1)
    claim.add_argument("--fault", choices=("after_lease", "before_commit"))

    complete = commands.add_parser("complete", help="commit a successful output")
    _add_common(complete)
    complete.add_argument("--instance", required=True)
    complete.add_argument(
        "--lease-token", "--lease_token", "--token",
        dest="lease_token", required=True
    )
    complete.add_argument(
        "--output-digest", "--output_digest", "--digest",
        dest="output_digest", required=True
    )
    complete.add_argument("--now", type=float)
    complete.add_argument("--fault", choices=("after_lease", "before_commit"))

    fail = commands.add_parser("fail", help="record a failed attempt")
    _add_common(fail)
    fail.add_argument("--instance", required=True)
    fail.add_argument(
        "--lease-token", "--lease_token", "--token",
        dest="lease_token", required=True
    )
    fail.add_argument(
        "--error", "--error-category", "--error_category", required=True
    )
    fail.add_argument("--now", type=float)
    fail.add_argument("--fault", choices=("after_lease", "before_commit"))

    status = commands.add_parser("status", help="show counts and instances")
    _add_common(status)
    return parser


def _plan_argument(args):
    value = getattr(args, "plan", None) or getattr(args, "plan_opt", None)
    if not value:
        raise ValueError("a plan JSON file, JSON string, or - is required")
    return _read_json(value)


def _now(value):
    return time.time() if value is None else value


def main(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)
    scheduler = None
    try:
        db = getattr(args, "db", None)
        if not db:
            parser.error("--db is required")
        scheduler = Scheduler(db)
        if args.command == "load":
            result = scheduler.load(_plan_argument(args), fault=args.fault)
        elif args.command == "extend":
            result = scheduler.extend(_plan_argument(args), fault=args.fault)
        elif args.command == "claim":
            result = scheduler.claim(
                args.worker,
                _now(args.now),
                args.lease_seconds,
                args.limit,
                fault=args.fault,
            )
        elif args.command == "complete":
            result = scheduler.complete(
                args.instance,
                args.lease_token,
                args.output_digest,
                _now(args.now),
                fault=args.fault,
            )
        elif args.command == "fail":
            result = scheduler.fail(
                args.instance,
                args.lease_token,
                args.error,
                _now(args.now),
                fault=args.fault,
            )
        else:
            result = scheduler.status()
        json.dump(result, sys.stdout, ensure_ascii=False, sort_keys=True)
        sys.stdout.write("\n")
        return 0
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    finally:
        if scheduler is not None:
            scheduler.close()


if __name__ == "__main__":
    raise SystemExit(main())
