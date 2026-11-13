"""Command line interface for sessionmill."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from .engine import SessionEngine

COMMANDS = ("ingest", "sessions", "changes")
EXPECTED_ARGS = {"ingest": 2, "sessions": 0, "changes": 1}
USAGE = (
    "usage: python -m sessionmill [--db PATH] ingest EVENTS WATERMARKS | "
    "sessions | changes SINCE"
)


class CliError(Exception):
    pass


def _emit(value):
    text = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    sys.stdout.write(text + "\n")


def _emit_error(exc):
    text = json.dumps(
        {"error": str(exc) or exc.__class__.__name__},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    sys.stderr.write(text + "\n")


def _extract_db_options(args):
    db = None
    cleaned = []
    index = 0
    while index < len(args):
        argument = args[index]
        if argument in ("--db", "-d"):
            if index + 1 >= len(args):
                raise CliError("--db requires a path")
            db = args[index + 1]
            index += 2
        elif argument.startswith("--db="):
            db = argument.split("=", 1)[1]
            index += 1
        else:
            cleaned.append(argument)
            index += 1
    if db is not None and not db:
        raise CliError("--db must not be empty")
    return db, cleaned


def _parse_args(argv):
    db, cleaned = _extract_db_options(argv)
    command_index = next(
        (index for index, item in enumerate(cleaned) if item in COMMANDS), None
    )
    if command_index is None:
        raise CliError("missing command; " + USAGE)

    command = cleaned[command_index]
    prefix = cleaned[:command_index]
    parameters = cleaned[command_index + 1 :]

    if prefix:
        if db is None and len(prefix) == 1:
            db = prefix[0]
        else:
            raise CliError("unexpected arguments before command; " + USAGE)

    expected = EXPECTED_ARGS[command]
    # Also accept the convenient ``COMMAND ARGS... DB`` form when no database
    # was supplied by --db or the environment.
    if db is None and len(parameters) == expected + 1:
        db = parameters[-1]
        parameters = parameters[:-1]
    if len(parameters) != expected:
        raise CliError(f"{command} expects {expected} argument(s); " + USAGE)

    if db is None:
        db = os.environ.get("SESSIONMILL_DB") or "sessionmill.sqlite3"
    return db, command, parameters


def _read_argument(argument, label):
    if argument == "-":
        return sys.stdin.read()
    path = Path(argument)
    if path.is_file():
        return path.read_text(encoding="utf-8")
    stripped = argument.lstrip()
    if stripped.startswith("[") or stripped.startswith("{"):
        return argument
    raise CliError(f"{label} file not found: {argument}")


def _load_events(argument):
    text = _read_argument(argument, "events")
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        rows = []
        for line_number, line in enumerate(text.splitlines(), 1):
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise CliError(
                    f"invalid events JSON on line {line_number}: {exc.msg}"
                ) from exc
        if not rows:
            raise CliError("events input is empty or invalid JSON")
        return rows
    if not isinstance(value, list):
        raise CliError("events JSON must be a list")
    return value


def _load_watermarks(argument):
    text = _read_argument(argument, "watermarks")
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise CliError(f"invalid watermarks JSON: {exc.msg}") from exc
    if not isinstance(value, dict):
        raise CliError("watermarks JSON must be an object")
    return value


def _parse_since(argument):
    try:
        value = int(argument, 10)
    except (TypeError, ValueError) as exc:
        raise CliError("SINCE must be an integer") from exc
    return value


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if any(item in ("--help", "-h") for item in argv):
        sys.stdout.write(USAGE + "\n")
        return 0
    try:
        db, command, parameters = _parse_args(argv)
        engine = SessionEngine(db)
        try:
            if command == "ingest":
                events = _load_events(parameters[0])
                watermarks = _load_watermarks(parameters[1])
                result = engine.ingest(events, watermarks)
            elif command == "sessions":
                result = engine.sessions()
            else:
                result = engine.changes(_parse_since(parameters[0]))
        finally:
            engine.close()
        _emit(result)
        return 0
    except Exception as exc:  # business errors never leak a traceback
        _emit_error(exc)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
