"""Reliable offline session materialization over SQLite.

The public contract is intentionally small: :class:`SessionEngine` keeps the
``ingest``/``sessions``/``changes`` interface used by the original prototype.
Everything durable (events, materialized sessions, per-tenant watermarks and
the changelog) is committed in one SQLite transaction.
"""

from __future__ import annotations

import sqlite3
import threading
import unicodedata
from collections.abc import Mapping


SCHEMA_VERSION = 2
GAP_SECONDS = 30
LATENESS_SECONDS = 60

EVENT_FIELDS = (
    "tenant",
    "user",
    "event_id",
    "event_time",
    "arrival_seq",
    "kind",
)
EVENT_KINDS = frozenset(("open", "ping", "close"))


class SessionMillError(Exception):
    """Base class for deterministic, user-facing failures."""


class ValidationError(SessionMillError, ValueError):
    """An input value violates the protocol contract."""


class ConflictError(SessionMillError, ValueError):
    """An event identity is bound to different content."""


class WatermarkError(SessionMillError, ValueError):
    """A watermark is missing, regressed or excludes an event."""


class SchemaVersionError(SessionMillError, RuntimeError, ValueError):
    """The database belongs to an unknown future schema version."""


class InjectedFault(SessionMillError, RuntimeError):
    """A deliberately injected fault used to verify transactional rollback."""


def _validate_text(value, name):
    if not isinstance(value, str):
        raise ValidationError(f"{name} must be a string")
    if not value:
        raise ValidationError(f"{name} must not be empty")
    if unicodedata.normalize("NFC", value) != value:
        raise ValidationError(f"{name} must already be NFC-normalized")
    for char in value:
        # Cc is the Unicode control category. Rejecting the whole C group also
        # excludes invisible format characters, surrogates and private-use
        # codepoints that should never appear in protocol identifiers.
        if unicodedata.category(char).startswith("C"):
            raise ValidationError(f"{name} must not contain control characters")
    return value


def _validate_integer(value, name):
    if type(value) is not int:
        raise ValidationError(f"{name} must be an integer")
    return value


def _validate_event(item, index):
    if not isinstance(item, Mapping):
        raise ValidationError(f"events[{index}] must be an object")
    if set(item.keys()) != set(EVENT_FIELDS):
        raise ValidationError(
            f"events[{index}] must have exactly the fields {', '.join(EVENT_FIELDS)}"
        )

    event = {
        "tenant": _validate_text(item["tenant"], f"events[{index}].tenant"),
        "user": _validate_text(item["user"], f"events[{index}].user"),
        "event_id": _validate_text(item["event_id"], f"events[{index}].event_id"),
        "event_time": _validate_integer(
            item["event_time"], f"events[{index}].event_time"
        ),
        "arrival_seq": _validate_integer(
            item["arrival_seq"], f"events[{index}].arrival_seq"
        ),
        "kind": item["kind"],
    }
    if not isinstance(event["kind"], str) or event["kind"] not in EVENT_KINDS:
        raise ValidationError(
            f"events[{index}].kind must be one of {', '.join(sorted(EVENT_KINDS))}"
        )
    return event


def _validate_events(events):
    if isinstance(events, (str, bytes, bytearray, Mapping)):
        raise ValidationError("events must be a list of event objects")
    try:
        iterator = iter(events)
    except TypeError as exc:
        raise ValidationError("events must be a list of event objects") from exc
    return [_validate_event(item, index) for index, item in enumerate(iterator)]


def _validate_watermarks(watermarks):
    if not isinstance(watermarks, Mapping):
        raise ValidationError("watermarks must be an object mapping tenant to integer")
    result = {}
    for tenant, watermark in watermarks.items():
        key = _validate_text(tenant, "watermark tenant")
        result[key] = _validate_integer(watermark, f"watermark for {key!r}")
    return result


def _dedupe_batch(events):
    """Validate batch-local identity bindings and drop exact duplicates."""

    by_id = {}
    by_seq = {}
    unique = []
    for event in events:
        previous = by_id.get(event["event_id"])
        if previous is not None:
            if previous != event:
                raise ConflictError(
                    f"event_id {event['event_id']!r} is bound to different content"
                )
            continue

        previous_seq = by_seq.get(event["arrival_seq"])
        if previous_seq is not None:
            raise ConflictError(
                f"arrival_seq {event['arrival_seq']} is bound to different content"
            )

        by_id[event["event_id"]] = event
        by_seq[event["arrival_seq"]] = event
        unique.append(event)
    return unique


def _session_dict(row):
    return {
        "session_id": row["session_id"],
        "tenant": row["tenant"],
        "user": row["user"],
        "start": row["start"],
        "end": row["end"],
        "event_count": row["event_count"],
        "closed": bool(row["closed"]),
        "revision": row["revision"],
    }


class SessionEngine:
    """Offline session materializer with an atomically updated changelog."""

    def __init__(self, db):
        self._lock = threading.RLock()
        self._closed = False
        self.db = sqlite3.connect(
            str(db),
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA busy_timeout = 30000")
        self.db.execute("PRAGMA foreign_keys = ON")
        self._initialize_schema()

    def __enter__(self):
        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def close(self):
        with self._lock:
            if not self._closed:
                self.db.close()
                self._closed = True

    def _ensure_open(self):
        if self._closed:
            raise SessionMillError("engine is closed")

    def _initialize_schema(self):
        with self._lock:
            version = self.db.execute("PRAGMA user_version").fetchone()[0]
            if version > SCHEMA_VERSION:
                self.db.close()
                self._closed = True
                raise SchemaVersionError(
                    f"database schema version {version} is newer than supported "
                    f"version {SCHEMA_VERSION}"
                )

            self.db.execute("BEGIN IMMEDIATE")
            try:
                # CREATE TABLE IF NOT EXISTS also upgrades the prototype's
                # version-0 database in place. The unique index is added
                # separately because an existing table cannot gain a constraint.
                self.db.execute(
                    "CREATE TABLE IF NOT EXISTS events("
                    "tenant TEXT NOT NULL,"
                    "user TEXT NOT NULL,"
                    "event_id TEXT PRIMARY KEY,"
                    "event_time INTEGER NOT NULL,"
                    "arrival_seq INTEGER NOT NULL,"
                    "kind TEXT NOT NULL)"
                )
                self.db.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS events_arrival_seq_idx "
                    "ON events(arrival_seq)"
                )
                self.db.execute(
                    "CREATE INDEX IF NOT EXISTS events_pair_time_idx "
                    "ON events(tenant, user, event_time, arrival_seq)"
                )
                self.db.execute(
                    "CREATE TABLE IF NOT EXISTS sessions("
                    "session_id TEXT PRIMARY KEY,"
                    "tenant TEXT NOT NULL,"
                    "user TEXT NOT NULL,"
                    "start INTEGER NOT NULL,"
                    "end INTEGER NOT NULL,"
                    "event_count INTEGER NOT NULL,"
                    "closed INTEGER NOT NULL,"
                    "revision INTEGER NOT NULL)"
                )
                self.db.execute(
                    "CREATE INDEX IF NOT EXISTS sessions_pair_idx "
                    "ON sessions(tenant, user)"
                )
                self.db.execute(
                    "CREATE TABLE IF NOT EXISTS watermarks("
                    "tenant TEXT PRIMARY KEY,"
                    "watermark INTEGER NOT NULL)"
                )
                self.db.execute(
                    "CREATE TABLE IF NOT EXISTS changelog("
                    "offset INTEGER PRIMARY KEY AUTOINCREMENT,"
                    "action TEXT NOT NULL,"
                    "session_id TEXT NOT NULL,"
                    "tenant TEXT NOT NULL,"
                    "user TEXT NOT NULL,"
                    "start INTEGER NOT NULL,"
                    "end INTEGER NOT NULL,"
                    "event_count INTEGER NOT NULL,"
                    "closed INTEGER NOT NULL,"
                    "revision INTEGER NOT NULL)"
                )
                self.db.execute(
                    "CREATE INDEX IF NOT EXISTS changelog_session_idx "
                    "ON changelog(session_id)"
                )
                if version < SCHEMA_VERSION:
                    self._materialize_legacy_events()
                self.db.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                self.db.execute("COMMIT")
            except BaseException:
                try:
                    self.db.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise

    def _materialize_legacy_events(self):
        """Rebuild sessions for version-0/version-1 databases with no facts."""

        pairs = self.db.execute(
            "SELECT DISTINCT tenant, user FROM events ORDER BY tenant, user"
        ).fetchall()
        for pair in pairs:
            existing = self.db.execute(
                "SELECT COUNT(*) FROM sessions WHERE tenant = ? AND user = ?",
                (pair["tenant"], pair["user"]),
            ).fetchone()[0]
            if existing:
                continue
            rows = self.db.execute(
                "SELECT tenant,user,event_id,event_time,arrival_seq,kind "
                "FROM events WHERE tenant = ? AND user = ? "
                "ORDER BY event_time ASC, arrival_seq ASC",
                (pair["tenant"], pair["user"]),
            ).fetchall()
            for session in self._build_sessions(rows):
                self.db.execute(
                    "INSERT INTO sessions(session_id,tenant,user,start,end,event_count,"
                    "closed,revision) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        session["session_id"],
                        session["tenant"],
                        session["user"],
                        session["start"],
                        session["end"],
                        session["event_count"],
                        1 if session["closed"] else 0,
                        1,
                    ),
                )

    def ingest(self, events, watermarks, fault=None):
        """Atomically ingest a batch and materialize affected sessions.

        ``fault`` may be one of ``after_events``, ``after_sessions`` or
        ``before_commit`` to inject a failure at that transaction stage.
        """

        events = _validate_events(events)
        watermarks = _validate_watermarks(watermarks)
        unique_events = _dedupe_batch(events)

        with self._lock:
            self._ensure_open()
            try:
                self.db.execute("BEGIN IMMEDIATE")
            except sqlite3.Error as exc:
                raise SessionMillError(
                    f"could not start ingest transaction: {exc}"
                ) from exc

            try:
                new_events = self._resolve_new_events(unique_events)
                self._validate_watermarks_for_batch(
                    new_events, unique_events, watermarks
                )
                self._insert_events(new_events)
                self._maybe_fault("after_events", fault)

                affected_pairs = sorted(
                    {(event["tenant"], event["user"]) for event in new_events}
                )
                retracts = []
                upserts = []
                for tenant, user in affected_pairs:
                    pair_retracts, pair_upserts = self._recompute_pair(tenant, user)
                    retracts.extend(pair_retracts)
                    upserts.extend(pair_upserts)

                self._persist_watermarks(watermarks)
                self._maybe_fault("after_sessions", fault)

                self._append_changelog(retracts, upserts)
                self._maybe_fault("before_commit", fault)

                session_count = self.db.execute(
                    "SELECT COUNT(*) FROM sessions"
                ).fetchone()[0]
                self.db.execute("COMMIT")
            except BaseException:
                try:
                    self.db.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise

            return {"accepted": len(new_events), "sessions": session_count}

    def _resolve_new_events(self, unique_events):
        new_events = []
        for event in unique_events:
            row_by_id = self.db.execute(
                "SELECT tenant,user,event_id,event_time,arrival_seq,kind "
                "FROM events WHERE event_id = ?",
                (event["event_id"],),
            ).fetchone()
            row_by_seq = self.db.execute(
                "SELECT tenant,user,event_id,event_time,arrival_seq,kind "
                "FROM events WHERE arrival_seq = ?",
                (event["arrival_seq"],),
            ).fetchone()

            if row_by_id is not None:
                existing = {
                    key: row_by_id[key]
                    for key in (
                        "tenant",
                        "user",
                        "event_id",
                        "event_time",
                        "arrival_seq",
                        "kind",
                    )
                }
                if existing != event:
                    raise ConflictError(
                        f"event_id {event['event_id']!r} is bound to different content"
                    )
            if row_by_seq is not None and row_by_seq["event_id"] != event["event_id"]:
                raise ConflictError(
                    f"arrival_seq {event['arrival_seq']} is bound to different content"
                )

            if row_by_id is None:
                new_events.append(event)
        return new_events

    def _validate_watermarks_for_batch(self, new_events, unique_events, watermarks):
        event_tenants = {event["tenant"] for event in unique_events}
        missing = sorted(event_tenants.difference(watermarks))
        if missing:
            raise WatermarkError(
                "missing watermark for tenant(s): "
                + ", ".join(repr(tenant) for tenant in missing)
            )

        tenants = set(watermarks) | event_tenants
        stored = {}
        for tenant in tenants:
            row = self.db.execute(
                "SELECT watermark FROM watermarks WHERE tenant = ?", (tenant,)
            ).fetchone()
            if row is not None:
                stored[tenant] = row["watermark"]

        for tenant, watermark in watermarks.items():
            previous = stored.get(tenant)
            if previous is not None and watermark < previous:
                raise WatermarkError(
                    f"watermark for tenant {tenant!r} regressed from "
                    f"{previous} to {watermark}"
                )

        # Exact duplicates are already durable and are therefore accepted
        # idempotently even after the lateness horizon has moved on. Only new
        # event content is subject to the lateness window.
        for event in new_events:
            watermark = watermarks[event["tenant"]]
            if not (
                watermark - LATENESS_SECONDS <= event["event_time"] <= watermark
            ):
                raise WatermarkError(
                    f"event {event['event_id']!r} at {event['event_time']} is outside "
                    f"the watermark window for tenant {event['tenant']!r}"
                )

    def _insert_events(self, new_events):
        try:
            for event in new_events:
                self.db.execute(
                    "INSERT INTO events(tenant,user,event_id,event_time,arrival_seq,kind) "
                    "VALUES(?,?,?,?,?,?)",
                    (
                        event["tenant"],
                        event["user"],
                        event["event_id"],
                        event["event_time"],
                        event["arrival_seq"],
                        event["kind"],
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise ConflictError(f"event identity conflict: {exc}") from exc

    def _persist_watermarks(self, watermarks):
        for tenant, watermark in watermarks.items():
            row = self.db.execute(
                "SELECT watermark FROM watermarks WHERE tenant = ?", (tenant,)
            ).fetchone()
            if row is None:
                self.db.execute(
                    "INSERT INTO watermarks(tenant, watermark) VALUES(?, ?)",
                    (tenant, watermark),
                )
            elif watermark > row["watermark"]:
                self.db.execute(
                    "UPDATE watermarks SET watermark = ? WHERE tenant = ?",
                    (watermark, tenant),
                )

    def _recompute_pair(self, tenant, user):
        old_rows = self.db.execute(
            "SELECT session_id,tenant,user,start,end,event_count,closed,revision "
            "FROM sessions WHERE tenant = ? AND user = ?",
            (tenant, user),
        ).fetchall()
        old = {row["session_id"]: _session_dict(row) for row in old_rows}

        event_rows = self.db.execute(
            "SELECT tenant,user,event_id,event_time,arrival_seq,kind "
            "FROM events WHERE tenant = ? AND user = ? "
            "ORDER BY event_time ASC, arrival_seq ASC",
            (tenant, user),
        ).fetchall()
        new_sessions = self._build_sessions(event_rows)

        retracts = []
        upserts = []
        new_by_id = {}

        for session in new_sessions:
            session_id = session["session_id"]
            previous = old.get(session_id)
            if previous is None:
                last_revision = self.db.execute(
                    "SELECT COALESCE(MAX(revision), 0) FROM changelog "
                    "WHERE session_id = ?",
                    (session_id,),
                ).fetchone()[0]
                session["revision"] = last_revision + 1
                upserts.append(session)
            else:
                unchanged = all(
                    previous[field] == session[field]
                    for field in (
                        "tenant",
                        "user",
                        "start",
                        "end",
                        "event_count",
                        "closed",
                    )
                )
                if unchanged:
                    session["revision"] = previous["revision"]
                else:
                    session["revision"] = previous["revision"] + 1
                    retracts.append(previous)
                    upserts.append(session)
            new_by_id[session_id] = session

        for session_id, previous in old.items():
            if session_id not in new_by_id:
                retracts.append(previous)

        # Rewriting the pair keeps the fact table exactly in sync with the
        # recomputed materialization. Unchanged sessions retain their revision.
        self.db.execute(
            "DELETE FROM sessions WHERE tenant = ? AND user = ?", (tenant, user)
        )
        for session in new_sessions:
            self.db.execute(
                "INSERT INTO sessions(session_id,tenant,user,start,end,event_count,"
                "closed,revision) VALUES(?,?,?,?,?,?,?,?)",
                (
                    session["session_id"],
                    session["tenant"],
                    session["user"],
                    session["start"],
                    session["end"],
                    session["event_count"],
                    1 if session["closed"] else 0,
                    session["revision"],
                ),
            )

        return retracts, upserts

    @staticmethod
    def _build_sessions(event_rows):
        sessions = []
        current = None
        previous = None

        for row in event_rows:
            event = {
                "tenant": row["tenant"],
                "user": row["user"],
                "event_id": row["event_id"],
                "event_time": row["event_time"],
                "arrival_seq": row["arrival_seq"],
                "kind": row["kind"],
            }
            starts_new = (
                current is None
                or event["event_time"] - previous["event_time"] > GAP_SECONDS
                or previous["kind"] == "close"
            )
            if starts_new:
                if current is not None:
                    sessions.append(SessionEngine._finalize_session(current))
                current = {
                    "tenant": event["tenant"],
                    "user": event["user"],
                    "start": event["event_time"],
                    "end": event["event_time"],
                    "event_count": 1,
                    "closed": event["kind"] == "close",
                    "ids": [event["event_id"]],
                }
            else:
                current["end"] = event["event_time"]
                current["event_count"] += 1
                current["closed"] = event["kind"] == "close"
                current["ids"].append(event["event_id"])
            previous = event

        if current is not None:
            sessions.append(SessionEngine._finalize_session(current))
        return sessions

    @staticmethod
    def _finalize_session(session):
        return {
            "session_id": min(session["ids"]),
            "tenant": session["tenant"],
            "user": session["user"],
            "start": session["start"],
            "end": session["end"],
            "event_count": session["event_count"],
            "closed": session["closed"],
        }

    def _append_changelog(self, retracts, upserts):
        # Both groups are sorted by ID; retracts always precede upserts.
        for action, records in (
            ("retract", sorted(retracts, key=lambda item: item["session_id"])),
            ("upsert", sorted(upserts, key=lambda item: item["session_id"])),
        ):
            for record in records:
                self.db.execute(
                    "INSERT INTO changelog(action,session_id,tenant,user,start,end,"
                    "event_count,closed,revision) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        action,
                        record["session_id"],
                        record["tenant"],
                        record["user"],
                        record["start"],
                        record["end"],
                        record["event_count"],
                        1 if record["closed"] else 0,
                        record["revision"],
                    ),
                )

    @staticmethod
    def _maybe_fault(stage, fault):
        if fault is None:
            return
        if isinstance(fault, str):
            if fault == stage:
                raise InjectedFault(f"injected fault at {stage}")
            return
        if isinstance(fault, BaseException):
            if stage == "before_commit":
                raise fault
            return
        if callable(fault):
            result = fault(stage)
            if isinstance(result, BaseException):
                raise result
            if result:
                raise InjectedFault(f"injected fault at {stage}")

    def sessions(self):
        with self._lock:
            self._ensure_open()
            rows = self.db.execute(
                "SELECT session_id,tenant,user,start,end,event_count,closed,revision "
                "FROM sessions ORDER BY tenant, user, start, session_id"
            ).fetchall()
            return [_session_dict(row) for row in rows]

    def changes(self, since=0):
        since = _validate_integer(since, "since")
        with self._lock:
            self._ensure_open()
            rows = self.db.execute(
                "SELECT offset,action,session_id,tenant,user,start,end,event_count,"
                "closed,revision FROM changelog WHERE offset > ? ORDER BY offset",
                (since,),
            ).fetchall()
            return [
                {
                    "offset": row["offset"],
                    "action": row["action"],
                    "session_id": row["session_id"],
                    "tenant": row["tenant"],
                    "user": row["user"],
                    "start": row["start"],
                    "end": row["end"],
                    "event_count": row["event_count"],
                    "closed": bool(row["closed"]),
                    "revision": row["revision"],
                }
                for row in rows
            ]
