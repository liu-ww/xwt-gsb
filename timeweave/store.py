"""Bitemporal dimension store and point-in-time fact join engine."""

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = 2
DIMENSION_FIELDS = (
    "entity",
    "valid_from",
    "valid_to",
    "system_from",
    "system_to",
    "revision",
    "attrs",
)
FACT_FIELDS = ("fact_id", "entity", "event_time", "observed_at", "metrics")
FAULT_STAGES = ("after_dimensions", "after_facts", "before_commit")
_NULL = "+INF"


class TemporalError(Exception):
    """Base class for business-level protocol errors."""


class ValidationError(TemporalError):
    """An input record violates the bitemporal protocol."""


class ConflictError(TemporalError):
    """A revision, interval or fact-id conflict was detected."""


class SchemaVersionError(TemporalError):
    """The database has an unsupported version or a corrupted structure."""


class ConcurrentUpdateError(TemporalError):
    """Another writer committed a competing revision first."""


def _parse_time(value, field):
    if not isinstance(value, str) or not value:
        raise ValidationError("%s must be an RFC3339 second-level string" % field)
    text = value
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise ValidationError("%s is not a valid RFC3339 timestamp: %r" % (field, value))
    if parsed.tzinfo is None:
        raise ValidationError("%s must carry a timezone: %r" % (field, value))
    if parsed.microsecond != 0:
        raise ValidationError("%s must be whole seconds: %r" % (field, value))
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def _norm(value, field):
    if value is None:
        return None
    return _parse_time(value, field).strftime("%Y-%m-%dT%H:%M:%SZ")


def _key(normalized):
    return _NULL if normalized is None else normalized


def _validate_dimension(record):
    if not isinstance(record, dict):
        raise ValidationError("dimension must be an object")
    if tuple(sorted(record)) != tuple(sorted(DIMENSION_FIELDS)):
        raise ValidationError("dimension fields must be exactly %s" % (DIMENSION_FIELDS,))
    entity = record["entity"]
    if not isinstance(entity, str) or not entity:
        raise ValidationError("entity must be a non-empty string")
    revision = record["revision"]
    if isinstance(revision, bool) or not isinstance(revision, int) or revision <= 0:
        raise ValidationError("revision must be a positive integer")
    if not isinstance(record["attrs"], dict):
        raise ValidationError("attrs must be an object")
    valid_from = _norm(record["valid_from"], "valid_from")
    valid_to = _norm(record["valid_to"], "valid_to")
    system_from = _norm(record["system_from"], "system_from")
    system_to = _norm(record["system_to"], "system_to")
    if valid_to is not None and valid_from >= valid_to:
        raise ValidationError("business interval must be non-empty and half-open")
    if system_to is not None and system_from >= system_to:
        raise ValidationError("system interval must be non-empty and half-open")
    return {
        "entity": entity,
        "valid_from": valid_from,
        "valid_to": valid_to,
        "system_from": system_from,
        "system_to": system_to,
        "revision": revision,
        "attrs": json.dumps(record["attrs"], sort_keys=True, separators=(",", ":")),
    }


def _validate_fact(record):
    if not isinstance(record, dict):
        raise ValidationError("fact must be an object")
    if tuple(sorted(record)) != tuple(sorted(FACT_FIELDS)):
        raise ValidationError("fact fields must be exactly %s" % (FACT_FIELDS,))
    fact_id = record["fact_id"]
    entity = record["entity"]
    if not isinstance(fact_id, str) or not fact_id:
        raise ValidationError("fact_id must be a non-empty string")
    if not isinstance(entity, str) or not entity:
        raise ValidationError("entity must be a non-empty string")
    if not isinstance(record["metrics"], dict):
        raise ValidationError("metrics must be an object")
    event_time = _norm(record["event_time"], "event_time")
    observed_at = _norm(record["observed_at"], "observed_at")
    return {
        "fact_id": fact_id,
        "entity": entity,
        "event_time": event_time,
        "observed_at": observed_at,
        "metrics": json.dumps(record["metrics"], sort_keys=True, separators=(",", ":")),
    }


class TemporalStore:
    def __init__(self, db):
        path = Path(db)
        if path.parent and not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
        self.path = str(path)
        self.db = sqlite3.connect(self.path, timeout=30.0)
        try:
            self.db.row_factory = sqlite3.Row
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA foreign_keys=ON")
            self.db.execute("PRAGMA busy_timeout=30000")
            self._initialize_schema()
        except BaseException:
            self.db.close()
            raise

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def _table_exists(self, name):
        row = self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
        return row is not None

    def _initialize_schema(self):
        if self._table_exists("meta"):
            self._verify_schema()
            return
        if self._table_exists("dimensions") or self._table_exists("facts"):
            raise SchemaVersionError(
                "database is missing metadata but already contains temporal tables"
            )
        self._create_schema()

    def _create_schema(self):
        self.db.execute("BEGIN IMMEDIATE")
        self.db.executescript(
            """
            CREATE TABLE meta(
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE dimensions(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity TEXT NOT NULL,
                valid_from TEXT NOT NULL,
                valid_to TEXT,
                system_from TEXT NOT NULL,
                system_to TEXT,
                revision INTEGER NOT NULL,
                attrs TEXT NOT NULL
            );
            CREATE TABLE facts(
                fact_id TEXT PRIMARY KEY,
                entity TEXT NOT NULL,
                event_time TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                metrics TEXT NOT NULL
            );
            """
        )
        self.db.execute(
            "INSERT INTO meta(key,value) VALUES('schema_version',?)", (str(SCHEMA_VERSION),)
        )
        self.db.execute(
            "CREATE INDEX idx_dim_entity_time ON dimensions(entity, valid_from, valid_to, system_from, system_to)"
        )
        self.db.execute("CREATE INDEX idx_facts_entity ON facts(entity)")
        self.db.commit()

    def _verify_schema(self):
        try:
            rows = self.db.execute("SELECT key,value FROM meta").fetchall()
        except sqlite3.DatabaseError as exc:
            raise SchemaVersionError("database metadata is unreadable: %s" % exc)
        meta = {row[0]: row[1] for row in rows}
        if "schema_version" not in meta:
            raise SchemaVersionError("database is missing schema_version metadata")
        try:
            version = int(meta["schema_version"])
        except ValueError:
            raise SchemaVersionError("schema_version is corrupted")
        if version != SCHEMA_VERSION:
            raise SchemaVersionError(
                "unsupported schema version %d; expected %d" % (version, SCHEMA_VERSION)
            )
        expected = {
            "dimensions": (
                "id",
                "entity",
                "valid_from",
                "valid_to",
                "system_from",
                "system_to",
                "revision",
                "attrs",
            ),
            "facts": ("fact_id", "entity", "event_time", "observed_at", "metrics"),
        }
        for table, columns in expected.items():
            if not self._table_exists(table):
                raise SchemaVersionError("required table %s is missing or corrupted" % table)
            actual = tuple(row[1] for row in self.db.execute("PRAGMA table_info(%s)" % table))
            if actual != columns:
                raise SchemaVersionError("table %s has a corrupted structure" % table)

    def apply(self, dimensions, facts, fault=None):
        if dimensions is None:
            dimensions = []
        if facts is None:
            facts = []
        if not isinstance(dimensions, (list, tuple)) or not isinstance(facts, (list, tuple)):
            raise ValidationError("dimensions and facts must be lists")
        if fault is not None and fault not in FAULT_STAGES:
            raise ValidationError("fault must be one of %s" % (FAULT_STAGES,))
        clean_dims = [_validate_dimension(item) for item in dimensions]
        clean_facts = [_validate_fact(item) for item in facts]
        self._reject_duplicate_facts(clean_facts)
        try:
            self.db.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            raise ConcurrentUpdateError("a concurrent revision is in progress: %s" % exc)
        try:
            for dimension in clean_dims:
                self._insert_dimension(dimension)
            if fault == "after_dimensions":
                raise RuntimeError("injected fault after_dimensions")
            for fact in clean_facts:
                self._insert_fact(fact)
            if fault == "after_facts":
                raise RuntimeError("injected fault after_facts")
            if fault == "before_commit":
                raise RuntimeError("injected fault before_commit")
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise
        return {"dimensions": len(clean_dims), "facts": len(clean_facts)}

    def _reject_duplicate_facts(self, facts):
        seen = set()
        for fact in facts:
            if fact["fact_id"] in seen:
                raise ConflictError("fact_id appears twice in one batch: %s" % fact["fact_id"])
            seen.add(fact["fact_id"])

    def _insert_dimension(self, dimension):
        entity = dimension["entity"]
        revision = dimension["revision"]
        existing = self.db.execute(
            "SELECT id,revision FROM dimensions WHERE entity=? AND revision=?",
            (entity, revision),
        ).fetchone()
        if existing is not None:
            raise ConflictError(
                "entity %r already has revision %d" % (entity, revision)
            )
        self._assert_no_business_overlap(dimension)
        self._assert_system_intervals_consistent(dimension)
        self.db.execute(
            """
            INSERT INTO dimensions
                (entity,valid_from,valid_to,system_from,system_to,revision,attrs)
            VALUES (?,?,?,?,?,?,?)
            """,
            (
                entity,
                dimension["valid_from"],
                dimension["valid_to"],
                dimension["system_from"],
                dimension["system_to"],
                revision,
                dimension["attrs"],
            ),
        )

    def _assert_no_business_overlap(self, dimension):
        entity = dimension["entity"]
        new_valid_start = dimension["valid_from"]
        new_valid_end = _key(dimension["valid_to"])
        new_system_start = dimension["system_from"]
        new_system_end = _key(dimension["system_to"])
        rows = self.db.execute(
            """
            SELECT valid_from, valid_to, system_from, system_to, revision
            FROM dimensions
            WHERE entity = ?
            """,
            (entity,),
        ).fetchall()
        for row in rows:
            business_overlap = (
                new_valid_start < _key(row["valid_to"])
                and row["valid_from"] < new_valid_end
            )
            system_overlap = (
                new_system_start < _key(row["system_to"])
                and row["system_from"] < new_system_end
            )
            if business_overlap and system_overlap:
                raise ConflictError(
                    "business intervals of entity %r overlap on a shared "
                    "system-time slice (existing revision %d)"
                    % (entity, row["revision"])
                )
    def _assert_system_intervals_consistent(self, dimension):
        entity = dimension["entity"]
        new_start = dimension["system_from"]
        new_end = _key(dimension["system_to"])
        rows = self.db.execute(
            """
            SELECT id, system_from, system_to, revision
            FROM dimensions
            WHERE entity = ?
            ORDER BY id
            """,
            (entity,),
        ).fetchall()
        open_rows = [row for row in rows if row["system_to"] is None]
        closed_target = None
        if dimension["system_to"] is None:
            if open_rows:
                target = open_rows[-1]
                if target["revision"] >= dimension["revision"]:
                    raise ConflictError(
                        "correction for entity %r requires a higher revision than %d"
                        % (entity, target["revision"])
                    )
                self.db.execute(
                    "UPDATE dimensions SET system_to=? WHERE id=? AND system_to IS NULL",
                    (new_start, target["id"]),
                )
                closed_target = target["id"]
            else:
                latest = self.db.execute(
                    "SELECT MAX(system_from) AS latest FROM dimensions WHERE entity=?",
                    (entity,),
                ).fetchone()
                if latest["latest"] is not None and new_start <= latest["latest"]:
                    raise ConflictError(
                        "new open system interval for entity %r must start after prior intervals"
                        % entity
                    )
        for row in rows:
            if closed_target is not None and row["id"] == closed_target:
                other_end = new_start
            else:
                other_end = _key(row["system_to"])
            other_start = row["system_from"]
            if new_start < other_end and other_start < new_end:
                raise ConflictError(
                    "system interval overlaps existing revision %d for entity %r"
                    % (row["revision"], entity)
                )
    def _insert_fact(self, fact):
        existing = self.db.execute(
            "SELECT entity,event_time,observed_at,metrics FROM facts WHERE fact_id=?",
            (fact["fact_id"],),
        ).fetchone()
        if existing is not None:
            current = (
                existing["entity"],
                existing["event_time"],
                existing["observed_at"],
                existing["metrics"],
            )
            incoming = (
                fact["entity"],
                fact["event_time"],
                fact["observed_at"],
                fact["metrics"],
            )
            if current != incoming:
                raise ConflictError(
                    "fact_id %r is permanently bound to different content" % fact["fact_id"]
                )
            return
        self.db.execute(
            """
            INSERT INTO facts(fact_id,entity,event_time,observed_at,metrics)
            VALUES (?,?,?,?,?)
            """,
            (
                fact["fact_id"],
                fact["entity"],
                fact["event_time"],
                fact["observed_at"],
                fact["metrics"],
            ),
        )

    def join(self, as_of):
        as_of_norm = _norm(as_of, "as_of")
        result = []
        facts = self.db.execute(
            """
            SELECT fact_id,entity,event_time,observed_at,metrics
            FROM facts
            ORDER BY fact_id
            """
        ).fetchall()
        for fact in facts:
            if as_of_norm < fact["observed_at"]:
                continue
            dimension = self.db.execute(
                """
                SELECT revision, attrs
                FROM dimensions
                WHERE entity = ?
                  AND valid_from <= ?
                  AND (valid_to IS NULL OR ? < valid_to)
                  AND system_from <= ?
                  AND (system_to IS NULL OR ? < system_to)
                ORDER BY revision DESC
                """,
                (
                    fact["entity"],
                    fact["event_time"],
                    fact["event_time"],
                    as_of_norm,
                    as_of_norm,
                ),
            ).fetchall()
            if len(dimension) > 1:
                raise ConflictError(
                    "multiple visible dimensions for fact %r at %s"
                    % (fact["fact_id"], as_of_norm)
                )
            chosen = dimension[0] if dimension else None
            result.append(
                {
                    "fact_id": fact["fact_id"],
                    "entity": fact["entity"],
                    "event_time": fact["event_time"],
                    "observed_at": fact["observed_at"],
                    "metrics": json.loads(fact["metrics"]),
                    "dimension": None if chosen is None else json.loads(chosen["attrs"]),
                    "dimension_revision": None if chosen is None else chosen["revision"],
                    "unmatched": chosen is None,
                }
            )
        return result


def main(argv=None):
    import argparse

    parser = argparse.ArgumentParser(prog="timeweave")
    sub = parser.add_subparsers(dest="command", required=True)

    apply_parser = sub.add_parser("apply")
    apply_parser.add_argument("db")
    apply_parser.add_argument("--input", required=True)
    apply_parser.add_argument(
        "--fault", choices=FAULT_STAGES, default=None
    )

    join_parser = sub.add_parser("join")
    join_parser.add_argument("db")
    join_parser.add_argument("--as-of", required=True)

    args = parser.parse_args(argv)
    try:
        store = TemporalStore(args.db)
        try:
            if args.command == "apply":
                with open(args.input, "r", encoding="utf-8") as handle:
                    payload = json.load(handle)
                dimensions = payload.get("dimensions", [])
                facts = payload.get("facts", [])
                summary = store.apply(dimensions, facts, fault=args.fault)
                print(json.dumps(summary, ensure_ascii=False))
            else:
                print(json.dumps(store.join(args.as_of), ensure_ascii=False))
        finally:
            store.close()
    except TemporalError as exc:
        print("error: %s" % exc, file=sys.stderr)
        return 2
    except FileNotFoundError as exc:
        print("error: %s" % exc, file=sys.stderr)
        return 2
    except json.JSONDecodeError as exc:
        print("error: invalid JSON input: %s" % exc, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
