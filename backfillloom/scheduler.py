"""Persistent deterministic scheduler for partition backfill DAGs.

The scheduler models work only.  It never executes commands referenced by the
plan; callers run the real commands with the lease token they receive.
"""

from __future__ import annotations

import contextlib
import datetime as _datetime
import json
import re
import secrets
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = 2
PENDING = "pending"
RUNNING = "running"
SUCCESS = "success"
FAILED = "failed"
BLOCKED = "blocked"
STATES = (PENDING, RUNNING, SUCCESS, FAILED, BLOCKED)

_PLAN_KEYS = frozenset(("plan_id", "partitions", "tasks"))
_TASK_KEYS = frozenset(
    ("name", "depends_on", "max_attempts", "fan_in", "command_digest")
)
_FAN_IN = frozenset(("same_partition", "all_previous"))
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_FAULT_PHASES = frozenset(("after_lease", "before_commit"))


class SchedulerError(Exception):
    """Base scheduler error."""


class PlanError(SchedulerError, ValueError):
    """Invalid plan, extension, or schema version."""


class LeaseError(SchedulerError, ValueError):
    """A lease is stale, expired, or otherwise unusable."""


class FaultInjected(SchedulerError, RuntimeError):
    """Raised at a requested transactional fault point."""


SchedulerFault = FaultInjected


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise LeaseError(f"{name} must be a number")
    return float(value)


def _error_text(error: Any) -> str:
    value = error
    if isinstance(error, dict):
        value = error.get("category", error.get("error", error))
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return str(value)


def _task_signature(tasks: Iterable[dict]) -> str:
    ordered = sorted(tasks, key=lambda task: task["name"])
    return json.dumps(
        ordered, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False,
    )


def _canonical_plan(plan: dict) -> str:
    payload = {
        "partitions": list(plan["partitions"]),
        "tasks": sorted(plan["tasks"], key=lambda task: task["name"]),
    }
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False,
    )


def _validate_plan(plan: Any) -> dict:
    if not isinstance(plan, dict):
        raise PlanError("plan must be an object")
    if set(plan) != _PLAN_KEYS:
        raise PlanError(
            "plan fields must be exactly plan_id,partitions,tasks"
        )

    plan_id = plan["plan_id"]
    if not isinstance(plan_id, str) or not plan_id:
        raise PlanError("plan_id must be a non-empty string")

    partitions = plan["partitions"]
    if not isinstance(partitions, list):
        raise PlanError("partitions must be a list")
    checked_partitions = []
    for partition in partitions:
        if not isinstance(partition, str) or not _DATE_RE.match(partition):
            raise PlanError(f"invalid partition date: {partition!r}")
        try:
            parsed = _datetime.date.fromisoformat(partition)
        except ValueError as exc:
            raise PlanError(f"invalid partition date: {partition!r}") from exc
        if parsed.isoformat() != partition:
            raise PlanError(f"invalid partition date: {partition!r}")
        checked_partitions.append(partition)
    if checked_partitions != sorted(checked_partitions):
        raise PlanError("partitions must be sorted")
    if len(set(checked_partitions)) != len(checked_partitions):
        raise PlanError("partitions must be unique")

    tasks = plan["tasks"]
    if not isinstance(tasks, list):
        raise PlanError("tasks must be a list")
    checked_tasks = []
    names = set()
    for task in tasks:
        if not isinstance(task, dict) or set(task) != _TASK_KEYS:
            raise PlanError(
                "task fields must be exactly "
                "name,depends_on,max_attempts,fan_in,command_digest"
            )
        name = task["name"]
        if not isinstance(name, str) or not name:
            raise PlanError("task name must be a non-empty string")
        if name in names:
            raise PlanError(f"duplicate task name: {name}")
        names.add(name)
        depends_on = task["depends_on"]
        if not isinstance(depends_on, list) or any(
            not isinstance(dependency, str) for dependency in depends_on
        ):
            raise PlanError(f"task {name}: depends_on must be a list of names")
        if len(set(depends_on)) != len(depends_on):
            raise PlanError(f"task {name}: duplicate dependency")
        max_attempts = task["max_attempts"]
        if not _is_int(max_attempts) or max_attempts <= 0:
            raise PlanError(f"task {name}: max_attempts must be positive")
        if task["fan_in"] not in _FAN_IN:
            raise PlanError(
                f"task {name}: fan_in must be same_partition or all_previous"
            )
        if not isinstance(task["command_digest"], str):
            raise PlanError(f"task {name}: command_digest must be a string")
        checked_tasks.append({
            "name": name,
            "depends_on": list(depends_on),
            "max_attempts": max_attempts,
            "fan_in": task["fan_in"],
            "command_digest": task["command_digest"],
        })

    for task in checked_tasks:
        for dependency in task["depends_on"]:
            if dependency not in names:
                raise PlanError(
                    f"task {task['name']}: unknown dependency {dependency!r}"
                )

    by_name = {task["name"]: task for task in checked_tasks}
    visiting = set()
    visited = set()

    def visit(name: str) -> None:
        if name in visited:
            return
        if name in visiting:
            raise PlanError(f"dependency cycle involving task {name!r}")
        visiting.add(name)
        for dependency in by_name[name]["depends_on"]:
            visit(dependency)
        visiting.remove(name)
        visited.add(name)

    for name in by_name:
        visit(name)

    return {
        "plan_id": plan_id,
        "partitions": checked_partitions,
        "tasks": checked_tasks,
    }


def _layers(tasks: Iterable[dict]) -> dict[str, int]:
    by_name = {task["name"]: task for task in tasks}
    result = {}

    def layer(name: str) -> int:
        if name not in result:
            deps = by_name[name]["depends_on"]
            result[name] = 0 if not deps else max(layer(dep) for dep in deps) + 1
        return result[name]

    for name in by_name:
        layer(name)
    return result


def _edges(plan: dict) -> set[tuple[str, str]]:
    partitions = plan["partitions"]
    indexes = {part: index for index, part in enumerate(partitions)}
    result = set()
    for task in plan["tasks"]:
        child_task = task["name"]
        for part in partitions:
            child = f"{child_task}@{part}"
            index = indexes[part]
            for dependency in task["depends_on"]:
                result.add((f"{dependency}@{part}", child))
                if task["fan_in"] == "all_previous":
                    for earlier in partitions[:index]:
                        result.add((f"{dependency}@{earlier}", child))
    return result


def _instance(task: str, partition: str) -> str:
    return f"{task}@{partition}"


class Scheduler:
    """SQLite-backed scheduler for one active plan."""

    def __init__(self, db: Any):
        self._lock = threading.RLock()
        self._owns = not isinstance(db, sqlite3.Connection)
        if isinstance(db, sqlite3.Connection):
            self.db = db
        else:
            if isinstance(db, Path):
                db = str(db)
            self.db = sqlite3.connect(
                db, timeout=30.0, isolation_level=None, check_same_thread=False
            )
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA busy_timeout = 30000")
        self.plan = None
        self.plan_id = None
        try:
            self._initialize()
        except BaseException:
            if self._owns:
                self.db.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def close(self):
        with self._lock:
            if self.db is not None and self._owns:
                self.db.close()
            self.db = None
            self.plan = None
            self.plan_id = None

    def _check(self):
        if self.db is None:
            raise SchedulerError("scheduler is closed")

    @contextlib.contextmanager
    def _tx(self, immediate=True):
        self._check()
        with self._lock:
            self.db.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            try:
                yield
            except BaseException:
                try:
                    self.db.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            else:
                try:
                    self.db.execute("COMMIT")
                except BaseException:
                    try:
                        self.db.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                    raise

    def _initialize(self):
        with self._tx():
            version = int(self.db.execute("PRAGMA user_version").fetchone()[0])
            if version not in (0, SCHEMA_VERSION):
                if version > SCHEMA_VERSION:
                    raise PlanError(
                        f"unsupported future SQLite schema version {version}; "
                        f"supports {SCHEMA_VERSION}"
                    )
                raise PlanError(f"unsupported SQLite schema version {version}")
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS plans ("
                "plan_id TEXT PRIMARY KEY, canonical TEXT NOT NULL,"
                "tasks_json TEXT NOT NULL, partitions_json TEXT NOT NULL,"
                "max_partition TEXT, updated_at REAL NOT NULL)"
            )
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS instances ("
                "plan_id TEXT NOT NULL, instance TEXT NOT NULL,"
                "task TEXT NOT NULL, partition TEXT NOT NULL,"
                "state TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,"
                "lease_token TEXT, lease_worker TEXT, lease_until REAL,"
                "output_digest TEXT, success_token TEXT, error TEXT,"
                "PRIMARY KEY (plan_id, instance),"
                "FOREIGN KEY (plan_id) REFERENCES plans(plan_id))"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS instances_lease_idx "
                "ON instances(plan_id, state, lease_until)"
            )
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS meta ("
                "key TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )
            if version == 0:
                self.db.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

    def _stored_plan(self, row):
        payload = json.loads(row["canonical"])
        return {
            "plan_id": row["plan_id"],
            "partitions": list(payload["partitions"]),
            "tasks": list(payload["tasks"]),
        }

    def _activate(self, row):
        self.plan = self._stored_plan(row)
        self.plan_id = self.plan["plan_id"]
        return self.plan

    def _set_active(self, plan_id):
        self.db.execute(
            "INSERT INTO meta(key,value) VALUES('active_plan_id',?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (plan_id,),
        )

    def _ensure_active(self):
        self._check()
        if self.plan is not None:
            return self.plan
        with self._tx():
            active = self.db.execute(
                "SELECT value FROM meta WHERE key='active_plan_id'"
            ).fetchone()
            row = None
            if active is not None:
                row = self.db.execute(
                    "SELECT * FROM plans WHERE plan_id=?", (active["value"],)
                ).fetchone()
            if row is None:
                rows = self.db.execute("SELECT * FROM plans ORDER BY plan_id").fetchall()
                if len(rows) == 1:
                    row = rows[0]
            if row is None:
                if not self.db.execute("SELECT COUNT(*) FROM plans").fetchone()[0]:
                    raise PlanError("no plan loaded")
                raise PlanError("multiple plans stored; call load(plan)")
            self._activate(row)
            self._set_active(row["plan_id"])
        return self.plan

    def _max_attempts(self, plan):
        return {task["name"]: task["max_attempts"] for task in plan["tasks"]}

    def _insert_instances(self, plan, partitions):
        added = set()
        for partition in partitions:
            for task in plan["tasks"]:
                instance = _instance(task["name"], partition)
                cursor = self.db.execute(
                    "INSERT OR IGNORE INTO instances "
                    "(plan_id, instance, task, partition, state, attempt) "
                    "VALUES (?, ?, ?, ?, ?, 0)",
                    (plan["plan_id"], instance, task["name"], partition, PENDING),
                )
                if cursor.rowcount:
                    added.add(instance)
        return added

    def _tail(self, old_partitions, new_partitions):
        if not old_partitions:
            return list(new_partitions)
        if new_partitions == old_partitions:
            return []
        if (
            len(new_partitions) > len(old_partitions)
            and new_partitions[:len(old_partitions)] == old_partitions
        ):
            tail = list(new_partitions[len(old_partitions):])
        else:
            maximum = old_partitions[-1]
            if not new_partitions or any(p <= maximum for p in new_partitions):
                return None
            tail = list(new_partitions)
        maximum = old_partitions[-1]
        if any(p <= maximum for p in tail):
            return None
        return tail

    def _apply_extension(self, existing, submitted):
        if existing["plan_id"] != submitted["plan_id"]:
            raise PlanError("extend must keep the same plan_id")
        if _task_signature(existing["tasks"]) != _task_signature(submitted["tasks"]):
            raise PlanError("extend may not change task definitions")
        tail = self._tail(existing["partitions"], submitted["partitions"])
        if tail is None:
            raise PlanError(
                "extend may only append partitions later than the current maximum"
            )
        effective = {
            "plan_id": existing["plan_id"],
            "partitions": existing["partitions"] + tail,
            "tasks": submitted["tasks"],
        }
        added = self._insert_instances(effective, tail)
        self.db.execute(
            "UPDATE plans SET canonical=?, tasks_json=?, partitions_json=?, "
            "max_partition=?, updated_at=? WHERE plan_id=?",
            (
                _canonical_plan(effective),
                _task_signature(effective["tasks"]),
                json.dumps(effective["partitions"], separators=(",", ":")),
                effective["partitions"][-1] if effective["partitions"] else None,
                time.time(),
                effective["plan_id"],
            ),
        )
        self._propagate(effective, only=added)
        return effective, added

    def _propagate(self, plan, only=None):
        rows = self.db.execute(
            "SELECT instance, state FROM instances WHERE plan_id=?",
            (plan["plan_id"],),
        ).fetchall()
        states = {row["instance"]: row["state"] for row in rows}
        edges = _edges(plan)
        blocked = set()
        changed = True
        while changed:
            changed = False
            for parent, child in edges:
                if only is not None and child not in only:
                    continue
                if states.get(parent) not in (FAILED, BLOCKED):
                    continue
                if states.get(child) not in (SUCCESS, FAILED, BLOCKED):
                    states[child] = BLOCKED
                    blocked.add(child)
                    changed = True
        for instance in blocked:
            self.db.execute(
                "UPDATE instances SET state=?, lease_token=NULL, "
                "lease_worker=NULL, lease_until=NULL, error=NULL "
                "WHERE plan_id=? AND instance=?",
                (BLOCKED, plan["plan_id"], instance),
            )
        return blocked

    # --------------------------------------------------------------
    # protocol API
    # --------------------------------------------------------------
    def load(self, plan, fault=None):
        checked = _validate_plan(plan)
        canonical = _canonical_plan(checked)
        with self._tx():
            row = self.db.execute(
                "SELECT * FROM plans WHERE plan_id=?", (checked["plan_id"],)
            ).fetchone()
            if row is None:
                self.db.execute(
                    "INSERT INTO plans "
                    "(plan_id, canonical, tasks_json, partitions_json, "
                    "max_partition, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        checked["plan_id"],
                        canonical,
                        _task_signature(checked["tasks"]),
                        json.dumps(checked["partitions"], separators=(",", ":")),
                        checked["partitions"][-1] if checked["partitions"] else None,
                        time.time(),
                    ),
                )
                self._insert_instances(checked, checked["partitions"])
                effective = checked
            else:
                existing = self._stored_plan(row)
                if row["canonical"] == canonical:
                    effective = existing
                    self._insert_instances(effective, effective["partitions"])
                else:
                    raise PlanError(
                        "plan_id is permanently bound to different plan content; "
                        "later partitions must be added with extend(plan)"
                    )
            self._set_active(effective["plan_id"])
            _inject_fault(fault)
            self.plan = effective
            self.plan_id = effective["plan_id"]
            total = self.db.execute(
                "SELECT COUNT(*) FROM instances WHERE plan_id=?",
                (effective["plan_id"],),
            ).fetchone()[0]
        return {"instances": int(total)}

    def extend(self, plan, fault=None):
        checked = _validate_plan(plan)
        self._ensure_active()
        with self._tx():
            plan_id = self.plan_id
            row = self.db.execute(
                "SELECT * FROM plans WHERE plan_id=?", (plan_id,)
            ).fetchone()
            if row is None:
                raise PlanError("active plan is no longer stored")
            if checked["plan_id"] != plan_id:
                raise PlanError("extend must keep the same plan_id")
            existing = self._stored_plan(row)
            effective, added = self._apply_extension(existing, checked)
            self._set_active(plan_id)
            _inject_fault(fault)
            self.plan = effective
            self.plan_id = plan_id
            total = self.db.execute(
                "SELECT COUNT(*) FROM instances WHERE plan_id=?", (plan_id,)
            ).fetchone()[0]
        return {"instances": int(total), "added": len(added)}

    def claim(self, worker, now, lease_seconds, limit=1, fault=None):
        if not isinstance(worker, str) or not worker:
            raise LeaseError("worker must be a non-empty string")
        now_value = _number(now, "now")
        lease_value = _number(lease_seconds, "lease_seconds")
        if lease_value < 0:
            raise LeaseError("lease_seconds must not be negative")
        if not _is_int(limit) or limit < 0:
            raise LeaseError("limit must be a non-negative integer")
        if fault is not None and fault not in _FAULT_PHASES:
            raise LeaseError(f"unknown fault phase: {fault}")

        plan = self._ensure_active()
        maximums = self._max_attempts(plan)
        out = []
        with self._tx():
            rows = self.db.execute(
                "SELECT * FROM instances WHERE plan_id=?", (plan["plan_id"],)
            ).fetchall()
            expired_failures = set()
            for row in rows:
                if (
                    row["state"] == RUNNING
                    and row["lease_until"] is not None
                    and now_value >= float(row["lease_until"])
                ):
                    if int(row["attempt"]) >= maximums.get(row["task"], 1):
                        expired_failures.add(row["instance"])
                    else:
                        self.db.execute(
                            "UPDATE instances SET state=?, lease_token=NULL, "
                            "lease_worker=NULL, lease_until=NULL "
                            "WHERE plan_id=? AND instance=?",
                            (PENDING, plan["plan_id"], row["instance"]),
                        )
            for instance in expired_failures:
                self.db.execute(
                    "UPDATE instances SET state=?, lease_token=NULL, "
                    "lease_worker=NULL, lease_until=NULL, error=? "
                    "WHERE plan_id=? AND instance=?",
                    (FAILED, "lease_expired", plan["plan_id"], instance),
                )
            if expired_failures:
                self._propagate(plan)

            if limit:
                rows = self.db.execute(
                    "SELECT * FROM instances WHERE plan_id=?", (plan["plan_id"],)
                ).fetchall()
                states = {row["instance"]: row["state"] for row in rows}
                attempts = {row["instance"]: int(row["attempt"]) for row in rows}
                layers = _layers(plan["tasks"])
                partitions = plan["partitions"]
                indexes = {part: index for index, part in enumerate(partitions)}
                tasks = {task["name"]: task for task in plan["tasks"]}

                ready = []
                for row in rows:
                    instance = row["instance"]
                    if states[instance] != PENDING:
                        continue
                    task = tasks.get(row["task"])
                    if task is None:
                        continue
                    partition = row["partition"]
                    index = indexes[partition]
                    ok = True
                    for dependency in task["depends_on"]:
                        if states.get(_instance(dependency, partition)) != SUCCESS:
                            ok = False
                            break
                        if task["fan_in"] == "all_previous":
                            for earlier in partitions[:index]:
                                if states.get(_instance(dependency, earlier)) != SUCCESS:
                                    ok = False
                                    break
                        if not ok:
                            break
                    if ok:
                        ready.append(instance)

                ready.sort(key=lambda inst: (
                    _partition_of(inst, tasks),
                    layers[_task_of(inst, tasks)],
                    _task_of(inst, tasks),
                    inst,
                ))

                granted = 0
                for instance in ready:
                    if granted >= limit:
                        break
                    row = self.db.execute(
                        "SELECT * FROM instances WHERE plan_id=? AND instance=?",
                        (plan["plan_id"], instance),
                    ).fetchone()
                    if row is None or row["state"] != PENDING:
                        continue
                    attempt = int(row["attempt"]) + 1
                    maximum = maximums.get(row["task"], 1)
                    if attempt > maximum:
                        self.db.execute(
                            "UPDATE instances SET state=?, lease_token=NULL, "
                            "lease_worker=NULL, lease_until=NULL, error=? "
                            "WHERE plan_id=? AND instance=?",
                            (FAILED, "attempts_exhausted", plan["plan_id"], instance),
                        )
                        self._propagate(plan)
                        continue
                    token = self._new_token()
                    self.db.execute(
                        "UPDATE instances SET state=?, attempt=?, lease_token=?, "
                        "lease_worker=?, lease_until=? "
                        "WHERE plan_id=? AND instance=?",
                        (
                            RUNNING, attempt, token, worker,
                            now_value + lease_value, plan["plan_id"], instance,
                        ),
                    )
                    out.append({
                        "instance": instance,
                        "task": row["task"],
                        "partition": row["partition"],
                        "attempt": attempt,
                        "lease_token": token,
                    })
                    granted += 1

            _inject_fault(fault, "after_lease")
            _inject_fault(fault, "before_commit")
        return out

    def _new_token(self):
        while True:
            token = secrets.token_hex(32)
            found = self.db.execute(
                "SELECT 1 FROM instances WHERE lease_token=? OR success_token=? LIMIT 1",
                (token, token),
            ).fetchone()
            if found is None:
                return token

    def complete(
        self, instance, lease_token, output_digest, now, fault=None
    ):
        if not isinstance(instance, str) or not instance:
            raise LeaseError("instance must be a non-empty string")
        if not isinstance(lease_token, str) or not lease_token:
            raise LeaseError("lease_token must be a non-empty string")
        if not isinstance(output_digest, str):
            raise LeaseError("output_digest must be a string")
        now_value = _number(now, "now")
        if fault is not None and fault not in _FAULT_PHASES:
            raise LeaseError(f"unknown fault phase: {fault}")

        plan = self._ensure_active()
        with self._tx():
            row = self.db.execute(
                "SELECT * FROM instances WHERE plan_id=? AND instance=?",
                (plan["plan_id"], instance),
            ).fetchone()
            if row is None:
                raise LeaseError(f"unknown instance: {instance}")
            if row["state"] == SUCCESS:
                if (
                    row["success_token"] == lease_token
                    and row["output_digest"] == output_digest
                ):
                    _inject_fault(fault)
                    return {
                        "instance": instance,
                        "status": SUCCESS,
                        "attempt": int(row["attempt"]),
                        "output_digest": row["output_digest"],
                    }
                raise LeaseError(
                    "success output is already bound; stale token or different digest"
                )
            if row["state"] != RUNNING:
                raise LeaseError(f"instance {instance} is not running")
            if row["lease_token"] != lease_token:
                raise LeaseError("lease token is stale")
            if row["lease_until"] is None or now_value >= float(row["lease_until"]):
                raise LeaseError("lease has expired")
            self.db.execute(
                "UPDATE instances SET state=?, output_digest=?, success_token=?, "
                "lease_token=NULL, lease_worker=NULL, lease_until=NULL, error=NULL "
                "WHERE plan_id=? AND instance=?",
                (SUCCESS, output_digest, lease_token, plan["plan_id"], instance),
            )
            _inject_fault(fault)
            attempt = int(row["attempt"])
        return {
            "instance": instance,
            "status": SUCCESS,
            "attempt": attempt,
            "output_digest": output_digest,
        }

    def fail(self, instance, lease_token, error, now, fault=None):
        if not isinstance(instance, str) or not instance:
            raise LeaseError("instance must be a non-empty string")
        if not isinstance(lease_token, str) or not lease_token:
            raise LeaseError("lease_token must be a non-empty string")
        now_value = _number(now, "now")
        if fault is not None and fault not in _FAULT_PHASES:
            raise LeaseError(f"unknown fault phase: {fault}")
        category = _error_text(error)

        plan = self._ensure_active()
        with self._tx():
            row = self.db.execute(
                "SELECT * FROM instances WHERE plan_id=? AND instance=?",
                (plan["plan_id"], instance),
            ).fetchone()
            if row is None:
                raise LeaseError(f"unknown instance: {instance}")
            if row["state"] != RUNNING:
                raise LeaseError(f"instance {instance} is not running")
            if row["lease_token"] != lease_token:
                raise LeaseError("lease token is stale")
            if row["lease_until"] is None or now_value >= float(row["lease_until"]):
                raise LeaseError("lease has expired")
            attempt = int(row["attempt"])
            maximum = self._max_attempts(plan).get(row["task"], 1)
            state = FAILED if attempt >= maximum else PENDING
            self.db.execute(
                "UPDATE instances SET state=?, lease_token=NULL, "
                "lease_worker=NULL, lease_until=NULL, error=? "
                "WHERE plan_id=? AND instance=?",
                (state, category, plan["plan_id"], instance),
            )
            if state == FAILED:
                self._propagate(plan)
            _inject_fault(fault)
        return {
            "instance": instance,
            "status": state,
            "attempt": attempt,
            "error": category,
        }

    def status(self):
        plan = self._ensure_active()
        with self._tx(immediate=False):
            rows = self.db.execute(
                "SELECT instance, task, partition, state, attempt, "
                "output_digest, error FROM instances "
                "WHERE plan_id=? ORDER BY instance COLLATE BINARY",
                (plan["plan_id"],),
            ).fetchall()
            maximums = self._max_attempts(plan)
            counts = {state: 0 for state in STATES}
            details = []
            for row in rows:
                state = row["state"]
                counts[state] = counts.get(state, 0) + 1
                details.append({
                    "instance": row["instance"],
                    "task": row["task"],
                    "partition": row["partition"],
                    "state": state,
                    "status": state,
                    "attempt": int(row["attempt"]),
                    "max_attempts": maximums.get(row["task"]),
                    "output_digest": row["output_digest"],
                    "error": row["error"],
                })
        result = {"counts": counts, "instances": details}
        # Keep direct ``status()['success']`` access convenient as well.
        for state, count in counts.items():
            result[state] = count
        return result


def _task_of(instance: str, tasks: dict) -> str:
    for name in sorted(tasks, key=len, reverse=True):
        if instance.startswith(name + "@"):
            return name
    return instance.split("@", 1)[0]


def _partition_of(instance: str, tasks: dict) -> str:
    task = _task_of(instance, tasks)
    return instance[len(task) + 1:]


def _inject_fault(fault, phase=None):
    if fault is None:
        return
    if fault not in _FAULT_PHASES:
        raise LeaseError(f"unknown fault phase: {fault}")
    if phase is None or fault == phase:
        raise FaultInjected(f"fault injected: {fault}")


__all__ = [
    "Scheduler",
    "SchedulerError",
    "PlanError",
    "LeaseError",
    "FaultInjected",
    "SchedulerFault",
    "SCHEMA_VERSION",
]
