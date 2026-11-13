import json
import shutil
import sqlite3
import subprocess
import sys
import threading
import unittest
import uuid
from pathlib import Path

from sessionmill import (
    ConflictError,
    InjectedFault,
    SchemaVersionError,
    SessionEngine,
    ValidationError,
    WatermarkError,
)


SESSION_KEYS = {
    "session_id",
    "tenant",
    "user",
    "start",
    "end",
    "event_count",
    "closed",
    "revision",
}


def ev(event_id, when, seq, kind="open", tenant="t", user="u"):
    return {
        "tenant": tenant,
        "user": user,
        "event_id": event_id,
        "event_time": when,
        "arrival_seq": seq,
        "kind": kind,
    }


class SessionMillTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(__file__).parent / ".work" / uuid.uuid4().hex
        self.root.mkdir(parents=True)
        self.db_path = self.root / "sessionmill.sqlite"
        self.engine = SessionEngine(self.db_path)

    def tearDown(self):
        self.engine.close()
        shutil.rmtree(self.root, ignore_errors=True)

    def raw_counts(self):
        with sqlite3.connect(self.db_path) as conn:
            return {
                table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in ("events", "sessions", "watermarks", "changelog")
            }

    def test_out_of_order_events_use_time_then_arrival_order(self):
        result = self.engine.ingest(
            [ev("z", 20, 2, "ping"), ev("a", 10, 1)], {"t": 20}
        )
        self.assertEqual(result, {"accepted": 2, "sessions": 1})
        sessions = self.engine.sessions()
        self.assertEqual(len(sessions), 1)
        self.assertEqual(sessions[0]["session_id"], "a")
        self.assertEqual(sessions[0]["start"], 10)
        self.assertEqual(sessions[0]["end"], 20)
        self.assertEqual(sessions[0]["event_count"], 2)
        self.assertFalse(sessions[0]["closed"])
        self.assertEqual(sessions[0]["revision"], 1)
        self.assertEqual(set(sessions[0]), SESSION_KEYS)
        self.assertEqual(
            [change["action"] for change in self.engine.changes(0)], ["upsert"]
        )

    def test_gap_boundary_and_close_starts_new_session(self):
        self.engine.ingest(
            [
                ev("a", 0, 1),
                ev("b", 30, 2, "ping"),
                ev("c", 60, 3, "close"),
            ],
            {"t": 60},
        )
        self.engine.ingest([ev("d", 61, 4)], {"t": 61})
        sessions = self.engine.sessions()
        self.assertEqual(len(sessions), 2)
        self.assertEqual(
            {key: sessions[0][key] for key in ("session_id", "start", "end", "event_count", "closed")},
            {"session_id": "a", "start": 0, "end": 60, "event_count": 3, "closed": True},
        )
        self.assertEqual(
            {key: sessions[1][key] for key in ("session_id", "start", "end", "event_count", "closed")},
            {"session_id": "d", "start": 61, "end": 61, "event_count": 1, "closed": False},
        )
        self.assertEqual(
            [change["session_id"] for change in self.engine.changes(0)], ["a", "d"]
        )

    def test_late_bridge_merges_two_sessions_and_retracts_old_states(self):
        first = self.engine.ingest(
            [ev("a", 0, 1), ev("c", 40, 2, "ping")], {"t": 40}
        )
        self.assertEqual(first["sessions"], 2)
        self.assertEqual(
            [change["session_id"] for change in self.engine.changes(0)], ["a", "c"]
        )

        second = self.engine.ingest([ev("b", 20, 3, "ping")], {"t": 40})
        self.assertEqual(second, {"accepted": 1, "sessions": 1})
        sessions = self.engine.sessions()
        self.assertEqual(sessions[0]["session_id"], "a")
        self.assertEqual(sessions[0]["event_count"], 3)
        self.assertEqual((sessions[0]["start"], sessions[0]["end"]), (0, 40))
        self.assertEqual(sessions[0]["revision"], 2)

        delta = self.engine.changes(2)
        self.assertEqual(
            [(item["action"], item["session_id"], item["revision"]) for item in delta],
            [
                ("retract", "a", 1),
                ("retract", "c", 1),
                ("upsert", "a", 2),
            ],
        )
        self.assertEqual([item["offset"] for item in delta], [3, 4, 5])

    def test_late_close_can_split_an_existing_session(self):
        self.engine.ingest(
            [ev("a", 0, 1), ev("b", 10, 2, "ping"), ev("c", 20, 3, "ping"), ev("d", 30, 4, "ping")],
            {"t": 30},
        )
        self.assertEqual(len(self.engine.sessions()), 1)

        self.engine.ingest([ev("e", 15, 5, "close")], {"t": 30})
        sessions = self.engine.sessions()
        self.assertEqual(len(sessions), 2)
        self.assertEqual(sessions[0]["session_id"], "a")
        self.assertEqual(sessions[0]["end"], 15)
        self.assertEqual(sessions[0]["event_count"], 3)
        self.assertTrue(sessions[0]["closed"])
        self.assertEqual(sessions[0]["revision"], 2)
        self.assertEqual(sessions[1]["session_id"], "c")
        self.assertEqual(sessions[1]["start"], 20)
        self.assertEqual(sessions[1]["event_count"], 2)
        self.assertEqual(sessions[1]["revision"], 1)

        delta = self.engine.changes(1)
        self.assertEqual(
            [(item["action"], item["session_id"], item["revision"]) for item in delta],
            [("retract", "a", 1), ("upsert", "a", 2), ("upsert", "c", 1)],
        )

    def test_revision_history_survives_retract_and_reappearance(self):
        self.engine.ingest([ev("a", 0, 1), ev("c", 40, 2, "ping")], {"t": 40})
        self.engine.ingest([ev("z", 20, 3, "ping")], {"t": 40})
        merged_changes = self.engine.changes(2)
        self.assertIn(
            ("retract", "c", 1),
            [
                (item["action"], item["session_id"], item["revision"])
                for item in merged_changes
            ],
        )
        self.assertEqual([s["session_id"] for s in self.engine.sessions()], ["a"])

        self.engine.ingest([ev("x", 10, 4, "close")], {"t": 40})
        sessions = {s["session_id"]: s for s in self.engine.sessions()}
        self.assertEqual(sessions["c"]["start"], 20)
        self.assertEqual(sessions["c"]["end"], 40)
        self.assertEqual(sessions["c"]["revision"], 2)
        c_upserts = [
            item
            for item in self.engine.changes(0)
            if item["session_id"] == "c" and item["action"] == "upsert"
        ]
        self.assertEqual([item["revision"] for item in c_upserts], [1, 2])

    def test_exact_duplicates_are_permanently_idempotent(self):
        batch = [ev("a", 0, 1), ev("b", 10, 2, "ping")]
        first = self.engine.ingest(batch, {"t": 10})
        self.assertEqual(first, {"accepted": 2, "sessions": 1})
        sessions_before = self.engine.sessions()
        changes_before = self.engine.changes(0)

        duplicate_batch = list(batch) + list(batch)
        second = self.engine.ingest(duplicate_batch, {"t": 10})
        self.assertEqual(second, {"accepted": 0, "sessions": 1})
        self.assertEqual(self.engine.sessions(), sessions_before)
        self.assertEqual(self.engine.changes(0), changes_before)

        # The original content remains idempotent even when the tenant
        # watermark has advanced beyond the lateness horizon.
        third = self.engine.ingest(batch, {"t": 120})
        self.assertEqual(third, {"accepted": 0, "sessions": 1})
        self.assertEqual(self.engine.changes(0), changes_before)

    def test_conflicting_identities_reject_the_entire_batch(self):
        self.engine.ingest([ev("a", 0, 1)], {"t": 0})
        with self.assertRaises(ConflictError):
            self.engine.ingest(
                [ev("b", 5, 2), ev("a", 6, 3, "ping")], {"t": 6}
            )
        self.assertEqual(len(self.engine.sessions()), 1)
        self.assertEqual(self.engine.sessions()[0]["event_count"], 1)
        self.assertEqual(self.raw_counts()["events"], 1)
        self.assertEqual(self.raw_counts()["changelog"], 1)

        with self.assertRaises(ConflictError):
            self.engine.ingest([ev("c", 7, 4), ev("c", 8, 5)], {"t": 8})

        with self.assertRaises(ConflictError):
            self.engine.ingest([ev("d", 9, 1)], {"t": 9})

    def test_watermark_window_regression_and_missing_mapping(self):
        self.engine.ingest([ev("a", 40, 1)], {"t": 100})
        self.assertEqual(len(self.engine.sessions()), 1)

        with self.assertRaises(WatermarkError):
            self.engine.ingest([ev("b", 39, 2)], {"t": 100})
        with self.assertRaises(WatermarkError):
            self.engine.ingest([ev("b", 101, 2)], {"t": 100})
        with self.assertRaises(WatermarkError):
            self.engine.ingest([], {"t": 90})
        with self.assertRaises(WatermarkError):
            self.engine.ingest([ev("x", 0, 3, "open", "other", "u")], {"t": 0})
        self.assertEqual(self.raw_counts()["events"], 1)
        self.assertEqual(len(self.engine.changes(0)), 1)

        self.engine.ingest([ev("b", 40, 2)], {"t": 100})
        self.assertEqual(self.engine.sessions()[0]["event_count"], 2)

    def test_watermark_only_advance_and_new_lateness_horizon(self):
        self.assertEqual(
            self.engine.ingest([], {"t": 100}),
            {"accepted": 0, "sessions": 0},
        )
        self.assertEqual(self.engine.changes(0), [])
        self.engine.ingest([ev("a", 40, 1)], {"t": 100})
        self.engine.ingest([], {"t": 120})

        # Duplicate replay still works, but genuinely new late content does not.
        self.assertEqual(self.engine.ingest([ev("a", 40, 1)], {"t": 120})["accepted"], 0)
        with self.assertRaises(WatermarkError):
            self.engine.ingest([ev("b", 59, 2)], {"t": 120})

    def test_fault_rollback_at_all_documented_stages(self):
        events = [ev("a", 0, 1)]
        for stage in ("after_events", "after_sessions", "before_commit"):
            with self.subTest(stage=stage):
                with self.assertRaises(InjectedFault):
                    self.engine.ingest(events, {"t": 0}, fault=stage)
                self.assertEqual(self.engine.sessions(), [])
                self.assertEqual(self.engine.changes(0), [])
                self.assertEqual(
                    self.raw_counts(),
                    {"events": 0, "sessions": 0, "watermarks": 0, "changelog": 0},
                )

        self.assertEqual(
            self.engine.ingest(events, {"t": 0}),
            {"accepted": 1, "sessions": 1},
        )
        self.assertEqual([item["offset"] for item in self.engine.changes(0)], [1])

    def test_schema_version_is_two_and_future_versions_are_rejected(self):
        with sqlite3.connect(self.db_path) as conn:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 2)

        future_path = self.root / "future.sqlite"
        with sqlite3.connect(future_path) as conn:
            conn.execute("PRAGMA user_version = 3")
        with self.assertRaises(SchemaVersionError):
            SessionEngine(future_path)

    def test_version_zero_database_is_upgraded_without_losing_events(self):
        legacy_path = self.root / "legacy.sqlite"
        with sqlite3.connect(legacy_path) as conn:
            conn.execute(
                "CREATE TABLE events(tenant TEXT,user TEXT,event_id TEXT PRIMARY KEY,"
                "event_time INTEGER,arrival_seq INTEGER,kind TEXT)"
            )
            conn.execute(
                "INSERT INTO events VALUES('t','u','a',0,1,'open')"
            )
        legacy = SessionEngine(legacy_path)
        try:
            with sqlite3.connect(legacy_path) as conn:
                self.assertEqual(
                    conn.execute("PRAGMA user_version").fetchone()[0], 2
                )
            self.assertEqual(len(legacy.sessions()), 1)
            self.assertEqual(legacy.sessions()[0]["session_id"], "a")
        finally:
            legacy.close()

    def test_validation_contract(self):
        bad_events = [
            {"tenant": "t", "user": "u", "event_id": "a", "event_time": 0, "arrival_seq": 1},
            dict(ev("a", 0, 1), extra="x"),
            dict(ev("a", 0, 1), kind="boom"),
            dict(ev("a", 0, 1), tenant=""),
            dict(ev("a", 0, 1), tenant="e\u0301"),
            dict(ev("a", 0, 1), tenant="bad\nname"),
            dict(ev("a", 0, 1), event_time=True),
            dict(ev("a", 0, 1), arrival_seq=1.5),
            "not-an-object",
        ]
        for index, item in enumerate(bad_events):
            with self.subTest(index=index):
                with self.assertRaises(ValidationError):
                    self.engine.ingest([item], {"t": 0})

        with self.assertRaises(ValidationError):
            self.engine.ingest([], [("t", 0)])
        with self.assertRaises(ValidationError):
            self.engine.ingest([], {"t": True})
        with self.assertRaises(ValidationError):
            self.engine.ingest([], {"": 0})
        with self.assertRaises(ValidationError):
            self.engine.changes("0")
        with self.assertRaises(ValidationError):
            self.engine.changes(True)

    def test_changes_offsets_since_and_unchanged_other_pairs(self):
        self.engine.ingest(
            [
                ev("a", 40, 1, "open", "t", "u1"),
                ev("b", 40, 2, "open", "t", "u2"),
                ev("c", 100, 3, "open", "t", "u1"),
            ],
            {"t": 100},
        )
        changes = self.engine.changes(0)
        self.assertEqual([item["offset"] for item in changes], [1, 2, 3])
        self.assertEqual([item["session_id"] for item in changes], ["a", "b", "c"])
        self.assertEqual(len(self.engine.changes(1)), 2)
        self.assertEqual(self.engine.changes(3), [])

        before_u2 = [s for s in self.engine.sessions() if s["user"] == "u2"]
        self.engine.ingest([ev("d", 50, 4, "ping", "t", "u1")], {"t": 100})
        after_u2 = [s for s in self.engine.sessions() if s["user"] == "u2"]
        self.assertEqual(before_u2, after_u2)
        self.assertEqual(
            [item["session_id"] for item in self.engine.changes(3)], ["a", "a"]
        )

    def test_sessions_are_sorted_by_tenant_user_start(self):
        self.engine.ingest(
            [
                ev("b1", 10, 1, "open", "b", "u"),
                ev("a1", 20, 2, "open", "a", "z"),
                ev("a2", 0, 3, "open", "a", "a"),
                ev("c1", 5, 4, "open", "b", "a"),
            ],
            {"a": 20, "b": 10},
        )
        sessions = self.engine.sessions()
        keys = [(item["tenant"], item["user"], item["start"]) for item in sessions]
        self.assertEqual(keys, sorted(keys))
        for session in sessions:
            self.assertEqual(set(session), SESSION_KEYS)
            self.assertIs(type(session["closed"]), bool)

    def test_persistence_survives_reopen(self):
        self.engine.ingest([ev("a", 0, 1), ev("b", 10, 2, "ping")], {"t": 10})
        expected_sessions = self.engine.sessions()
        expected_changes = self.engine.changes(0)
        self.engine.close()

        self.engine = SessionEngine(self.db_path)
        self.assertEqual(self.engine.sessions(), expected_sessions)
        self.assertEqual(self.engine.changes(0), expected_changes)

    def test_concurrent_redelivery_has_one_effective_writer(self):
        events = [
            ev("a", 40, 1),
            ev("b", 50, 2, "ping"),
            ev("c", 60, 3, "ping"),
            ev("d", 100, 4, "ping"),
        ]
        barrier = threading.Barrier(2)
        results = []
        errors = []

        def worker():
            engine = SessionEngine(self.db_path)
            try:
                barrier.wait(timeout=10)
                results.append(engine.ingest(events, {"t": 100}))
            except Exception as exc:  # pragma: no cover - surfaced by assertion
                errors.append(exc)
            finally:
                engine.close()

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        self.assertEqual(sum(result["accepted"] for result in results), 4)
        sessions = self.engine.sessions()
        self.assertEqual(len(sessions), 2)
        self.assertEqual(len(self.engine.changes(0)), 2)

    def test_cli_success_and_business_error_behavior(self):
        root = Path(__file__).resolve().parents[1]
        cli_db = self.root / "cli.sqlite"
        events_path = self.root / "events.json"
        watermarks_path = self.root / "watermarks.json"
        events_path.write_text(
            json.dumps([ev("a", 0, 1), ev("b", 10, 2, "ping")]),
            encoding="utf-8",
        )
        watermarks_path.write_text(json.dumps({"t": 10}), encoding="utf-8")

        base = [sys.executable, "-m", "sessionmill", "--db", str(cli_db)]
        ingest = subprocess.run(
            base + ["ingest", str(events_path), str(watermarks_path)],
            cwd=root,
            capture_output=True,
            text=True,
        )
        self.assertEqual(ingest.returncode, 0, ingest.stderr)
        self.assertEqual(json.loads(ingest.stdout), {"accepted": 2, "sessions": 1})
        self.assertEqual(len(ingest.stdout.strip().splitlines()), 1)

        listing = subprocess.run(
            base + ["sessions"], cwd=root, capture_output=True, text=True
        )
        self.assertEqual(listing.returncode, 0, listing.stderr)
        parsed = json.loads(listing.stdout)
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["session_id"], "a")

        changes = subprocess.run(
            base + ["changes", "0"], cwd=root, capture_output=True, text=True
        )
        self.assertEqual(changes.returncode, 0, changes.stderr)
        self.assertEqual(json.loads(changes.stdout)[0]["action"], "upsert")

        missing = subprocess.run(
            base + ["ingest", str(events_path), str(self.root / "missing.json")],
            cwd=root,
            capture_output=True,
            text=True,
        )
        self.assertEqual(missing.returncode, 2)
        self.assertNotIn("Traceback", missing.stderr)
        self.assertEqual(len(missing.stderr.strip().splitlines()), 1)


if __name__ == "__main__":
    unittest.main()
