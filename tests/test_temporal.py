import json
import shutil
import subprocess
import sys
import threading
import unittest
import uuid
from pathlib import Path

from timeweave import TemporalStore
from timeweave.store import (
    ConflictError,
    SchemaVersionError,
    TemporalError,
    ValidationError,
)


def dim(entity="e", **overrides):
    base = {
        "entity": entity,
        "valid_from": "2024-01-01T00:00:00Z",
        "valid_to": None,
        "system_from": "2024-01-01T00:00:00Z",
        "system_to": None,
        "revision": 1,
        "attrs": {"tier": "a"},
    }
    base.update(overrides)
    return base


def fact(fid="f1", entity="e", **overrides):
    base = {
        "fact_id": fid,
        "entity": entity,
        "event_time": "2024-02-01T00:00:00Z",
        "observed_at": "2024-02-02T00:00:00Z",
        "metrics": {"v": 1},
    }
    base.update(overrides)
    return base


class StoreCase(unittest.TestCase):
    def setUp(self):
        self.root = Path(__file__).parent / ".work" / uuid.uuid4().hex
        self.root.mkdir(parents=True)
        self.db_path = self.root / "t.sqlite"
        self.store = TemporalStore(self.db_path)

    def tearDown(self):
        self.store.close()
        shutil.rmtree(self.root, ignore_errors=True)


class StrictTimeTests(StoreCase):
    def test_timezone_offset_is_normalized_to_utc(self):
        self.store.apply(
            [dim(system_from="2024-01-01T08:00:00+08:00")], []
        )
        rows = self.store.db.execute("SELECT system_from FROM dimensions").fetchall()
        self.assertEqual(rows[0][0], "2024-01-01T00:00:00Z")

    def test_naive_timestamp_rejected(self):
        with self.assertRaises(ValidationError):
            self.store.apply([dim(valid_from="2024-01-01T00:00:00")], [])

    def test_subsecond_timestamp_rejected(self):
        with self.assertRaises(ValidationError):
            self.store.apply([dim(valid_from="2024-01-01T00:00:00.5Z")], [])

    def test_garbage_timestamp_rejected(self):
        with self.assertRaises(ValidationError):
            self.store.apply([dim(valid_from="not-a-time")], [])

    def test_empty_and_wrong_shape_rejected(self):
        bad = dim()
        del bad["attrs"]
        with self.assertRaises(ValidationError):
            self.store.apply([bad], [])
        with self.assertRaises(ValidationError):
            self.store.apply([dim(revision=0)], [])
        with self.assertRaises(ValidationError):
            self.store.apply([dim(revision=True)], [])


class HalfOpenIntervalTests(StoreCase):
    def test_business_lower_bound_inclusive(self):
        self.store.apply([dim(valid_from="2024-02-01T00:00:00Z")], [fact()])
        row = self.store.join("2024-03-01T00:00:00Z")[0]
        self.assertEqual(row["dimension_revision"], 1)
        self.assertFalse(row["unmatched"])

    def test_business_upper_bound_exclusive(self):
        self.store.apply(
            [dim(valid_from="2024-01-01T00:00:00Z", valid_to="2024-02-01T00:00:00Z")],
            [fact()],
        )
        row = self.store.join("2024-03-01T00:00:00Z")[0]
        self.assertTrue(row["unmatched"])
        self.assertIsNone(row["dimension"])

    def test_system_upper_bound_exclusive(self):
        self.store.apply([dim(system_to="2024-03-01T00:00:00Z")], [fact()])
        self.assertEqual(
            self.store.join("2024-02-28T23:59:59Z")[0]["dimension_revision"], 1
        )
        boundary = self.store.join("2024-03-01T00:00:00Z")[0]
        self.assertTrue(boundary["unmatched"])
        self.assertIsNone(boundary["dimension_revision"])

    def test_empty_intervals_rejected(self):
        with self.assertRaises(ValidationError):
            self.store.apply(
                [dim(valid_from="2024-01-01T00:00:00Z", valid_to="2024-01-01T00:00:00Z")],
                [],
            )
        with self.assertRaises(ValidationError):
            self.store.apply(
                [dim(system_from="2024-01-02T00:00:00Z", system_to="2024-01-01T00:00:00Z")],
                [],
            )

    def test_touching_half_open_business_intervals_are_allowed(self):
        self.store.apply(
            [
                dim(
                    revision=1,
                    valid_from="2024-01-01T00:00:00Z",
                    valid_to="2024-02-01T00:00:00Z",
                ),
                dim(
                    revision=2,
                    valid_from="2024-02-01T00:00:00Z",
                    valid_to=None,
                    system_from="2024-01-02T00:00:00Z",
                ),
            ],
            [],
        )


class CorrectionTests(StoreCase):
    def test_correction_closes_old_system_interval(self):
        self.store.apply(
            [dim(revision=1, attrs={"tier": "old"})],
            [fact()],
        )
        self.store.apply(
            [
                dim(
                    revision=2,
                    attrs={"tier": "new"},
                    system_from="2024-05-01T00:00:00Z",
                )
            ],
            [],
        )
        before = self.store.join("2024-04-01T00:00:00Z")[0]
        after = self.store.join("2024-05-01T00:00:00Z")[0]
        self.assertEqual(before["dimension"], {"tier": "old"})
        self.assertEqual(before["dimension_revision"], 1)
        self.assertEqual(after["dimension"], {"tier": "new"})
        self.assertEqual(after["dimension_revision"], 2)
        closed = self.store.db.execute(
            "SELECT system_to FROM dimensions WHERE revision=1"
        ).fetchone()[0]
        self.assertEqual(closed, "2024-05-01T00:00:00Z")

    def test_history_cannot_be_tampered_with_same_revision(self):
        self.store.apply([dim(revision=1)], [])
        with self.assertRaises(ConflictError):
            self.store.apply([dim(revision=1, attrs={"tier": "z"})], [])

    def test_correction_requires_higher_revision(self):
        self.store.apply([dim(revision=2)], [])
        with self.assertRaises(ConflictError):
            self.store.apply(
                [dim(revision=1, system_from="2024-06-01T00:00:00Z")], []
            )

    def test_system_interval_overlap_rejected(self):
        self.store.apply(
            [dim(revision=1, system_to="2024-05-01T00:00:00Z")], []
        )
        with self.assertRaises(ConflictError):
            self.store.apply(
                [
                    dim(
                        revision=2,
                        system_from="2024-04-01T00:00:00Z",
                        system_to="2024-06-01T00:00:00Z",
                    )
                ],
                [],
            )

    def test_business_interval_overlap_rejected(self):
        self.store.apply(
            [
                dim(
                    revision=1,
                    valid_from="2024-01-01T00:00:00Z",
                    valid_to="2024-03-01T00:00:00Z",
                    system_from="2024-01-01T00:00:00Z",
                    system_to="2024-06-01T00:00:00Z",
                )
            ],
            [],
        )
        with self.assertRaises(ConflictError):
            self.store.apply(
                [
                    dim(
                        revision=2,
                        valid_from="2024-02-01T00:00:00Z",
                        valid_to="2024-04-01T00:00:00Z",
                        system_from="2024-02-01T00:00:00Z",
                        system_to="2024-05-01T00:00:00Z",
                    )
                ],
                [],
            )

    def test_overlapping_business_but_disjoint_system_allowed(self):
        self.store.apply(
            [
                dim(
                    revision=1,
                    valid_from="2024-01-01T00:00:00Z",
                    valid_to="2024-03-01T00:00:00Z",
                    system_from="2024-01-01T00:00:00Z",
                    system_to="2024-02-01T00:00:00Z",
                )
            ],
            [],
        )
        self.store.apply(
            [
                dim(
                    revision=2,
                    valid_from="2024-02-01T00:00:00Z",
                    valid_to="2024-04-01T00:00:00Z",
                    system_from="2024-02-01T00:00:00Z",
                    system_to="2024-05-01T00:00:00Z",
                )
            ],
            [],
        )
        self.assertEqual(
            self.store.db.execute("SELECT COUNT(*) FROM dimensions").fetchone()[0], 2
        )

    def test_retroactive_correction_with_disjoint_system_interval_allowed(self):
        self.store.apply(
            [
                dim(
                    revision=1,
                    valid_from="2024-01-01T00:00:00Z",
                    valid_to="2024-03-01T00:00:00Z",
                    system_from="2024-01-01T00:00:00Z",
                    system_to="2024-02-01T00:00:00Z",
                )
            ],
            [],
        )
        self.store.apply(
            [
                dim(
                    revision=2,
                    valid_from="2024-02-01T00:00:00Z",
                    valid_to="2024-04-01T00:00:00Z",
                    system_from="2024-02-01T00:00:00Z",
                    system_to=None,
                )
            ],
            [],
        )
        self.assertEqual(
            self.store.db.execute("SELECT COUNT(*) FROM dimensions").fetchone()[0], 2
        )


class FactTests(StoreCase):
    def test_fact_invisible_before_observed_at(self):
        self.store.apply([dim()], [fact()])
        self.assertEqual(self.store.join("2024-02-01T23:59:59Z"), [])
        visible = self.store.join("2024-02-02T00:00:00Z")
        self.assertEqual(len(visible), 1)

    def test_unmatched_fact_marked(self):
        self.store.apply([dim(entity="other")], [fact(entity="e")])
        row = self.store.join("2024-03-01T00:00:00Z")[0]
        self.assertTrue(row["unmatched"])
        self.assertIsNone(row["dimension"])
        self.assertIsNone(row["dimension_revision"])

    def test_results_sorted_by_fact_id(self):
        self.store.apply(
            [dim()],
            [fact(fid="z"), fact(fid="a"), fact(fid="m")],
        )
        rows = self.store.join("2024-03-01T00:00:00Z")
        self.assertEqual([r["fact_id"] for r in rows], ["a", "m", "z"])

    def test_identical_redelivery_is_idempotent(self):
        payload = fact()
        first = self.store.apply([dim()], [payload])
        second = self.store.apply([], [fact()])
        self.assertEqual((first["facts"], second["facts"]), (1, 1))
        count = self.store.db.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
        self.assertEqual(count, 1)

    def test_same_id_different_content_conflicts(self):
        self.store.apply([dim()], [fact(metrics={"v": 1})])
        with self.assertRaises(ConflictError):
            self.store.apply([], [fact(metrics={"v": 2})])

    def test_duplicate_fact_id_in_one_batch_rejected(self):
        with self.assertRaises(ConflictError):
            self.store.apply([], [fact(fid="x"), fact(fid="x", metrics={"v": 9})])

    def test_output_contains_original_fact_fields(self):
        self.store.apply([dim(attrs={"tier": "a"})], [fact()])
        row = self.store.join("2024-03-01T00:00:00Z")[0]
        self.assertEqual(
            set(row),
            {
                "fact_id",
                "entity",
                "event_time",
                "observed_at",
                "metrics",
                "dimension",
                "dimension_revision",
                "unmatched",
            },
        )
        self.assertEqual(row["metrics"], {"v": 1})


class AtomicityTests(StoreCase):
    def test_each_fault_stage_rolls_back(self):
        self.store.apply([dim(revision=1)], [fact(fid="seed")])
        for stage in ("after_dimensions", "after_facts", "before_commit"):
            with self.assertRaises(RuntimeError):
                self.store.apply(
                    [dim(revision=9, system_from="2024-07-01T00:00:00Z")],
                    [fact(fid="rolled-%s" % stage)],
                    fault=stage,
                )
            self.assertEqual(
                self.store.db.execute("SELECT COUNT(*) FROM dimensions").fetchone()[0],
                1,
            )
            self.assertEqual(
                self.store.db.execute("SELECT COUNT(*) FROM facts").fetchone()[0],
                1,
            )

    def test_conflict_rolls_back_entire_batch(self):
        self.store.apply([dim(revision=1, system_to="2024-02-01T00:00:00Z")], [])
        with self.assertRaises(ConflictError):
            self.store.apply(
                [
                    dim(revision=2, system_from="2024-03-01T00:00:00Z"),
                    dim(revision=2, system_from="2024-04-01T00:00:00Z"),
                ],
                [fact(fid="should-not-exist")],
            )
        self.assertEqual(
            self.store.db.execute("SELECT COUNT(*) FROM dimensions").fetchone()[0], 1
        )
        self.assertEqual(
            self.store.db.execute("SELECT COUNT(*) FROM facts").fetchone()[0], 0
        )


class ConcurrencyTests(StoreCase):
    def test_only_one_concurrent_revision_wins(self):
        barrier = threading.Barrier(2)
        outcomes = []
        lock = threading.Lock()

        def worker(rev):
            local = TemporalStore(self.db_path)
            try:
                barrier.wait()
                with lock:
                    local.apply(
                        [dim(revision=2, system_from="2024-08-0%dT00:00:00Z" % rev)],
                        [],
                    )
                outcomes.append("ok")
            except TemporalError:
                outcomes.append("conflict")
            finally:
                local.close()

        t1 = threading.Thread(target=worker, args=(2,))
        t2 = threading.Thread(target=worker, args=(3,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        self.assertEqual(sorted(outcomes), ["conflict", "ok"])
        self.assertEqual(
            self.store.db.execute("SELECT COUNT(*) FROM dimensions").fetchone()[0], 1
        )


class SchemaVersionTests(StoreCase):
    def test_future_version_rejected(self):
        self.store.db.execute("UPDATE meta SET value='3' WHERE key='schema_version'")
        self.store.db.commit()
        self.store.close()
        with self.assertRaises(SchemaVersionError):
            rejected = TemporalStore(self.db_path)
            rejected.close()

    def test_corrupted_structure_rejected(self):
        self.store.db.execute("DROP TABLE dimensions")
        self.store.db.commit()
        self.store.close()
        with self.assertRaises(SchemaVersionError):
            rejected = TemporalStore(self.db_path)
            rejected.close()

    def test_corrupted_column_rejected(self):
        self.store.db.execute("ALTER TABLE facts RENAME TO facts_old")
        self.store.db.execute(
            "CREATE TABLE facts(fact_id TEXT PRIMARY KEY, wrong TEXT)"
        )
        self.store.db.commit()
        self.store.close()
        with self.assertRaises(SchemaVersionError):
            rejected = TemporalStore(self.db_path)
            rejected.close()

    def test_foreign_schema_without_metadata_rejected(self):
        self.store.close()
        import sqlite3 as _sql
        other = self.root / "foreign.sqlite"
        conn = _sql.connect(other)
        conn.execute("CREATE TABLE dimensions(x)")
        conn.commit()
        conn.close()
        with self.assertRaises(SchemaVersionError):
            rejected = TemporalStore(other)
            rejected.close()

    def test_reopen_preserves_data(self):
        self.store.apply([dim()], [fact()])
        self.store.close()
        reopened = TemporalStore(self.db_path)
        try:
            self.assertEqual(
                reopened.join("2024-03-01T00:00:00Z")[0]["dimension_revision"], 1
            )
        finally:
            reopened.close()


class CliTests(StoreCase):
    def _run(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "timeweave", *args],
            cwd=Path(__file__).resolve().parents[1],
            capture_output=True,
            text=True,
        )

    def test_apply_and_join_round_trip(self):
        payload = self.root / "batch.json"
        payload.write_text(
            json.dumps({"dimensions": [dim()], "facts": [fact()]}), encoding="utf-8"
        )
        applied = self._run("apply", str(self.db_path), "--input", str(payload))
        self.assertEqual(applied.returncode, 0, applied.stderr)
        joined = self._run("join", str(self.db_path), "--as-of", "2024-03-01T00:00:00Z")
        self.assertEqual(joined.returncode, 0, joined.stderr)
        rows = json.loads(joined.stdout)
        self.assertEqual(rows[0]["dimension"], {"tier": "a"})

    def test_business_error_exits_two_without_traceback(self):
        payload = self.root / "bad.json"
        bad = dim(revision=0)
        payload.write_text(json.dumps({"dimensions": [bad], "facts": []}), encoding="utf-8")
        proc = self._run("apply", str(self.db_path), "--input", str(payload))
        self.assertEqual(proc.returncode, 2)
        self.assertNotIn("Traceback", proc.stderr)
        self.assertIn("error:", proc.stderr)

    def test_fault_cli_rolls_back_and_exits_nonzero(self):
        good = self.root / "good.json"
        good.write_text(
            json.dumps({"dimensions": [dim(revision=1)], "facts": [fact(fid="s")]}),
            encoding="utf-8",
        )
        self.assertEqual(
            self._run("apply", str(self.db_path), "--input", str(good)).returncode, 0
        )
        bad = self.root / "fault.json"
        bad.write_text(
            json.dumps(
                {
                    "dimensions": [
                        dim(revision=2, system_from="2024-09-01T00:00:00Z")
                    ],
                    "facts": [],
                }
            ),
            encoding="utf-8",
        )
        proc = self._run(
            "apply", str(self.db_path), "--input", str(bad), "--fault", "before_commit"
        )
        self.assertNotEqual(proc.returncode, 0)
        self.store.close()
        check = TemporalStore(self.db_path)
        try:
            self.assertEqual(
                check.db.execute("SELECT COUNT(*) FROM dimensions").fetchone()[0], 1
            )
        finally:
            check.close()


if __name__ == "__main__":
    unittest.main()
