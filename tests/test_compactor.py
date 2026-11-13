import hashlib
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import unittest
import uuid
from pathlib import Path

from icecrate import (
    CatalogVersionError,
    Compactor,
    ConflictError,
    CorruptionError,
    ParentCycleError,
)


class CompactorTests(unittest.TestCase):
    def setUp(self):
        self.root = (
            Path(__file__).resolve().parent
            / ".work"
            / f"compactor-{uuid.uuid4().hex}"
        )
        self.root.mkdir(parents=True)
        self.compactor = Compactor(self.root / "warehouse")

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def row(self, pk, partition, version, op="upsert", data=None):
        return {
            "pk": pk,
            "partition": partition,
            "version": version,
            "op": op,
            "data": {"v": version} if data is None else data,
        }

    def segment(self, name, rows):
        path = self.root / name
        path.write_text(
            "".join(
                json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
                for row in rows
            ),
            encoding="utf-8",
        )
        return path

    def manifest(self, snapshot_id):
        return json.loads(
            (
                self.compactor.catalog_dir
                / "snapshots"
                / f"{snapshot_id}.json"
            ).read_text(encoding="utf-8")
        )

    def test_versions_tombstones_and_parent_inheritance(self):
        first = self.segment(
            "first.jsonl",
            [
                self.row("1", "d=1", 1),
                self.row("2", "d=1", 2),
                self.row("3", "d=2", 1),
            ],
        )
        second = self.segment(
            "second.jsonl",
            [
                self.row("2", "d=1", 3, data={"v": 33}),
                self.row("1", "d=1", 2),
                self.row("9", "d=2", 1, op="delete", data=None),
            ],
        )
        result = self.compactor.commit("s1", None, [first, second])
        self.assertTrue(result["applied"])
        rows = self.compactor.read()
        self.assertEqual([row["pk"] for row in rows], ["1", "2", "3"])
        self.assertEqual((rows[0]["version"], rows[1]["version"]), (2, 3))

        later = self.segment("later.jsonl", [self.row("10", "d=1", 1)])
        self.compactor.commit("s2", "s1", [later])
        self.assertEqual(
            [row["pk"] for row in self.compactor.read()],
            ["1", "10", "2", "3"],
        )
        self.assertEqual(
            [row["pk"] for row in self.compactor.read("s1")], ["1", "2", "3"]
        )

        tombstone = self.segment(
            "tombstone.jsonl",
            [self.row("10", "d=1", 2, op="delete", data=None)],
        )
        self.compactor.commit("s3", "s2", [tombstone])
        lower = self.segment("lower.jsonl", [self.row("10", "d=1", 1)])
        self.compactor.commit("s4", "s3", [lower])
        self.assertNotIn("10", [row["pk"] for row in self.compactor.read()])

    def test_canonical_chunks_and_manifest(self):
        rows = [self.row(str(index), "p", 1) for index in range(7)]
        self.compactor.commit("s1", None, [self.segment("seven.jsonl", rows)])
        manifest_path = (
            self.compactor.catalog_dir / "snapshots" / "s1.json"
        )
        manifest_text = manifest_path.read_text(encoding="utf-8")
        manifest = json.loads(manifest_text)
        self.assertEqual(
            manifest_text,
            json.dumps(
                manifest,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ),
        )
        self.assertEqual(manifest["rows"], 7)
        self.assertEqual(manifest["generation"], 1)
        self.assertEqual(
            [entry["path"] for entry in manifest["objects"]],
            sorted(entry["path"] for entry in manifest["objects"]),
        )
        total_lines = 0
        for entry in manifest["objects"]:
            path = self.root / "warehouse" / entry["path"]
            data = path.read_bytes()
            self.assertEqual(
                hashlib.sha256(data).hexdigest(), entry["sha256"]
            )
            line_count = len(data.decode("utf-8").splitlines())
            self.assertLessEqual(line_count, 3)
            total_lines += line_count
        self.assertEqual(total_lines, 7)

    def test_idempotence_conflicts_and_cas(self):
        path = self.segment("a.jsonl", [self.row("1", "p", 1)])
        result = self.compactor.commit("s1", None, [path])
        self.assertTrue(result["applied"])
        replay = self.compactor.commit("s1", None, [path])
        self.assertFalse(replay["applied"])
        self.assertEqual(replay["generation"], 1)
        with self.assertRaises(ConflictError):
            self.compactor.commit("s1", None, [self.segment("b.jsonl", [self.row("2", "p", 1)])])
        with self.assertRaises(ConflictError):
            self.compactor.commit("other", None, [path])

    def test_historical_read_and_compact_are_stable(self):
        first = self.segment("a.jsonl", [self.row("1", "p", 1)])
        self.compactor.commit("s1", None, [first])
        second = self.segment("b.jsonl", [self.row("2", "p", 1)])
        self.compactor.commit("s2", "s1", [second])
        before = (
            self.compactor.catalog_dir / "snapshots" / "s2.json"
        ).read_bytes()
        result = self.compactor.compact("s2")
        self.assertEqual(result["generation"], 2)
        self.assertEqual(len(self.compactor.read("s2")), 2)
        self.assertEqual(len(self.compactor.read("s1")), 1)
        after = (
            self.compactor.catalog_dir / "snapshots" / "s2.json"
        ).read_bytes()
        self.assertEqual(before, after)

    def test_faults_are_atomic(self):
        for stage in ("after_objects", "before_manifest", "before_head"):
            warehouse = self.root / f"fault-{stage}"
            compactor = Compactor(warehouse)
            first = self.segment(
                f"{stage}-base.jsonl", [self.row(stage, "p", 1)]
            )
            compactor.commit("s1", None, [first])
            objects_before = sorted(
                path.name for path in (warehouse / "objects").glob("*")
            )
            path = self.segment(
                f"{stage}-change.jsonl", [self.row(f"new-{stage}", "p", 1)]
            )
            with self.assertRaises(RuntimeError):
                compactor.commit(f"fault-{stage}", "s1", [path], fault=stage)
            self.assertEqual(compactor.head, "s1")
            self.assertFalse(
                (
                    compactor.catalog_dir
                    / "snapshots"
                    / f"fault-{stage}.json"
                ).exists()
            )
            self.assertEqual(
                sorted(
                    item.name
                    for item in (warehouse / "objects").glob("*")
                ),
                objects_before,
            )
            self.assertTrue(
                compactor.commit(f"retry-{stage}", "s1", [path])["applied"]
            )
            self.assertEqual(compactor.head, f"retry-{stage}")

    def test_corruption_is_rejected(self):
        path = self.segment("a.jsonl", [self.row("1", "p", 1)])
        self.compactor.commit("s1", None, [path])
        manifest = self.manifest("s1")
        manifest_path = (
            self.compactor.catalog_dir / "snapshots" / "s1.json"
        )

        bad_version = dict(manifest)
        bad_version["catalog_version"] = 99
        manifest_path.write_text(json.dumps(bad_version), encoding="utf-8")
        with self.assertRaises(CatalogVersionError):
            self.compactor.read("s1")

        cycle = dict(manifest)
        cycle["parent"] = "s1"
        manifest_path.write_text(
            json.dumps(cycle, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        with self.assertRaises(ParentCycleError):
            self.compactor.read("s1")

        manifest_path.write_text(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        object_path = self.root / "warehouse" / manifest["objects"][0]["path"]
        object_path.write_bytes(object_path.read_bytes() + b"\n")
        with self.assertRaises(CorruptionError):
            self.compactor.read("s1")

    def test_vacuum_retention_grace_and_shared_objects(self):
        def commit(snapshot_id, parent, name, row):
            return self.compactor.commit(
                snapshot_id, parent, [self.segment(name, [row])]
            )

        commit("s1", None, "a.jsonl", self.row("1", "p", 1, data={"x": 1}))
        commit("s2", "s1", "b.jsonl", self.row("2", "p", 1, data={"x": 2}))
        commit("s3", "s2", "c.jsonl", self.row("3", "p", 1, data={"x": 3}))
        old = time.time() - 3600
        manifests = [self.manifest(name) for name in ("s1", "s2", "s3")]
        for manifest in manifests[:2]:
            for entry in manifest["objects"]:
                os.utime(self.root / "warehouse" / entry["path"], (old, old))
        cutoff = time.time() - 60
        deleted = self.compactor.vacuum(1, cutoff)
        self.assertEqual(
            deleted, sorted(entry["path"] for entry in manifests[0]["objects"])
        )
        self.assertTrue(
            all(
                (self.root / "warehouse" / entry["path"]).exists()
                for manifest in manifests[1:]
                for entry in manifest["objects"]
            )
        )
        os.utime(
            self.root / "warehouse" / manifests[1]["objects"][0]["path"],
            (old, old),
        )
        self.assertEqual(
            self.compactor.vacuum(0, cutoff),
            sorted(entry["path"] for entry in manifests[1]["objects"]),
        )

        commit("s4", "s3", "d.jsonl", self.row("4", "p", 1, data={"x": 4}))
        third = self.manifest("s3")
        fourth = self.manifest("s4")
        shared = set(entry["path"] for entry in third["objects"]) & set(
            entry["path"] for entry in fourth["objects"]
        )
        self.assertTrue(shared)
        for manifest in (third, fourth):
            for entry in manifest["objects"]:
                os.utime(self.root / "warehouse" / entry["path"], (old, old))
        deleted = self.compactor.vacuum(1, cutoff)
        self.assertFalse(any(path in deleted for path in shared))

    def test_concurrent_commits_do_not_fork(self):
        base = self.segment("base.jsonl", [self.row("base", "p", 1)])
        self.compactor.commit("base", None, [base])
        results = {}
        guard = threading.Lock()

        def worker(index):
            path = self.segment(
                f"thread-{index}.jsonl", [self.row(f"k{index}", "p", 1)]
            )
            try:
                result = self.compactor.commit(
                    f"thread-{index}", "base", [path]
                )
            except ConflictError as error:
                result = error
            with guard:
                results[index] = result

        threads = [
            threading.Thread(target=worker, args=(index,)) for index in range(8)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        winners = [result for result in results.values() if isinstance(result, dict)]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(results), 8)
        self.assertEqual(self.compactor.head, winners[0]["snapshot_id"])
        self.assertEqual(len(self.compactor.read()), 2)

    def test_cli_success_and_error_contract(self):
        project = Path(__file__).resolve().parents[1]
        segment = self.segment("cli.jsonl", [self.row("1", "p", 1)])

        def run(arguments):
            return subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "icecrate",
                    "--root",
                    str(self.root / "cli-warehouse"),
                    *arguments,
                ],
                cwd=project,
                capture_output=True,
                text=True,
            )

        result = run(["commit", "s1", "--parent", "null", str(segment)])
        self.assertEqual(result.returncode, 0)
        self.assertEqual(json.loads(result.stdout)["snapshot_id"], "s1")
        result = run(["read"])
        self.assertEqual(result.returncode, 0)
        self.assertEqual(json.loads(result.stdout)[0]["pk"], "1")
        result = run(["read", "missing"])
        self.assertEqual(result.returncode, 2)
        self.assertNotIn("Traceback", result.stderr)
        self.assertIn("error:", result.stderr)
