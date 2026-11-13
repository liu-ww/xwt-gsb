import hashlib
import io
import json
import os
import shutil
import stat
import struct
import subprocess
import sys
import threading
import unicodedata
import unittest
import uuid
import warnings
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from vaultunpack import ExtractionError, Extractor


POLICY = {
    "max_files": 20,
    "max_file_size": 10000,
    "max_total_size": 20000,
    "max_ratio": 20,
    "max_depth": 5,
    "allowed_methods": ["stored", "deflated"],
}


def _info(name, **attributes):
    value = zipfile.ZipInfo("safe")
    value.filename = name
    value.orig_filename = name
    for key, item in attributes.items():
        setattr(value, key, item)
    return value


def _central_offsets(data):
    offsets = []
    cursor = 0
    while True:
        cursor = data.find(b"PK\x01\x02", cursor)
        if cursor < 0:
            return offsets
        offsets.append(cursor)
        cursor += 4


class Security(unittest.TestCase):
    def setUp(self):
        self.root = Path(__file__).parent / ".work" / uuid.uuid4().hex
        self.root.mkdir(parents=True)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def archive(self, entries, compression=zipfile.ZIP_DEFLATED):
        path = self.root / "input.zip"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with zipfile.ZipFile(path, "w", compression=compression) as output:
                for entry in entries:
                    name, data = entry
                    output.writestr(name, data)
        return path

    def rejects(self, archive, policy=None):
        destination = self.root / "out"
        with self.assertRaises(Exception):
            Extractor(policy or POLICY).extract(archive, destination)
        self.assertFalse(destination.exists())
        leftovers = [item.name for item in self.root.iterdir() if item.name.startswith(".vaultunpack")]
        self.assertEqual(leftovers, [])

    def test_invalid_names(self):
        names = [
            "",
            "/absolute",
            "C:/drive",
            "C:drive",
            "back\\slash",
            "a//b",
            "a/./b",
            "a/../b",
            "a/../../b",
            "control\x01name",
            "trailingdot.",
            "trailing space ",
            "CON",
            "com1.txt",
            "nested/LPT9.log",
            "a:b",
        ]
        for name in names:
            with self.subTest(name=name):
                self.rejects(self.archive([(_info(name), b"x")]))

    def test_nfc_depth_and_collisions(self):
        for name in ("e\u0301.txt", "e\u0301/inside.txt"):
            with self.subTest(name=name):
                self.rejects(self.archive([(_info(name), b"x")]))
        self.rejects(self.archive([("a/b/c/d/e/f.txt", b"x")]))
        self.rejects(self.archive([("A.txt", b"1"), ("a.TXT", b"2")]))
        entry = _info("Dir/one.txt")
        second = _info("dir/two.txt")
        self.rejects(self.archive([(entry, b"1"), (second, b"2")]))
        self.rejects(self.archive([("MANIFEST.json", b"forged")]))
        self.rejects(self.archive([("manifest.json/child", b"x")]))

    def test_unsafe_member_types(self):
        for kind in (
            stat.S_IFLNK,
            stat.S_IFCHR,
            stat.S_IFBLK,
            stat.S_IFIFO,
            stat.S_IFSOCK,
        ):
            with self.subTest(kind=oct(kind)):
                entry = _info("special")
                entry.create_system = 3
                entry.external_attr = (kind | 0o777) << 16
                self.rejects(self.archive([(entry, b"x")]))
        entry = _info("directory")
        entry.create_system = 3
        entry.external_attr = stat.S_IFDIR << 16
        self.rejects(self.archive([(entry, b"")]))
        self.rejects(self.archive([("dir/", b"not empty")]))

    def test_encryption_and_methods(self):
        encrypted = self.archive([("a.txt", b"data")])
        data = bytearray(encrypted.read_bytes())
        for offset in _central_offsets(data):
            flags = struct.unpack_from("<H", data, offset + 8)[0]
            struct.pack_into("<H", data, offset + 8, flags | 1)
        encrypted.write_bytes(data)
        self.rejects(encrypted)

        unsupported = self.archive([("a.txt", b"data")])
        data = bytearray(unsupported.read_bytes())
        for offset in _central_offsets(data):
            struct.pack_into("<H", data, offset + 10, 99)
        unsupported.write_bytes(data)
        self.rejects(unsupported)

        stored = self.archive([("a.txt", b"data")], zipfile.ZIP_STORED)
        self.rejects(stored, dict(POLICY, allowed_methods=["deflated"]))

    def test_crc_and_forged_sizes(self):
        corrupt = self.archive([("a.txt", b"checksum")])
        data = bytearray(corrupt.read_bytes())
        offsets = _central_offsets(data)
        struct.pack_into("<I", data, offsets[0] + 16, 0x12345678)
        with zipfile.ZipFile(corrupt) as loaded:
            local = loaded.infolist()[0].header_offset
        struct.pack_into("<I", data, local + 14, 0x12345678)
        corrupt.write_bytes(data)
        self.rejects(corrupt)

        forged = self.archive([("a.txt", b"x" * 500)])
        data = bytearray(forged.read_bytes())
        offsets = _central_offsets(data)
        struct.pack_into("<I", data, offsets[0] + 24, 1)
        with zipfile.ZipFile(forged) as loaded:
            local = loaded.infolist()[0].header_offset
        struct.pack_into("<I", data, local + 22, 1)
        forged.write_bytes(data)
        self.rejects(forged)

        forged_large = self.archive([("a.txt", b"short")])
        data = bytearray(forged_large.read_bytes())
        offsets = _central_offsets(data)
        struct.pack_into("<I", data, offsets[0] + 24, 999)
        with zipfile.ZipFile(forged_large) as loaded:
            local = loaded.infolist()[0].header_offset
        struct.pack_into("<I", data, local + 22, 999)
        forged_large.write_bytes(data)
        self.rejects(forged_large)

    def test_overlap(self):
        path = self.archive([("one.txt", b"one"), ("two.txt", b"two")])
        data = bytearray(path.read_bytes())
        offsets = _central_offsets(data)
        with zipfile.ZipFile(path) as loaded:
            first = loaded.infolist()[0].header_offset
        struct.pack_into("<I", data, offsets[1] + 42, first)
        path.write_bytes(data)
        self.rejects(path)

    def test_limits_and_bomb_ratio(self):
        self.rejects(self.archive([(f"f{i}.txt", b"x") for i in range(3)]), dict(POLICY, max_files=2))
        self.rejects(self.archive([("big.txt", b"x" * 11)]), dict(POLICY, max_file_size=10))
        self.rejects(
            self.archive([("one.txt", b"x" * 6), ("two.txt", b"y" * 6)]),
            dict(POLICY, max_total_size=10),
        )
        self.rejects(
            self.archive([("bomb.txt", b"A" * 9000)]),
            dict(POLICY, max_file_size=10000, max_total_size=10000, max_ratio=2),
        )

    def test_success_manifest_modes_and_determinism(self):
        mode_entry = _info("b.txt", external_attr=0o777 << 16, create_system=3)
        mode_entry.date_time = (2001, 1, 1, 1, 1, 2)
        first = Extractor(POLICY).extract(
            self.archive([("nested/", b""), (mode_entry, b"second"), ("a.txt", b"first")]),
            self.root / "out1",
        )
        second = Extractor(POLICY).extract(
            self.archive([("nested/", b""), (mode_entry, b"second"), ("a.txt", b"first")]),
            self.root / "out2",
        )
        self.assertEqual(first["files"], 2)
        self.assertEqual(second["files"], 2)
        self.assertTrue((self.root / "out1/nested").is_dir())
        self.assertEqual((self.root / "out1/a.txt").read_bytes(), b"first")
        manifest = json.loads((self.root / "out1/MANIFEST.json").read_text(encoding="utf-8"))
        paths = [item["path"] for item in manifest["files"]]
        self.assertEqual(paths, ["a.txt", "b.txt"])
        self.assertEqual(
            manifest["files"][1],
            {
                "path": "b.txt",
                "size": 6,
                "sha256": hashlib.sha256(b"second").hexdigest(),
            },
        )
        self.assertNotIn("MANIFEST.json", paths)
        first_manifest = (self.root / "out1/MANIFEST.json").read_bytes()
        second_manifest = (self.root / "out2/MANIFEST.json").read_bytes()
        self.assertEqual(first_manifest, second_manifest)
        if os.name == "posix":
            self.assertEqual(stat.S_IMODE((self.root / "out1").stat().st_mode), 0o750)
            self.assertEqual(stat.S_IMODE((self.root / "out1/a.txt").stat().st_mode), 0o640)
            self.assertEqual(stat.S_IMODE((self.root / "out1/nested").stat().st_mode), 0o750)
            self.assertEqual(stat.S_IMODE((self.root / "out1/MANIFEST.json").stat().st_mode), 0o640)

    def test_fault_stages_roll_back(self):
        for stage in ("after_member", "before_manifest", "before_publish"):
            with self.subTest(stage=stage):
                destination = self.root / f"out-{stage}"
                path = self.archive([("a.txt", b"data"), ("b.txt", b"more")])

                def fault(value, target=stage):
                    if value == target:
                        raise RuntimeError(target)

                with self.assertRaises(RuntimeError):
                    Extractor(POLICY).extract(path, destination, fault=fault)
                self.assertFalse(destination.exists())
                leftovers = [
                    item.name for item in self.root.iterdir() if item.name.startswith(".vaultunpack")
                ]
                self.assertEqual(leftovers, [])

    def test_destination_existing_is_untouched(self):
        path = self.archive([("a.txt", b"new")])
        destination = self.root / "out"
        destination.mkdir()
        (destination / "old.txt").write_bytes(b"old")
        with self.assertRaises(FileExistsError):
            Extractor(POLICY).extract(path, destination)
        self.assertEqual((destination / "old.txt").read_bytes(), b"old")
        leftovers = [item.name for item in self.root.iterdir() if item.name.startswith(".vaultunpack")]
        self.assertEqual(leftovers, [])

    def test_concurrent_destination_has_one_winner(self):
        path = self.archive([("a.txt", b"data")])
        destination = self.root / "out"
        barrier = threading.Barrier(2)

        def run():
            barrier.wait()
            try:
                Extractor(POLICY).extract(path, destination)
                return True
            except Exception:
                return False

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _value: run(), range(2)))
        self.assertEqual(results.count(True), 1)
        self.assertEqual((destination / "a.txt").read_bytes(), b"data")
        self.assertFalse(any(item.name.startswith(".vaultunpack") for item in self.root.iterdir()))

    def test_parent_symlink_rejected(self):
        target = self.root / "target"
        target.mkdir()
        link = self.root / "link"
        try:
            os.symlink(target, link, target_is_directory=True)
        except (OSError, NotImplementedError):
            self.skipTest("directory symlinks are unavailable")
        path = self.archive([("a.txt", b"data")])
        with self.assertRaises(ExtractionError):
            Extractor(POLICY).extract(path, link / "out")
        self.assertFalse((target / "out").exists())

    def test_empty_archive(self):
        path = self.archive([])
        destination = self.root / "out"
        result = Extractor(POLICY).extract(path, destination)
        self.assertEqual(result["files"], 0)
        manifest = json.loads((destination / "MANIFEST.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest, {"files": []})

    def test_cli(self):
        path = self.archive([("a.txt", b"ok")])
        destination = self.root / "out"
        command = [
            sys.executable,
            "-m",
            "vaultunpack",
            str(path),
            str(destination),
            json.dumps(POLICY),
        ]
        root = Path(__file__).resolve().parents[1]
        success = subprocess.run(command, cwd=root, capture_output=True, text=True, timeout=30)
        self.assertEqual(success.returncode, 0, success.stderr)
        self.assertEqual(len(success.stdout.splitlines()), 1)
        result = json.loads(success.stdout)
        self.assertEqual(
            success.stdout.strip(),
            json.dumps(result, sort_keys=True, ensure_ascii=False, separators=(",", ":")),
        )
        bad_policy = dict(POLICY, max_files=0)
        failure = subprocess.run(
            [
                sys.executable,
                "-m",
                "vaultunpack",
                str(path),
                str(self.root / "bad"),
                json.dumps(bad_policy),
            ],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(failure.returncode, 2)
        self.assertNotIn("Traceback", failure.stderr)
        broken = self.root / "broken.zip"
        broken.write_bytes(b"not a zip")
        malformed = subprocess.run(
            [
                sys.executable,
                "-m",
                "vaultunpack",
                str(broken),
                str(self.root / "broken-out"),
                json.dumps(POLICY),
            ],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(malformed.returncode, 2)
        self.assertNotIn("Traceback", malformed.stderr)

    def test_zip64_and_data_descriptor(self):
        zip64_path = self.root / "zip64.zip"
        item = _info("z.txt")
        item.compress_type = zipfile.ZIP_DEFLATED
        with zipfile.ZipFile(zip64_path, "w", allowZip64=True) as output:
            with output.open(item, "w", force_zip64=True) as stream:
                stream.write(b"zip64 data")
        destination = self.root / "zip64-out"
        Extractor(POLICY).extract(zip64_path, destination)
        self.assertEqual((destination / "z.txt").read_bytes(), b"zip64 data")

        class NonSeekable:
            def __init__(self, target):
                self.target = target

            def write(self, data):
                return self.target.write(data)

            def flush(self):
                self.target.flush()

            def tell(self):
                raise OSError("not seekable")

            def seek(self, *args):
                raise OSError("not seekable")

        buffer = io.BytesIO()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with zipfile.ZipFile(
                NonSeekable(buffer), "w", compression=zipfile.ZIP_DEFLATED
            ) as output:
                output.writestr("stream.txt", b"descriptor data")
        descriptor_path = self.root / "descriptor.zip"
        descriptor_path.write_bytes(buffer.getvalue())
        destination = self.root / "descriptor-out"
        Extractor(POLICY).extract(descriptor_path, destination)
        self.assertEqual((destination / "stream.txt").read_bytes(), b"descriptor data")

    def test_non_utf8_flagged_unicode_name_rejected(self):
        path = self.archive([("caf\u00e9.txt", b"data")])
        data = bytearray(path.read_bytes())
        for offset in _central_offsets(data):
            flags = struct.unpack_from("<H", data, offset + 8)[0]
            struct.pack_into("<H", data, offset + 8, flags & ~0x0800)
        path.write_bytes(data)
        self.rejects(path)
