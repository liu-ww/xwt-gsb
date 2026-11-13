from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


CATALOG_VERSION = 1
CHUNK_ROWS = 3


class IceCrateError(Exception):
    """Base class for Ice Crate errors."""


class ConflictError(IceCrateError, ValueError):
    """The requested change conflicts with the catalog."""


class CorruptionError(IceCrateError, ValueError):
    """Stored metadata or data failed an integrity check."""


class CatalogVersionError(CorruptionError):
    """A manifest was written by a newer catalog implementation."""


class ParentCycleError(CorruptionError):
    """The snapshot parent chain contains a cycle."""


class SnapshotNotFoundError(IceCrateError, KeyError):
    """The requested snapshot does not exist."""


class FaultInjected(RuntimeError):
    """A fault requested through the commit fault hook."""


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number is not allowed: {value}")


def _pairs_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _json_loads(text: str) -> Any:
    return json.loads(
        text,
        object_pairs_hook=_pairs_without_duplicates,
        parse_constant=_reject_constant,
    )


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except BaseException:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


_THREAD_LOCK_GUARD = threading.Lock()
_THREAD_LOCKS: dict[str, threading.RLock] = {}


def _thread_lock_for(path: Path) -> threading.RLock:
    key = os.path.normcase(str(path.resolve()))
    with _THREAD_LOCK_GUARD:
        lock = _THREAD_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _THREAD_LOCKS[key] = lock
        return lock


class _CatalogLock:
    def __init__(self, root: Path) -> None:
        self._lock_path = root / "catalog" / ".lock"
        self._thread_lock = _thread_lock_for(root)
        self._stream: Any = None

    def __enter__(self) -> "_CatalogLock":
        self._thread_lock.acquire()
        try:
            self._lock_path.parent.mkdir(parents=True, exist_ok=True)
            self._stream = open(self._lock_path, "a+b")
            self._stream.seek(0, os.SEEK_END)
            if self._stream.tell() == 0:
                self._stream.write(b"\0")
                self._stream.flush()
            self._lock_stream()
        except BaseException:
            if self._stream is not None:
                self._stream.close()
                self._stream = None
            self._thread_lock.release()
            raise
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        try:
            self._unlock_stream()
        finally:
            if self._stream is not None:
                self._stream.close()
                self._stream = None
            self._thread_lock.release()

    def _lock_stream(self) -> None:
        if os.name == "nt":
            import msvcrt

            while True:
                try:
                    self._stream.seek(0)
                    msvcrt.locking(self._stream.fileno(), msvcrt.LK_LOCK, 1)
                    return
                except OSError:
                    time.sleep(0.05)
        else:
            import fcntl

            fcntl.flock(self._stream.fileno(), fcntl.LOCK_EX)

    def _unlock_stream(self) -> None:
        if self._stream is None:
            return
        if os.name == "nt":
            import msvcrt

            try:
                self._stream.seek(0)
                msvcrt.locking(self._stream.fileno(), msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
        else:
            import fcntl

            try:
                fcntl.flock(self._stream.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass


def _validate_snapshot_id(snapshot_id: Any) -> str:
    if not isinstance(snapshot_id, str) or not snapshot_id:
        raise ValueError("snapshot_id must be a non-empty string")
    if snapshot_id in {".", ".."} or "\x00" in snapshot_id:
        raise ValueError("snapshot_id contains an unsafe character")
    if "/" in snapshot_id or "\\" in snapshot_id:
        raise ValueError("snapshot_id contains an unsafe character")
    return snapshot_id


def _is_positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _is_nonnegative_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _parse_row_text(text: str) -> dict[str, Any]:
    try:
        row = _json_loads(text)
    except (ValueError, json.JSONDecodeError) as error:
        raise CorruptionError(f"invalid JSON row: {error}") from error
    _validate_row(row)
    return row


def _validate_row(row: Any) -> None:
    if not isinstance(row, dict):
        raise CorruptionError("a row must be a JSON object")
    required = {"pk", "partition", "version", "op", "data"}
    if set(row) != required:
        raise CorruptionError("a row must contain exactly pk, partition, version, op, data")
    if not isinstance(row["pk"], str):
        raise CorruptionError("row pk must be a string")
    if not isinstance(row["partition"], str):
        raise CorruptionError("row partition must be a string")
    if not _is_positive_int(row["version"]):
        raise CorruptionError("row version must be a positive integer")
    if row["op"] not in {"upsert", "delete"}:
        raise CorruptionError("row op must be upsert or delete")
    try:
        _canonical_json(row["data"])
    except (TypeError, ValueError) as error:
        raise CorruptionError(f"row data is not canonical JSON: {error}") from error


def _row_content_key(row: dict[str, Any]) -> tuple[str, str]:
    return row["op"], _canonical_json(row["data"])


def _coerce_grace_before(value: Any) -> float:
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, bool):
        raise TypeError("grace_before must be a timestamp, datetime, or ISO string")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        try:
            return float(stripped)
        except ValueError:
            pass
        try:
            parsed = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError(f"invalid grace_before: {value!r}") from error
        return parsed.timestamp()
    raise TypeError("grace_before must be a timestamp, datetime, or ISO string")


class Compactor:
    def __init__(self, root: Any) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    @property
    def catalog_dir(self) -> Path:
        return self.root / "catalog"

    @property
    def snapshots_dir(self) -> Path:
        return self.catalog_dir / "snapshots"

    @property
    def objects_dir(self) -> Path:
        return self.root / "objects"

    @property
    def head(self) -> str | None:
        with self._lock():
            return self._read_head_locked()

    def _lock(self) -> _CatalogLock:
        return _CatalogLock(self.root)

    def _snapshot_path(self, snapshot_id: str) -> Path:
        return self.snapshots_dir / f"{snapshot_id}.json"

    def _manifest_candidates(self, snapshot_id: str) -> Iterable[Path]:
        yield self._snapshot_path(snapshot_id)
        yield self.snapshots_dir / snapshot_id / "manifest.json"
        yield self.catalog_dir / "manifests" / f"{snapshot_id}.json"
        if snapshot_id not in {"head", "HEAD", "catalog"}:
            yield self.catalog_dir / f"{snapshot_id}.json"

    def _find_manifest(self, snapshot_id: str) -> Path | None:
        seen: set[Path] = set()
        for candidate in self._manifest_candidates(snapshot_id):
            if candidate in seen:
                continue
            seen.add(candidate)
            if candidate.is_file():
                return candidate
        return None

    def _read_head_locked(self) -> str | None:
        head_json = self.catalog_dir / "head.json"
        plain_head = self.catalog_dir / "head"
        for path in (head_json, plain_head):
            if not path.is_file():
                continue
            try:
                text = path.read_text(encoding="utf-8").strip()
            except OSError as error:
                raise CorruptionError(f"cannot read catalog head: {error}") from error
            if not text:
                raise CorruptionError("catalog head is empty")
            try:
                value = _json_loads(text)
            except (ValueError, json.JSONDecodeError):
                value = text
            if isinstance(value, dict):
                for key in ("snapshot_id", "head", "id"):
                    if key in value:
                        value = value[key]
                        break
            if not isinstance(value, str) or not value:
                raise CorruptionError("catalog head is malformed")
            return _validate_snapshot_id(value)
        return None

    def _write_head_locked(self, snapshot_id: str) -> None:
        self.catalog_dir.mkdir(parents=True, exist_ok=True)
        _atomic_write(self.catalog_dir / "head", snapshot_id.encode("utf-8"))
        payload = _canonical_json({"snapshot_id": snapshot_id}).encode("utf-8")
        _atomic_write(self.catalog_dir / "head.json", payload)

    def _read_raw_manifest(self, snapshot_id: str) -> tuple[Path, dict[str, Any]]:
        path = self._find_manifest(snapshot_id)
        if path is None:
            raise SnapshotNotFoundError(f"unknown snapshot: {snapshot_id}")
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as error:
            raise CorruptionError(f"cannot read snapshot manifest: {error}") from error
        try:
            manifest = _json_loads(raw)
        except (ValueError, json.JSONDecodeError) as error:
            raise CorruptionError(f"invalid snapshot manifest: {error}") from error
        if not isinstance(manifest, dict):
            raise CorruptionError("snapshot manifest must be a JSON object")
        return path, manifest

    def _validated_manifest(self, snapshot_id: str) -> dict[str, Any]:
        _, manifest = self._read_raw_manifest(snapshot_id)
        versions = []
        for key in ("catalog_version", "version"):
            if key in manifest:
                value = manifest[key]
                if not _is_positive_int(value):
                    raise CatalogVersionError(f"manifest {key} is invalid")
                versions.append(value)
        if versions and max(versions) > CATALOG_VERSION:
            raise CatalogVersionError(
                f"snapshot {snapshot_id} uses catalog version {max(versions)}; "
                f"supported version is {CATALOG_VERSION}"
            )
        if "parent" not in manifest:
            raise CorruptionError("snapshot manifest is missing parent")
        parent = manifest["parent"]
        if parent is not None:
            if not isinstance(parent, str) or not parent:
                raise CorruptionError("snapshot manifest parent is invalid")
            _validate_snapshot_id(parent)
        if not _is_positive_int(manifest.get("generation")):
            raise CorruptionError("snapshot manifest generation is invalid")
        row_counts = []
        for key in ("rows", "row_count"):
            if key in manifest:
                value = manifest[key]
                if not _is_nonnegative_int(value):
                    raise CorruptionError(f"snapshot manifest {key} is invalid")
                row_counts.append(value)
        if not row_counts:
            raise CorruptionError("snapshot manifest is missing row count")
        if len(set(row_counts)) != 1:
            raise CorruptionError("snapshot manifest row counts disagree")
        objects = manifest.get("objects")
        if not isinstance(objects, list):
            raise CorruptionError("snapshot manifest objects must be a list")
        if "snapshot_id" in manifest and manifest["snapshot_id"] != snapshot_id:
            raise CorruptionError("snapshot manifest id does not match its path")
        for entry in objects:
            self._entry_path_and_hash(entry)
        tombstones = manifest.get("tombstones", [])
        if not isinstance(tombstones, list):
            raise CorruptionError("snapshot manifest tombstones must be a list")
        previous_key: tuple[str, str] | None = None
        for row in tombstones:
            _validate_row(row)
            if row["op"] != "delete":
                raise CorruptionError("manifest tombstones must contain delete rows")
            key = (row["partition"], row["pk"])
            if previous_key is not None and previous_key >= key:
                raise CorruptionError("manifest tombstones are not sorted and unique")
            previous_key = key
        return manifest

    def _entry_path_and_hash(self, entry: Any) -> tuple[str, str]:
        if isinstance(entry, str):
            path = entry
            name = Path(path).name
            hash_value = name[:-6] if name.endswith(".jsonl") else ""
        elif isinstance(entry, dict):
            path = entry.get("path")
            candidates = [entry[key] for key in ("hash", "sha256") if key in entry]
            if candidates and len(set(candidates)) != 1:
                raise CorruptionError("object entry hashes disagree")
            hash_value = candidates[0] if candidates else ""
            if not hash_value and isinstance(path, str):
                name = Path(path).name
                hash_value = name[:-6] if name.endswith(".jsonl") else ""
        else:
            raise CorruptionError("object entry must be a path string or object")
        if not isinstance(path, str) or not path:
            raise CorruptionError("object entry path is invalid")
        if not isinstance(hash_value, str) or not hash_value:
            raise CorruptionError("object entry hash is invalid")
        normalized = hash_value.strip().lower()
        if normalized.startswith("sha256:"):
            normalized = normalized[7:]
        if len(normalized) != 64 or any(
            ch not in "0123456789abcdef" for ch in normalized
        ):
            raise CorruptionError("object entry hash is not a SHA-256 digest")
        name = Path(path).name
        if not name.endswith(".jsonl") or name[:-6].lower() != normalized:
            raise CorruptionError("object path is not named by its content hash")
        return path.replace("\\", "/"), normalized

    def _manifest_row_count(self, manifest: dict[str, Any]) -> int:
        return manifest.get("rows", manifest.get("row_count"))

    def _validate_lineage_locked(
        self, snapshot_id: str
    ) -> dict[str, dict[str, Any]]:
        seen: set[str] = set()
        manifests: dict[str, dict[str, Any]] = {}
        current: str | None = snapshot_id
        child: dict[str, Any] | None = None
        while current is not None:
            if current in seen:
                raise ParentCycleError(f"snapshot parent cycle at {current}")
            seen.add(current)
            manifest = self._validated_manifest(current)
            manifests[current] = manifest
            if child is not None and manifest["generation"] != child["generation"] - 1:
                raise CorruptionError("snapshot parent generations are inconsistent")
            if manifest["parent"] is None and manifest["generation"] != 1:
                raise CorruptionError("root snapshot generation must be 1")
            child = manifest
            current = manifest["parent"]
        return manifests

    def _object_candidates(self, reference: str) -> Iterable[Path]:
        normalized = reference.replace("\\", "/")
        parts = Path(normalized).parts
        if os.path.isabs(normalized) or ".." in parts or normalized.startswith("/"):
            raise CorruptionError("object reference escapes the catalog")
        yield self.root / normalized
        yield self.catalog_dir / normalized
        if not normalized.startswith("objects/"):
            yield self.objects_dir / normalized

    def _resolve_object(self, reference: str, expected_hash: str) -> Path:
        for candidate in self._object_candidates(reference):
            if candidate.is_file():
                path = candidate
                break
        else:
            raise CorruptionError(f"missing object: {reference}")
        name = path.name
        if not name.endswith(".jsonl") or name[:-6].lower() != expected_hash:
            raise CorruptionError(f"object filename does not match hash: {reference}")
        try:
            data = path.read_bytes()
        except OSError as error:
            raise CorruptionError(f"cannot read object: {error}") from error
        actual = hashlib.sha256(data).hexdigest()
        if actual != expected_hash:
            raise CorruptionError(f"object hash mismatch: {reference}")
        return path

    def _parse_object(self, path: Path) -> list[dict[str, Any]]:
        try:
            data = path.read_bytes()
        except OSError as error:
            raise CorruptionError(f"cannot read object: {error}") from error
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as error:
            raise CorruptionError(f"object is not UTF-8: {error}") from error
        lines = text.splitlines()
        if len(lines) > CHUNK_ROWS:
            raise CorruptionError(f"object contains more than {CHUNK_ROWS} rows")
        rows = []
        for line in lines:
            if not line.strip():
                raise CorruptionError("object contains an empty JSONL line")
            rows.append(_parse_row_text(line))
        for previous, current in zip(rows, rows[1:]):
            if (previous["partition"], previous["pk"]) >= (
                current["partition"],
                current["pk"],
            ):
                raise CorruptionError("object rows are not sorted and unique")
        return rows

    def _load_rows_locked(self, snapshot_id: str) -> list[dict[str, Any]]:
        self._validate_lineage_locked(snapshot_id)
        manifest = self._validated_manifest(snapshot_id)
        rows: list[dict[str, Any]] = []
        references: set[Path] = set()
        for entry in manifest["objects"]:
            reference, expected_hash = self._entry_path_and_hash(entry)
            path = self._resolve_object(reference, expected_hash)
            resolved = path.resolve()
            if resolved in references:
                raise CorruptionError(
                    f"snapshot references the same object twice: {reference}"
                )
            references.add(resolved)
            rows.extend(self._parse_object(path))
        live_rows = [row for row in rows if row["op"] == "upsert"]
        keys: set[tuple[str, str]] = set()
        for row in live_rows:
            key = (row["partition"], row["pk"])
            if key in keys:
                raise CorruptionError(f"snapshot contains duplicate live row {key!r}")
            keys.add(key)
        if len(live_rows) != self._manifest_row_count(manifest):
            raise CorruptionError("snapshot row count does not match its objects")
        return sorted(live_rows, key=lambda row: (row["partition"], row["pk"]))

    def read(self, snapshot_id: Any = None) -> list[dict[str, Any]]:
        if snapshot_id is not None:
            snapshot_id = _validate_snapshot_id(snapshot_id)
        with self._lock():
            head = self._read_head_locked()
            target = snapshot_id if snapshot_id is not None else head
            if target is None:
                return []
            rows = self._load_rows_locked(target)
            return [dict(row) for row in rows]

    def _read_segments(
        self, segments: Any
    ) -> tuple[list[bytes], list[dict[str, Any]]]:
        if isinstance(segments, (str, os.PathLike)):
            segment_list = [segments]
        else:
            segment_list = list(segments)
        raw_segments: list[bytes] = []
        rows: list[dict[str, Any]] = []
        for segment in segment_list:
            try:
                data = Path(segment).read_bytes()
            except OSError as error:
                raise CorruptionError(f"cannot read input segment: {error}") from error
            raw_segments.append(data)
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError as error:
                raise CorruptionError(f"input segment is not UTF-8: {error}") from error
            seen: set[tuple[str, str, int]] = set()
            for line in text.splitlines():
                if not line.strip():
                    raise CorruptionError("input segment contains an empty JSONL line")
                row = _parse_row_text(line)
                key = (row["partition"], row["pk"], row["version"])
                if key in seen:
                    raise CorruptionError(f"duplicate row key in segment: {key!r}")
                seen.add(key)
                rows.append(row)
        return raw_segments, rows

    def _binding_hash(self, parent: str | None, raw_segments: list[bytes]) -> str:
        digest = hashlib.sha256()
        digest.update(b"icecrate-bind-v1\0")
        if parent is None:
            digest.update(b"null\0")
        else:
            digest.update(b"s\0" + parent.encode("utf-8") + b"\0")
        for data in raw_segments:
            digest.update(len(data).to_bytes(8, "big"))
            digest.update(data)
        return digest.hexdigest()

    def _merge_rows(
        self, rows: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        selected: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            key = (row["partition"], row["pk"])
            current = selected.get(key)
            if current is None or row["version"] > current["version"]:
                selected[key] = row
            elif row["version"] == current["version"]:
                if _row_content_key(row) != _row_content_key(current):
                    raise ConflictError(
                        f"conflicting versions for partition={key[0]!r} "
                        f"pk={key[1]!r} version={row['version']}"
                    )
        live = [row for row in selected.values() if row["op"] == "upsert"]
        tombstones = [row for row in selected.values() if row["op"] == "delete"]
        return (
            sorted(live, key=lambda row: (row["partition"], row["pk"])),
            sorted(tombstones, key=lambda row: (row["partition"], row["pk"])),
        )

    def _chunks(self, rows: list[dict[str, Any]]) -> list[bytes]:
        result = []
        for offset in range(0, len(rows), CHUNK_ROWS):
            chunk = rows[offset : offset + CHUNK_ROWS]
            payload = b"".join(
                (_canonical_json(row) + "\n").encode("utf-8") for row in chunk
            )
            result.append(payload)
        return result

    def _publish_objects(
        self, chunks: list[bytes]
    ) -> tuple[list[dict[str, str]], list[Path]]:
        entries: list[dict[str, str]] = []
        created: list[Path] = []
        self.objects_dir.mkdir(parents=True, exist_ok=True)
        try:
            for payload in chunks:
                digest = hashlib.sha256(payload).hexdigest()
                relative = f"objects/{digest}.jsonl"
                destination = self.root / relative
                if destination.exists():
                    self._resolve_object(relative, digest)
                else:
                    _atomic_write(destination, payload)
                    created.append(destination)
                entries.append({"hash": digest, "path": relative, "sha256": digest})
        except BaseException:
            self._cleanup_published(created, None)
            raise
        entries.sort(key=lambda entry: (entry["path"], entry["sha256"]))
        return entries, created

    def _cleanup_published(
        self, created_objects: list[Path], manifest_path: Path | None
    ) -> None:
        if manifest_path is not None:
            try:
                manifest_path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass
        for path in created_objects:
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass

    def _maybe_fault(self, fault: Any, stage: str) -> None:
        target = fault.get("stage") if isinstance(fault, dict) else fault
        if callable(target):
            if target(stage):
                raise FaultInjected(f"injected fault: {stage}")
        elif target == stage:
            raise FaultInjected(f"injected fault: {stage}")

    def commit(
        self,
        snapshot_id: Any,
        parent: Any,
        segments: Any,
        fault: Any = None,
    ) -> dict[str, Any]:
        snapshot_id = _validate_snapshot_id(snapshot_id)
        if parent is not None:
            parent = _validate_snapshot_id(parent)
        raw_segments, rows = self._read_segments(segments)
        binding = self._binding_hash(parent, raw_segments)
        with self._lock():
            head = self._read_head_locked()
            if self._find_manifest(snapshot_id) is not None:
                manifest = self._validated_manifest(snapshot_id)
                stored_binding = manifest.get("input_sha256")
                if manifest["parent"] == parent and stored_binding == binding:
                    return {
                        "applied": False,
                        "generation": manifest["generation"],
                        "objects": len(manifest["objects"]),
                        "rows": self._manifest_row_count(manifest),
                        "snapshot_id": snapshot_id,
                    }
                raise ConflictError(
                    f"snapshot {snapshot_id} is already bound to different content"
                )
            parent_rows: list[dict[str, Any]] = []
            parent_tombstones: list[dict[str, Any]] = []
            if parent is None:
                if head is not None:
                    raise ConflictError(
                        f"parent must be current head {head!r}, not null"
                    )
                generation = 1
            else:
                if head != parent:
                    raise ConflictError(
                        f"parent {parent!r} is not current head {head!r}"
                    )
                self._validate_lineage_locked(parent)
                parent_manifest = self._validated_manifest(parent)
                parent_rows = self._load_rows_locked(parent)
                parent_tombstones = list(parent_manifest.get("tombstones", []))
                generation = parent_manifest["generation"] + 1
            live_rows, tombstones = self._merge_rows(
                parent_rows + parent_tombstones + rows
            )
            chunks = self._chunks(live_rows)
            entries: list[dict[str, str]] = []
            created: list[Path] = []
            manifest_path = self._snapshot_path(snapshot_id)
            manifest_written = False
            try:
                entries, created = self._publish_objects(chunks)
                self._maybe_fault(fault, "after_objects")
                self._maybe_fault(fault, "before_manifest")
                manifest = {
                    "catalog_version": CATALOG_VERSION,
                    "generation": generation,
                    "input_sha256": binding,
                    "objects": entries,
                    "parent": parent,
                    "row_count": len(live_rows),
                    "rows": len(live_rows),
                    "snapshot_id": snapshot_id,
                    "version": CATALOG_VERSION,
                }
                if tombstones:
                    manifest["tombstones"] = tombstones
                _atomic_write(
                    manifest_path,
                    _canonical_json(manifest).encode("utf-8"),
                )
                manifest_written = True
                self._maybe_fault(fault, "before_head")
                current_head = self._read_head_locked()
                if current_head != parent:
                    raise ConflictError(
                        f"parent {parent!r} is not current head {current_head!r}"
                    )
                self._write_head_locked(snapshot_id)
            except BaseException:
                self._cleanup_published(
                    created, manifest_path if manifest_written else None
                )
                raise
            return {
                "applied": True,
                "generation": generation,
                "objects": len(entries),
                "rows": len(live_rows),
                "snapshot_id": snapshot_id,
            }

    def compact(self, snapshot_id: Any) -> dict[str, Any]:
        snapshot_id = _validate_snapshot_id(snapshot_id)
        with self._lock():
            rows = self._load_rows_locked(snapshot_id)
            manifest = self._validated_manifest(snapshot_id)
            chunks = self._chunks(rows)
            entries: list[dict[str, str]] = []
            created: list[Path] = []
            manifest_path = self._snapshot_path(snapshot_id)
            current_payload: bytes | None = None
            try:
                entries, created = self._publish_objects(chunks)
                replacement = dict(manifest)
                replacement["objects"] = entries
                payload = _canonical_json(replacement).encode("utf-8")
                try:
                    current_payload = manifest_path.read_bytes()
                except OSError:
                    current_payload = None
                if current_payload != payload:
                    _atomic_write(manifest_path, payload)
            except BaseException:
                self._cleanup_published(created, None)
                raise
            return {
                "compacted": current_payload != payload,
                "generation": manifest["generation"],
                "objects": len(entries),
                "rows": len(rows),
                "snapshot_id": snapshot_id,
            }

    def _iter_objects(self) -> Iterable[Path]:
        for directory in (self.objects_dir, self.catalog_dir / "objects"):
            if not directory.is_dir():
                continue
            for path in sorted(directory.glob("*.jsonl")):
                if path.is_file():
                    yield path

    def vacuum(self, retain: Any, grace_before: Any) -> list[str]:
        if not _is_nonnegative_int(retain):
            raise ValueError("retain must be a non-negative integer")
        cutoff = _coerce_grace_before(grace_before)
        with self._lock():
            head = self._read_head_locked()
            protected: set[Path] = set()
            protected_references: set[str] = set()
            if head is not None:
                self._validate_lineage_locked(head)
                current: str | None = head
                depth = 0
                while current is not None and depth <= retain:
                    manifest = self._validated_manifest(current)
                    self._load_rows_locked(current)
                    for entry in manifest["objects"]:
                        reference, _ = self._entry_path_and_hash(entry)
                        protected_references.add(reference)
                        for candidate in self._object_candidates(reference):
                            protected.add(candidate.resolve())
                    current = manifest["parent"]
                    depth += 1
            deletions: list[str] = []
            for path in self._iter_objects():
                resolved = path.resolve()
                if path.is_relative_to(self.root):
                    reference = path.relative_to(self.root).as_posix()
                else:
                    reference = ""
                if resolved in protected or reference in protected_references:
                    continue
                try:
                    modified = path.stat().st_mtime
                except OSError:
                    continue
                if modified >= cutoff:
                    continue
                try:
                    path.unlink()
                except FileNotFoundError:
                    continue
                deletions.append(reference)
            return sorted(deletions)
