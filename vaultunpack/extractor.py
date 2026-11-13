import hashlib
import json
import math
import os
import re
import shutil
import stat
import struct
import unicodedata
import uuid
import zipfile
import zlib
from collections.abc import Mapping
from numbers import Real
from pathlib import Path


POLICY_FIELDS = {
    "max_files",
    "max_file_size",
    "max_total_size",
    "max_ratio",
    "max_depth",
    "allowed_methods",
}

METHOD_NAMES = {
    "stored": zipfile.ZIP_STORED,
    "deflated": zipfile.ZIP_DEFLATED,
}

WINDOWS_RESERVED = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "CLOCK$",
    "CONIN$",
    "CONOUT$",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}

CHUNK_SIZE = 64 * 1024
OUTPUT_CHUNK = 1024 * 1024
LOCAL_HEADER = struct.Struct("<4sHHHHHIIIHH")
DRIVE_LETTER = re.compile(r"^[A-Za-z]:")
ZIP64_EXTRA_ID = 0x0001


class ExtractionError(ValueError):
    pass


def _validate_policy(policy):
    if not isinstance(policy, Mapping):
        raise ExtractionError("policy must be a mapping")
    if set(policy) != POLICY_FIELDS:
        missing = sorted(POLICY_FIELDS - set(policy))
        extra = sorted(str(key) for key in set(policy) - POLICY_FIELDS)
        detail = []
        if missing:
            detail.append("missing " + ", ".join(missing))
        if extra:
            detail.append("unknown " + ", ".join(extra))
        raise ExtractionError("invalid policy fields: " + "; ".join(detail))

    for name in ("max_files", "max_file_size", "max_total_size", "max_depth"):
        value = policy[name]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ExtractionError(f"{name} must be a positive integer")

    ratio = policy["max_ratio"]
    if isinstance(ratio, bool) or not isinstance(ratio, (int, Real)):
        raise ExtractionError("max_ratio must be a positive number")
    if isinstance(ratio, int):
        if ratio <= 0:
            raise ExtractionError("max_ratio must be a positive finite number")
        ratio_value = ratio
    else:
        if not math.isfinite(ratio) or ratio <= 0:
            raise ExtractionError("max_ratio must be a positive finite number")
        ratio_value = float(ratio)

    methods = policy["allowed_methods"]
    if isinstance(methods, str) or not isinstance(methods, (list, tuple, set, frozenset)):
        raise ExtractionError("allowed_methods must be a collection of names")
    invalid = [method for method in methods if not isinstance(method, str) or method not in METHOD_NAMES]
    if invalid:
        raise ExtractionError("allowed_methods permits only stored and deflated")

    return {
        "max_files": policy["max_files"],
        "max_file_size": policy["max_file_size"],
        "max_total_size": policy["max_total_size"],
        "max_ratio": ratio_value,
        "max_depth": policy["max_depth"],
        "allowed_methods": frozenset(methods),
    }


def _is_reparse(stat_result):
    if stat.S_ISLNK(stat_result.st_mode):
        return True
    attributes = getattr(stat_result, "st_file_attributes", 0)
    reparse = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(attributes & reparse)


def _lstat(path):
    try:
        return os.lstat(path)
    except FileNotFoundError:
        return None


def _set_mode(path, mode):
    if os.name == "nt":
        return
    os.chmod(path, mode)


def _remove_tree(path):
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        return
    except OSError:
        if os.name != "nt":
            raise
        shutil.rmtree(path, ignore_errors=True)


def _create_stage(parent):
    for _attempt in range(32):
        path = Path(parent) / f".vaultunpack-stage-{uuid.uuid4().hex}"
        try:
            os.mkdir(path, 0o750)
            return path
        except FileExistsError:
            continue
    raise ExtractionError("could not allocate a staging directory")


def _check_parent_chain(path):
    absolute = Path(os.path.abspath(os.fspath(path)))
    current = Path(absolute.anchor)
    identities = []
    parts = absolute.parts
    if parts and parts[0] == absolute.anchor:
        parts = parts[1:]
    for part in parts:
        current = current / part
        result = _lstat(current)
        if result is None:
            raise ExtractionError("destination parent does not exist")
        if _is_reparse(result):
            raise ExtractionError("destination parent uses a symbolic link or junction")
        if not stat.S_ISDIR(result.st_mode):
            raise ExtractionError("destination parent is not a directory")
        identities.append((str(current), result.st_dev, result.st_ino, result.st_mode))
    return absolute, tuple(identities)


def _ensure_absent(path):
    if _lstat(path) is not None:
        raise FileExistsError(f"destination already exists: {path}")


def _check_stage(stage, identity):
    current = _lstat(stage)
    if (
        current is None
        or _is_reparse(current)
        or identity != (current.st_dev, current.st_ino)
    ):
        raise ExtractionError("staging directory changed unexpectedly")


def _inject(fault, stage):
    if fault is None:
        return
    if isinstance(fault, str):
        if fault == stage:
            raise RuntimeError(stage)
        return
    if isinstance(fault, (list, tuple, set, frozenset, dict)):
        if stage in fault:
            raise RuntimeError(stage)
        return
    if callable(fault):
        fault(stage)


def _fold(value):
    return unicodedata.normalize("NFC", value).casefold()


def _validate_name(name, max_depth):
    if not name:
        raise ExtractionError("empty member name")
    if any(unicodedata.category(char) == "Cc" for char in name):
        raise ExtractionError("member name contains a control character")
    if unicodedata.normalize("NFC", name) != name:
        raise ExtractionError("member name is not NFC")
    if name.startswith("/"):
        raise ExtractionError("absolute member path")
    if "\\" in name:
        raise ExtractionError("member name contains a backslash")
    if DRIVE_LETTER.match(name) or ":" in name:
        raise ExtractionError("member name contains a drive or stream separator")

    is_dir = name.endswith("/")
    body = name[:-1] if is_dir else name
    if not body or body.endswith("/"):
        raise ExtractionError("member path contains an empty segment")

    parts = body.split("/")
    if len(parts) > max_depth:
        raise ExtractionError("member path exceeds max_depth")

    for part in parts:
        if part in ("", ".", ".."):
            raise ExtractionError("member path contains an empty or relative segment")
        if part.endswith(".") or part.endswith(" "):
            raise ExtractionError("member name has a trailing dot or space")
        stem = part.split(".", 1)[0].upper()
        if part.upper() in WINDOWS_RESERVED or stem in WINDOWS_RESERVED:
            raise ExtractionError("member name uses a Windows reserved name")

    canonical = "/".join(parts)
    if is_dir:
        canonical += "/"
    return canonical, tuple(parts), is_dir


def _check_zipinfo_shape(info, is_dir):
    flags = info.flag_bits
    if flags & 0x0001 or flags & 0x0020 or flags & 0x0040:
        raise ExtractionError("encrypted or patched member")
    if info.volume != 0:
        raise ExtractionError("multi-volume member")

    mode = info.external_attr >> 16
    file_type = stat.S_IFMT(mode)
    if file_type == stat.S_IFLNK:
        raise ExtractionError("symbolic link member")
    if file_type in (stat.S_IFCHR, stat.S_IFBLK, stat.S_IFIFO, stat.S_IFSOCK):
        raise ExtractionError("special file member")
    if file_type not in (0, stat.S_IFREG, stat.S_IFDIR):
        raise ExtractionError("unsupported member type")
    if file_type == stat.S_IFDIR and not is_dir:
        raise ExtractionError("directory member lacks a trailing slash")
    if info.external_attr & 0x10 and not is_dir:
        raise ExtractionError("directory member lacks a trailing slash")

    if not isinstance(info.file_size, int) or not isinstance(info.compress_size, int):
        raise ExtractionError("invalid member size")
    if info.file_size < 0 or info.compress_size < 0:
        raise ExtractionError("negative member size")
    if not isinstance(info.CRC, int) or not 0 <= info.CRC <= 0xFFFFFFFF:
        raise ExtractionError("invalid member CRC")

    if is_dir:
        if info.file_size != 0:
            raise ExtractionError("directory member carries data")
        if info.compress_type == zipfile.ZIP_STORED and info.compress_size != 0:
            raise ExtractionError("directory member carries data")
        if info.CRC != 0:
            raise ExtractionError("directory member has a nonzero CRC")
    if info.compress_type == zipfile.ZIP_STORED and not is_dir:
        if info.file_size != info.compress_size:
            raise ExtractionError("stored member has inconsistent sizes")


def _member_utf8(info):
    if any(ord(char) > 127 for char in info.orig_filename):
        if not info.flag_bits & 0x0800:
            raise ExtractionError("member name is not UTF-8")


def _folded_parts(parts):
    return tuple(_fold(part) for part in parts)


class _Plan:
    __slots__ = (
        "info",
        "path",
        "parts",
        "is_dir",
        "declared_size",
        "sha256",
        "actual_size",
        "compressed_size",
    )

    def __init__(self, info, path, parts, is_dir):
        self.info = info
        self.path = path
        self.parts = parts
        self.is_dir = is_dir
        self.declared_size = info.file_size
        self.sha256 = None
        self.actual_size = 0
        self.compressed_size = info.compress_size


class _Result:
    __slots__ = ("size", "sha256", "compressed_size", "start", "end")

    def __init__(self, size, sha256, compressed_size, start, end):
        self.size = size
        self.sha256 = sha256
        self.compressed_size = compressed_size
        self.start = start
        self.end = end


class _DestinationLock:
    def __init__(self, parent, destination):
        digest = hashlib.sha256(os.path.normcase(str(destination)).encode("utf-8")).hexdigest()[:24]
        self.path = Path(parent) / f".vaultunpack-{digest}.lock"
        self.held = False

    def acquire(self):
        try:
            descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            raise FileExistsError("another extraction is already using this destination") from None
        try:
            os.write(descriptor, f"{os.getpid()}\n".encode("ascii"))
        except Exception:
            os.close(descriptor)
            try:
                os.unlink(self.path)
            finally:
                raise
        else:
            os.close(descriptor)
        self.held = True

    def release(self):
        if self.held:
            try:
                os.unlink(self.path)
            except FileNotFoundError:
                pass
            self.held = False


class Extractor:
    def __init__(self, policy):
        _validate_policy(policy)
        self.policy = dict(policy)

    def extract(self, archive, destination, fault=None):
        policy = _validate_policy(self.policy)
        archive_path = Path(os.fspath(archive))
        destination_input = str(destination)
        destination_path = Path(os.path.abspath(os.fspath(destination)))
        if destination_path == destination_path.parent:
            raise ExtractionError("invalid destination")
        parent = destination_path.parent
        _, parent_identity = _check_parent_chain(parent)
        _ensure_absent(destination_path)

        lock = _DestinationLock(parent, destination_path)
        lock.acquire()
        stage = None
        stage_identity = None
        published = False
        try:
            _check_parent_chain(parent)
            _ensure_absent(destination_path)

            with open(archive_path, "rb") as archive_file:
                with zipfile.ZipFile(archive_file, "r") as zip_file:
                    archive_size = os.fstat(archive_file.fileno()).st_size
                    plans, files = self._preflight(zip_file, archive_file, archive_size, policy)
                    if _check_parent_chain(parent)[1] != parent_identity:
                        raise ExtractionError("destination parent changed during extraction")
                    stage = _create_stage(parent)
                    stage_stat = os.lstat(stage)
                    stage_identity = (stage_stat.st_dev, stage_stat.st_ino)
                    _set_mode(stage, 0o750)
                    self._materialize(
                        archive_file,
                        archive_size,
                        zip_file.start_dir,
                        plans,
                        stage,
                        policy,
                        fault,
                        stage_identity,
                    )
                    _inject(fault, "before_manifest")
                    _check_stage(stage, stage_identity)
                    manifest_files = [
                        {
                            "path": plan.path,
                            "size": plan.actual_size,
                            "sha256": plan.sha256,
                        }
                        for plan in sorted(files, key=lambda item: item.path)
                    ]
                    self._write_manifest(stage, manifest_files)
                    _check_stage(stage, stage_identity)
                    _set_mode(stage, 0o750)
                    _inject(fault, "before_publish")
                    _check_stage(stage, stage_identity)

            current_identity = _check_parent_chain(parent)[1]
            if current_identity != parent_identity:
                raise ExtractionError("destination parent changed during extraction")
            _ensure_absent(destination_path)
            os.rename(stage, destination_path)
            published = True
        finally:
            try:
                if stage is not None and not published:
                    current = _lstat(stage)
                    if (
                        current is not None
                        and not _is_reparse(current)
                        and stage_identity == (current.st_dev, current.st_ino)
                    ):
                        _remove_tree(stage)
            finally:
                lock.release()

        return {"destination": destination_input, "files": len(files)}

    def _preflight(self, zip_file, archive_file, archive_size, policy):
        infos = list(zip_file.filelist)
        if len(infos) > policy["max_files"]:
            raise ExtractionError("archive has too many members")

        allowed_types = {METHOD_NAMES[name] for name in policy["allowed_methods"]}
        plans = []
        declared_total = 0
        for info in infos:
            _member_utf8(info)
            path, parts, is_dir = _validate_name(info.orig_filename, policy["max_depth"])
            if _fold(parts[0]) == "manifest.json":
                raise ExtractionError("member collides with MANIFEST.json")
            _check_zipinfo_shape(info, is_dir)
            if info.compress_type not in METHOD_NAMES.values():
                raise ExtractionError("unsupported compression method")
            if info.compress_type not in allowed_types:
                raise ExtractionError("compression method is not allowed")
            if info.file_size > policy["max_file_size"]:
                raise ExtractionError("declared member size exceeds max_file_size")
            declared_total += info.file_size
            if declared_total > policy["max_total_size"]:
                raise ExtractionError("declared total size exceeds max_total_size")
            plans.append(_Plan(info, path, parts, is_dir))

        self._validate_layout(plans)

        files = []
        actual_total = 0
        ranges = []
        for plan in plans:
            result = self._consume(
                archive_file,
                plan,
                archive_size,
                zip_file.start_dir,
                policy,
                None,
            )
            plan.actual_size = result.size
            plan.compressed_size = result.compressed_size
            ranges.append((result.start, result.end, plan.path))
            actual_total += result.size
            if actual_total > policy["max_total_size"]:
                raise ExtractionError("actual total size exceeds max_total_size")
            if not plan.is_dir:
                plan.sha256 = result.sha256
                files.append(plan)

        ranges.sort(key=lambda item: item[0])
        previous_end = 0
        for start, end, path in ranges:
            if start < previous_end:
                raise ExtractionError(f"overlapping member data: {path}")
            if end < start:
                raise ExtractionError(f"invalid member range: {path}")
            previous_end = end
        return plans, files

    def _validate_layout(self, plans):
        member_names = {}
        node_names = {}
        node_kinds = {}

        for plan in plans:
            folded = _folded_parts(plan.parts)
            if folded in member_names:
                raise ExtractionError("duplicate or case-colliding member name")
            member_names[folded] = plan.parts

            for length in range(1, len(plan.parts) + 1):
                prefix = folded[:length]
                raw_prefix = plan.parts[:length]
                existing = node_names.get(prefix)
                if existing is None:
                    node_names[prefix] = raw_prefix
                elif existing != raw_prefix:
                    raise ExtractionError("Unicode or case-colliding path prefix")

            for length in range(1, len(plan.parts)):
                prefix = folded[:length]
                if node_kinds.get(prefix) == "file":
                    raise ExtractionError("member path has a file as a parent")
                node_kinds[prefix] = "dir"

            if plan.is_dir:
                if node_kinds.get(folded) == "file":
                    raise ExtractionError("directory collides with a file")
                node_kinds[folded] = "dir"
            else:
                if folded in node_kinds:
                    raise ExtractionError("file collides with a directory or child")
                node_kinds[folded] = "file"

    def _consume(self, archive_file, plan, archive_size, start_dir, policy, writer):
        info = plan.info
        data_start, data_end = self._check_local_header(
            archive_file, plan, archive_size, start_dir
        )
        start = info.header_offset
        compressed_size = info.compress_size

        if plan.is_dir:
            if compressed_size == 0:
                return _Result(0, None, 0, start, data_end)
            if info.compress_type != zipfile.ZIP_DEFLATED:
                raise ExtractionError("directory member carries data")
            archive_file.seek(data_start)
            sha = hashlib.sha256()
            size, crc = self._inflate(
                archive_file,
                compressed_size,
                0,
                policy,
                sha,
                None,
            )
            if size != 0 or (crc & 0xFFFFFFFF) != info.CRC:
                raise ExtractionError("directory member carries data")
            return _Result(0, None, compressed_size, start, data_end)

        archive_file.seek(data_start)
        sha = hashlib.sha256()
        crc = 0
        size = 0

        if info.compress_type == zipfile.ZIP_STORED:
            remaining = compressed_size
            while remaining:
                take = min(CHUNK_SIZE, remaining)
                chunk = archive_file.read(take)
                if len(chunk) != take:
                    raise ExtractionError("truncated member data")
                remaining -= take
                size += len(chunk)
                if size > policy["max_file_size"]:
                    raise ExtractionError("actual member size exceeds max_file_size")
                crc = zlib.crc32(chunk, crc)
                sha.update(chunk)
                if writer is not None:
                    writer(chunk)
        elif info.compress_type == zipfile.ZIP_DEFLATED:
            size, crc = self._inflate(
                archive_file,
                compressed_size,
                plan.declared_size,
                policy,
                sha,
                writer,
            )
        else:
            raise ExtractionError("unsupported compression method")

        if size != plan.declared_size:
            raise ExtractionError("member actual size does not match declared size")
        if (crc & 0xFFFFFFFF) != info.CRC:
            raise ExtractionError("member CRC mismatch")
        self._check_ratio(size, compressed_size, policy)

        return _Result(size, sha.hexdigest(), compressed_size, start, data_end)

    def _inflate(self, archive_file, compressed_size, output_limit, policy, sha, writer):
        decompressor = zlib.decompressobj(-15)
        remaining = compressed_size
        size = 0
        crc = 0

        while remaining:
            take = min(CHUNK_SIZE, remaining)
            chunk = archive_file.read(take)
            if len(chunk) != take:
                raise ExtractionError("truncated member data")
            remaining -= take
            if decompressor.eof:
                raise ExtractionError("compressed member data exceeds declared size")

            tail = chunk
            while tail:
                room = min(OUTPUT_CHUNK, output_limit - size + 1)
                if room <= 0:
                    raise ExtractionError("actual member size exceeds declared size")
                try:
                    output = decompressor.decompress(tail, room)
                except zlib.error as error:
                    raise ExtractionError("corrupt deflated member data") from error
                if output:
                    size += len(output)
                    if size > output_limit:
                        raise ExtractionError("actual member size exceeds declared size")
                    self._check_ratio(size, compressed_size, policy)
                    crc = zlib.crc32(output, crc)
                    sha.update(output)
                    if writer is not None:
                        writer(output)
                if decompressor.eof:
                    if decompressor.unused_data or decompressor.unconsumed_tail:
                        raise ExtractionError("compressed member data exceeds declared size")
                    tail = b""
                    break
                unconsumed = decompressor.unconsumed_tail
                if not unconsumed:
                    break
                if unconsumed == tail and not output:
                    raise ExtractionError("invalid deflated member stream")
                tail = unconsumed

            if decompressor.eof and remaining:
                raise ExtractionError("compressed member data exceeds declared size")

        if not decompressor.eof:
            raise ExtractionError("truncated or size-mismatched deflated member")
        try:
            tail_output = decompressor.flush()
        except zlib.error as error:
            raise ExtractionError("corrupt deflated member data") from error
        if tail_output:
            size += len(tail_output)
            if size > output_limit:
                raise ExtractionError("actual member size exceeds declared size")
            crc = zlib.crc32(tail_output, crc)
            sha.update(tail_output)
            if writer is not None:
                writer(tail_output)
        return size, crc

    def _check_ratio(self, size, compressed_size, policy):
        if size == 0:
            return
        if compressed_size <= 0:
            raise ExtractionError("member compression ratio exceeds max_ratio")
        if size > policy["max_ratio"] * compressed_size:
            raise ExtractionError("member compression ratio exceeds max_ratio")

    def _check_local_header(self, archive_file, plan, archive_size, start_dir):
        info = plan.info
        offset = info.header_offset
        if not isinstance(offset, int) or offset < 0 or offset >= start_dir:
            raise ExtractionError("invalid local header offset")
        archive_file.seek(offset)
        header = archive_file.read(LOCAL_HEADER.size)
        if len(header) != LOCAL_HEADER.size:
            raise ExtractionError("truncated local header")
        (
            signature,
            _version,
            flags,
            method,
            _mtime,
            _mdate,
            crc,
            local_compressed,
            local_uncompressed,
            name_length,
            extra_length,
        ) = LOCAL_HEADER.unpack(header)
        if signature != b"PK\x03\x04":
            raise ExtractionError("invalid local header signature")
        if flags & 0x0001 or flags & 0x0020 or flags & 0x0040:
            raise ExtractionError("encrypted or patched local header")
        if method != info.compress_type:
            raise ExtractionError("local and central compression methods differ")
        key_flags = 0x0001 | 0x0008 | 0x0020 | 0x0040 | 0x0800
        if (flags & key_flags) != (info.flag_bits & key_flags):
            raise ExtractionError("local and central flags differ")

        name_bytes = archive_file.read(name_length)
        if len(name_bytes) != name_length:
            raise ExtractionError("truncated local member name")
        extra = archive_file.read(extra_length)
        if len(extra) != extra_length:
            raise ExtractionError("truncated local member extra data")
        try:
            if flags & 0x0800:
                local_name = name_bytes.decode("utf-8")
            else:
                local_name = name_bytes.decode("cp437")
        except UnicodeDecodeError as error:
            raise ExtractionError("invalid local member name encoding") from error
        if local_name != info.orig_filename:
            raise ExtractionError("local and central member names differ")

        data_start = offset + LOCAL_HEADER.size + name_length + extra_length
        data_end = data_start + info.compress_size
        if data_start > start_dir or data_end > start_dir or data_end > archive_size:
            raise ExtractionError("member data overlaps the central directory or archive end")

        resolved_uncompressed, resolved_compressed = self._local_sizes(
            extra, local_uncompressed, local_compressed
        )
        descriptor = bool(flags & 0x0008)
        if not descriptor:
            if resolved_uncompressed != info.file_size:
                raise ExtractionError("local and central sizes differ")
            if resolved_compressed != info.compress_size:
                raise ExtractionError("local and central sizes differ")
            if (crc & 0xFFFFFFFF) != info.CRC:
                raise ExtractionError("local and central CRCs differ")
        else:
            if resolved_uncompressed not in (0, info.file_size):
                raise ExtractionError("local and central sizes differ")
            if resolved_compressed not in (0, info.compress_size):
                raise ExtractionError("local and central sizes differ")
            if crc and (crc & 0xFFFFFFFF) != info.CRC:
                raise ExtractionError("local and central CRCs differ")

        return data_start, data_end

    @staticmethod
    def _local_sizes(extra, uncompressed, compressed):
        needs_uncompressed = uncompressed == 0xFFFFFFFF
        needs_compressed = compressed == 0xFFFFFFFF
        if not needs_uncompressed and not needs_compressed:
            return uncompressed, compressed

        position = 0
        zip64 = None
        while position + 4 <= len(extra):
            tag, length = struct.unpack_from("<HH", extra, position)
            position += 4
            if position + length > len(extra):
                raise ExtractionError("truncated local extra field")
            if tag == ZIP64_EXTRA_ID:
                zip64 = extra[position : position + length]
            position += length
        if position != len(extra):
            raise ExtractionError("truncated local extra field")
        if zip64 is None:
            raise ExtractionError("missing ZIP64 local sizes")
        if len(zip64) % 8:
            raise ExtractionError("invalid ZIP64 local sizes")

        values = []
        for index in range(0, len(zip64) - 7, 8):
            values.append(struct.unpack_from("<Q", zip64, index)[0])
        used = 0
        if needs_uncompressed:
            if used >= len(values):
                raise ExtractionError("missing ZIP64 uncompressed size")
            uncompressed = values[used]
            used += 1
        if needs_compressed:
            if used >= len(values):
                raise ExtractionError("missing ZIP64 compressed size")
            compressed = values[used]
        return uncompressed, compressed

    def _materialize(
        self,
        archive_file,
        archive_size,
        start_dir,
        plans,
        stage,
        policy,
        fault,
        stage_identity,
    ):
        for plan in plans:
            _check_stage(stage, stage_identity)
            if plan.is_dir:
                self._make_directory(stage, plan.parts)
                result = self._consume(
                    archive_file,
                    plan,
                    archive_size,
                    start_dir,
                    policy,
                    None,
                )
                if result.size != 0:
                    raise ExtractionError("directory member carries data")
            else:
                self._ensure_directory_path(stage, plan.parts[:-1])
                file_path = stage.joinpath(*plan.parts)
                no_follow = getattr(os, "O_NOFOLLOW", 0)
                descriptor = os.open(
                    file_path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | no_follow,
                    0o640,
                )
                try:
                    with os.fdopen(descriptor, "wb") as output:
                        result = self._consume(
                            archive_file,
                            plan,
                            archive_size,
                            start_dir,
                            policy,
                            output.write,
                        )
                    _set_mode(file_path, 0o640)
                except Exception:
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
                    raise
                if result.size != plan.actual_size or result.sha256 != plan.sha256:
                    raise ExtractionError("archive changed during extraction")
            _inject(fault, "after_member")

    def _make_directory(self, stage, parts):
        self._ensure_directory_path(stage, parts)

    def _ensure_directory_path(self, stage, parts):
        current = stage
        for part in parts:
            current = current / part
            existing = _lstat(current)
            if existing is None:
                os.mkdir(current, 0o750)
                _set_mode(current, 0o750)
            else:
                if _is_reparse(existing) or not stat.S_ISDIR(existing.st_mode):
                    raise ExtractionError("member path traverses a non-directory")
                _set_mode(current, 0o750)

    def _write_manifest(self, stage, files):
        payload = json.dumps(
            {"files": files},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8") + b"\n"
        path = stage / "MANIFEST.json"
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
        try:
            with os.fdopen(descriptor, "wb") as output:
                output.write(payload)
                output.flush()
                os.fsync(output.fileno())
            _set_mode(path, 0o640)
        except Exception:
            try:
                os.close(descriptor)
            except OSError:
                pass
            raise
