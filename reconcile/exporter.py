"""Recoverable single-file NDJSON change stream export."""
import os
import time


class _FileLock:
    """Cross-process advisory lock backed by a sidecar file."""

    def __init__(self, target_path):
        self.lock_path = os.path.abspath(target_path) + '.lock'
        self.handle = None

    def __enter__(self):
        self.handle = open(self.lock_path, 'a+b')
        deadline = time.monotonic() + 30
        while True:
            try:
                if os.name == 'nt':
                    import msvcrt
                    msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except OSError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)

    def __exit__(self, exc_type, exc, traceback):
        try:
            if os.name == 'nt':
                import msvcrt
                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()


def export_stream(ledger, path, fault):
    from .store import Conflict

    with _FileLock(path):
        frames = ledger.committed_frames()
        lines = {}
        fragment = b''
        if not os.path.exists(path):
            open(path, 'wb').close()
        if os.path.exists(path):
            with open(path, 'rb') as existing:
                data = existing.read()
            if data.startswith(b'\xef\xbb\xbf'):
                raise Conflict('export file must not contain a BOM')
            complete, newline, fragment = data.rpartition(b'\n')
            if newline:
                for index, raw_line in enumerate(complete.split(b'\n'), start=1):
                    lines[index] = raw_line + b'\n'
            elif data:
                fragment = data
        for index in sorted(lines):
            if index > len(frames) or lines[index] != frames[index - 1]:
                raise Conflict('export file is not a committed frame prefix')
        if fragment:
            with open(path, 'r+b') as stream:
                stream.seek(-len(fragment), os.SEEK_END)
                stream.truncate()
                stream.flush()
                os.fsync(stream.fileno())
        appended = 0
        next_index = len(lines) + 1
        if next_index <= len(frames):
            with open(path, 'ab') as stream:
                for index in range(next_index, len(frames) + 1):
                    stream.write(frames[index - 1])
                    stream.flush()
                    os.fsync(stream.fileno())
                    appended += 1
                if fault and appended:
                    fault('after_append')
        return appended
