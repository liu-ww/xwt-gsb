"""CDC Forge v2 deterministic offline CDC materializer.

The SQLite database is the durable source of truth.  The ``current``
directory is a deterministic projection of the committed database state and
is republished atomically whenever a batch transaction commits.
"""

from __future__ import annotations

import contextlib
import datetime
import hashlib
import inspect
import json
import math
import os
import re
import shutil
import sqlite3
import threading
import unicodedata
import uuid
from pathlib import Path

BATCH_FILE_NAMES = ('manifest.json', 'schema.json', 'events.jsonl')
MANIFEST_FIELDS = frozenset({
    'format', 'batch_id', 'source', 'previous_batch', 'schema_version',
    'schema_sha256', 'events_sha256', 'event_count', 'watermark',
})
SCHEMA_FIELDS = frozenset({'version', 'fields'})
FIELD_FIELDS = frozenset({'name', 'type', 'nullable', 'default'})
EVENT_FIELDS = frozenset({
    'op', 'pk', 'event_time', 'source_seq', 'schema_version', 'data',
})
FIELD_NAME_RE = re.compile(r'[a-z][a-z0-9_]{0,31}\Z')
HEX_RE = re.compile(r'[0-9a-f]{64}\Z')
SCHEMA_TYPES = frozenset({'string', 'int', 'float', 'bool'})
WATERMARK_WINDOW = 86400
MAX_PART_ROWS = 2


def _canon(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'))


def _canon_bytes(value):
    return _canon(value).encode('utf-8')


class _JsonInputError(ValueError):
    pass


def _reject_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise _JsonInputError(f'duplicate object key {key!r}')
        result[key] = value
    return result


def _reject_constant(token):
    raise _JsonInputError(f'non-finite number {token!r} is not allowed')


def _finite_float(token):
    value = float(token)
    if not math.isfinite(value):
        raise _JsonInputError(f'non-finite number {token!r} is not allowed')
    return value


def _loads_canonical(raw, label):
    try:
        text = raw.decode('utf-8')
    except UnicodeDecodeError as exc:
        raise ValueError(f'{label}: invalid UTF-8 ({exc.reason})') from exc
    try:
        value = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_constant,
            parse_float=_finite_float,
        )
    except _JsonInputError as exc:
        raise ValueError(f'{label}: {exc}') from exc
    except ValueError as exc:
        raise ValueError(f'{label}: invalid JSON') from exc
    try:
        canonical = _canon_bytes(value)
    except (UnicodeError, TypeError) as exc:
        raise ValueError(f'{label}: JSON contains invalid Unicode') from exc
    if canonical != raw:
        raise ValueError(f'{label}: JSON is not canonical')
    return value


def _check_text(value, label, allow_empty=False):
    if type(value) is not str:
        raise ValueError(f'{label} must be a string')
    if not allow_empty and value == '':
        raise ValueError(f'{label} must not be empty')
    try:
        normalized = unicodedata.normalize('NFC', value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f'{label} contains invalid Unicode') from exc
    if normalized != value:
        raise ValueError(f'{label} must be NFC-normalized')
    for character in value:
        code = ord(character)
        if 0xD800 <= code <= 0xDFFF or unicodedata.category(character) in ('Cc', 'Cs'):
            raise ValueError(f'{label} contains a forbidden character')
    return value


def _check_value(value, type_name, nullable, label):
    if value is None:
        if nullable:
            return
        raise ValueError(f'{label} must not be null')
    if type_name == 'string':
        if type(value) is not str:
            raise ValueError(f'{label} must be a string')
        _check_text(value, label, allow_empty=True)
    elif type_name == 'int':
        if type(value) is not int:
            raise ValueError(f'{label} must be an integer')
    elif type_name == 'float':
        if type(value) is not float or not math.isfinite(value):
            raise ValueError(f'{label} must be a finite float')
    elif type_name == 'bool':
        if type(value) is not bool:
            raise ValueError(f'{label} must be a boolean')
    else:
        raise ValueError(f'{label} has an unsupported schema type')


def _parse_manifest(raw):
    value = _loads_canonical(raw, 'manifest.json')
    if type(value) is not dict or set(value) != MANIFEST_FIELDS:
        raise ValueError('manifest.json: unexpected or missing fields')
    if type(value['format']) is not int or value['format'] != 1:
        raise ValueError('manifest.json: format must be the integer 1')
    _check_text(value['batch_id'], 'manifest.json: batch_id')
    _check_text(value['source'], 'manifest.json: source')
    if value['previous_batch'] is not None:
        _check_text(value['previous_batch'], 'manifest.json: previous_batch')
    for key in ('schema_version', 'event_count', 'watermark'):
        if type(value[key]) is not int:
            raise ValueError(f'manifest.json: {key} must be an integer')
    if value['schema_version'] < 1:
        raise ValueError('manifest.json: schema_version must be positive')
    if value['event_count'] < 0:
        raise ValueError('manifest.json: event_count must not be negative')
    for key in ('schema_sha256', 'events_sha256'):
        digest = value[key]
        if type(digest) is not str or HEX_RE.fullmatch(digest) is None:
            raise ValueError(f'manifest.json: {key} must be a lowercase SHA-256 digest')
    return value


def _parse_schema(raw):
    value = _loads_canonical(raw, 'schema.json')
    if type(value) is not dict or set(value) != SCHEMA_FIELDS:
        raise ValueError('schema.json: unexpected or missing fields')
    if type(value['version']) is not int or value['version'] < 1:
        raise ValueError('schema.json: version must be a positive integer')
    fields = value['fields']
    if type(fields) is not list:
        raise ValueError('schema.json: fields must be an array')
    seen = set()
    for index, field in enumerate(fields):
        label = f'schema.json: field {index}'
        if type(field) is not dict or set(field) != FIELD_FIELDS:
            raise ValueError(f'{label}: unexpected or missing fields')
        name = field['name']
        if type(name) is not str or FIELD_NAME_RE.fullmatch(name) is None:
            raise ValueError(f'{label}: invalid field name')
        if name in seen:
            raise ValueError(f'{label}: duplicate field name {name!r}')
        seen.add(name)
        type_name = field['type']
        if type(type_name) is not str or type_name not in SCHEMA_TYPES:
            raise ValueError(f'{label}: invalid field type')
        if type(field['nullable']) is not bool:
            raise ValueError(f'{label}: nullable must be a boolean')
        if field['default'] is not None:
            _check_value(field['default'], type_name, True, f'{label} default')
    return value


def _validate_upsert_data(data, fields, label):
    if type(data) is not dict:
        raise ValueError(f'{label}: data must be an object')
    expected = {field['name'] for field in fields}
    if set(data) != expected:
        raise ValueError(f'{label}: data fields do not exactly match the schema')
    by_name = {field['name']: field for field in fields}
    for name, value in data.items():
        field = by_name[name]
        _check_value(value, field['type'], field['nullable'], f'{label}.{name}')


def _parse_events(raw, manifest, schema):
    if raw in (b'', b'\n'):
        if manifest['event_count'] != 0:
            raise ValueError('events.jsonl: event_count does not match content')
        lines = []
    else:
        if b'\r' in raw:
            raise ValueError('events.jsonl: only LF line endings are allowed')
        if not raw.endswith(b'\n') or raw.endswith(b'\n\n'):
            raise ValueError('events.jsonl: file must end with exactly one newline')
        lines = raw[:-1].split(b'\n')
        if any(line == b'' for line in lines):
            raise ValueError('events.jsonl: blank lines are not allowed')
    if len(lines) != manifest['event_count']:
        raise ValueError('events.jsonl: event_count does not match content')
    events = []
    for number, line in enumerate(lines, 1):
        label = f'events.jsonl: line {number}'
        event = _loads_canonical(line, label)
        if type(event) is not dict or set(event) != EVENT_FIELDS:
            raise ValueError(f'{label}: unexpected or missing fields')
        if type(event['op']) is not str or event['op'] not in ('upsert', 'delete'):
            raise ValueError(f'{label}: op must be upsert or delete')
        _check_text(event['pk'], f'{label}: pk')
        if type(event['event_time']) is not int:
            raise ValueError(f'{label}: event_time must be an integer')
        if type(event['source_seq']) is not int or event['source_seq'] <= 0:
            raise ValueError(f'{label}: source_seq must be a positive integer')
        if type(event['schema_version']) is not int or event['schema_version'] < 1:
            raise ValueError(f'{label}: schema_version must be a positive integer')
        if event['schema_version'] > manifest['schema_version']:
            raise ValueError(f'{label}: schema_version is newer than the batch schema')
        if event['op'] == 'delete':
            if event['data'] is not None:
                raise ValueError(f'{label}: delete event data must be null')
        else:
            if event['schema_version'] == manifest['schema_version']:
                _validate_upsert_data(event['data'], schema['fields'], label)
        events.append(event)
    return events


def _batch_digest(manifest_raw, schema_raw, events_raw):
    digest = hashlib.sha256()
    for name, raw in (
        ('manifest.json', manifest_raw),
        ('schema.json', schema_raw),
        ('events.jsonl', events_raw),
    ):
        digest.update(name.encode('ascii'))
        digest.update(b'\x00')
        digest.update(raw)
        digest.update(b'\x00')
    return digest.hexdigest()


class Warehouse:
    """Offline CDC warehouse that materializes deterministic snapshots."""

    _TABLE_COLUMNS = {
        'meta': (('key', 'TEXT'), ('value', 'TEXT')),
        'sources': (('source', 'TEXT'), ('head_batch', 'TEXT'), ('watermark', 'INTEGER')),
        'batches': (('batch_id', 'TEXT'), ('source', 'TEXT'),
                    ('digest', 'TEXT'), ('generation', 'INTEGER')),
        'schema_history': (('version', 'INTEGER'), ('schema_json', 'TEXT'),
                           ('sha256', 'TEXT')),
        'events': (('source', 'TEXT'), ('source_seq', 'INTEGER'),
                   ('event_json', 'TEXT')),
    }
    _TABLE_PRIMARY_KEYS = {
        'meta': ('key',),
        'sources': ('source',),
        'batches': ('batch_id',),
        'schema_history': ('version',),
        'events': ('source', 'source_seq'),
    }
    _TABLE_NOT_NULL = {
        'meta': {'value'},
        'sources': {'watermark'},
        'batches': {'source', 'digest', 'generation'},
        'schema_history': {'schema_json', 'sha256'},
        'events': {'event_json'},
    }

    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.db = None
        try:
            self.db = sqlite3.connect(
                str(self.root / 'state.sqlite'),
                timeout=30,
                isolation_level=None,
                check_same_thread=False,
            )
            self.db.execute('PRAGMA busy_timeout=30000')
            self._initialize_or_validate()
            self._recover()
        except ValueError:
            if self.db is not None:
                with contextlib.suppress(Exception):
                    self.db.close()
                self.db = None
            raise
        except sqlite3.Error as exc:
            if self.db is not None:
                with contextlib.suppress(Exception):
                    self.db.close()
                self.db = None
            raise ValueError(f'state database is invalid: {exc}') from exc
        except Exception:
            if self.db is not None:
                with contextlib.suppress(Exception):
                    self.db.close()
                self.db = None
            raise

    def close(self):
        with self._lock:
            if self.db is not None:
                self.db.close()
                self.db = None

    # -- database lifecycle -------------------------------------------------

    def _initialize_or_validate(self):
        active = False
        try:
            self.db.execute('BEGIN IMMEDIATE')
            active = True
            user_version = int(self.db.execute('PRAGMA user_version').fetchone()[0])
            tables = self._table_names()
            expected_tables = set(self._TABLE_COLUMNS)
            if user_version == 0 and not tables:
                self._create_tables()
                self.db.execute('PRAGMA user_version=2')
                self.db.execute("INSERT INTO meta(key,value) VALUES('generation','0')")
            elif user_version == 2 and tables == expected_tables:
                self._validate_table_structure()
                row = self.db.execute(
                    "SELECT value FROM meta WHERE key='generation'"
                ).fetchone()
                if row is None:
                    raise ValueError('state database is missing generation metadata')
                try:
                    int(row[0])
                except ValueError as exc:
                    raise ValueError('state database generation metadata is invalid') from exc
            elif user_version > 2:
                raise ValueError(f'unsupported state database version {user_version}')
            else:
                raise ValueError('state database structure is invalid')
            self.db.execute('COMMIT')
            active = False
        except Exception:
            if active:
                with contextlib.suppress(Exception):
                    self.db.execute('ROLLBACK')
            raise

    def _table_names(self):
        rows = self.db.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        return {row[0] for row in rows}

    def _create_tables(self):
        self.db.execute('CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)')
        self.db.execute(
            'CREATE TABLE sources(source TEXT PRIMARY KEY, head_batch TEXT, '
            'watermark INTEGER NOT NULL)'
        )
        self.db.execute(
            'CREATE TABLE batches(batch_id TEXT PRIMARY KEY, source TEXT NOT NULL, '
            'digest TEXT NOT NULL, generation INTEGER NOT NULL)'
        )
        self.db.execute(
            'CREATE TABLE schema_history(version INTEGER PRIMARY KEY, '
            'schema_json TEXT NOT NULL, sha256 TEXT NOT NULL)'
        )
        self.db.execute(
            'CREATE TABLE events(source TEXT NOT NULL, source_seq INTEGER NOT NULL, '
            'event_json TEXT NOT NULL, PRIMARY KEY(source, source_seq))'
        )

    def _validate_table_structure(self):
        for table, expected in self._TABLE_COLUMNS.items():
            info = self.db.execute(f'PRAGMA table_info("{table}")').fetchall()
            actual = tuple((row[1], (row[2] or '').upper()) for row in info)
            wanted = tuple((name, type_name.upper()) for name, type_name in expected)
            if actual != wanted:
                raise ValueError(f'state database table {table!r} has an invalid structure')
            primary = tuple(
                row[1] for row in sorted(
                    (row for row in info if row[5] > 0),
                    key=lambda row: row[5],
                )
            )
            if primary != self._TABLE_PRIMARY_KEYS[table]:
                raise ValueError(f'state database table {table!r} has an invalid primary key')
            required = self._TABLE_NOT_NULL[table]
            for row in info:
                name = row[1]
                if name in self._TABLE_PRIMARY_KEYS[table]:
                    continue
                if bool(row[3]) != (name in required):
                    raise ValueError(f'state database table {table!r} has an invalid nullability')

    def _cleanup_temporary(self):
        try:
            entries = list(self.root.iterdir())
        except OSError:
            return
        for entry in entries:
            if entry.name.startswith('.stage-') or entry.name.startswith('.backup-'):
                with contextlib.suppress(Exception):
                    self._remove_path(entry)

    def _remove_path(self, path):
        path = Path(path)
        try:
            if path.is_dir() and not path.is_symlink():
                shutil.rmtree(path)
            else:
                path.unlink()
        except FileNotFoundError:
            pass

    def _recover(self):
        with self._lock:
            active = False
            try:
                self.db.execute('BEGIN IMMEDIATE')
                active = True
                self._cleanup_temporary()
                generation = self._generation()
                snapshot = self._build_snapshot()
                current = self.root / 'current'
                if generation == 0 and snapshot is None:
                    if current.exists():
                        self._remove_path(current)
                else:
                    if snapshot is None:
                        raise ValueError(
                            'state database has no schema for a non-zero generation'
                        )
                    if not self._current_matches(snapshot):
                        self._write_snapshot_now(snapshot)
                self.db.execute('COMMIT')
                active = False
            except Exception:
                if active:
                    with contextlib.suppress(Exception):
                        self.db.execute('ROLLBACK')
                raise

    # -- public API ---------------------------------------------------------

    def apply_batch(self, batch, fault=None):
        return self._apply([batch], fault)[0]

    def apply_batches(self, batches, fault=None):
        return self._apply(list(batches), fault)

    def query(self):
        with self._lock:
            _, _, rows = self._materialize()
        return rows

    # -- batch parsing ------------------------------------------------------

    def _parse_batch(self, path):
        path = Path(path)
        if path.is_symlink():
            raise ValueError('batch path must not be a symbolic link')
        if not path.is_dir():
            raise ValueError('batch path is not a directory')
        try:
            entries = list(path.iterdir())
        except OSError as exc:
            raise ValueError(f'cannot read batch directory: {exc}') from exc
        if {entry.name for entry in entries} != set(BATCH_FILE_NAMES):
            raise ValueError(
                'batch directory must contain exactly manifest.json, '
                'schema.json and events.jsonl'
            )
        for entry in entries:
            if entry.is_symlink() or not entry.is_file():
                raise ValueError('batch entries must be regular files')
        try:
            manifest_raw = (path / 'manifest.json').read_bytes()
            schema_raw = (path / 'schema.json').read_bytes()
            events_raw = (path / 'events.jsonl').read_bytes()
        except OSError as exc:
            raise ValueError(f'cannot read batch files: {exc}') from exc
        manifest = _parse_manifest(manifest_raw)
        schema = _parse_schema(schema_raw)
        if manifest['schema_version'] != schema['version']:
            raise ValueError('manifest schema_version does not match schema.json')
        if hashlib.sha256(schema_raw).hexdigest() != manifest['schema_sha256']:
            raise ValueError('manifest schema_sha256 does not match schema.json')
        if hashlib.sha256(events_raw).hexdigest() != manifest['events_sha256']:
            raise ValueError('manifest events_sha256 does not match events.jsonl')
        events = _parse_events(events_raw, manifest, schema)
        return {
            'path': path,
            'manifest': manifest,
            'schema': schema,
            'schema_raw': schema_raw,
            'events': events,
            'digest': _batch_digest(manifest_raw, schema_raw, events_raw),
        }

    # -- transaction --------------------------------------------------------

    def _apply(self, paths, fault):
        parsed = [self._parse_batch(path) for path in paths]
        with self._lock:
            results = []
            stage = None
            backup = None
            published = False
            active = False
            try:
                self.db.execute('BEGIN IMMEDIATE')
                active = True
                state = self._load_state()
                any_applied = False
                for item in parsed:
                    batch_id = item['manifest']['batch_id']
                    existing = self.db.execute(
                        'SELECT digest,generation FROM batches WHERE batch_id=?',
                        (batch_id,),
                    ).fetchone()
                    if existing is not None:
                        if existing[0] != item['digest']:
                            raise ValueError(
                                f'batch {batch_id!r}: batch_id already bound to '
                                'different content'
                            )
                        results.append({
                            'batch_id': batch_id,
                            'generation': int(existing[1]),
                            'applied': False,
                        })
                        continue
                    results.append(self._apply_one(item, state, fault))
                    any_applied = True
                if any_applied:
                    snapshot = self._build_snapshot()
                    if snapshot is None:
                        raise ValueError('cannot build a snapshot for applied batches')
                    stage = self._stage(snapshot)
                    self._trigger_fault(fault, 'after_stage')
                    self._trigger_fault(fault, 'before_publish')
                    backup = self._publish(stage)
                    stage = None
                    published = True
                self.db.execute('COMMIT')
                active = False
                if published:
                    with contextlib.suppress(Exception):
                        self._discard_backup(backup)
                    backup = None
                return results
            except BaseException:
                if active:
                    with contextlib.suppress(Exception):
                        self.db.execute('ROLLBACK')
                if published:
                    with contextlib.suppress(Exception):
                        self._rollback_publish(backup)
                if stage is not None:
                    with contextlib.suppress(Exception):
                        self._remove_path(stage)
                raise

    def _apply_one(self, item, state, fault):
        manifest = item['manifest']
        source = manifest['source']
        batch_id = manifest['batch_id']
        watermark = manifest['watermark']
        version = manifest['schema_version']
        schema_raw = item['schema_raw']
        schema = item['schema']
        schemas = state['schemas']
        if version in schemas:
            if schemas[version][0] != schema_raw:
                raise ValueError(
                    f'schema version {version} bytes differ from the stored schema'
                )
        else:
            maximum = max(schemas) if schemas else 0
            if version != maximum + 1:
                raise ValueError('schema versions must increase by exactly one')
            if maximum == 0:
                if version != 1:
                    raise ValueError('the first schema version must be 1')
            else:
                old_fields = schemas[maximum][1]['fields']
                new_fields = schema['fields']
                if len(new_fields) <= len(old_fields):
                    raise ValueError('a schema upgrade must append at least one field')
                if new_fields[:len(old_fields)] != old_fields:
                    raise ValueError(
                        'a schema upgrade must preserve existing fields exactly'
                    )
                for field in new_fields[len(old_fields):]:
                    if not field['nullable'] and field['default'] is None:
                        raise ValueError(
                            'new schema fields must be nullable or have a non-null default'
                        )
            self._insert_schema(version, schema_raw, schema)
            schemas[version] = (schema_raw, schema)
            self._trigger_fault(fault, 'after_state')

        for event in item['events']:
            event_schema = schemas.get(event['schema_version'])
            if event_schema is None:
                raise ValueError(
                    'event schema_version is not present in schema history'
                )
            if event['op'] == 'upsert':
                _validate_upsert_data(
                    event['data'], event_schema[1]['fields'],
                    f"event source_seq {event['source_seq']}",
                )

        previous = manifest['previous_batch']
        sources = state['sources']
        if source in sources:
            head, stored_watermark = sources[source]
            if previous != head:
                raise ValueError('previous_batch does not match the source head')
            if watermark < stored_watermark:
                raise ValueError('watermark must not regress')
        else:
            if previous is not None:
                raise ValueError('the first batch for a source must have previous_batch null')

        lower_bound = watermark - WATERMARK_WINDOW
        for event in item['events']:
            if not (lower_bound <= event['event_time'] <= watermark):
                raise ValueError('event_time is outside the watermark window')

        for event in item['events']:
            canonical = _canon(event)
            existing = self.db.execute(
                'SELECT event_json FROM events WHERE source=? AND source_seq=?',
                (source, event['source_seq']),
            ).fetchone()
            if existing is not None:
                if existing[0] != canonical:
                    raise ValueError(
                        'source_seq is already bound to different event content'
                    )
                continue
            self.db.execute(
                'INSERT INTO events(source,source_seq,event_json) VALUES(?,?,?)',
                (source, event['source_seq'], canonical),
            )
            self._trigger_fault(fault, 'after_state')

        self.db.execute(
            'INSERT OR REPLACE INTO sources(source,head_batch,watermark) VALUES(?,?,?)',
            (source, batch_id, watermark),
        )
        self._trigger_fault(fault, 'after_state')

        new_generation = state['generation'] + 1
        self.db.execute(
            'INSERT INTO batches(batch_id,source,digest,generation) VALUES(?,?,?,?)',
            (batch_id, source, item['digest'], new_generation),
        )
        self._trigger_fault(fault, 'after_state')
        self.db.execute(
            "UPDATE meta SET value=? WHERE key='generation'",
            (str(new_generation),),
        )
        self._trigger_fault(fault, 'after_state')

        state['generation'] = new_generation
        state['sources'][source] = (batch_id, watermark)
        return {
            'batch_id': batch_id,
            'generation': new_generation,
            'applied': True,
        }

    def _trigger_fault(self, fault, stage):
        if fault is None:
            return
        if isinstance(fault, str):
            if fault == stage:
                raise RuntimeError(f'injected fault at {stage}')
            return
        if isinstance(fault, dict):
            if stage in fault:
                value = fault[stage]
                if isinstance(value, BaseException):
                    raise value
                if value:
                    raise RuntimeError(f'injected fault at {stage}')
            return
        if isinstance(fault, (set, frozenset, list, tuple)):
            if stage in fault:
                raise RuntimeError(f'injected fault at {stage}')
            return
        if not callable(fault):
            raise TypeError('fault must be callable, a stage name, or a stage collection')
        try:
            parameters = inspect.signature(fault).parameters.values()
        except (TypeError, ValueError):
            fault(stage)
            return
        positional = [
            parameter for parameter in parameters
            if parameter.kind in (parameter.POSITIONAL_ONLY,
                                  parameter.POSITIONAL_OR_KEYWORD)
        ]
        has_varargs = any(
            parameter.kind == parameter.VAR_POSITIONAL
            for parameter in parameters
        )
        if positional or has_varargs:
            fault(stage)
        else:
            fault()

    # -- state helpers ------------------------------------------------------

    def _generation(self):
        row = self.db.execute(
            "SELECT value FROM meta WHERE key='generation'"
        ).fetchone()
        if row is None:
            raise ValueError('state database is missing generation metadata')
        try:
            generation = int(row[0])
        except ValueError as exc:
            raise ValueError('state database generation metadata is invalid') from exc
        if generation < 0:
            raise ValueError('state database generation metadata is invalid')
        return generation

    def _load_state(self):
        return {
            'generation': self._generation(),
            'schemas': self._load_schemas(),
            'sources': self._load_sources(),
        }

    def _load_schemas(self):
        schemas = {}
        for version, text in self.db.execute(
            'SELECT version,schema_json FROM schema_history ORDER BY version'
        ):
            try:
                schema = json.loads(text)
            except ValueError as exc:
                raise ValueError('stored schema is invalid') from exc
            schemas[int(version)] = (text.encode('utf-8'), schema)
        return schemas

    def _load_sources(self):
        return {
            source: (head, int(watermark))
            for source, head, watermark in self.db.execute(
                'SELECT source,head_batch,watermark FROM sources'
            )
        }

    def _insert_schema(self, version, raw, schema):
        text = raw.decode('utf-8')
        self.db.execute(
            'INSERT INTO schema_history(version,schema_json,sha256) VALUES(?,?,?)',
            (version, text, hashlib.sha256(raw).hexdigest()),
        )


    # -- materialization ----------------------------------------------------

    def _materialize(self):
        schema_row = self.db.execute(
            'SELECT version,schema_json FROM schema_history '
            'ORDER BY version DESC LIMIT 1'
        ).fetchone()
        winners = {}
        for source, source_seq, text in self.db.execute(
            'SELECT source,source_seq,event_json FROM events'
        ):
            try:
                event = json.loads(text)
            except ValueError as exc:
                raise ValueError('stored event is invalid') from exc
            pk = event['pk']
            key = (event['event_time'], source, event['source_seq'])
            current = winners.get(pk)
            if current is None or key > current[0]:
                winners[pk] = (key, event)
        if schema_row is None:
            return None, None, []
        schema_text = schema_row[1]
        schema_bytes = schema_text.encode('utf-8')
        try:
            schema = json.loads(schema_text)
        except ValueError as exc:
            raise ValueError('stored schema is invalid') from exc
        fields = schema['fields']
        rows = []
        for pk, (key, event) in winners.items():
            if event['op'] != 'upsert':
                continue
            row = {
                '_pk': pk,
                '_event_time': key[0],
                '_source': key[1],
                '_source_seq': key[2],
            }
            data = event['data']
            for field in fields:
                name = field['name']
                if name in data:
                    row[name] = data[name]
                else:
                    row[name] = field['default']
            rows.append(row)
        rows.sort(key=lambda value: value['_pk'])
        return schema, schema_bytes, rows

    def _build_snapshot(self):
        generation = self._generation()
        schema, schema_bytes, rows = self._materialize()
        if schema is None:
            return None
        groups = {}
        for row in rows:
            day = self._utc_date(row['_event_time'])
            groups.setdefault(day, []).append(row)
        parts = {}
        partitions = []
        for day in sorted(groups):
            day_rows = groups[day]
            for index in range(0, len(day_rows), MAX_PART_ROWS):
                chunk = day_rows[index:index + MAX_PART_ROWS]
                path = f'date={day}/part-{index // MAX_PART_ROWS:05d}.jsonl'
                raw = ''.join(_canon(row) + '\n' for row in chunk).encode('utf-8')
                parts[path] = raw
                partitions.append({
                    'path': path,
                    'rows': len(chunk),
                    'sha256': hashlib.sha256(raw).hexdigest(),
                })
        partitions.sort(key=lambda part: part['path'])
        manifest = {
            'generation': generation,
            'schema_version': schema['version'],
            'schema_sha256': hashlib.sha256(schema_bytes).hexdigest(),
            'row_count': len(rows),
            'partitions': partitions,
        }
        return {'manifest': manifest, 'parts': parts}

    def _utc_date(self, value):
        try:
            moment = (
                datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
                + datetime.timedelta(seconds=value)
            )
        except (OverflowError, ValueError) as exc:
            raise ValueError('event_time is outside the supported UTC range') from exc
        return moment.date().isoformat()

    # -- snapshot files -----------------------------------------------------

    def _stage(self, snapshot):
        stage = self.root / f'.stage-{uuid.uuid4().hex}'
        stage.mkdir()
        try:
            (stage / 'manifest.json').write_bytes(
                _canon_bytes(snapshot['manifest'])
            )
            for relative, raw in snapshot['parts'].items():
                target = stage / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(raw)
        except Exception:
            with contextlib.suppress(Exception):
                self._remove_path(stage)
            raise
        return stage

    def _publish(self, stage):
        current = self.root / 'current'
        backup = None
        if current.exists():
            backup = self.root / f'.backup-{uuid.uuid4().hex}'
            os.rename(current, backup)
        try:
            os.rename(stage, current)
        except Exception:
            if backup is not None and not current.exists():
                with contextlib.suppress(Exception):
                    os.rename(backup, current)
            raise
        return backup

    def _rollback_publish(self, backup):
        current = self.root / 'current'
        if current.exists():
            self._remove_path(current)
        if backup is not None and backup.exists():
            os.rename(backup, current)

    def _discard_backup(self, backup):
        if backup is not None and backup.exists():
            self._remove_path(backup)

    def _write_snapshot_now(self, snapshot):
        stage = self._stage(snapshot)
        try:
            backup = self._publish(stage)
        except Exception:
            with contextlib.suppress(Exception):
                self._remove_path(stage)
            raise
        with contextlib.suppress(Exception):
            self._discard_backup(backup)

    def _current_matches(self, snapshot):
        current = self.root / 'current'
        if snapshot is None:
            return not current.exists()
        if not current.is_dir() or current.is_symlink():
            return False
        expected_files = {
            'manifest.json': _canon_bytes(snapshot['manifest']),
        }
        expected_files.update(snapshot['parts'])
        expected_dirs = set()
        for relative in snapshot['parts']:
            parent = Path(relative).parent.as_posix()
            if parent not in ('', '.'):
                expected_dirs.add(parent)
        actual_files = set()
        actual_dirs = set()
        try:
            for dirpath, dirnames, filenames in os.walk(current, followlinks=False):
                base = Path(dirpath)
                for name in dirnames:
                    candidate = base / name
                    if candidate.is_symlink():
                        return False
                    actual_dirs.add(candidate.relative_to(current).as_posix())
                for name in filenames:
                    candidate = base / name
                    if candidate.is_symlink():
                        return False
                    actual_files.add(candidate.relative_to(current).as_posix())
        except OSError:
            return False
        if actual_files != set(expected_files):
            return False
        if actual_dirs != expected_dirs:
            return False
        for relative, raw in expected_files.items():
            try:
                if (current / relative).read_bytes() != raw:
                    return False
            except OSError:
                return False
        return True
