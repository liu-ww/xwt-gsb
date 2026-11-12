import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import threading
import unittest
import uuid
from pathlib import Path

from cdcforge import Warehouse


def canon(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':')).encode('utf-8')


def field(name, type_name, nullable=False, default=None):
    return {'name': name, 'type': type_name, 'nullable': nullable,
            'default': default}


def schema(version, fields):
    return {'version': version, 'fields': fields}


def upsert(pk, event_time, source_seq, schema_version, data):
    return {'op': 'upsert', 'pk': pk, 'event_time': event_time,
            'source_seq': source_seq, 'schema_version': schema_version,
            'data': data}


def delete(pk, event_time, source_seq, schema_version):
    return {'op': 'delete', 'pk': pk, 'event_time': event_time,
            'source_seq': source_seq, 'schema_version': schema_version,
            'data': None}


class Case(unittest.TestCase):
    def setUp(self):
        self.work = Path(__file__).parent / '.work' / uuid.uuid4().hex
        self.work.mkdir(parents=True)
        self.root = self.work / 'warehouse'

    def tearDown(self):
        shutil.rmtree(self.work, ignore_errors=True)

    def make_batch(self, name, *, batch_id, source, previous_batch, schema_value,
                   events, watermark, manifest_bytes=None, schema_bytes=None,
                   events_bytes=None, extra_file=None):
        directory = self.work / name
        directory.mkdir(parents=True)
        if schema_bytes is None:
            schema_bytes = canon(schema_value)
        if events_bytes is None:
            if events:
                events_bytes = b''.join(canon(event) + b'\n' for event in events)
            else:
                events_bytes = b''
        if manifest_bytes is None:
            manifest = {
                'format': 1,
                'batch_id': batch_id,
                'source': source,
                'previous_batch': previous_batch,
                'schema_version': schema_value['version'],
                'schema_sha256': hashlib.sha256(schema_bytes).hexdigest(),
                'events_sha256': hashlib.sha256(events_bytes).hexdigest(),
                'event_count': len(events) if events_bytes != b'' else 0,
                'watermark': watermark,
            }
            manifest_bytes = canon(manifest)
        (directory / 'manifest.json').write_bytes(manifest_bytes)
        (directory / 'schema.json').write_bytes(schema_bytes)
        (directory / 'events.jsonl').write_bytes(events_bytes)
        if extra_file is not None:
            (directory / extra_file).write_bytes(b'junk')
        return directory

    def open(self):
        return Warehouse(self.root)

    def snapshot(self):
        current = self.root / 'current'
        manifest_raw = (current / 'manifest.json').read_bytes()
        manifest = json.loads(manifest_raw)
        parts = {}
        for item in manifest['partitions']:
            parts[item['path']] = (current / item['path']).read_bytes()
        return manifest, manifest_raw, parts

    def temp_leftovers(self):
        return [entry.name for entry in self.root.iterdir()
                if entry.name.startswith('.stage-') or entry.name.startswith('.backup-')]


class SnapshotTests(Case):
    def test_layout_order_and_hashes(self):
        v1 = schema(1, [field('name', 'string')])
        events = [upsert(chr(ord('a') + index), index + 1, index + 1, 1,
                         {'name': 'v' + str(index)}) for index in range(5)]
        batch = self.make_batch('b1', batch_id='b1', source='s', previous_batch=None,
                                schema_value=v1, events=events, watermark=5)
        warehouse = self.open()
        result = warehouse.apply_batch(batch)
        self.assertTrue(result['applied'])
        self.assertEqual(result['generation'], 1)
        manifest, manifest_raw, parts = self.snapshot()
        self.assertEqual(set(manifest), {'generation', 'schema_version', 'schema_sha256',
                                         'row_count', 'partitions'})
        self.assertEqual(manifest['generation'], 1)
        self.assertEqual(manifest['schema_version'], 1)
        self.assertEqual(manifest['row_count'], 5)
        self.assertEqual([part['path'] for part in manifest['partitions']],
                         ['date=1970-01-01/part-00000.jsonl',
                          'date=1970-01-01/part-00001.jsonl',
                          'date=1970-01-01/part-00002.jsonl'])
        self.assertEqual([part['rows'] for part in manifest['partitions']], [2, 2, 1])
        for part in manifest['partitions']:
            raw = parts[part['path']]
            self.assertEqual(hashlib.sha256(raw).hexdigest(), part['sha256'])
            self.assertTrue(raw.endswith(b'\n'))
            self.assertFalse(raw.endswith(b'\n\n'))
            lines = [json.loads(line) for line in raw.splitlines()]
            self.assertLessEqual(len(lines), 2)
            self.assertEqual(sorted(row['_pk'] for row in lines),
                             [row['_pk'] for row in lines])
        current = self.root / 'current'
        actual_files = {item.relative_to(current).as_posix()
                        for item in current.rglob('*') if item.is_file()}
        self.assertEqual(actual_files, {'manifest.json'} | set(parts))
        self.assertEqual(self.temp_leftovers(), [])
        all_rows = [row for raw in parts.values() for row in raw.splitlines()]
        self.assertEqual(len(all_rows), 5)
        self.assertTrue(all(isinstance(row, bytes) for row in all_rows))
        rows = warehouse.query()
        self.assertEqual([row['_pk'] for row in rows], ['a', 'b', 'c', 'd', 'e'])
        for row in rows:
            self.assertEqual(set(row), {'_pk', '_event_time', '_source',
                                        '_source_seq', 'name'})
        warehouse.close()

    def test_stale_partition_removed(self):
        v1 = schema(1, [field('name', 'string')])
        b1 = self.make_batch(
            'b1', batch_id='b1', source='s', previous_batch=None,
            schema_value=v1,
            events=[upsert('keep', 4000, 1, 1, {'name': 'k'}),
                    upsert('drop', 90000, 2, 1, {'name': 'd'})],
            watermark=90000,
        )
        b2 = self.make_batch(
            'b2', batch_id='b2', source='s', previous_batch='b1',
            schema_value=v1,
            events=[delete('drop', 90100, 3, 1)],
            watermark=90200,
        )
        warehouse = self.open()
        warehouse.apply_batches([b1, b2])
        manifest, _, parts = self.snapshot()
        self.assertEqual(manifest['generation'], 2)
        self.assertEqual(manifest['row_count'], 1)
        self.assertTrue(all(part['path'].startswith('date=1970-01-01/')
                            for part in manifest['partitions']))
        self.assertFalse((self.root / 'current' / 'date=1970-01-02').exists())
        self.assertEqual(warehouse.query()[0]['_pk'], 'keep')
        warehouse.close()


class WinnerTests(Case):
    def test_cross_source_tombstone_and_late_event(self):
        v1 = schema(1, [field('name', 'string')])
        a1 = self.make_batch('a1', batch_id='a1', source='a', previous_batch=None,
                             schema_value=v1, watermark=100,
                             events=[upsert('x', 100, 1, 1, {'name': 'old'})])
        b1 = self.make_batch('b1', batch_id='b1', source='b', previous_batch=None,
                             schema_value=v1, watermark=200,
                             events=[delete('x', 200, 1, 1)])
        a2 = self.make_batch('a2', batch_id='a2', source='a', previous_batch='a1',
                             schema_value=v1, watermark=200,
                             events=[upsert('x', 150, 2, 1, {'name': 'late'})])
        warehouse = self.open()
        warehouse.apply_batches([a1, b1, a2])
        self.assertEqual(warehouse.query(), [])
        manifest, _, parts = self.snapshot()
        self.assertEqual(manifest['row_count'], 0)
        self.assertEqual(manifest['partitions'], [])
        self.assertEqual(parts, {})
        warehouse.close()

    def test_input_order_independent_and_source_tiebreak(self):
        v1 = schema(1, [field('name', 'string')])
        first = self.make_batch(
            'first', batch_id='first', source='aa', previous_batch=None,
            schema_value=v1, watermark=300,
            events=[upsert('t', 300, 1, 1, {'name': 'from-aa'}),
                    upsert('u', 299, 2, 1, {'name': 'u-aa'})],
        )
        second = self.make_batch(
            'second', batch_id='second', source='ab', previous_batch=None,
            schema_value=v1, watermark=300,
            events=[upsert('t', 300, 1, 1, {'name': 'from-ab'}),
                    upsert('u', 299, 2, 1, {'name': 'u-ab'})],
        )
        warehouse = self.open()
        warehouse.apply_batches([second, first])
        rows = {row['_pk']: row for row in warehouse.query()}
        self.assertEqual(rows['t']['name'], 'from-ab')
        self.assertEqual(rows['u']['name'], 'u-ab')
        warehouse.close()

    def test_same_sequence_cross_batch_is_idempotent_event(self):
        v1 = schema(1, [field('name', 'string')])
        shared = upsert('x', 100, 1, 1, {'name': 'first'})
        b1 = self.make_batch('b1', batch_id='b1', source='s', previous_batch=None,
                             schema_value=v1, events=[shared], watermark=100)
        b2 = self.make_batch('b2', batch_id='b2', source='s', previous_batch='b1',
                             schema_value=v1, watermark=200,
                             events=[shared, upsert('x', 200, 2, 1, {'name': 'second'})])
        warehouse = self.open()
        warehouse.apply_batches([b1, b2])
        self.assertEqual(warehouse.query()[0]['name'], 'second')
        self.assertEqual(warehouse.apply_batch(b1)['generation'], 1)
        self.assertFalse(warehouse.apply_batch(b1)['applied'])
        warehouse.close()

    def test_duplicate_sequence_conflict_rolls_back_batch(self):
        v1 = schema(1, [field('name', 'string')])
        b1 = self.make_batch('b1', batch_id='b1', source='s', previous_batch=None,
                             schema_value=v1, watermark=100,
                             events=[upsert('x', 100, 1, 1, {'name': 'one'})])
        b2 = self.make_batch('b2', batch_id='b2', source='s', previous_batch='b1',
                             schema_value=v1, watermark=200,
                             events=[upsert('x', 150, 1, 1, {'name': 'changed'}),
                                     upsert('y', 200, 2, 1, {'name': 'new'})])
        warehouse = self.open()
        warehouse.apply_batch(b1)
        with self.assertRaises(ValueError):
            warehouse.apply_batch(b2)
        warehouse.close()
        warehouse = self.open()
        rows = warehouse.query()
        self.assertEqual([row['_pk'] for row in rows], ['x'])
        self.assertEqual(rows[0]['name'], 'one')
        self.assertEqual(self.snapshot()[0]['generation'], 1)
        warehouse.close()


class EvolutionTests(Case):
    def test_mixed_known_event_versions_use_declared_schema(self):
        v1 = schema(1, [field('name', 'string')])
        v2 = schema(2, [field('name', 'string'),
                        field('score', 'int', nullable=False, default=7)])
        b1 = self.make_batch('b1', batch_id='b1', source='s', previous_batch=None,
                             schema_value=v1, watermark=100,
                             events=[upsert('x', 100, 1, 1, {'name': 'a'})])
        b2 = self.make_batch('b2', batch_id='b2', source='s', previous_batch='b1',
                             schema_value=v2, watermark=200,
                             events=[upsert('y', 200, 2, 2, {'name': 'b', 'score': 9}),
                                     upsert('x', 150, 3, 1, {'name': 'a-late'})])
        warehouse = self.open()
        warehouse.apply_batches([b1, b2])
        rows = {row['_pk']: row for row in warehouse.query()}
        self.assertEqual(rows['x']['name'], 'a-late')
        self.assertEqual(rows['x']['score'], 7)
        self.assertEqual(rows['y']['score'], 9)
        warehouse.close()

    def test_unknown_event_schema_version_rejected(self):
        v1 = schema(1, [field('name', 'string')])
        v2 = schema(2, [field('name', 'string'),
                        field('score', 'int', nullable=False, default=7)])
        b1 = self.make_batch('b1', batch_id='b1', source='s', previous_batch=None,
                             schema_value=v1, events=[], watermark=100)
        bad = self.make_batch('bad', batch_id='bad', source='s', previous_batch='b1',
                              schema_value=v2, watermark=100,
                              events=[upsert('x', 100, 1, 9, {'name': 'x'})])
        warehouse = self.open()
        warehouse.apply_batch(b1)
        with self.assertRaises(ValueError):
            warehouse.apply_batch(bad)
        warehouse.close()

    def test_backfill_defaults_and_null_default(self):
        v1 = schema(1, [field('name', 'string')])
        v2 = schema(2, [field('name', 'string'),
                        field('score', 'int', nullable=False, default=7)])
        v3 = schema(3, [field('name', 'string'),
                        field('score', 'int', nullable=False, default=7),
                        field('note', 'string', nullable=True, default=None)])
        b1 = self.make_batch('b1', batch_id='b1', source='s', previous_batch=None,
                             schema_value=v1, watermark=100,
                             events=[upsert('x', 100, 1, 1, {'name': 'a'})])
        b2 = self.make_batch('b2', batch_id='b2', source='s', previous_batch='b1',
                             schema_value=v2, watermark=100, events=[])
        b3 = self.make_batch('b3', batch_id='b3', source='s', previous_batch='b2',
                             schema_value=v3, watermark=100, events=[])
        warehouse = self.open()
        warehouse.apply_batches([b1, b2, b3])
        row = warehouse.query()[0]
        self.assertEqual(row['name'], 'a')
        self.assertEqual(row['score'], 7)
        self.assertIsNone(row['note'])
        manifest, _, _ = self.snapshot()
        self.assertEqual(manifest['schema_version'], 3)
        self.assertEqual(manifest['generation'], 3)
        v3_raw = canon(v3)
        self.assertEqual(manifest['schema_sha256'],
                         hashlib.sha256(v3_raw).hexdigest())
        warehouse.close()

    def test_invalid_upgrades_rejected(self):
        v1 = schema(1, [field('name', 'string')])
        v2 = schema(2, [field('name', 'string'),
                        field('score', 'int', nullable=False, default=None)])
        b1 = self.make_batch('b1', batch_id='b1', source='s', previous_batch=None,
                             schema_value=v1, watermark=1, events=[])
        bad = self.make_batch('bad', batch_id='bad', source='s', previous_batch='b1',
                              schema_value=v2, watermark=1, events=[])
        warehouse = self.open()
        warehouse.apply_batch(b1)
        with self.assertRaises(ValueError):
            warehouse.apply_batch(bad)
        self.assertEqual([row for row in warehouse.query()], [])
        warehouse.close()


class AtomicityTests(Case):
    def _batches(self):
        v1 = schema(1, [field('name', 'string')])
        b1 = self.make_batch('b1', batch_id='b1', source='s', previous_batch=None,
                             schema_value=v1, watermark=100,
                             events=[upsert('x', 100, 1, 1, {'name': 'x1'})])
        b2 = self.make_batch('b2', batch_id='b2', source='s', previous_batch='b1',
                             schema_value=v1, watermark=100,
                             events=[upsert('y', 100, 2, 1, {'name': 'y1'})])
        return b1, b2

    def db_generation(self):
        connection = sqlite3.connect(str(self.root / 'state.sqlite'))
        try:
            row = connection.execute(
                "SELECT value FROM meta WHERE key='generation'"
            ).fetchone()
            return int(row[0])
        finally:
            connection.close()

    def test_faults_roll_back_entire_batch_set(self):
        b1, b2 = self._batches()
        for fault in ('after_state', 'after_stage', 'before_publish'):
            with self.subTest(fault=fault):
                warehouse = self.open()
                with self.assertRaises(RuntimeError):
                    warehouse.apply_batches([b1, b2], fault=fault)
                warehouse.close()
                self.assertEqual(self.db_generation(), 0)
                self.assertFalse((self.root / 'current').exists())
                self.assertEqual(self.temp_leftovers(), [])
                reopened = self.open()
                self.assertEqual(reopened.query(), [])
                reopened.close()

    def test_faulted_single_batch_leaves_nothing(self):
        b1, _ = self._batches()
        warehouse = self.open()
        with self.assertRaises(RuntimeError):
            warehouse.apply_batch(b1, fault='before_publish')
        warehouse.close()
        self.assertEqual(self.db_generation(), 0)
        self.assertFalse((self.root / 'current').exists())
        self.assertEqual(self.temp_leftovers(), [])

    def test_later_validation_failure_rolls_back_earlier_batch(self):
        v1 = schema(1, [field('name', 'string')])
        good = self.make_batch('good', batch_id='good', source='s',
                               previous_batch=None, schema_value=v1, watermark=100,
                               events=[upsert('x', 100, 1, 1, {'name': 'x'})])
        bad = self.make_batch('bad', batch_id='bad', source='s',
                              previous_batch='not-the-head', schema_value=v1,
                              watermark=100,
                              events=[upsert('y', 100, 2, 1, {'name': 'y'})])
        warehouse = self.open()
        with self.assertRaises(ValueError):
            warehouse.apply_batches([good, bad])
        warehouse.close()
        self.assertEqual(self.db_generation(), 0)
        self.assertFalse((self.root / 'current').exists())
        reopened = self.open()
        self.assertEqual(reopened.query(), [])
        reopened.close()

    def test_apply_many_results_and_mixed_idempotency(self):
        b1, b2 = self._batches()
        warehouse = self.open()
        results = warehouse.apply_batches([b1, b2, b1])
        self.assertEqual([result['generation'] for result in results], [1, 2, 1])
        self.assertEqual([result['applied'] for result in results], [True, True, False])
        self.assertEqual(self.snapshot()[0]['generation'], 2)
        self.assertEqual([row['_pk'] for row in warehouse.query()], ['x', 'y'])
        warehouse.close()

    def test_same_batch_id_different_content_rejected(self):
        v1 = schema(1, [field('name', 'string')])
        b1 = self.make_batch('b1', batch_id='b1', source='s', previous_batch=None,
                             schema_value=v1, watermark=100,
                             events=[upsert('x', 100, 1, 1, {'name': 'x1'})])
        edited = self.make_batch('edited', batch_id='b1', source='s',
                                 previous_batch=None, schema_value=v1, watermark=100,
                                 events=[upsert('x', 100, 1, 1, {'name': 'x2'})])
        warehouse = self.open()
        self.assertTrue(warehouse.apply_batch(b1)['applied'])
        with self.assertRaises(ValueError):
            warehouse.apply_batch(edited)
        warehouse.close()


class RecoveryTests(Case):
    def _one_batch(self):
        v1 = schema(1, [field('name', 'string')])
        return self.make_batch(
            'b1', batch_id='b1', source='s', previous_batch=None,
            schema_value=v1, watermark=5,
            events=[upsert(str(index), index, index, 1, {'name': 'n'})
                    for index in range(1, 6)],
        )

    def test_missing_and_corrupt_snapshot_is_rebuilt(self):
        batch = self._one_batch()
        warehouse = self.open()
        warehouse.apply_batch(batch)
        manifest, manifest_raw, parts = self.snapshot()
        warehouse.close()

        victim = sorted(parts)[0]
        (self.root / 'current' / victim).unlink()
        warehouse = self.open()
        self.assertEqual(warehouse.query()[0]['name'], 'n')
        warehouse.close()
        new_manifest, new_manifest_raw, new_parts = self.snapshot()
        self.assertEqual(new_manifest_raw, manifest_raw)
        self.assertEqual(new_parts, parts)

        (self.root / 'current' / 'manifest.json').write_bytes(manifest_raw + b'\n')
        warehouse = self.open()
        warehouse.close()
        self.assertEqual(self.snapshot()[1], manifest_raw)

        shutil.rmtree(self.root / 'current')
        warehouse = self.open()
        warehouse.close()
        self.assertEqual(self.snapshot()[0]['generation'], manifest['generation'])
        self.assertEqual(self.snapshot()[2], parts)

    def test_stale_snapshot_after_uncommitted_publish_is_discarded(self):
        batch = self._one_batch()
        warehouse = self.open()
        original = warehouse.apply_batch(batch)
        manifest, manifest_raw, parts = self.snapshot()
        warehouse.close()
        newer = {
            'generation': 99,
            'schema_version': 1,
            'schema_sha256': manifest['schema_sha256'],
            'row_count': 0,
            'partitions': [],
        }
        (self.root / 'current' / 'manifest.json').write_bytes(canon(newer))
        warehouse = self.open()
        warehouse.close()
        self.assertEqual(self.snapshot()[0]['generation'], original['generation'])
        self.assertEqual(self.snapshot()[1], manifest_raw)
        self.assertEqual(self.snapshot()[2], parts)


class ConcurrencyTests(Case):
    def test_concurrent_same_batch_only_once(self):
        v1 = schema(1, [field('name', 'string')])
        batch = self.make_batch(
            'b1', batch_id='b1', source='s', previous_batch=None,
            schema_value=v1, watermark=100,
            events=[upsert('x', 100, 1, 1, {'name': 'x'})],
        )
        workers = 8
        barrier = threading.Barrier(workers)
        results = []
        errors = []
        lock = threading.Lock()

        def worker():
            warehouse = None
            try:
                warehouse = Warehouse(self.root)
                barrier.wait()
                result = warehouse.apply_batch(batch)
                with lock:
                    results.append(result)
            except Exception as exc:  # pragma: no cover - failure path
                with lock:
                    errors.append(exc)
            finally:
                if warehouse is not None:
                    warehouse.close()

        threads = [threading.Thread(target=worker) for _ in range(workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)
        self.assertEqual(errors, [])
        self.assertEqual(len(results), workers)
        self.assertEqual(sum(1 for result in results if result['applied']), 1)
        self.assertTrue(all(result['generation'] == 1 for result in results))
        reopened = self.open()
        self.assertEqual(len(reopened.query()), 1)
        reopened.close()


class DatabaseGuardTests(Case):
    def test_future_version_rejected(self):
        warehouse = self.open()
        warehouse.close()
        connection = sqlite3.connect(str(self.root / 'state.sqlite'))
        connection.execute('PRAGMA user_version=3')
        connection.commit()
        connection.close()
        with self.assertRaises(ValueError):
            Warehouse(self.root)

    def test_damaged_structure_rejected(self):
        warehouse = self.open()
        warehouse.close()
        connection = sqlite3.connect(str(self.root / 'state.sqlite'))
        connection.execute('DROP TABLE events')
        connection.commit()
        connection.close()
        with self.assertRaises(ValueError):
            Warehouse(self.root)

    def test_unexpected_table_rejected(self):
        warehouse = self.open()
        warehouse.close()
        connection = sqlite3.connect(str(self.root / 'state.sqlite'))
        connection.execute('CREATE TABLE surprise(value TEXT)')
        connection.commit()
        connection.close()
        with self.assertRaises(ValueError):
            Warehouse(self.root)


class ValidationTests(Case):
    def v1(self):
        return schema(1, [field('name', 'string')])

    def valid_batch(self, name='b1', batch_id='b1', previous_batch=None,
                    watermark=100, event_time=100, data=None, extra_file=None):
        data = {'name': 'ok'} if data is None else data
        return self.make_batch(
            name, batch_id=batch_id, source='s', previous_batch=previous_batch,
            schema_value=self.v1(), watermark=watermark,
            events=[upsert('x', event_time, 1, 1, data)],
            extra_file=extra_file,
        )

    def test_non_canonical_and_duplicate_keys(self):
        directory = self.work / 'badjson'
        directory.mkdir()
        event = upsert('x', 100, 1, 1, {'name': 'ok'})
        schema_raw = canon(self.v1())
        events_raw = (json.dumps(event, indent=2).encode('utf-8') + b'\n')
        manifest_raw = canon({
            'format': 1, 'batch_id': 'b', 'source': 's', 'previous_batch': None,
            'schema_version': 1,
            'schema_sha256': hashlib.sha256(schema_raw).hexdigest(),
            'events_sha256': hashlib.sha256(events_raw).hexdigest(),
            'event_count': 1, 'watermark': 100,
        })
        (directory / 'manifest.json').write_bytes(manifest_raw)
        (directory / 'schema.json').write_bytes(schema_raw)
        (directory / 'events.jsonl').write_bytes(events_raw)
        with self.assertRaises(ValueError):
            self.open().apply_batch(directory)

        schema_raw = b'{"fields":[],"version":1,"version":1}'
        events_raw = b''
        manifest_raw = canon({
            'format': 1, 'batch_id': 'dup', 'source': 's', 'previous_batch': None,
            'schema_version': 1,
            'schema_sha256': hashlib.sha256(schema_raw).hexdigest(),
            'events_sha256': hashlib.sha256(events_raw).hexdigest(),
            'event_count': 0, 'watermark': 100,
        })
        duplicate = self.work / 'dup'
        duplicate.mkdir()
        (duplicate / 'manifest.json').write_bytes(manifest_raw)
        (duplicate / 'schema.json').write_bytes(schema_raw)
        (duplicate / 'events.jsonl').write_bytes(events_raw)
        with self.assertRaises(ValueError):
            self.open().apply_batch(duplicate)

    def test_bool_as_integer_and_bad_hash(self):
        v1_raw = canon(self.v1())
        events_raw = b''
        manifest_raw = canon({
            'format': True, 'batch_id': 'b', 'source': 's',
            'previous_batch': None, 'schema_version': 1,
            'schema_sha256': hashlib.sha256(v1_raw).hexdigest(),
            'events_sha256': hashlib.sha256(events_raw).hexdigest(),
            'event_count': 0, 'watermark': 100,
        })
        batch = self.make_batch('bool', batch_id='b', source='s',
                                previous_batch=None, schema_value=self.v1(),
                                events=[], watermark=100, manifest_bytes=manifest_raw)
        with self.assertRaises(ValueError):
            self.open().apply_batch(batch)

        manifest_raw = canon({
            'format': 1, 'batch_id': 'h', 'source': 's',
            'previous_batch': None, 'schema_version': 1,
            'schema_sha256': '0' * 64,
            'events_sha256': hashlib.sha256(events_raw).hexdigest(),
            'event_count': 0, 'watermark': 100,
        })
        batch = self.make_batch('hash', batch_id='h', source='s',
                                previous_batch=None, schema_value=self.v1(),
                                events=[], watermark=100, manifest_bytes=manifest_raw)
        with self.assertRaises(ValueError):
            self.open().apply_batch(batch)

    def test_directory_and_count_checks(self):
        batch = self.valid_batch('extra', extra_file='extra.txt')
        with self.assertRaises(ValueError):
            self.open().apply_batch(batch)
        batch = self.valid_batch('count')
        manifest_raw = canon({
            'format': 1, 'batch_id': 'count', 'source': 's',
            'previous_batch': None, 'schema_version': 1,
            'schema_sha256': hashlib.sha256(canon(self.v1())).hexdigest(),
            'events_sha256': hashlib.sha256(
                (canon(upsert('x', 100, 1, 1, {'name': 'ok'})) + b'\n')
            ).hexdigest(),
            'event_count': 2, 'watermark': 100,
        })
        (batch / 'manifest.json').write_bytes(manifest_raw)
        with self.assertRaises(ValueError):
            self.open().apply_batch(batch)

    def test_chain_watermark_and_window(self):
        v1 = self.v1()
        first = self.make_batch('first', batch_id='first', source='s',
                                previous_batch='ghost', schema_value=v1,
                                events=[], watermark=100)
        with self.assertRaises(ValueError):
            self.open().apply_batch(first)
        good = self.make_batch('good', batch_id='good', source='s',
                               previous_batch=None, schema_value=v1,
                               events=[], watermark=100)
        wrong_prev = self.make_batch('wrong-prev', batch_id='wrong-prev', source='s',
                                     previous_batch=None, schema_value=v1,
                                     events=[], watermark=150)
        regress = self.make_batch('regress', batch_id='regress', source='s',
                                  previous_batch='good', schema_value=v1,
                                  events=[], watermark=99)
        outside = self.make_batch('outside', batch_id='outside', source='s',
                                  previous_batch='good', schema_value=v1,
                                  events=[upsert('x', 100 - 86401, 1, 1, {'name': 'x'})],
                                  watermark=100)
        boundary = self.make_batch('boundary', batch_id='boundary', source='s',
                                   previous_batch='good', schema_value=v1,
                                   events=[upsert('x', 100 - 86400, 1, 1, {'name': 'x'})],
                                   watermark=100)
        warehouse = self.open()
        warehouse.apply_batch(good)
        for batch in (wrong_prev, regress, outside):
            with self.assertRaises(ValueError):
                warehouse.apply_batch(batch)
        self.assertTrue(warehouse.apply_batch(boundary)['applied'])
        warehouse.close()

    def test_zero_event_file_forms(self):
        v1 = self.v1()
        empty = self.make_batch('empty', batch_id='empty', source='s',
                                previous_batch=None, schema_value=v1,
                                events=[], events_bytes=b'', watermark=100)
        newline = self.make_batch('newline', batch_id='newline', source='s',
                                  previous_batch='empty', schema_value=v1,
                                  events=[], events_bytes=b'\n', watermark=100)
        warehouse = self.open()
        results = warehouse.apply_batches([empty, newline])
        self.assertEqual([item['applied'] for item in results], [True, True])
        self.assertEqual(warehouse.query(), [])
        warehouse.close()

    def test_schema_version_gap_and_same_version_drift(self):
        v1 = self.v1()
        v2 = schema(2, [field('name', 'string'),
                        field('extra', 'int', nullable=False, default=1)])
        v3 = schema(3, [field('name', 'string'),
                        field('extra', 'int', nullable=False, default=1),
                        field('other', 'bool', nullable=True, default=None)])
        b1 = self.make_batch('b1', batch_id='b1', source='s', previous_batch=None,
                             schema_value=v1, events=[], watermark=100)
        gap = self.make_batch('gap', batch_id='gap', source='s', previous_batch='b1',
                              schema_value=v3, events=[], watermark=100)
        drift_schema = schema(1, [field('name', 'string'),
                                  field('drifting', 'int', nullable=True, default=None)])
        drift = self.make_batch('drift', batch_id='drift', source='s',
                                previous_batch='b1', schema_value=drift_schema,
                                events=[], watermark=100)
        warehouse = self.open()
        warehouse.apply_batch(b1)
        for batch in (gap, drift):
            with self.assertRaises(ValueError):
                warehouse.apply_batch(batch)
        warehouse.close()

    def test_delete_data_and_event_version_checks(self):
        v1 = self.v1()
        bad_delete = self.make_batch('bad-delete', batch_id='bad-delete', source='s',
                                     previous_batch=None, schema_value=v1,
                                     watermark=100,
                                     events=[{'op': 'delete', 'pk': 'x',
                                              'event_time': 100, 'source_seq': 1,
                                              'schema_version': 1,
                                              'data': {'name': 'x'}}])
        bad_version = self.make_batch('bad-version', batch_id='bad-version', source='s',
                                      previous_batch=None, schema_value=v1,
                                      watermark=100,
                                      events=[upsert('x', 100, 1, 2, {'name': 'x'})])
        for batch in (bad_delete, bad_version):
            with self.assertRaises(ValueError):
                self.open().apply_batch(batch)

    def test_text_nfc_control_and_surrogate(self):
        v1_raw = canon(self.v1())
        cases = {
            'decomposed-pk': canon(upsert('e\u0301', 100, 1, 1, {'name': 'ok'})),
            'control-value': canon(upsert('x', 100, 1, 1, {'name': 'a\tb'})),
            'surrogate-value': (
                b'{"data":{"name":"\\ud800"},"event_time":100,"op":"upsert",'
                b'"pk":"x","schema_version":1,"source_seq":1}'
            ),
        }
        for name, event_line in cases.items():
            with self.subTest(name=name):
                events_raw = event_line + b'\n'
                manifest_raw = canon({
                    'format': 1, 'batch_id': name, 'source': 's',
                    'previous_batch': None, 'schema_version': 1,
                    'schema_sha256': hashlib.sha256(v1_raw).hexdigest(),
                    'events_sha256': hashlib.sha256(events_raw).hexdigest(),
                    'event_count': 1, 'watermark': 100,
                })
                directory = self.work / name
                directory.mkdir()
                (directory / 'manifest.json').write_bytes(manifest_raw)
                (directory / 'schema.json').write_bytes(v1_raw)
                (directory / 'events.jsonl').write_bytes(events_raw)
                with self.assertRaises(ValueError):
                    self.open().apply_batch(directory)

    def test_float_bool_and_data_shape(self):
        v1 = schema(1, [field('ratio', 'float'), field('flag', 'bool')])
        good_data = {'ratio': 1.5, 'flag': True}
        good = self.make_batch('good', batch_id='good', source='s',
                               previous_batch=None, schema_value=v1, watermark=100,
                               events=[upsert('x', 100, 1, 1, good_data)])
        warehouse = self.open()
        self.assertTrue(warehouse.apply_batch(good)['applied'])
        self.assertEqual(warehouse.query()[0]['ratio'], 1.5)

        bad_batches = []
        bad_batches.append(self.make_batch(
            'int-for-float', batch_id='int-for-float', source='s',
            previous_batch='good', schema_value=v1, watermark=100,
            events=[upsert('x', 100, 2, 1, {'ratio': 1, 'flag': True})],
        ))
        bad_batches.append(self.make_batch(
            'int-for-bool', batch_id='int-for-bool', source='s',
            previous_batch='good', schema_value=v1, watermark=100,
            events=[upsert('x', 100, 3, 1, {'ratio': 1.5, 'flag': 1})],
        ))
        bad_batches.append(self.make_batch(
            'missing-field', batch_id='missing-field', source='s',
            previous_batch='good', schema_value=v1, watermark=100,
            events=[upsert('x', 100, 4, 1, {'ratio': 1.5})],
        ))
        bad_batches.append(self.make_batch(
            'extra-field', batch_id='extra-field', source='s',
            previous_batch='good', schema_value=v1, watermark=100,
            events=[upsert('x', 100, 5, 1,
                           {'ratio': 1.5, 'flag': True, 'extra': 1})],
        ))
        infinite_data = {'ratio': float('inf'), 'flag': True}
        infinite = self.make_batch(
            'infinite', batch_id='infinite', source='s', previous_batch='good',
            schema_value=v1, watermark=100,
            events=[upsert('x', 100, 6, 1, infinite_data)],
        )
        bad_batches.append(infinite)
        for batch in bad_batches:
            with self.assertRaises(ValueError):
                warehouse.apply_batch(batch)
        self.assertEqual(warehouse.query()[0]['ratio'], 1.5)
        warehouse.close()

    def test_existing_field_order_and_type_drift_rejected(self):
        v1 = schema(1, [field('name', 'string'),
                        field('score', 'int', nullable=False, default=1)])
        reordered = schema(2, [field('score', 'int', nullable=False, default=1),
                               field('name', 'string'),
                               field('note', 'string', nullable=True, default=None)])
        retyped = schema(2, [field('name', 'int', nullable=False, default=0),
                             field('score', 'int', nullable=False, default=1),
                             field('note', 'string', nullable=True, default=None)])
        b1 = self.make_batch('b1', batch_id='b1', source='s', previous_batch=None,
                             schema_value=v1, events=[], watermark=100)
        r1 = self.make_batch('r1', batch_id='r1', source='s', previous_batch='b1',
                             schema_value=reordered, events=[], watermark=100)
        r2 = self.make_batch('r2', batch_id='r2', source='s', previous_batch='b1',
                             schema_value=retyped, events=[], watermark=100)
        warehouse = self.open()
        warehouse.apply_batch(b1)
        for batch in (r1, r2):
            with self.assertRaises(ValueError):
                warehouse.apply_batch(batch)
        warehouse.close()


class CliTests(Case):
    def run_cli(self, *arguments):
        return subprocess.run(
            [sys.executable, '-m', 'cdcforge', '--warehouse', str(self.root), *arguments],
            cwd=str(Path(__file__).parents[1]),
            capture_output=True,
            text=True,
            encoding='utf-8',
        )

    def test_apply_and_query(self):
        v1 = schema(1, [field('name', 'string')])
        batch = self.make_batch('b1', batch_id='b1', source='s', previous_batch=None,
                                schema_value=v1, watermark=100,
                                events=[upsert('x', 100, 1, 1, {'name': 'x'})])
        applied = self.run_cli('apply', str(batch))
        self.assertEqual(applied.returncode, 0, applied.stderr)
        self.assertEqual(len(applied.stdout.splitlines()), 1)
        self.assertTrue(json.loads(applied.stdout)['applied'])
        queried = self.run_cli('query')
        self.assertEqual(queried.returncode, 0, queried.stderr)
        self.assertEqual(len(queried.stdout.splitlines()), 1)
        self.assertEqual(json.loads(queried.stdout)[0]['name'], 'x')

    def test_business_error_has_no_traceback(self):
        batch = self.make_batch('bad', batch_id='bad', source='s',
                                previous_batch=None,
                                schema_value=schema(1, [field('name', 'string')]),
                                events=[], watermark=100, extra_file='extra.txt')
        result = self.run_cli('apply', str(batch))
        self.assertEqual(result.returncode, 2)
        payload = json.loads(result.stdout)
        self.assertIn('error', payload)
        self.assertIn('message', payload)
        self.assertNotIn('Traceback', result.stderr)

    def test_apply_many(self):
        v1 = schema(1, [field('name', 'string')])
        b1 = self.make_batch('b1', batch_id='b1', source='s', previous_batch=None,
                             schema_value=v1, watermark=100,
                             events=[upsert('x', 100, 1, 1, {'name': 'x'})])
        b2 = self.make_batch('b2', batch_id='b2', source='s', previous_batch='b1',
                             schema_value=v1, watermark=100,
                             events=[upsert('y', 100, 2, 1, {'name': 'y'})])
        result = self.run_cli('apply-many', str(b1), str(b2))
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual([item['generation'] for item in payload], [1, 2])
        self.assertEqual([item['applied'] for item in payload], [True, True])

    def test_missing_command_is_json_error(self):
        result = self.run_cli()
        self.assertEqual(result.returncode, 2)
        payload = json.loads(result.stdout)
        self.assertIn('error', payload)
        self.assertIn('message', payload)
        self.assertNotIn('Traceback', result.stderr)

    def test_utf8_output(self):
        v1 = schema(1, [field('name', 'string')])
        batch = self.make_batch('b1', batch_id='b1', source='s', previous_batch=None,
                                schema_value=v1, watermark=100,
                                events=[upsert('x', 100, 1, 1, {'name': '名字'})])
        applied = self.run_cli('apply', str(batch))
        self.assertEqual(applied.returncode, 0, applied.stderr)
        queried = self.run_cli('query')
        self.assertEqual(queried.returncode, 0, queried.stderr)
        self.assertEqual(json.loads(queried.stdout)[0]['name'], '名字')


if __name__ == '__main__':
    unittest.main()
