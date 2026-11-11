import json
import sqlite3
import subprocess
import sys
import unittest
import uuid
from pathlib import Path

from reconcile.projection import canonical
from reconcile.store import Conflict, Invalid, Ledger

ROOT = Path(__file__).resolve().parents[1]


def make_v1(path, partitions, records):
    db = sqlite3.connect(path)
    db.execute('CREATE TABLE partitions(partition TEXT PRIMARY KEY, owner TEXT NOT NULL,'
               ' epoch INTEGER NOT NULL, checkpoint INTEGER NOT NULL)')
    db.execute('CREATE TABLE history(partition TEXT NOT NULL, seq INTEGER NOT NULL,'
               ' event_id TEXT NOT NULL, entity TEXT NOT NULL, entity_key TEXT NOT NULL,'
               ' rev INTEGER NOT NULL, value_json TEXT NOT NULL, PRIMARY KEY(partition,seq))')
    for row in partitions:
        db.execute('INSERT INTO partitions VALUES(?,?,?,?)', row)
    for row in records:
        partition, seq, event_id, entity, key, rev, value = row
        db.execute('INSERT INTO history VALUES(?,?,?,?,?,?,?)',
                   (partition, seq, event_id, entity, key, rev, json.dumps(value)))
    db.execute('PRAGMA user_version=1')
    db.commit()
    db.close()


def user_version(path):
    db = sqlite3.connect(path)
    version = db.execute('PRAGMA user_version').fetchone()[0]
    db.close()
    return version


class MigrationCase(unittest.TestCase):
    def setUp(self):
        root = ROOT / 'artifacts' / 'tmp'
        root.mkdir(parents=True, exist_ok=True)
        self.case_dir = root / uuid.uuid4().hex
        self.case_dir.mkdir()
        self.path = str(self.case_dir / 'ledger.sqlite')

    def tearDown(self):
        for child in self.case_dir.iterdir():
            child.unlink()
        self.case_dir.rmdir()


class MigrationTests(MigrationCase):
    def test_documented_fixture_migrates(self):
        target = str(self.case_dir / 'legacy.sqlite')
        subprocess.run([sys.executable, str(ROOT / 'tools' / 'make_v1.py'), target],
                       check=True, cwd=ROOT)
        ledger = Ledger(target)
        view = ledger.view()
        self.assertEqual(view['groups'], [
            {'day': 0, 'segment': 'standard', 'count': 1,
             'gross_cents': 300, 'net_cents': 270}])
        self.assertEqual(view['pending'], [])
        self.assertEqual(view['checkpoints'], {'legacy': 3})
        self.assertEqual(view['generation'], 1)
        self.assertEqual(user_version(target), 2)
        epoch = ledger.claim('legacy', 'worker')
        self.assertEqual(epoch, 8)
        replay = [
            {'seq': 1, 'event_id': 'e1', 'entity': 'plan', 'key': 'plan-a', 'rev': 1,
             'value': {'customer': 'c1', 'start': 0, 'end': None, 'segment': 'standard',
                       'discount_bps': 1000, 'priority': 0}},
            {'seq': 2, 'event_id': 'e2', 'entity': 'order', 'key': 'o1', 'rev': 2,
             'value': {'customer': 'c1', 'at': 1, 'gross_cents': 300}},
            {'seq': 3, 'event_id': 'e3', 'entity': 'order', 'key': 'o1', 'rev': 1,
             'value': {'customer': 'c1', 'at': 1, 'gross_cents': 100}},
        ]
        result = ledger.ingest('legacy', 'worker', epoch, replay)
        self.assertEqual(result['applied'], 0)
        result = ledger.ingest('legacy', 'worker', epoch, replay + [
            {'seq': 4, 'event_id': 'e4', 'entity': 'order', 'key': 'o2', 'rev': 1,
             'value': {'customer': 'c1', 'at': 5, 'gross_cents': 50}}])
        self.assertEqual(result['applied'], 1)
        self.assertEqual(ledger.view()['groups'][0]['count'], 2)
        ledger.close()

    def test_reopen_does_not_rebuild(self):
        make_v1(self.path, [('p', 'w', 3, 1)], [
            ('p', 1, 'e1', 'order', 'o1', 1, {'customer': 'c', 'at': 0, 'gross_cents': 10})])
        ledger = Ledger(self.path)
        self.assertEqual(ledger.view()['generation'], 1)
        ledger.close()
        ledger = Ledger(self.path)
        self.assertEqual(ledger.view()['generation'], 1)
        self.assertEqual(ledger.export(str(self.case_dir / 'out')), 1)
        ledger.close()

    def test_empty_v1_gives_generation_zero(self):
        make_v1(self.path, [('p', 'w', 1, 0)], [])
        ledger = Ledger(self.path)
        view = ledger.view()
        self.assertEqual(view['generation'], 0)
        self.assertEqual(view['checkpoints'], {'p': 0})
        ledger.close()

    def test_pending_orders_produce_initial_frame(self):
        make_v1(self.path, [('p', 'w', 1, 1)], [
            ('p', 1, 'e1', 'order', 'o1', 1, {'customer': 'c', 'at': 0, 'gross_cents': 10})])
        ledger = Ledger(self.path)
        view = ledger.view()
        self.assertEqual(view['pending'], ['o1'])
        self.assertEqual(view['generation'], 1)
        out = str(self.case_dir / 'out')
        ledger.export(out)
        frame = json.loads(Path(out).read_text(encoding='utf-8'))
        self.assertEqual(frame['pending_add'], ['o1'])
        self.assertEqual(frame['previous'], '0' * 64)
        ledger.close()

    def test_gap_in_history_keeps_v1_untouched(self):
        make_v1(self.path, [('p', 'w', 1, 2)], [
            ('p', 1, 'e1', 'order', 'o1', 1, {'customer': 'c', 'at': 0, 'gross_cents': 10})])
        with self.assertRaises(Invalid):
            Ledger(self.path)
        self.assertEqual(user_version(self.path), 1)
        db = sqlite3.connect(self.path)
        count = db.execute('SELECT COUNT(*) FROM history').fetchone()[0]
        tables = {row[0] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        db.close()
        self.assertEqual(count, 1)
        self.assertNotIn('frames', tables)

    def test_conflicting_versions_reject_migration(self):
        make_v1(self.path, [('a', 'w', 1, 1), ('b', 'w', 1, 1)], [
            ('a', 1, 'e1', 'order', 'o1', 1, {'customer': 'c', 'at': 0, 'gross_cents': 10}),
            ('b', 1, 'e2', 'order', 'o1', 1, {'customer': 'c', 'at': 0, 'gross_cents': 11})])
        with self.assertRaises(Conflict):
            Ledger(self.path)
        self.assertEqual(user_version(self.path), 1)

    def test_conflicting_event_identity_rejects_migration(self):
        make_v1(self.path, [('a', 'w', 1, 1), ('b', 'w', 1, 1)], [
            ('a', 1, 'e1', 'order', 'o1', 1, {'customer': 'c', 'at': 0, 'gross_cents': 10}),
            ('b', 1, 'e1', 'order', 'o2', 1, {'customer': 'c', 'at': 0, 'gross_cents': 10})])
        with self.assertRaises(Conflict):
            Ledger(self.path)
        self.assertEqual(user_version(self.path), 1)

    def test_unsupported_version_rejected(self):
        db = sqlite3.connect(self.path)
        db.execute('PRAGMA user_version=7')
        db.commit()
        db.close()
        with self.assertRaises(Invalid):
            Ledger(self.path)
        self.assertEqual(user_version(self.path), 7)

    def test_fresh_file_becomes_v2(self):
        ledger = Ledger(self.path)
        self.assertEqual(user_version(self.path), 2)
        self.assertEqual(ledger.view(), {'groups': [], 'pending': [],
                                         'checkpoints': {}, 'generation': 0})
        ledger.close()

    def test_cross_partition_read_order_does_not_matter(self):
        records = [
            ('b', 1, 'e2', 'order', 'o1', 2, {'customer': 'c', 'at': 0, 'gross_cents': 200}),
            ('a', 1, 'e1', 'order', 'o1', 1, {'customer': 'c', 'at': 0, 'gross_cents': 100}),
            ('a', 2, 'e3', 'plan', 'pl', 1,
             {'customer': 'c', 'start': 0, 'end': None, 'segment': 's',
              'discount_bps': 0, 'priority': 0}),
        ]
        make_v1(self.path, [('a', 'w', 1, 2), ('b', 'w', 1, 1)], records)
        ledger = Ledger(self.path)
        self.assertEqual(ledger.view()['groups'][0]['gross_cents'], 200)
        ledger.close()
