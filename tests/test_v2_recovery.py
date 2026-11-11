import hashlib
import json
import multiprocessing
import os
import sqlite3
import sys
import threading
import unittest
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from reconcile.projection import canonical
from reconcile.store import Conflict, Fenced, Invalid, Ledger


def order(event, key, seq, rev, customer, at, gross):
    return {'seq': seq, 'event_id': event, 'entity': 'order', 'key': key, 'rev': rev,
            'value': None if customer is None else
            {'customer': customer, 'at': at, 'gross_cents': gross}}


def plan(event, key, seq, rev, customer, start, end, segment, bps, priority):
    return {'seq': seq, 'event_id': event, 'entity': 'plan', 'key': key, 'rev': rev,
            'value': None if customer is None else
            {'customer': customer, 'start': start, 'end': end, 'segment': segment,
             'discount_bps': bps, 'priority': priority}}


def batch1():
    return [plan('p1', 'pl', 1, 1, 'c', 0, None, 'basic', 1000, 0),
            order('o1', 'o1', 2, 1, 'c', 0, 100)]


def _crash_ingest(db_path, stage):
    ledger = Ledger(db_path)
    epoch = ledger.claim('p', 'w')

    def fault(s):
        if s == stage:
            os._exit(1)
    ledger.ingest('p', 'w', epoch, batch1(), fault=fault)
    os._exit(0)


def _crash_export(db_path, out_path):
    ledger = Ledger(db_path)

    def fault(stage):
        if stage == 'after_append':
            os._exit(1)
    ledger.export(out_path, fault=fault)
    os._exit(0)


def _export_worker(db_path, out_path):
    ledger = Ledger(db_path)
    ledger.export(out_path)
    ledger.close()


class RecoveryCase(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parents[1] / 'artifacts' / 'tmp'
        root.mkdir(parents=True, exist_ok=True)
        self.case_dir = root / uuid.uuid4().hex
        self.case_dir.mkdir()
        self.path = str(self.case_dir / 'ledger.sqlite')
        self.ledger = Ledger(self.path)

    def tearDown(self):
        self.ledger.close()
        for child in self.case_dir.iterdir():
            child.unlink()
        self.case_dir.rmdir()

    def run_child(self, target, args):
        process = multiprocessing.Process(target=target, args=args)
        process.start()
        process.join(30)
        self.assertIsNotNone(process.exitcode)
        return process.exitcode

    def out(self, name='out.ndjson'):
        return str(self.case_dir / name)


class IngestFaultTests(RecoveryCase):
    def test_raised_fault_rolls_back_everything(self):
        epoch = self.ledger.claim('p', 'w')
        for stage in ('after_history', 'after_projection', 'before_commit'):
            def fault(s, stage=stage):
                if s == stage:
                    raise RuntimeError(stage)
            with self.assertRaises(RuntimeError):
                self.ledger.ingest('p', 'w', epoch, batch1(), fault=fault)
            view = self.ledger.view()
            self.assertEqual(view['checkpoints'], {'p': 0})
            self.assertEqual(view['groups'], [])
            self.assertEqual(view['generation'], 0)
        result = self.ledger.ingest('p', 'w', epoch, batch1())
        self.assertEqual(result['checkpoint'], 2)
        self.assertEqual(self.ledger.view()['generation'], 1)

    def test_pure_replay_does_not_trigger_faults(self):
        epoch = self.ledger.claim('p', 'w')
        self.ledger.ingest('p', 'w', epoch, batch1())
        seen = []
        result = self.ledger.ingest('p', 'w', epoch, batch1(), fault=seen.append)
        self.assertEqual(result['applied'], 0)
        self.assertEqual(seen, [])

    def test_process_death_at_each_stage_leaves_no_partial_state(self):
        for stage in ('after_history', 'after_projection', 'before_commit'):
            with self.subTest(stage=stage):
                db_path = str(self.case_dir / f'crash-{stage}.sqlite')
                ledger = Ledger(db_path)
                ledger.close()
                code = self.run_child(_crash_ingest, (db_path, stage))
                self.assertEqual(code, 1)
                ledger = Ledger(db_path)
                try:
                    view = ledger.view()
                    self.assertEqual(view['checkpoints'].get('p', 0), 0)
                    self.assertEqual(view['groups'], [])
                    self.assertEqual(view['generation'], 0)
                    epoch = ledger.claim('p', 'w')
                    result = ledger.ingest('p', 'w', epoch, batch1())
                    self.assertEqual(result['checkpoint'], 2)
                    self.assertEqual(ledger.view()['generation'], 1)
                finally:
                    ledger.close()


class ExportTests(RecoveryCase):
    def read_frames(self):
        return [json.loads(line) for line in
                Path(self.out()).read_bytes().decode('utf-8').splitlines()]

    def test_empty_database_exports_empty_file(self):
        self.assertEqual(self.ledger.export(self.out()), 0)
        self.assertEqual(Path(self.out()).read_bytes(), b'')

    def test_frame_chain_and_canonical_lines(self):
        epoch = self.ledger.claim('p', 'w')
        self.ledger.ingest('p', 'w', epoch, batch1())
        self.ledger.ingest('p', 'w', epoch, [
            order('o2', 'o2', 3, 1, 'c', 86400, 200)])
        self.assertEqual(self.ledger.export(self.out()), 2)
        raw = Path(self.out()).read_bytes()
        lines = raw.split(b'\n')
        self.assertEqual(lines[-1], b'')
        previous = '0' * 64
        for index, line in enumerate(lines[:-1], start=1):
            frame = json.loads(line)
            self.assertEqual(line.decode('utf-8'), canonical(frame))
            self.assertEqual(frame['generation'], index)
            self.assertEqual(frame['previous'], previous)
            body = {key: frame[key] for key in
                    ('generation', 'previous', 'changes', 'pending_add', 'pending_remove')}
            previous = hashlib.sha256(canonical(body).encode('utf-8')).hexdigest()
            self.assertEqual(frame['digest'], previous)
        self.assertEqual(self.ledger.export(self.out()), 0)

    def test_export_crash_after_append_is_recovered(self):
        epoch = self.ledger.claim('p', 'w')
        self.ledger.ingest('p', 'w', epoch, batch1())
        self.ledger.ingest('p', 'w', epoch, [order('o2', 'o2', 3, 1, 'c', 0, 200)])
        code = self.run_child(_crash_export, (self.path, self.out()))
        self.assertEqual(code, 1)
        self.assertEqual(len(Path(self.out()).read_bytes().splitlines()), 2)
        self.assertEqual(self.ledger.export(self.out()), 0)
        self.ledger.ingest('p', 'w', epoch, [order('o3', 'o3', 4, 1, 'c', 0, 50)])
        self.assertEqual(self.ledger.export(self.out()), 1)
        self.assertEqual(len(self.read_frames()), 3)

    def test_trailing_fragment_is_truncated(self):
        epoch = self.ledger.claim('p', 'w')
        self.ledger.ingest('p', 'w', epoch, batch1())
        self.assertEqual(self.ledger.export(self.out()), 1)
        with open(self.out(), 'ab') as stream:
            stream.write(b'{"generation":2,"pre')
        self.ledger.ingest('p', 'w', epoch, [order('o2', 'o2', 3, 1, 'c', 0, 5)])
        self.assertEqual(self.ledger.export(self.out()), 1)
        frames = self.read_frames()
        self.assertEqual([frame['generation'] for frame in frames], [1, 2])

    def test_complete_json_without_newline_is_still_a_fragment(self):
        epoch = self.ledger.claim('p', 'w')
        self.ledger.ingest('p', 'w', epoch, batch1())
        self.ledger.export(self.out())
        with open(self.out(), 'ab') as stream:
            stream.write(b'{"forged":true}')
        self.assertEqual(self.ledger.export(self.out()), 0)
        self.assertEqual(len(self.read_frames()), 1)

    def test_tampered_or_noncanonical_line_is_conflict_and_file_untouched(self):
        epoch = self.ledger.claim('p', 'w')
        self.ledger.ingest('p', 'w', epoch, batch1())
        self.ledger.export(self.out())
        original = Path(self.out()).read_bytes()
        frame = json.loads(original)
        forged = dict(frame)
        forged['changes'] = []
        for payload in (
                canonical(forged).encode('utf-8') + b'\n',
                json.dumps(frame, indent=2).encode('utf-8') + b'\n',
                b'\xef\xbb\xbf' + original):
            Path(self.out()).write_bytes(payload)
            with self.assertRaises(Conflict):
                self.ledger.export(self.out())
            self.assertEqual(Path(self.out()).read_bytes(), payload)
        Path(self.out()).write_bytes(original)
        self.assertEqual(self.ledger.export(self.out()), 0)

    def test_export_does_not_create_generation(self):
        epoch = self.ledger.claim('p', 'w')
        self.ledger.ingest('p', 'w', epoch, batch1())
        self.ledger.export(self.out())
        self.assertEqual(self.ledger.view()['generation'], 1)

    def test_independent_targets(self):
        epoch = self.ledger.claim('p', 'w')
        self.ledger.ingest('p', 'w', epoch, batch1())
        self.assertEqual(self.ledger.export(self.out('a.ndjson')), 1)
        self.assertEqual(self.ledger.export(self.out('b.ndjson')), 1)
        self.ledger.ingest('p', 'w', epoch, [order('o2', 'o2', 3, 1, 'c', 0, 5)])
        self.assertEqual(self.ledger.export(self.out('a.ndjson')), 1)
        self.assertEqual(self.ledger.export(self.out('b.ndjson')), 1)


class ConcurrencyTests(RecoveryCase):
    def test_concurrent_claims_get_unique_epochs(self):
        barrier = threading.Barrier(6)
        epochs = []
        errors = []

        def worker(index):
            ledger = Ledger(self.path)
            try:
                barrier.wait()
                epochs.append(ledger.claim('p', f'w{index}'))
            except Exception as exc:
                errors.append(exc)
            finally:
                ledger.close()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(sorted(epochs), [1, 2, 3, 4, 5, 6])

    def test_concurrent_exporters_produce_clean_prefix(self):
        epoch = self.ledger.claim('p', 'w')
        for index in range(5):
            self.ledger.ingest('p', 'w', epoch, [
                order(f'o{index}', f'o{index}', index + 1, 1, 'c', 0, 100)])
        barrier = threading.Barrier(2)
        errors = []

        def worker():
            ledger = Ledger(self.path)
            try:
                barrier.wait()
                ledger.export(self.out())
            except Exception as exc:
                errors.append(exc)
            finally:
                ledger.close()

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        frames = self.ledger.committed_frames()
        self.assertEqual(Path(self.out()).read_bytes(), b''.join(frames))

    def test_concurrent_ingest_and_export(self):
        partitions = [('pa', 'wa'), ('pb', 'wb')]
        for partition, owner in partitions:
            self.ledger.claim(partition, owner)
        self.ledger.ingest('pa', 'wa', 1, [
            plan('plan-1', 'pl', 1, 1, 'c', 0, None, 'basic', 0, 0)])
        barrier = threading.Barrier(3)
        errors = []

        def ingester(partition, owner):
            ledger = Ledger(self.path)
            try:
                barrier.wait()
                start = 2 if partition == 'pa' else 1
                for index in range(10):
                    ledger.ingest(partition, owner, 1, [
                        order(f'{partition}-e{index}', f'{partition}-o{index}',
                              start + index, 1, 'c', 0, 100)])
            except Exception as exc:
                errors.append(exc)
            finally:
                ledger.close()

        def exporter():
            ledger = Ledger(self.path)
            try:
                barrier.wait()
                for _ in range(10):
                    ledger.export(self.out())
            except Exception as exc:
                errors.append(exc)
            finally:
                ledger.close()

        threads = [threading.Thread(target=ingester, args=pair) for pair in partitions]
        threads.append(threading.Thread(target=exporter))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        view = self.ledger.view()
        self.assertEqual(view['checkpoints'], {'pa': 11, 'pb': 10})
        total = sum(group['count'] for group in view['groups'])
        self.assertEqual(total, 20)
        self.ledger.export(self.out())
        frames = self.ledger.committed_frames()
        self.assertEqual(Path(self.out()).read_bytes(), b''.join(frames))

    def test_concurrent_export_processes_do_not_duplicate_or_interleave(self):
        epoch = self.ledger.claim('p', 'w')
        for index in range(6):
            self.ledger.ingest('p', 'w', epoch, [
                order(f'e{index}', f'o{index}', index + 1, 1, 'c', 0, 100)])
        self.ledger.close()
        processes = [multiprocessing.Process(
            target=_export_worker, args=(self.path, self.out())) for _ in range(3)]
        for process in processes:
            process.start()
        for process in processes:
            process.join(30)
            self.assertEqual(process.exitcode, 0)
        self.ledger = Ledger(self.path)
        frames = self.ledger.committed_frames()
        self.assertEqual(Path(self.out()).read_bytes(), b''.join(frames))
