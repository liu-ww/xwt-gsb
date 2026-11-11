import json
import os
import sqlite3
import unittest
from pathlib import Path
import uuid

from reconcile.projection import canonical, net_cents
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


class LedgerCase(unittest.TestCase):
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

    def claim(self, partition='p', owner='w'):
        return self.ledger.claim(partition, owner)


class RoundingTests(unittest.TestCase):
    def test_half_even_including_negatives(self):
        cases = {
            (100, 5): 100,
            (105, 0): 105,
            (150, 0): 150,
            (1, 5000): 0,
            (3, 5000): 2,
            (-1, 5000): 0,
            (-3, 5000): -2,
            (-5, 5000): -2,
            (5, 5000): 2,
            (100, 1000): 90,
            (-100, 1000): -90,
        }
        for (gross, bps), expected in cases.items():
            self.assertEqual(net_cents(gross, bps), expected, (gross, bps))


class ValidationTests(LedgerCase):
    def test_bool_rejected_as_integer(self):
        epoch = self.claim()
        bad = order('e', 'o1', 1, 1, 'c1', True, 100)
        with self.assertRaises(Invalid):
            self.ledger.ingest('p', 'w', epoch, [bad])

    def test_structure_errors_are_invalid(self):
        epoch = self.claim()
        cases = [
            [{'seq': 2, 'event_id': 'e', 'entity': 'order', 'key': 'o', 'rev': 1,
              'value': {'customer': 'c', 'at': 0, 'gross_cents': 1}}],
            [order('e', 'o', 1, 1, 'c', 0, 1), order('e2', 'o2', 3, 1, 'c', 0, 1)],
            [plan('e', 'pl', 1, 1, 'c', 5, 5, 's', 0, 0)],
            [plan('e', 'pl', 1, 1, 'c', 0, None, 's', 10001, 0)],
            [order('e', '', 1, 1, 'c', 0, 1)],
            [order('e', 'o', 0, 1, 'c', 0, 1)],
        ]
        for rows in cases:
            with self.assertRaises(Invalid):
                self.ledger.ingest('p', 'w', epoch, rows)
        self.assertEqual(self.ledger.view()['checkpoints'], {'p': 0})

    def test_empty_batch_still_checks_lease(self):
        self.claim()
        with self.assertRaises(Fenced):
            self.ledger.ingest('p', 'other', 1, [])
        result = self.ledger.ingest('p', 'w', 1, [])
        self.assertEqual(result, {'checkpoint': 0, 'generation': 0,
                                  'applied': 0, 'recomputed': 0})

    def test_unknown_partition_is_fenced(self):
        with self.assertRaises(Fenced):
            self.ledger.ingest('ghost', 'w', 1, [])


class ContractViewTests(LedgerCase):
    def test_contract_view_example_exact(self):
        epoch = self.claim('p', 'w')
        rows = json.loads((Path(__file__).resolve().parents[1]
                           / 'examples' / 'batch.json').read_text(encoding='utf-8'))
        result = self.ledger.ingest('p', 'w', epoch, rows)
        self.assertEqual(result, {'checkpoint': 2, 'generation': 1,
                                  'applied': 2, 'recomputed': 1})
        self.assertEqual(self.ledger.view(), {
            'groups': [{'day': 0, 'segment': 'basic', 'count': 1,
                        'gross_cents': 100, 'net_cents': 90}],
            'pending': [],
            'checkpoints': {'p': 2},
            'generation': 1})

    def test_zero_amount_group_survives_but_empty_group_vanishes(self):
        epoch = self.claim()
        self.ledger.ingest('p', 'w', epoch, [
            plan('p1', 'pl', 1, 1, 'c', 0, None, 'basic', 0, 0),
            order('o1', 'o1', 2, 1, 'c', 0, 0)])
        self.assertEqual(self.ledger.view()['groups'], [
            {'day': 0, 'segment': 'basic', 'count': 1, 'gross_cents': 0, 'net_cents': 0}])
        self.ledger.ingest('p', 'w', epoch, [order('o2', 'o1', 3, 2, None, 0, 0)])
        self.assertEqual(self.ledger.view()['groups'], [])

    def test_semantically_identical_new_winner_keeps_generation(self):
        epoch = self.claim()
        self.ledger.ingest('p', 'w', epoch, [
            plan('p1', 'pl', 1, 1, 'c', 0, None, 'basic', 0, 0),
            order('o1', 'o1', 2, 1, 'c', 0, 100)])
        result = self.ledger.ingest('p', 'w', epoch, [
            order('o2', 'o1', 3, 2, 'c', 0, 100)])
        self.assertEqual(result['recomputed'], 1)
        self.assertEqual(result['generation'], 1)
        self.assertEqual(self.ledger.view()['generation'], 1)

    def test_cross_partition_redelivery_consumes_position_without_recompute(self):
        first = self.claim('pa', 'wa')
        second = self.claim('pb', 'wb')
        self.ledger.ingest('pa', 'wa', first, [
            plan('p1', 'pl', 1, 1, 'c', 0, None, 'basic', 0, 0),
            order('e1', 'o1', 2, 1, 'c', 0, 100)])
        result = self.ledger.ingest('pb', 'wb', second, [
            order('e1', 'o1', 1, 1, 'c', 0, 100)])
        self.assertEqual(result['applied'], 1)
        self.assertEqual(result['recomputed'], 0)
        view = self.ledger.view()
        self.assertEqual(view['checkpoints'], {'pa': 2, 'pb': 1})
        self.assertEqual(view['generation'], 1)
        self.assertEqual(view['groups'][0]['count'], 1)

    def test_plan_tombstone_falls_back_to_next_best_plan(self):
        epoch = self.claim()
        self.ledger.ingest('p', 'w', epoch, [
            plan('p1', 'pl1', 1, 1, 'c', 0, None, 'best', 0, 10),
            plan('p2', 'pl2', 2, 1, 'c', 0, None, 'fallback', 0, 0),
            order('o1', 'o1', 3, 1, 'c', 0, 100)])
        self.assertEqual(self.ledger.view()['groups'][0]['segment'], 'best')
        result = self.ledger.ingest('p', 'w', epoch, [
            plan('p3', 'pl1', 4, 2, None, 0, None, '', 0, 0)])
        self.assertEqual(result['recomputed'], 1)
        self.assertEqual(self.ledger.view()['groups'][0]['segment'], 'fallback')
        self.ledger.ingest('p', 'w', epoch, [
            plan('p4', 'pl2', 5, 2, None, 0, None, '', 0, 0)])
        self.assertEqual(self.ledger.view()['groups'], [])
        self.assertEqual(self.ledger.view()['pending'], ['o1'])


class RevisionTests(LedgerCase):
    def test_max_rev_wins_and_late_old_rev_is_ignored(self):
        epoch = self.claim()
        rows = [
            plan('p1', 'pl', 1, 1, 'c', 0, None, 'basic', 1000, 0),
            order('o2', 'o1', 2, 2, 'c', 100, 300),
            order('o1', 'o1', 3, 1, 'c', 100, 100),
        ]
        result = self.ledger.ingest('p', 'w', epoch, rows)
        view = self.ledger.view()
        self.assertEqual(view['groups'], [
            {'day': 0, 'segment': 'basic', 'count': 1, 'gross_cents': 300,
             'net_cents': 270}])
        self.assertEqual(result['recomputed'], 1)
        self.assertEqual(result['generation'], 1)

    def test_tombstone_deletes_and_higher_rev_restores(self):
        epoch = self.claim()
        self.ledger.ingest('p', 'w', epoch, [
            plan('p1', 'pl', 1, 1, 'c', 0, None, 'basic', 0, 0),
            order('o1', 'o1', 2, 1, 'c', 0, 100)])
        self.ledger.ingest('p', 'w', epoch, [
            order('o2', 'o1', 3, 2, None, 0, 0)])
        self.assertEqual(self.ledger.view()['groups'], [])
        self.ledger.ingest('p', 'w', epoch, [
            order('o3', 'o1', 4, 3, 'c', 0, 100)])
        groups = self.ledger.view()['groups']
        self.assertEqual(groups, [
            {'day': 0, 'segment': 'basic', 'count': 1, 'gross_cents': 100,
             'net_cents': 100}])

    def test_same_revision_different_value_is_conflict_in_any_order(self):
        epoch = self.claim()
        self.ledger.ingest('p', 'w', epoch, [
            order('o1', 'o1', 1, 2, 'c', 0, 100)])
        with self.assertRaises(Conflict):
            self.ledger.ingest('p', 'w', epoch, [
                order('o2', 'o1', 2, 2, 'c', 0, 101)])
        self.ledger.ingest('p', 'w', epoch, [
            order('o3', 'o1', 2, 1, 'c', 0, 100)])
        with self.assertRaises(Conflict):
            self.ledger.ingest('p', 'w', epoch, [
                order('o4', 'o1', 3, 1, 'c', 0, 101)])
        self.assertEqual(self.ledger.view()['checkpoints'], {'p': 2})

    def test_event_identity_conflict_rejects_whole_batch(self):
        epoch = self.claim()
        self.ledger.ingest('p', 'w', epoch, [
            order('e1', 'o1', 1, 1, 'c', 0, 100)])
        with self.assertRaises(Conflict):
            self.ledger.ingest('p', 'w', epoch, [
                order('e1', 'o9', 2, 1, 'c', 0, 100)])

    def test_replay_must_match_and_redelivery_consumes_new_position(self):
        epoch = self.claim()
        self.ledger.ingest('p', 'w', epoch, [
            order('e1', 'o1', 1, 1, 'c', 0, 100)])
        with self.assertRaises(Conflict):
            self.ledger.ingest('p', 'w', epoch, [
                order('e1', 'o1', 1, 1, 'c', 0, 101)])
        result = self.ledger.ingest('p', 'w', epoch, [
            order('e1', 'o1', 1, 1, 'c', 0, 100),
            order('e1', 'o1', 2, 1, 'c', 0, 100)])
        self.assertEqual(result['applied'], 1)
        self.assertEqual(result['recomputed'], 0)
        self.assertEqual(self.ledger.view()['generation'], 1)

    def test_json_field_order_is_not_significant(self):
        epoch = self.claim()
        self.ledger.ingest('p', 'w', epoch, [
            {'seq': 1, 'event_id': 'e1', 'entity': 'order', 'key': 'o1', 'rev': 1,
             'value': {'customer': 'c', 'at': 0, 'gross_cents': 100}}])
        reordered = {'seq': 1, 'event_id': 'e1', 'entity': 'order', 'key': 'o1', 'rev': 1,
                     'value': {'gross_cents': 100, 'at': 0, 'customer': 'c'}}
        result = self.ledger.ingest('p', 'w', epoch, [reordered])
        self.assertEqual(result['applied'], 0)

    def test_semantic_duplicate_does_not_advance_generation(self):
        epoch = self.claim()
        first = self.ledger.ingest('p', 'w', epoch, [
            plan('p1', 'pl', 1, 1, 'c', 0, None, 'basic', 0, 0),
            order('o1', 'o1', 2, 1, 'c', 0, 100)])
        second = self.ledger.ingest('p', 'w', epoch, [
            order('o2', 'o2', 3, 1, 'c', 0, 100)])
        third = self.ledger.ingest('p', 'w', epoch, [
            order('o3', 'o2', 4, 1, 'c', 0, 100)])
        self.assertEqual(first['generation'], 1)
        self.assertEqual(second['generation'], 2)
        self.assertEqual(third['generation'], 2)

    def test_multiple_revisions_in_one_batch_only_shows_winner(self):
        epoch = self.claim()
        result = self.ledger.ingest('p', 'w', epoch, [
            plan('p1', 'pl', 1, 1, 'c', 0, None, 'basic', 0, 0),
            order('o1', 'o1', 2, 1, 'c', 0, 100),
            order('o2', 'o1', 3, 2, 'c', 0, 200),
            order('o3', 'o1', 4, 3, 'c', 0, 300)])
        self.assertEqual(result['recomputed'], 1)
        self.assertEqual(self.ledger.view()['groups'][0]['gross_cents'], 300)


class MatchingTests(LedgerCase):
    def test_pending_day_floor_and_plan_tie_breakers(self):
        epoch = self.claim()
        self.ledger.ingest('p', 'w', epoch, [
            order('o1', 'o1', 1, 1, 'c', -1, 100),
            order('o2', 'o2', 2, 1, 'd', 86399, 100),
            order('o3', 'o3', 3, 1, 'e', 0, 100),
            plan('p1', 'a', 4, 1, 'c', -100, 10, 'low', 0, 0),
            plan('p2', 'b', 5, 1, 'c', -100, 10, 'high', 0, 1),
            plan('p3', 'c', 6, 1, 'd', 0, None, 'key-tie-loses', 0, 0),
            plan('p4', 'd', 7, 1, 'd', 0, None, 'key-tie-wins', 0, 0),
            plan('p5', 'e', 8, 1, 'd', 5, None, 'start-wins', 0, 0)])
        view = self.ledger.view()
        self.assertEqual(view['pending'], ['o3'])
        groups = {(g['day'], g['segment']): g for g in view['groups']}
        self.assertEqual(groups[(-1, 'high')]['count'], 1)
        self.assertEqual(groups[(0, 'start-wins')]['count'], 1)

    def test_plan_revision_only_recomputes_affected_customer_window(self):
        epoch = self.claim()
        self.ledger.ingest('p', 'w', epoch, [
            order('o1', 'o1', 1, 1, 'c', 100, 100),
            order('o2', 'o2', 2, 1, 'c', 1000, 100),
            order('o3', 'o3', 3, 1, 'other', 100, 100),
            plan('p1', 'pl', 4, 1, 'c', 0, 500, 'old', 0, 0)])
        before = self.ledger.view()
        self.assertEqual(before['pending'], ['o2', 'o3'])
        result = self.ledger.ingest('p', 'w', epoch, [
            plan('p2', 'pl', 5, 2, 'c', 0, None, 'new', 0, 0)])
        self.assertEqual(result['recomputed'], 2)
        view = self.ledger.view()
        self.assertEqual(view['pending'], ['o3'])
        segments = {g['segment'] for g in view['groups']}
        self.assertEqual(segments, {'new'})
        self.assertEqual(view['generation'], before['generation'] + 1)

    def test_plan_cross_customer_rewrite_moves_only_its_orders(self):
        epoch = self.claim()
        self.ledger.ingest('p', 'w', epoch, [
            order('o1', 'o1', 1, 1, 'c1', 10, 100),
            order('o2', 'o2', 2, 1, 'c2', 10, 100),
            plan('p1', 'pl', 3, 1, 'c1', 0, None, 'seg1', 0, 0),
            plan('p2', 'p2', 4, 1, 'c2', 0, None, 'seg2', 0, 0)])
        self.ledger.ingest('p', 'w', epoch, [
            plan('p3', 'pl', 5, 2, 'c2', 0, None, 'seg2', 0, 0)])
        view = self.ledger.view()
        placement = {g['segment']: g['count'] for g in view['groups']}
        self.assertEqual(placement, {'seg2': 1})
        self.assertEqual(view['pending'], ['o1'])


class LeaseTests(LedgerCase):
    def test_claim_increments_and_fences_old_owner(self):
        first = self.ledger.claim('p', 'one')
        second = self.ledger.claim('p', 'one')
        third = self.ledger.claim('p', 'two')
        self.assertEqual((first, second, third), (1, 2, 3))
        with self.assertRaises(Fenced):
            self.ledger.ingest('p', 'one', second, [])
        with self.assertRaises(Fenced):
            self.ledger.ingest('p', 'two', second, [])
        result = self.ledger.ingest('p', 'two', third, [])
        self.assertEqual(result['checkpoint'], 0)

    def test_checkpoint_survives_takeover(self):
        first = self.ledger.claim('p', 'one')
        self.ledger.ingest('p', 'one', first, [
            order('e1', 'o1', 1, 1, 'c', 0, 100)])
        second = self.ledger.claim('p', 'two')
        with self.assertRaises(Fenced):
            self.ledger.ingest('p', 'one', first, [
                order('e2', 'o2', 2, 1, 'c', 0, 100)])
        result = self.ledger.ingest('p', 'two', second, [
            order('e2', 'o2', 2, 1, 'c', 0, 100)])
        self.assertEqual(result['checkpoint'], 2)
