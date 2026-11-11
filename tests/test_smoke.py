import json
from pathlib import Path
import uuid
import unittest
from reconcile.store import Ledger, Fenced


class Smoke(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parents[1]/'artifacts'
        root.mkdir(exist_ok=True)
        self.case = root/uuid.uuid4().hex
        self.case.mkdir()
        self.ledger = Ledger(str(self.case/'ledger.sqlite'))

    def tearDown(self):
        self.ledger.close()

    def test_basic_order(self):
        rows = json.loads((Path(__file__).resolve().parents[1]/'examples/batch.json').read_text(encoding='utf-8'))
        epoch = self.ledger.claim('p','one')
        self.ledger.ingest('p','one',epoch,rows)
        self.assertEqual(self.ledger.view()['groups'],[{'day':0,'segment':'basic','count':1,'gross_cents':100,'net_cents':90}])
        self.assertEqual(self.ledger.view()['pending'],[])

    def test_takeover(self):
        epoch = self.ledger.claim('p','one')
        self.ledger.claim('p','two')
        with self.assertRaises(Fenced):
            self.ledger.ingest('p','one',epoch,[])

    def test_empty_database(self):
        self.assertEqual(self.ledger.view()['groups'],[])
        self.assertEqual(self.ledger.view()['pending'],[])


if __name__ == '__main__':
    unittest.main()
