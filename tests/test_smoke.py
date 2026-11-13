import shutil, unittest, uuid
from pathlib import Path
from sessionmill import SessionEngine

class Smoke(unittest.TestCase):
    def test_ordered_events(self):
        root=Path(__file__).parent/'.work'/uuid.uuid4().hex;root.mkdir(parents=True)
        try:
            engine=SessionEngine(root/'s.sqlite');events=[{'tenant':'t','user':'u','event_id':'a','event_time':10,'arrival_seq':1,'kind':'open'},{'tenant':'t','user':'u','event_id':'b','event_time':20,'arrival_seq':2,'kind':'ping'}]
            engine.ingest(events,{'t':20});self.assertEqual(engine.sessions()[0]['event_count'],2);engine.close()
        finally:shutil.rmtree(root,ignore_errors=True)

