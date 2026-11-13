import shutil,unittest,uuid
from pathlib import Path
from timeweave import TemporalStore
class Smoke(unittest.TestCase):
 def test_latest(s):
  r=Path(__file__).parent/'.work'/uuid.uuid4().hex;r.mkdir(parents=True)
  try:
   x=TemporalStore(r/'x.sqlite');d={'entity':'e','valid_from':'2024-01-01T00:00:00Z','valid_to':None,'system_from':'2024-01-01T00:00:00Z','system_to':None,'revision':1,'attrs':{'tier':'a'}};f={'fact_id':'f','entity':'e','event_time':'2024-02-01T00:00:00Z','observed_at':'2024-02-02T00:00:00Z','metrics':{'v':1}};x.apply([d],[f]);s.assertEqual(x.join('2024-03-01T00:00:00Z')[0]['dimension']['tier'],'a');x.close()
  finally:shutil.rmtree(r,ignore_errors=True)

