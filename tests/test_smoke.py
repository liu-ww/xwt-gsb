import shutil,unittest,uuid
from pathlib import Path
from backfillloom import Scheduler
class Smoke(unittest.TestCase):
 def test_claim(s):
  r=Path(__file__).parent/'.work'/uuid.uuid4().hex;r.mkdir(parents=True)
  try:
   x=Scheduler(r/'x.sqlite');x.load({'plan_id':'p','partitions':['2024-01-01'],'tasks':[{'name':'a','depends_on':[],'max_attempts':2,'fan_in':'same_partition','command_digest':'0'*64}]});s.assertEqual(x.claim('w',0,10)[0]['instance'],'a@2024-01-01');x.close()
  finally:shutil.rmtree(r,ignore_errors=True)

