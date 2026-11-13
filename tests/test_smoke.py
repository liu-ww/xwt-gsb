import json,shutil,unittest,uuid
from pathlib import Path
from icecrate import Compactor
class Smoke(unittest.TestCase):
 def test_commit(s):
  r=Path(__file__).parent/'.work'/uuid.uuid4().hex;r.mkdir(parents=True)
  try:
   p=r/'a.jsonl';p.write_text(json.dumps({'pk':'1','partition':'d=1','version':1,'op':'upsert','data':{'v':1}})+'\n');c=Compactor(r/'c');c.commit('s1',None,[p]);s.assertEqual(c.read()[0]['pk'],'1')
  finally:shutil.rmtree(r,ignore_errors=True)

