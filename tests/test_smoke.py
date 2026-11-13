import json,shutil,unittest,uuid
from pathlib import Path
from rulequarry import QualityRun
class Smoke(unittest.TestCase):
 def test_not_null(s):
  r=Path(__file__).parent/'.work'/uuid.uuid4().hex;r.mkdir(parents=True)
  try:
   csv=r/'a.csv';csv.write_text('id,name\n1,A\n2,\n');plan={'run_id':'r1','datasets':{'a':{'path':str(csv),'primary_key':'id','schema':{'id':'int','name':'string'}}},'rules':[{'id':'n','type':'not_null','dataset':'a','column':'name','params':{},'severity':'error','depends_on':[]}]};out=QualityRun(r/'run').evaluate(plan);s.assertEqual(out['rules'][0]['violations'],1)
  finally:shutil.rmtree(r,ignore_errors=True)

