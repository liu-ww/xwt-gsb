import csv,json
from pathlib import Path
class QualityRun:
 def __init__(self,root):self.root=Path(root);self.root.mkdir(parents=True,exist_ok=True)
 def evaluate(self,plan,fault=None):
  plan=json.loads(Path(plan).read_text(encoding='utf-8')) if not isinstance(plan,dict) else plan;results=[]
  for rule in plan['rules']:
   if rule['type']!='not_null':continue
   ds=plan['datasets'][rule['dataset']]
   with open(ds['path'],newline='',encoding='utf-8') as f:rows=list(csv.DictReader(f))
   bad=[r[ds['primary_key']] for r in rows if not r.get(rule['column'])]
   results.append({'id':rule['id'],'status':'fail' if bad else 'pass','checked':len(rows),'violations':len(bad),'violation_keys':bad,'digest':''})
  return {'run_id':plan['run_id'],'status':'failed' if any(x['status']=='fail' for x in results) else 'passed','rules':results}

