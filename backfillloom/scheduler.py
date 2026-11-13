import sqlite3
from pathlib import Path
class Scheduler:
 def __init__(self,db):self.db=sqlite3.connect(Path(db));self.plan=None;self.done=set()
 def close(self):self.db.close()
 def load(self,plan):self.plan=plan;return {'instances':len(plan['partitions'])*len(plan['tasks'])}
 def extend(self,plan):self.plan=plan
 def claim(self,worker,now,lease_seconds,limit=1,fault=None):
  out=[]
  for p in self.plan['partitions']:
   for t in self.plan['tasks']:
    k=t['name']+'@'+p
    if k not in self.done and len(out)<limit:out.append({'instance':k,'task':t['name'],'partition':p,'attempt':1,'lease_token':worker+'-'+k})
  return out
 def complete(self,instance,lease_token,output_digest,now):self.done.add(instance);return {'instance':instance,'status':'success'}
 def fail(self,instance,lease_token,error,now):return {'instance':instance,'status':'pending'}
 def status(self):return {'success':len(self.done)}

