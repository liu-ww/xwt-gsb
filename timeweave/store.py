import sqlite3,json
from pathlib import Path
class TemporalStore:
 def __init__(self,db):
  self.db=sqlite3.connect(Path(db));self.db.execute('CREATE TABLE IF NOT EXISTS dims(entity TEXT,revision INTEGER,attrs TEXT)');self.db.execute('CREATE TABLE IF NOT EXISTS facts(fact_id TEXT PRIMARY KEY,entity TEXT,event_time TEXT,observed_at TEXT,metrics TEXT)');self.db.commit()
 def close(self):self.db.close()
 def apply(self,dimensions,facts,fault=None):
  for d in dimensions:self.db.execute('INSERT INTO dims VALUES(?,?,?)',(d['entity'],d['revision'],json.dumps(d['attrs'])))
  for f in facts:self.db.execute('INSERT OR IGNORE INTO facts VALUES(?,?,?,?,?)',(f['fact_id'],f['entity'],f['event_time'],f['observed_at'],json.dumps(f['metrics'])))
  self.db.commit();return {'dimensions':len(dimensions),'facts':len(facts)}
 def join(self,as_of):
  out=[]
  for fid,e,et,oa,m in self.db.execute('SELECT fact_id,entity,event_time,observed_at,metrics FROM facts ORDER BY fact_id'):
   d=self.db.execute('SELECT revision,attrs FROM dims WHERE entity=? ORDER BY revision DESC LIMIT 1',(e,)).fetchone();out.append({'fact_id':fid,'entity':e,'event_time':et,'observed_at':oa,'metrics':json.loads(m),'dimension':None if d is None else json.loads(d[1]),'dimension_revision':None if d is None else d[0],'unmatched':d is None})
  return out

