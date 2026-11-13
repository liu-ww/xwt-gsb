import json
from pathlib import Path
class Compactor:
 def __init__(self,root):self.root=Path(root);self.root.mkdir(parents=True,exist_ok=True);self.snapshots={};self.head=None
 def commit(self,snapshot_id,parent,segments,fault=None):
  rows=[]
  for p in segments:rows.extend(json.loads(x) for x in Path(p).read_text(encoding='utf-8').splitlines() if x)
  self.snapshots[snapshot_id]={'parent':parent,'rows':rows,'generation':len(self.snapshots)+1};self.head=snapshot_id;return {'snapshot_id':snapshot_id,'generation':len(self.snapshots),'applied':True}
 def read(self,snapshot_id=None):return sorted(self.snapshots[snapshot_id or self.head]['rows'],key=lambda x:(x['partition'],x['pk']))
 def compact(self,snapshot_id):return {'snapshot_id':snapshot_id,'rows':len(self.read(snapshot_id))}
 def vacuum(self,retain,grace_before):return []

