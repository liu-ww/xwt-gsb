import json,zipfile
from pathlib import Path
class Extractor:
 def __init__(self,policy):self.policy=policy
 def extract(self,archive,destination,fault=None):
  destination=Path(destination);destination.mkdir(parents=True)
  with zipfile.ZipFile(archive) as z:z.extractall(destination);names=z.namelist()
  (destination/'MANIFEST.json').write_text(json.dumps({'files':sorted(names)}),encoding='utf-8')
  return {'destination':str(destination),'files':len(names)}

