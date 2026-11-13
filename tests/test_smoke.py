import shutil,unittest,uuid,zipfile
from pathlib import Path
from vaultunpack import Extractor
class Smoke(unittest.TestCase):
 def test_plain_zip(s):
  r=Path(__file__).parent/'.work'/uuid.uuid4().hex;r.mkdir(parents=True)
  try:
   z=r/'a.zip'
   with zipfile.ZipFile(z,'w') as f:f.writestr('a.txt',b'ok')
   p={'max_files':5,'max_file_size':100,'max_total_size':100,'max_ratio':10,'max_depth':3,'allowed_methods':['stored','deflated']};Extractor(p).extract(z,r/'out');s.assertEqual((r/'out/a.txt').read_bytes(),b'ok')
  finally:shutil.rmtree(r,ignore_errors=True)

