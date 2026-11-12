import base64, hashlib, hmac, json, shutil, unittest, uuid
from pathlib import Path
from securehook import Gateway


def b64(data): return base64.urlsafe_b64encode(data).rstrip(b'=').decode()


class Smoke(unittest.TestCase):
    def test_basic_hs256(self):
        root=Path(__file__).parent/'.work'/uuid.uuid4().hex
        root.mkdir(parents=True)
        try:
            secret=b's'*32
            keys={'version':2,'issuer':'issuer','audience':'hooks','epoch':1,'keys':[{'kid':'h1','kty':'oct','alg':'HS256','material':b64(secret),'epoch':1,'active_from':0,'retire_at':None,'tenants':['t']}]}
            path=root/'keys.json'; path.write_text(json.dumps(keys),encoding='utf-8')
            body=b'{}'; request={'method':'POST','target':'/hook?a=1','body_b64':b64(body)}
            header={'alg':'HS256','kid':'h1','typ':'SHK2'}
            payload={'iss':'issuer','sub':'svc','aud':'hooks','tenant':'t','iat':100,'nbf':100,'exp':200,'jti':'j1','seq':1,'method':'POST','target':'/hook?a=1','body_sha256':hashlib.sha256(body).hexdigest(),'key_epoch':1}
            first=b64(json.dumps(header,separators=(',',':'),sort_keys=True).encode()); second=b64(json.dumps(payload,separators=(',',':'),sort_keys=True).encode())
            token=first+'.'+second+'.'+b64(hmac.new(secret,(first+'.'+second).encode(),hashlib.sha256).digest())
            gateway=Gateway(root/'state.sqlite',path)
            self.assertEqual(gateway.verify(request,token,120)['jti'],'j1')
            gateway.close()
        finally:
            shutil.rmtree(root,ignore_errors=True)


if __name__ == '__main__': unittest.main()
