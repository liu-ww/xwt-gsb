import hashlib
import hmac
import json
import sqlite3
from pathlib import Path

from .codec import b64decode, loads


class Invalid(ValueError): pass
class Replay(ValueError): pass
class Sequence(ValueError): pass
class Signature(ValueError): pass
class Fenced(ValueError): pass


class Gateway:
    """Incomplete v1 prototype retained as the bugfix starting point."""
    def __init__(self, db, keys):
        self.db = sqlite3.connect(db, timeout=30)
        self.keys = loads(Path(keys).read_bytes() if isinstance(keys, (str, Path)) else keys)
        self.db.execute('CREATE TABLE IF NOT EXISTS seen(jti TEXT PRIMARY KEY)')
        self.db.commit()

    def close(self):
        self.db.close()

    def verify(self, request, token, now, fault=None):
        if isinstance(request, (str, Path)):
            request = loads(Path(request).read_bytes())
        parts = token.strip().split('.')
        if len(parts) != 3:
            raise Invalid('bad token')
        header, payload = loads(b64decode(parts[0])), loads(b64decode(parts[1]))
        key = next((item for item in self.keys['keys'] if item['kid'] == header['kid']), None)
        if key is None:
            raise Signature('unknown key')
        expected = hmac.new(b64decode(key.get('material', key.get('n', ''))),
                            (parts[0] + '.' + parts[1]).encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(expected, b64decode(parts[2])):
            raise Signature('bad signature')
        if now >= payload['exp']:
            raise Invalid('expired')
        if request['method'] != payload['method'] or request['target'] != payload['target']:
            raise Invalid('request mismatch')
        return {'issuer': payload['iss'], 'subject': payload['sub'], 'tenant': payload['tenant'],
                'jti': payload['jti'], 'seq': payload['seq']}

    def verify_many(self, items, now, fault=None):
        return [self.verify(item['request'], item['token'], now, fault) for item in items]

