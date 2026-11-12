import base64
import json


def b64decode(text):
    return base64.urlsafe_b64decode(text + '=' * (-len(text) % 4))


def loads(data):
    if isinstance(data, bytes):
        data = data.decode('utf-8')
    return json.loads(data)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)

