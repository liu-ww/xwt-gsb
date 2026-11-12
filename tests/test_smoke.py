import hashlib
import json
import shutil
import unittest
import uuid
from pathlib import Path

from cdcforge import Warehouse


def canon(value): return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()


class Smoke(unittest.TestCase):
    def test_single_batch(self):
        root = Path(__file__).parent / '.work' / uuid.uuid4().hex
        root.mkdir(parents=True)
        try:
            batch = root / 'batch'; batch.mkdir()
            schema = {'version': 1, 'fields': [{'name': 'name', 'type': 'string', 'nullable': False, 'default': None}]}
            events = [{'op': 'upsert', 'pk': '1', 'event_time': 100, 'source_seq': 1, 'schema_version': 1, 'data': {'name': 'A'}}]
            raw = b''.join(canon(event) + b'\n' for event in events)
            (batch / 'schema.json').write_bytes(canon(schema)); (batch / 'events.jsonl').write_bytes(raw)
            manifest = {'format': 1, 'batch_id': 'b1', 'source': 'crm', 'previous_batch': None, 'schema_version': 1,
                        'schema_sha256': hashlib.sha256(canon(schema)).hexdigest(), 'events_sha256': hashlib.sha256(raw).hexdigest(),
                        'event_count': 1, 'watermark': 100}
            (batch / 'manifest.json').write_bytes(canon(manifest))
            warehouse = Warehouse(root / 'warehouse')
            self.assertTrue(warehouse.apply_batch(batch)['applied'])
            self.assertEqual(warehouse.query()[0]['name'], 'A')
            warehouse.close()
        finally:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == '__main__': unittest.main()

