import hashlib
import json
import sqlite3
from pathlib import Path


class Warehouse:
    """Deliberately incomplete append-style prototype used as the bugfix baseline."""

    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.root / 'state.sqlite', timeout=30)
        self.db.execute('CREATE TABLE IF NOT EXISTS batches(batch_id TEXT PRIMARY KEY, digest TEXT, generation INTEGER)')
        self.db.execute('CREATE TABLE IF NOT EXISTS records(pk TEXT PRIMARY KEY, event_time INTEGER, payload TEXT)')
        self.db.commit()

    def close(self):
        self.db.close()

    def apply_batch(self, batch, fault=None):
        batch = Path(batch)
        manifest = json.loads((batch / 'manifest.json').read_text(encoding='utf-8'))
        existing = self.db.execute('SELECT generation FROM batches WHERE batch_id=?', (manifest['batch_id'],)).fetchone()
        if existing:
            return {'batch_id': manifest['batch_id'], 'generation': existing[0], 'applied': False}
        events = [json.loads(line) for line in (batch / 'events.jsonl').read_text(encoding='utf-8').splitlines() if line]
        for event in events:
            if event['op'] == 'delete':
                self.db.execute('DELETE FROM records WHERE pk=?', (event['pk'],))
            else:
                self.db.execute('INSERT OR REPLACE INTO records VALUES(?,?,?)',
                                (event['pk'], event['event_time'], json.dumps(event['data'], sort_keys=True)))
        generation = self.db.execute('SELECT COALESCE(MAX(generation),0)+1 FROM batches').fetchone()[0]
        digest = hashlib.sha256((batch / 'manifest.json').read_bytes()).hexdigest()
        self.db.execute('INSERT INTO batches VALUES(?,?,?)', (manifest['batch_id'], digest, generation))
        self.db.commit()
        self._write_snapshot(generation)
        return {'batch_id': manifest['batch_id'], 'generation': generation, 'applied': True}

    def apply_batches(self, batches, fault=None):
        return [self.apply_batch(batch, fault) for batch in batches]

    def query(self):
        rows = []
        for pk, event_time, payload in self.db.execute('SELECT pk,event_time,payload FROM records ORDER BY pk'):
            rows.append({'_pk': pk, '_event_time': event_time, **json.loads(payload)})
        return rows

    def _write_snapshot(self, generation):
        current = self.root / 'current'
        current.mkdir(exist_ok=True)
        data = current / 'data.jsonl'
        data.write_text(''.join(json.dumps(row, sort_keys=True, separators=(',', ':')) + '\n' for row in self.query()), encoding='utf-8')
        (current / 'manifest.json').write_text(json.dumps({'generation': generation, 'row_count': len(self.query())}, sort_keys=True), encoding='utf-8')

