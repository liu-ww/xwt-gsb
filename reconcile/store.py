import json
import sqlite3
from .projection import rebuild


class Conflict(ValueError):
    pass


class Fenced(ValueError):
    pass


class Invalid(ValueError):
    pass


class Ledger:
    def __init__(self, path):
        self.db = sqlite3.connect(path, timeout=30)
        self.db.execute('CREATE TABLE IF NOT EXISTS partitions(partition TEXT PRIMARY KEY, owner TEXT NOT NULL, epoch INTEGER NOT NULL, checkpoint INTEGER NOT NULL)')
        self.db.execute('CREATE TABLE IF NOT EXISTS history(partition TEXT NOT NULL, seq INTEGER NOT NULL, event_id TEXT NOT NULL, entity TEXT NOT NULL, entity_key TEXT NOT NULL, rev INTEGER NOT NULL, value_json TEXT NOT NULL, PRIMARY KEY(partition,seq))')
        if self.db.execute('PRAGMA user_version').fetchone()[0] == 0:
            self.db.execute('PRAGMA user_version=1')
        self.db.commit()

    def claim(self, partition, owner):
        old = self.db.execute('SELECT epoch FROM partitions WHERE partition=?', (partition,)).fetchone()
        epoch = old[0]+1 if old else 1
        self.db.execute('INSERT INTO partitions VALUES(?,?,?,0) ON CONFLICT(partition) DO UPDATE SET owner=excluded.owner,epoch=excluded.epoch', (partition,owner,epoch))
        self.db.commit()
        return epoch

    def ingest(self, partition, owner, epoch, rows, fault=None):
        p = self.db.execute('SELECT owner,epoch,checkpoint FROM partitions WHERE partition=?', (partition,)).fetchone()
        if not p or p[:2] != (owner,epoch):
            raise Fenced('collector no longer owns partition')
        applied = 0
        with self.db:
            for row in rows:
                if row['seq'] <= p[2]:
                    continue
                self.db.execute('INSERT INTO history VALUES(?,?,?,?,?,?,?)', (partition,row['seq'],row['event_id'],row['entity'],row['key'],row['rev'],json.dumps(row['value'])))
                self.db.execute('UPDATE partitions SET checkpoint=? WHERE partition=?', (row['seq'],partition))
                applied += 1
            if fault:
                fault('after_history')
        return dict(checkpoint=self.view()['checkpoints'][partition], generation=len(self.view()['groups']), applied=applied, recomputed=applied)

    def view(self):
        value = rebuild(self.db)
        value['checkpoints'] = dict(self.db.execute('SELECT partition,checkpoint FROM partitions ORDER BY partition'))
        value['generation'] = len(value['groups'])
        return value

    def export(self, path, fault=None):
        from .exporter import append_snapshot
        return append_snapshot(self, path, fault)

    def close(self):
        self.db.close()
