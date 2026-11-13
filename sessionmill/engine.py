import sqlite3
from pathlib import Path


class SessionEngine:
    """Incomplete ordered-event prototype."""
    def __init__(self, db):
        self.db = sqlite3.connect(Path(db))
        self.db.execute('CREATE TABLE IF NOT EXISTS events(tenant TEXT,user TEXT,event_id TEXT PRIMARY KEY,event_time INTEGER,arrival_seq INTEGER,kind TEXT)')
        self.db.commit()

    def close(self): self.db.close()

    def ingest(self, events, watermarks, fault=None):
        for item in events:
            self.db.execute('INSERT OR IGNORE INTO events VALUES(?,?,?,?,?,?)', (item['tenant'], item['user'], item['event_id'], item['event_time'], item['arrival_seq'], item['kind']))
        self.db.commit()
        return {'accepted': len(events), 'sessions': len(self.sessions())}

    def sessions(self):
        rows = []
        for tenant, user, event_id, when, seq, kind in self.db.execute('SELECT tenant,user,event_id,event_time,arrival_seq,kind FROM events ORDER BY tenant,user,arrival_seq'):
            if not rows or rows[-1]['tenant'] != tenant or rows[-1]['user'] != user or when - rows[-1]['end'] > 30:
                rows.append({'session_id': event_id, 'tenant': tenant, 'user': user, 'start': when, 'end': when, 'event_count': 1, 'closed': kind == 'close', 'revision': 1})
            else:
                rows[-1]['end'] = when; rows[-1]['event_count'] += 1; rows[-1]['closed'] = kind == 'close'
        return rows

    def changes(self, since=0): return []

