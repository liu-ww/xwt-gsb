"""Generate the documented legacy fixture, never overwrite an existing file."""
import json
from pathlib import Path
import sqlite3
import sys


def create(path):
    if Path(path).exists():
        raise FileExistsError(path)
    db = sqlite3.connect(path)
    db.execute('CREATE TABLE partitions(partition TEXT PRIMARY KEY, owner TEXT NOT NULL, epoch INTEGER NOT NULL, checkpoint INTEGER NOT NULL)')
    db.execute('CREATE TABLE history(partition TEXT NOT NULL, seq INTEGER NOT NULL, event_id TEXT NOT NULL, entity TEXT NOT NULL, entity_key TEXT NOT NULL, rev INTEGER NOT NULL, value_json TEXT NOT NULL, PRIMARY KEY(partition,seq))')
    db.execute('INSERT INTO partitions VALUES("legacy","worker",7,3)')
    records = [('e1','plan','plan-a',1,{'customer':'c1','start':0,'end':None,'segment':'standard','discount_bps':1000,'priority':0}),('e2','order','o1',2,{'customer':'c1','at':1,'gross_cents':300}),('e3','order','o1',1,{'customer':'c1','at':1,'gross_cents':100})]
    for seq,(event,entity,key,rev,value) in enumerate(records,1):
        db.execute('INSERT INTO history VALUES(?,?,?,?,?,?,?)',('legacy',seq,event,entity,key,rev,json.dumps(value)))
    db.execute('PRAGMA user_version=1'); db.commit(); db.close()


if __name__ == '__main__':
    create(sys.argv[1])
