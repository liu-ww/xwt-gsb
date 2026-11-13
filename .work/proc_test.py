import json, subprocess, sys, uuid, shutil, time
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))
from backfillloom import Scheduler
plan={'plan_id':'proc','partitions':['2024-01-01'],'tasks':[dict(name='a',depends_on=[],max_attempts=2,fan_in='same_partition',command_digest='a')]}
d=Path('.work')/('proc-'+uuid.uuid4().hex); d.mkdir(parents=True)
db=d/'x.sqlite'; s=Scheduler(str(db)); s.load(plan); s.close()
start=d/'go'; outdir=d/'out'; outdir.mkdir()
procs=[]
for i in range(6):
    out=outdir/str(i)
    procs.append(subprocess.Popen([sys.executable, str(Path('.work/proc_runner.py').resolve()), str(db), str(start), str(out), str(i)]))
time.sleep(0.2); start.write_text('go')
codes=[p.wait(timeout=30) for p in procs]
assert all(c==0 for c in codes), codes
results=[json.loads(p.read_text()) for p in outdir.iterdir()]
wins=[r for r in results if r]
assert len(wins)==1 and wins[0][0]['attempt']==1, results
s=Scheduler(str(db)); st=s.status(); assert st['running']==1 and st['instances'][0]['attempt']==1, st; s.close()
shutil.rmtree(d)
print('PROCESS CONCURRENCY PASSED')