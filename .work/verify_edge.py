import shutil
import uuid
from pathlib import Path

from rulequarry import QualityRun

PASS = 0
FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("ok   -", name)
    else:
        FAIL += 1
        print("FAIL -", name, detail)


def rule(rid, rtype, col, params=None, severity="error", depends_on=None, ds="d"):
    return {"id": rid, "type": rtype, "dataset": ds, "column": col,
            "params": params or {}, "severity": severity, "depends_on": depends_on or []}


SCHEMA = {"id": "int", "v": "string"}
CLEAN = "id,v\n1,a\n2,b\n3,c\n"
DUP = "id,v\n1,a\n2,a\n"


def plan(run_id, rules, path="s.csv"):
    return {"run_id": run_id,
            "datasets": {"d": {"path": path, "primary_key": "id", "schema": SCHEMA}},
            "rules": rules}


base = Path(__file__).parent / ("edge-" + uuid.uuid4().hex)
base.mkdir(parents=True, exist_ok=True)
try:
    # ---- DAG: declaration order preserved, deps still gate execution ----
    store = base / "store"
    store.mkdir(parents=True, exist_ok=True)
    (store / "s.csv").write_text(CLEAN, encoding="utf-8")
    dag = plan("dag", [
        rule("z-root", "not_null", "v"),
        rule("a-root", "not_null", "v"),
        rule("m-mid", "regex", "v", {"pattern": "[abc]"}, depends_on=["z-root", "a-root"]),
        rule("b-leaf", "unique", "v", depends_on=["m-mid"]),
    ])
    out = QualityRun(store).evaluate(dag)
    order = [r["id"] for r in out["rules"]]
    check("output keeps plan declaration order", order == ["z-root", "a-root", "m-mid", "b-leaf"], order)
    check("all dag rules pass", all(r["status"] == "pass" for r in out["rules"]))

    # ---- warn-only failure leaves overall status passed ----
    warn_store = base / "warn"
    warn_store.mkdir(parents=True, exist_ok=True)
    (warn_store / "s.csv").write_text(DUP, encoding="utf-8")
    wout = QualityRun(warn_store).evaluate(plan("warnonly", [rule("w", "unique", "v", severity="warn")]))
    check("warn rule itself fails", wout["rules"][0]["status"] == "fail")
    check("warn-only overall passed", wout["status"] == "passed", wout["status"])

    # ---- cache reuse equals full parse, identical across reruns ----
    full_root = base / "full"
    inc_root = base / "inc"
    for target in (full_root, inc_root):
        target.mkdir(parents=True, exist_ok=True)
        (target / "s.csv").write_text(CLEAN, encoding="utf-8")
    cq = plan("cache-eq", [rule("n", "not_null", "v"), rule("u", "unique", "v"),
                           rule("rx", "regex", "v", {"pattern": "[abc]"})])
    full = QualityRun(full_root).evaluate(cq)
    first = QualityRun(inc_root).evaluate(cq)
    second = QualityRun(inc_root).evaluate(cq)
    check("rerun with cache identical", first == second)
    check("cached result equals full run", second == full)
    check("cache file present", (inc_root / "cache" / "d.json").is_file())

    # ---- deterministic published bytes across independent roots ----
    det_root = base / "det"
    det_root.mkdir(parents=True, exist_ok=True)
    (det_root / "s.csv").write_text(CLEAN, encoding="utf-8")
    QualityRun(det_root).evaluate(cq)
    s1 = next((full_root / "runs" / "cache-eq").glob("summary-*.json")).read_bytes()
    s2 = next((det_root / "runs" / "cache-eq").glob("summary-*.json")).read_bytes()
    check("summary bytes deterministic", s1 == s2)

    # ---- clean rerun removes a previously published quarantine file ----
    q_root = base / "qclean"
    q_root.mkdir(parents=True, exist_ok=True)
    (q_root / "s.csv").write_text(DUP, encoding="utf-8")
    qrun = QualityRun(q_root)
    qrun.evaluate(plan("q-run-1", [rule("u", "unique", "v")]))
    check("failing run publishes quarantine", (q_root / "quarantine" / "d.jsonl").is_file())
    (q_root / "s.csv").write_text(CLEAN, encoding="utf-8")
    qrun.evaluate(plan("q-run-2", [rule("u", "unique", "v")]))
    check("clean run drops stale quarantine file", not (q_root / "quarantine" / "d.jsonl").exists())

    print()
    print("PASS", PASS, "FAIL", FAIL)
finally:
    shutil.rmtree(base, ignore_errors=True)
raise SystemExit(1 if FAIL else 0)