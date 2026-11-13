import json
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

from rulequarry import QualityRun
from rulequarry.quality import QualityError

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


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def rule(rid, rtype, ds, col, params=None, severity="error", depends_on=None):
    return {"id": rid, "type": rtype, "dataset": ds, "column": col,
            "params": params or {}, "severity": severity, "depends_on": depends_on or []}


root = Path(__file__).parent / ("rq-verify-" + uuid.uuid4().hex)
root.mkdir(parents=True, exist_ok=True)
RUN_ROOT = root / "run"
try:
    # ---- datasets ------------------------------------------------------
    write(RUN_ROOT / "emp.csv",
          "id,name,age,score,active,dept_id\n"
          "1,Ada,30,1.5,true,10\n"
          "2,Bob,,2.0,false,10\n"
          "3,Cyd,25,,TRUE,99\n"
          "4,Dee,40,3.14,0,11\n"
          "5,Eve,17,0.0,1,\n")
    write(RUN_ROOT / "dept.csv", "id,label\n10,Eng\n11,Sales\n")

    emp_schema = {"id": "int", "name": "string", "age": "int", "score": "float",
                  "active": "bool", "dept_id": "int"}
    dept_schema = {"id": "int", "label": "string"}
    datasets = {
        "emp": {"path": "emp.csv", "primary_key": "id", "schema": emp_schema},
        "dept": {"path": "dept.csv", "primary_key": "id", "schema": dept_schema},
    }
    plan = {
        "run_id": "run-1",
        "datasets": datasets,
        "rules": [
            rule("nn", "not_null", "emp", "name"),
            rule("nn_age", "not_null", "emp", "age"),
            rule("uniq", "unique", "emp", "name"),
            rule("rng", "range", "emp", "age", {"inclusive_min": 18, "inclusive_max": 65}),
            rule("rgx", "regex", "emp", "name", {"pattern": "[A-Z][a-z]+"}),
            rule("fk", "foreign_key", "emp", "dept_id", {"dataset": "dept", "column": "id"}),
            rule("agg", "aggregate_count", "dept", "id", {"min": 1, "max": 100}),
            rule("warn_only", "not_null", "emp", "score", severity="warn"),
            rule("child", "not_null", "emp", "name", depends_on=["nn"]),
            rule("blocked_child", "not_null", "emp", "name", depends_on=["fk"]),
        ],
    }

    qr = QualityRun(RUN_ROOT)
    out = qr.evaluate(plan)
    by_id = {r["id"]: r for r in out["rules"]}

    check("overall failed on error", out["status"] == "failed", out["status"])
    check("not_null name passes", by_id["nn"]["status"] == "pass" and by_id["nn"]["violations"] == 0)
    check("not_null age finds null row 2", by_id["nn_age"]["violation_keys"] == ["2"])
    check("unique null ignored / no dup", by_id["uniq"]["status"] == "pass")
    check("range null ignored, row5 age17 fails", by_id["rng"]["violation_keys"] == ["5"])
    check("regex fullmatch all pass", by_id["rgx"]["status"] == "pass")
    check("fk null ignored, row3 99 fails", by_id["fk"]["violation_keys"] == ["3"])
    check("aggregate checked groups", by_id["agg"]["checked"] == 2 and by_id["agg"]["status"] == "pass")
    check("warn rule fails but warn", by_id["warn_only"]["status"] == "fail" and by_id["warn_only"]["violations"] == 1)
    check("child after passing dep runs", by_id["child"]["status"] == "pass")
    check("blocked when dependency failed", by_id["blocked_child"]["status"] == "blocked")
    check("blocked checked=0", by_id["blocked_child"]["checked"] == 0)
    check("all have digest hex", len(by_id["nn"]["digest"]) == 64)
    check("violation keys sorted & unique", by_id["nn_age"]["violation_keys"] == sorted(by_id["nn_age"]["violation_keys"]))

    # ---- quarantine ----------------------------------------------------
    qfile = root / "run" / "quarantine" / "emp.jsonl"
    check("quarantine file exists", qfile.is_file())
    qrows = [json.loads(line) for line in qfile.read_text(encoding="utf-8").splitlines()]
    qmap = {str(row["record"]["id"]): row for row in qrows}
    check("quarantine distinct rows once", len(qrows) == len(qmap))
    check("quarantine row3 has fk + warn score rule_ids",
          sorted(qmap["3"]["rule_ids"]) == ["fk", "warn_only"], qmap["3"]["rule_ids"])
    check("quarantine rule_ids sorted", all(row["rule_ids"] == sorted(row["rule_ids"]) for row in qrows))
    check("quarantine preserves typed record", qmap["5"]["record"]["active"] is True and qmap["5"]["record"]["age"] == 17 and qmap["5"]["record"]["score"] == 0.0 and qmap["5"]["record"]["dept_id"] is None)

    # ---- determinism ---------------------------------------------------
    qr2 = QualityRun(root / "run")
    out2 = qr2.evaluate(plan)
    check("idempotent identical result", out == out2)
    s1 = sorted((root / "run" / "runs" / "run-1").glob("summary-*.json"))
    check("single summary content-addressed", len(s1) == 1)

    # ---- binding conflict ---------------------------------------------
    conflict = json.loads(json.dumps(plan))
    conflict["rules"][0]["severity"] = "warn"
    raised = False
    try:
        QualityRun(root / "run").evaluate(conflict)
    except QualityError:
        raised = True
    check("different input same run_id conflicts", raised)

    # ---- cache self-heal ----------------------------------------------
    cache = root / "run" / "cache" / "emp.json"
    check("cache written", cache.is_file())
    cache.write_text("{ broken json", encoding="utf-8")
    out3 = QualityRun(root / "run").evaluate(plan)
    check("corrupt cache rebuilt, same result", out3 == out)
    cache.write_text(json.dumps({"version": 999, "content_hash": "x"}), encoding="utf-8")
    out4 = QualityRun(root / "run").evaluate(plan)
    check("stale version cache rebuilt", out4 == out)

    # ---- faults rollback ----------------------------------------------
    for stage in ("after_parse", "after_rules", "before_publish"):
        fresh = RUN_ROOT.parent / ("fault-" + stage)
        fresh.mkdir(parents=True, exist_ok=True)
        shutil.copy2(RUN_ROOT / "emp.csv", fresh / "emp.csv")
        shutil.copy2(RUN_ROOT / "dept.csv", fresh / "dept.csv")
        raised = False
        try:
            QualityRun(fresh).evaluate(plan, fault=stage)
        except QualityError:
            raised = True
        check("fault raises " + stage, raised)
        check("no run dir after rollback (" + stage + ")", not (fresh / "runs" / "run-1").exists())
        check("no quarantine after rollback (" + stage + ")", not (fresh / "quarantine").exists())
        check("no staging leftovers (" + stage + ")", not list(fresh.rglob(".stage-*")))
    clean_fresh = RUN_ROOT.parent / "fault-clean"
    clean_fresh.mkdir(parents=True, exist_ok=True)
    shutil.copy2(RUN_ROOT / "emp.csv", clean_fresh / "emp.csv")
    shutil.copy2(RUN_ROOT / "dept.csv", clean_fresh / "dept.csv")
    good = QualityRun(clean_fresh).evaluate(plan)
    check("clean run after faults works", good["status"] == "failed")

    # ---- strict parsing failures --------------------------------------
    bad_dir = root / "bad"
    cases = {
        "dup_header.csv": "id,id,x\n1,2,a\n",
        "col_count.csv": "id,name\n1\n",
        "bad_int.csv": "id\n01x\n",
        "bad_float.csv": "id\nnan\n",
        "dup_pk.csv": "id\n1\n1\n",
    }
    base_schema = None
    for fname, text in cases.items():
        write(bad_dir / fname, text)
        cols = text.splitlines()[0].split(",")
        schema = {c: ("int" if c == "id" else "string") for c in cols}
        badplan = {"run_id": "bad-" + fname, "datasets": {
            "d": {"path": fname, "primary_key": "id", "schema": schema}},
            "rules": [rule("r", "not_null", "d", cols[0])]}
        raised = False
        try:
            QualityRun(bad_dir / "store").evaluate(badplan)
        except QualityError:
            raised = True
        check("rejects " + fname, raised)

    # ---- cycle / unknown dep / bad fields ------------------------------
    cyc = json.loads(json.dumps(plan))
    cyc["run_id"] = "cyc"
    cyc["rules"] = [rule("a", "not_null", "emp", "name", depends_on=["b"]),
                    rule("b", "not_null", "emp", "name", depends_on=["a"])]
    raised = False
    try:
        QualityRun(root / "run").evaluate(cyc)
    except QualityError:
        raised = True
    check("cycle rejected", raised)

    extra = json.loads(json.dumps(plan))
    extra["run_id"] = "extra"
    extra["bogus"] = 1
    raised = False
    try:
        QualityRun(root / "run").evaluate(extra)
    except QualityError:
        raised = True
    check("extra plan field rejected", raised)

    # ---- path escape ---------------------------------------------------
    esc = json.loads(json.dumps(plan))
    esc["run_id"] = "esc"
    esc["datasets"]["emp"]["path"] = "../../../secret.csv"
    raised = False
    try:
        QualityRun(root / "run").evaluate(esc)
    except QualityError:
        raised = True
    check("relative path escape rejected", raised)

    # ---- CLI exit code 2, no traceback --------------------------------
    bad_plan = root / "badplan.json"
    bad_plan.write_text("{not json", encoding="utf-8")
    proc = subprocess.run([sys.executable, "-m", "rulequarry", str(root / "cli"), str(bad_plan)],
                          capture_output=True, text=True, cwd=Path(__file__).parent)
    check("cli exit code 2", proc.returncode == 2, proc.returncode)
    check("cli no traceback", "Traceback" not in proc.stderr, proc.stderr)
    check("cli error message", proc.stderr.startswith("error:"))

    print()
    print("PASS", PASS, "FAIL", FAIL)
finally:
    shutil.rmtree(root, ignore_errors=True)
sys.exit(1 if FAIL else 0)