"""Rule Quarry: deterministic data-quality evaluation and quarantine."""
import csv
import hashlib
import io
import json
import math
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path

VERSION = 1
CACHE_VERSION = 1
RULE_FIELDS = ("id", "type", "dataset", "column", "params", "severity", "depends_on")
DATASET_FIELDS = ("path", "primary_key", "schema")
PLAN_FIELDS = ("run_id", "datasets", "rules")
RULE_TYPES = ("not_null", "unique", "range", "regex", "foreign_key", "aggregate_count")
TYPES = ("string", "int", "float", "bool")
SEVERITIES = ("error", "warn")
TRUE_TOKENS = ("true", "1")
FALSE_TOKENS = ("false", "0")


class QualityError(Exception):
    """Protocol violation or unparseable input."""


def _sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


def _safe_join(root, destination):
    root_real = os.path.realpath(str(root))
    if os.path.isabs(destination):
        return os.path.realpath(destination)
    normalized = os.path.normpath(destination)
    if normalized == ".." or normalized.startswith(".." + os.sep) or os.path.isabs(normalized):
        raise QualityError("path escapes root: " + destination)
    target_real = os.path.realpath(os.path.join(root_real, normalized))
    if target_real != root_real and not target_real.startswith(root_real + os.sep):
        raise QualityError("path escapes root: " + destination)
    return target_real

def _atomic_write(path, data, binary=False):
    path = str(path)
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    descriptor, tmp_name = tempfile.mkstemp(dir=directory, prefix=".tmp-rq-")
    try:
        mode = "wb" if binary else "w"
        kwargs = {} if binary else {"encoding": "utf-8", "newline": ""}
        with os.fdopen(descriptor, mode, **kwargs) as handle:
            handle.write(data)
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
        raise


def _strict_bool(token):
    lowered = token.lower()
    if lowered in TRUE_TOKENS:
        return True
    if lowered in FALSE_TOKENS:
        return False
    raise ValueError("invalid bool: " + token)


def _convert_value(token, type_name):
    if type_name == "string":
        return token
    if type_name == "int":
        if not re.fullmatch(r"[+-]?[0-9]+", token):
            raise ValueError("invalid int: " + token)
        return int(token)
    if type_name == "float":
        lowered = token.lower()
        if (not re.fullmatch(r"[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?", token)
                or lowered in ("nan", "inf", "-inf", "+inf")):
            raise ValueError("invalid float: " + token)
        value = float(token)
        if not math.isfinite(value):
            raise ValueError("non-finite float: " + token)
        return value
    if type_name == "bool":
        return _strict_bool(token)
    raise QualityError("unknown type: " + type_name)


def _parse_csv_bytes(raw, schema, primary_key):
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise QualityError("csv is not valid utf-8") from exc
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        raise QualityError("csv has no header")
    header = rows[0]
    if not header:
        raise QualityError("empty csv header")
    if len(header) != len(set(header)):
        raise QualityError("duplicate csv header columns")
    expected = list(schema.keys())
    if sorted(header) != sorted(expected):
        raise QualityError("csv columns do not match schema")
    if primary_key not in header:
        raise QualityError("primary key not in csv header")
    records = []
    seen_keys = set()
    for line_index, raw_row in enumerate(rows[1:], start=2):
        if len(raw_row) != len(header):
            raise QualityError("column count mismatch at line " + str(line_index))
        record = {}
        for column, token in zip(header, raw_row):
            if token == "":
                record[column] = None
            else:
                try:
                    record[column] = _convert_value(token, schema[column])
                except ValueError as exc:
                    raise QualityError(
                        "line " + str(line_index) + " column " + column + ": " + str(exc))
        key = record[primary_key]
        if key is None:
            raise QualityError("null primary key at line " + str(line_index))
        key_text = str(key)
        if key_text in seen_keys:
            raise QualityError("duplicate primary key at line " + str(line_index))
        seen_keys.add(key_text)
        records.append(record)
    return header, records

def _validate_plan(plan):
    if not isinstance(plan, dict) or set(plan.keys()) != set(PLAN_FIELDS):
        raise QualityError("plan fields must be exactly run_id,datasets,rules")
    run_id = plan["run_id"]
    if not isinstance(run_id, str) or not run_id:
        raise QualityError("run_id must be a non-empty string")
    if ("/" in run_id or (chr(92) in run_id) or run_id in (".", "..")
            or any(part in ("", ".", "..") for part in re.split(r"[\\/]", run_id))):
        raise QualityError("invalid run_id: " + run_id)
    datasets = plan["datasets"]
    rules = plan["rules"]
    if not isinstance(datasets, dict) or not datasets:
        raise QualityError("datasets must be a non-empty object")
    if not isinstance(rules, list) or not rules:
        raise QualityError("rules must be a non-empty list")
    for name, definition in datasets.items():
        if not isinstance(name, str) or not name:
            raise QualityError("dataset name must be a non-empty string")
        if not isinstance(definition, dict) or set(definition.keys()) != set(DATASET_FIELDS):
            raise QualityError("dataset fields must be exactly path,primary_key,schema: " + name)
        path = definition["path"]
        primary_key = definition["primary_key"]
        schema = definition["schema"]
        if not isinstance(path, str) or not path:
            raise QualityError("dataset path must be a non-empty string: " + name)
        if not isinstance(primary_key, str) or not primary_key:
            raise QualityError("primary_key must be a non-empty string: " + name)
        if not isinstance(schema, dict) or not schema:
            raise QualityError("schema must be a non-empty object: " + name)
        if any(not isinstance(column, str) or not column for column in schema):
            raise QualityError("schema column names must be non-empty strings: " + name)
        if any(type_name not in TYPES for type_name in schema.values()):
            raise QualityError("schema contains unsupported type: " + name)
        if primary_key not in schema:
            raise QualityError("primary key missing from schema: " + name)
        if "/" in name or (chr(92) in name) or name in (".", ".."):
            raise QualityError("invalid dataset name: " + name)
    rule_ids = set()
    for rule in rules:
        if not isinstance(rule, dict) or set(rule.keys()) != set(RULE_FIELDS):
            raise QualityError("rule fields must be exactly id,type,dataset,column,params,severity,depends_on")
        rule_id = rule["id"]
        if not isinstance(rule_id, str) or not rule_id:
            raise QualityError("rule id must be a non-empty string")
        if rule_id in rule_ids:
            raise QualityError("duplicate rule id: " + rule_id)
        rule_ids.add(rule_id)
        if rule["type"] not in RULE_TYPES:
            raise QualityError("unsupported rule type: " + rule["type"])
        if rule["dataset"] not in datasets:
            raise QualityError("rule references unknown dataset: " + rule["dataset"])
        if not isinstance(rule["column"], str) or not rule["column"]:
            raise QualityError("rule column must be a non-empty string: " + rule_id)
        if rule["column"] not in datasets[rule["dataset"]]["schema"]:
            raise QualityError("rule column missing from dataset schema: " + rule_id)
        if not isinstance(rule["params"], dict):
            raise QualityError("rule params must be an object: " + rule_id)
        if rule["severity"] not in SEVERITIES:
            raise QualityError("severity must be error or warn: " + rule_id)
        depends_on = rule["depends_on"]
        if not isinstance(depends_on, list) or any(not isinstance(dep, str) for dep in depends_on):
            raise QualityError("depends_on must be a list of rule ids: " + rule_id)
    for rule in rules:
        for dependency in rule["depends_on"]:
            if dependency not in rule_ids:
                raise QualityError("unknown dependency: " + dependency)
            if dependency == rule["id"]:
                raise QualityError("rule cannot depend on itself: " + rule["id"])
    _build_layers(rules)
    _validate_rule_params(rules, datasets)


def _validate_rule_params(rules, datasets):
    for rule in rules:
        rule_type = rule["type"]
        params = rule["params"]
        schema = datasets[rule["dataset"]]["schema"]
        column_type = schema[rule["column"]]
        if rule_type == "range":
            if column_type not in ("int", "float"):
                raise QualityError("range requires numeric column: " + rule["id"])
            if "inclusive_min" not in params or "inclusive_max" not in params:
                raise QualityError("range requires inclusive_min and inclusive_max: " + rule["id"])
            minimum = params["inclusive_min"]
            maximum = params["inclusive_max"]
            if not isinstance(minimum, (int, float)) or isinstance(minimum, bool):
                raise QualityError("inclusive_min must be a number: " + rule["id"])
            if not isinstance(maximum, (int, float)) or isinstance(maximum, bool):
                raise QualityError("inclusive_max must be a number: " + rule["id"])
            if minimum > maximum:
                raise QualityError("inclusive_min must be <= inclusive_max: " + rule["id"])
        elif rule_type == "regex":
            pattern = params.get("pattern")
            if not isinstance(pattern, str):
                raise QualityError("regex requires a string pattern: " + rule["id"])
            try:
                re.compile(pattern)
            except re.error as exc:
                raise QualityError("invalid regex pattern: " + rule["id"]) from exc
        elif rule_type == "foreign_key":
            reference = params.get("dataset")
            if not isinstance(reference, str) or reference not in datasets:
                raise QualityError("foreign_key requires an existing reference dataset: " + rule["id"])
            ref_column = params.get("column")
            if not isinstance(ref_column, str) or ref_column not in datasets[reference]["schema"]:
                raise QualityError("foreign_key reference column missing: " + rule["id"])
        elif rule_type == "aggregate_count":
            if "min" not in params and "max" not in params:
                raise QualityError("aggregate_count requires min or max: " + rule["id"])
            for bound in ("min", "max"):
                if bound in params:
                    value = params[bound]
                    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                        raise QualityError("aggregate_count " + bound + " must be a non-negative int: " + rule["id"])
            if "min" in params and "max" in params and params["min"] > params["max"]:
                raise QualityError("aggregate_count min must be <= max: " + rule["id"])


def _build_layers(rules):
    remaining = {rule["id"]: set(rule["depends_on"]) for rule in rules}
    layers = []
    while remaining:
        ready = sorted(rule_id for rule_id, deps in remaining.items() if not deps)
        if not ready:
            raise QualityError("rule dependencies contain a cycle")
        layers.append(ready)
        for rule_id in ready:
            del remaining[rule_id]
        for deps in remaining.values():
            deps.difference_update(ready)
    return layers

class QualityRun:
    """Evaluate a quality plan deterministically and publish quarantined rows."""

    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.cache_dir = self.root / "cache"
        self.quarantine_dir = self.root / "quarantine"
        self.runs_dir = self.root / "runs"
        self._parsed = {}

    def evaluate(self, plan, fault=None):
        if fault not in (None, "after_parse", "after_rules", "before_publish"):
            raise QualityError("unknown fault stage: " + str(fault))
        if isinstance(plan, dict):
            plan_obj = plan
        elif isinstance(plan, (str, os.PathLike)):
            plan_text = Path(plan).read_text(encoding="utf-8")
            plan_obj = json.loads(plan_text)
        else:
            raise QualityError("plan must be an object or a JSON path")
        _validate_plan(plan_obj)

        dataset_payloads, parsed_data = self._load_datasets(plan_obj["datasets"])
        if fault == "after_parse":
            raise QualityError("injected fault after_parse")

        binding = self._binding_digest(plan_obj, dataset_payloads)
        results, quarantine = self._run_rules(plan_obj, parsed_data)
        if fault == "after_rules":
            raise QualityError("injected fault after_rules")

        summary = self._build_summary(plan_obj, binding, results)
        if fault == "before_publish":
            raise QualityError("injected fault before_publish")
        manifest = self._publish(summary, quarantine)
        return self._public_result(summary, manifest)

    def _load_datasets(self, datasets):
        payloads = {}
        parsed = {}
        for name in sorted(datasets):
            definition = datasets[name]
            csv_real = _safe_join(self.root, definition["path"])
            if not os.path.isfile(csv_real):
                raise QualityError("csv not found for dataset: " + name)
            raw = Path(csv_real).read_bytes()
            content_hash = _sha256_hex(raw)
            payloads[name] = raw
            cached = self._read_cache(name, content_hash, definition)
            if cached is not None:
                parsed[name] = cached
            else:
                header, records = _parse_csv_bytes(raw, definition["schema"], definition["primary_key"])
                parsed[name] = {"header": header, "records": records, "content_hash": content_hash}
                self._write_cache(name, definition, parsed[name])
        return payloads, parsed

    def _cache_path(self, name):
        return os.path.join(_safe_join(self.root, "cache"), name + ".json")

    def _read_cache(self, name, content_hash, definition):
        path = self._cache_path(name)
        if not os.path.isfile(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as handle:
                blob = json.load(handle)
            if not isinstance(blob, dict):
                return None
            if blob.get("version") != CACHE_VERSION:
                return None
            if blob.get("content_hash") != content_hash:
                return None
            if blob.get("schema") != definition["schema"]:
                return None
            if blob.get("primary_key") != definition["primary_key"]:
                return None
            header = blob["header"]
            records = blob["records"]
            if not isinstance(header, list) or not isinstance(records, list):
                return None
            if any(not isinstance(record, dict) for record in records):
                return None
            return {"header": header, "records": records, "content_hash": content_hash}
        except (OSError, ValueError, TypeError):
            return None

    def _write_cache(self, name, definition, parsed):
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        blob = {
            "version": CACHE_VERSION,
            "content_hash": parsed["content_hash"],
            "schema": definition["schema"],
            "primary_key": definition["primary_key"],
            "header": parsed["header"],
            "records": parsed["records"],
        }
        target = self._cache_path(name)
        _atomic_write(target, json.dumps(blob, ensure_ascii=False, sort_keys=True))

    def _binding_digest(self, plan, payloads):
        hasher = hashlib.sha256()
        canonical_plan = json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        hasher.update(b"plan|")
        hasher.update(str(len(canonical_plan)).encode("ascii"))
        hasher.update(b"|")
        hasher.update(canonical_plan)
        for name in sorted(payloads):
            raw = payloads[name]
            hasher.update(b"|csv|")
            hasher.update(name.encode("utf-8"))
            hasher.update(b"|")
            hasher.update(str(len(raw)).encode("ascii"))
            hasher.update(b"|")
            hasher.update(raw)
        return hasher.hexdigest()

    def _run_rules(self, plan, parsed):
        rules = plan["rules"]
        datasets = plan["datasets"]
        by_id = {rule["id"]: rule for rule in rules}
        layers = _build_layers(rules)
        results_by_id = {}
        quarantine = {}
        for layer in layers:
            for rule_id in layer:
                rule = by_id[rule_id]
                blocked = any(results_by_id[dep]["status"] != "pass" for dep in rule["depends_on"])
                if blocked:
                    result = self._blocked_result(rule)
                else:
                    result = self._evaluate_rule(rule, datasets, parsed, quarantine)
                results_by_id[rule_id] = result
        ordered_results = [results_by_id[rule["id"]] for rule in rules]
        return ordered_results, quarantine

    def _blocked_result(self, rule):
        return {
            "id": rule["id"],
            "status": "blocked",
            "checked": 0,
            "violations": 0,
            "violation_keys": [],
            "digest": _sha256_hex(("blocked:" + rule["id"]).encode("utf-8")),
        }

    def _evaluate_rule(self, rule, datasets, parsed, quarantine):
        rule_type = rule["type"]
        dataset_name = rule["dataset"]
        definition = datasets[dataset_name]
        primary_key = definition["primary_key"]
        data = parsed[dataset_name]
        records = data["records"]
        column = rule["column"]
        params = rule["params"]

        if rule_type == "aggregate_count":
            return self._eval_aggregate(rule, records, primary_key, column, params, quarantine, dataset_name)

        violation_keys = []
        pattern = None
        if rule_type == "regex":
            pattern = re.compile(params["pattern"])
        reference_values = None
        if rule_type == "foreign_key":
            ref_name = params["dataset"]
            ref_column = params["column"]
            reference_values = self._reference_values(parsed[ref_name]["records"], ref_column)

        for record in records:
            value = record[column]
            violated = False
            if rule_type == "not_null":
                violated = value is None
            elif value is None:
                violated = False
            elif rule_type == "unique":
                violated = False
            elif rule_type == "range":
                violated = value < params["inclusive_min"] or value > params["inclusive_max"]
            elif rule_type == "regex":
                violated = pattern.fullmatch(str(value)) is None
            elif rule_type == "foreign_key":
                violated = value not in reference_values
            if violated:
                violation_keys.append(str(record[primary_key]))

        if rule_type == "unique":
            violation_keys = self._unique_violations(records, column, primary_key)

        violation_keys = sorted(set(violation_keys))
        return self._rule_result(rule, len(records), violation_keys, records, primary_key, quarantine, dataset_name)

    @staticmethod
    def _unique_violations(records, column, primary_key):
        groups = {}
        for record in records:
            value = record[column]
            if value is None:
                continue
            groups.setdefault(value, []).append(str(record[primary_key]))
        keys = []
        for members in groups.values():
            if len(members) > 1:
                keys.extend(members)
        return keys

    @staticmethod
    def _reference_values(records, column):
        values = set()
        for record in records:
            value = record[column]
            if value is not None:
                values.add(value)
        return values

    def _eval_aggregate(self, rule, records, primary_key, column, params, quarantine, dataset_name):
        groups = {}
        for record in records:
            key = record[column]
            if key is None:
                continue
            groups.setdefault(key, 0)
            groups[key] += 1
        failing_groups = set()
        for key, count in groups.items():
            if "min" in params and count < params["min"]:
                failing_groups.add(key)
            if "max" in params and count > params["max"]:
                failing_groups.add(key)
        violation_keys = set()
        for record in records:
            if record[column] in failing_groups:
                violation_keys.add(str(record[primary_key]))
        violation_keys = sorted(violation_keys)
        return self._rule_result(rule, len(groups), violation_keys, records, primary_key, quarantine, dataset_name)

    def _rule_result(self, rule, checked, violation_keys, records, primary_key, quarantine, dataset_name):
        failed = bool(violation_keys)
        status = "fail" if failed else "pass"
        key_set = set(violation_keys)
        if failed:
            bucket = quarantine.setdefault(dataset_name, {})
            for record in records:
                key_text = str(record[primary_key])
                if key_text in key_set:
                    entry = bucket.setdefault(key_text, {"record": record, "rule_ids": set()})
                    entry["rule_ids"].add(rule["id"])
        digest_payload = {
            "id": rule["id"],
            "status": status,
            "checked": checked,
            "violation_keys": violation_keys,
        }
        digest = _sha256_hex(
            json.dumps(digest_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"))
        return {
            "id": rule["id"],
            "status": status,
            "checked": checked,
            "violations": len(violation_keys),
            "violation_keys": violation_keys,
            "digest": digest,
        }
    def _build_summary(self, plan, binding, results):
        error_failed = any(
            result["status"] == "fail" and self._is_error_rule(plan, result["id"])
            for result in results)
        blocked_present = any(result["status"] == "blocked" for result in results)
        if error_failed:
            overall = "failed"
        elif blocked_present:
            overall = "blocked"
        else:
            overall = "passed"
        return {
            "version": VERSION,
            "run_id": plan["run_id"],
            "status": overall,
            "input_digest": binding,
            "rules": results,
        }

    @staticmethod
    def _is_error_rule(plan, rule_id):
        for rule in plan["rules"]:
            if rule["id"] == rule_id:
                return rule["severity"] == "error"
        return False

    def _publish(self, summary, quarantine):
        run_dir = self.runs_dir / summary["run_id"]
        marker = run_dir / "binding.json"
        quarantine_dir = _safe_join(self.root, "quarantine")
        if run_dir.exists():
            if marker.is_file():
                try:
                    existing = json.loads(marker.read_text(encoding="utf-8"))
                except ValueError as exc:
                    raise QualityError("corrupt run binding: " + summary["run_id"]) from exc
                if existing.get("input_digest") != summary["input_digest"]:
                    raise QualityError("run_id bound to different input: " + summary["run_id"])
            else:
                raise QualityError("run directory missing binding: " + summary["run_id"])
        else:
            run_dir.mkdir(parents=True, exist_ok=True)

        summary_text = json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        summary_digest = _sha256_hex(summary_text.encode("utf-8"))
        summary_name = "summary-" + summary_digest[:16] + ".json"

        staging = run_dir / (".stage-" + summary_digest[:12])
        if staging.exists():
            shutil.rmtree(staging)
        staging.mkdir(parents=True)
        try:
            _atomic_write(staging / summary_name, summary_text)
            binding = {
                "version": VERSION,
                "run_id": summary["run_id"],
                "input_digest": summary["input_digest"],
                "summary": summary_name,
                "summary_digest": summary_digest,
            }
            _atomic_write(staging / "binding.json",
                          json.dumps(binding, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
            staged_quarantine = staging / "quarantine"
            staged_quarantine.mkdir()
            quarantine_manifest = {}
            for dataset_name in sorted(quarantine):
                bucket = quarantine[dataset_name]
                lines = []
                for key_text in sorted(bucket):
                    entry = bucket[key_text]
                    line_obj = {"record": entry["record"], "rule_ids": sorted(entry["rule_ids"])}
                    lines.append(json.dumps(line_obj, ensure_ascii=False, sort_keys=True))
                payload = ("\n".join(lines) + "\n") if lines else ""
                file_name = dataset_name + ".jsonl"
                _atomic_write(staged_quarantine / file_name, payload)
                quarantine_manifest[file_name] = {
                    "digest": _sha256_hex(payload.encode("utf-8")),
                    "rows": len(lines),
                }
            binding["quarantine"] = quarantine_manifest
            _atomic_write(staging / "binding.json",
                          json.dumps(binding, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
            self._swap_quarantine(staging, quarantine_dir)
            os.replace(str(staging / summary_name), str(run_dir / summary_name))
            os.replace(str(staging / "binding.json"), str(marker))
        finally:
            if staging.exists():
                shutil.rmtree(staging, ignore_errors=True)
        return {"summary": summary_name, "summary_digest": summary_digest, "quarantine": quarantine_manifest}

    def _swap_quarantine(self, staging, quarantine_dir):
        os.makedirs(quarantine_dir, exist_ok=True)
        staged_dir = str(staging / "quarantine")
        backup_dir = str(staging / "quarantine-backup")
        if os.path.isdir(quarantine_dir):
            os.replace(quarantine_dir, backup_dir)
        try:
            os.replace(staged_dir, quarantine_dir)
        except BaseException:
            if os.path.isdir(backup_dir) and not os.path.exists(quarantine_dir):
                os.replace(backup_dir, quarantine_dir)
            raise
        if os.path.isdir(backup_dir):
            shutil.rmtree(backup_dir, ignore_errors=True)

    def _public_result(self, summary, manifest):
        return {
            "run_id": summary["run_id"],
            "status": summary["status"],
            "input_digest": summary["input_digest"],
            "summary_digest": manifest["summary_digest"],
            "rules": summary["rules"],
        }


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 2:
        print("usage: python -m rulequarry.quality <root> <plan.json>", file=sys.stderr)
        return 2
    root, plan_path = argv
    try:
        result = QualityRun(root).evaluate(plan_path)
    except Exception as exc:
        print("error: " + str(exc), file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
