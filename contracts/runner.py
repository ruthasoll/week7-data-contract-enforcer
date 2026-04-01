#!/usr/bin/env python3
"""
contracts/runner.py

ValidationRunner: execute Bitol contract checks against a JSONL snapshot and emit the structured validation report.

Usage (example for Week 3):
python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/week3_validation_20260101_1200.json

This script implements:
 - structural checks (required, types, formats, enums)
 - range checks (min/max)
 - nested field support (extracted_facts[*].confidence)
 - statistical drift checks using schema_snapshots/baselines.json
 - special rule for any "confidence" field (fail if mean>1.0 or max>1.0)
 - never crash on bad data; produce ERROR entries instead

Output JSON follows the schema in Phase 2A of the challenge document.
"""

import argparse
import os
import sys
import json
import yaml
import uuid
import hashlib
import datetime
import traceback
from collections import defaultdict

import pandas as pd
import numpy as np

# Paths
BASELINE_PATH = os.path.join("schema_snapshots", "baselines.json")

# Severity mapping
SEVERITY_CRITICAL = "CRITICAL"
SEVERITY_HIGH = "HIGH"
SEVERITY_MEDIUM = "MEDIUM"
SEVERITY_LOW = "LOW"
SEVERITY_WARNING = "WARNING"

# Status values
STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
STATUS_WARN = "WARN"
STATUS_ERROR = "ERROR"

# Helper utilities

def sha256_of_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def load_contract(path):
    with open(path, 'r', encoding='utf-8') as fh:
        return yaml.safe_load(fh)


def safe_load_jsonl_to_df(path):
    try:
        return pd.read_json(path, lines=True)
    except Exception:
        # fallback: read line-by-line into list of dicts
        records = []
        with open(path, 'r', encoding='utf-8') as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except Exception:
                    # ignore bad line
                    records.append({"__bad_line": line})
        return pd.DataFrame(records)


def record_sample_ids(series, id_col=None, limit=5):
    # return up to `limit` distinct sample identifiers (fact_id or index)
    samples = []
    if id_col is not None and id_col in series.columns:
        samples = list(series[id_col].dropna().astype(str).unique()[:limit])
    else:
        samples = list(series.dropna().astype(str).unique()[:limit])
    return samples

# Nested field helper: supports "extracted_facts[*].confidence" style

def expand_nested(df, field_path):
    """Given a pandas DataFrame and a field path like 'extracted_facts[*].confidence',
    return a DataFrame with columns: _parent_index, value, and optional fact_id if present.
    If path is top-level (no [*]) return a DataFrame with index and value.
    """
    if "[*]" not in field_path:
        # top-level
        col = field_path
        if col not in df.columns:
            return pd.DataFrame(columns=["_parent_index", "value"])
        s = df[col]
        out = pd.DataFrame({"_parent_index": s.index, "value": s.values})
        return out

    # parse
    arr_col, sub_field = field_path.split("[*].")
    if arr_col not in df.columns:
        return pd.DataFrame(columns=["_parent_index", "value", "fact_id"])

    rows = []
    for idx, arr in df[arr_col].items():
        if isinstance(arr, list):
            for item in arr:
                if isinstance(item, dict):
                    val = item.get(sub_field)
                    fid = item.get('fact_id') or item.get('id')
                    rows.append({"_parent_index": idx, "value": val, "fact_id": fid})
                else:
                    # scalar item
                    rows.append({"_parent_index": idx, "value": item, "fact_id": None})
        else:
            # null or not-list
            rows.append({"_parent_index": idx, "value": None, "fact_id": None})
    return pd.DataFrame(rows)

# Type checking helper

# Type checking helper (renamed to avoid closure/name shadowing issues)
# The original implementation used the name `check_type`, which later in this
# module is also used as a local variable name when iterating check definitions.
# That produced a closure/scoping problem when lambdas captured the name.
# Renaming to `is_type` and avoiding late-bound lambdas fixes the issue.
def is_type(value, expected_type):
    """Return True if value matches expected_type. Treats nulls as valid (handled by required checks)."""
    # consider pandas NA as null
    if pd.isnull(value):
        return True
    if expected_type == 'string':
        return isinstance(value, str)
    if expected_type == 'integer':
        return isinstance(value, (int, np.integer)) and not isinstance(value, bool)
    if expected_type == 'number':
        return isinstance(value, (int, float, np.number)) and not isinstance(value, bool)
    if expected_type == 'boolean':
        return isinstance(value, (bool, np.bool_))
    if expected_type == 'array':
        return isinstance(value, list)
    # fallback: accept
    return True

# Format checks (uuid/date-time simple checks)
from datetime import datetime
import re
UUID_RE = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')

def check_format(value, fmt):
    if pd.isnull(value):
        return True
    try:
        if fmt == 'uuid':
            return bool(UUID_RE.match(str(value)))
        if fmt == 'date-time' or fmt == 'date':
            # accept ISO-like
            try:
                datetime.fromisoformat(str(value).replace('Z', '+00:00'))
                return True
            except Exception:
                return False
    except Exception:
        return False
    return True

# Load baselines (mean/stddev per contract_id -> column)

def load_baselines():
    if not os.path.exists(BASELINE_PATH):
        return {}
    try:
        with open(BASELINE_PATH, 'r', encoding='utf-8') as fh:
            return json.load(fh)
    except Exception:
        return {}


def save_baselines(baselines):
    safe_dir = os.path.dirname(BASELINE_PATH)
    if safe_dir and not os.path.exists(safe_dir):
        os.makedirs(safe_dir, exist_ok=True)
    with open(BASELINE_PATH, 'w', encoding='utf-8') as fh:
        json.dump(baselines, fh, indent=2, sort_keys=True)

# Main check runner

def run_checks(contract, df, snapshot_id):
    results = []
    total_checks = 0
    passed = failed = warned = errored = 0

    baselines = load_baselines()
    contract_id = contract.get('id') or 'contract'
    contract_baselines = baselines.get(contract_id, {})

    # 1) Structural checks from contract['schema']
    schema = contract.get('schema', {})
    for field_name, field_def in schema.items():
        try:
            # required check
            required = field_def.get('required', False)
            col_present = field_name in df.columns
            if required:
                total_checks += 1
                check_id = f"{contract_id}.{field_name}.required"
                column_name = field_name
                if not col_present:
                    status = STATUS_ERROR
                    severity = SEVERITY_CRITICAL
                    message = f"column {field_name} missing"
                    records_failing = 0
                    sample_failing = []
                    errored += 1
                else:
                    null_frac = float(df[field_name].isnull().sum()) / max(1, len(df))
                    if null_frac > 0.0:
                        status = STATUS_FAIL
                        severity = SEVERITY_CRITICAL
                        message = f"required field {field_name} has null fraction {null_frac:.3f}"
                        records_failing = int(df[field_name].isnull().sum())
                        sample_failing = record_sample_ids(df[field_name].loc[df[field_name].isnull()])
                        failed += 1
                    else:
                        status = STATUS_PASS
                        severity = SEVERITY_CRITICAL
                        message = f"required field {field_name} present with null_fraction=0"
                        records_failing = 0
                        sample_failing = []
                        passed += 1
                results.append({
                    "check_id": check_id,
                    "column_name": column_name,
                    "check_type": "required",
                    "status": status,
                    "actual_value": f"null_fraction={null_frac:.6g}" if col_present else "missing",
                    "expected": "null_fraction==0",
                    "severity": severity,
                    "records_failing": records_failing,
                    "sample_failing": sample_failing,
                    "message": message
                })

            # type/format/enum/range for top-level fields
            typ = field_def.get('type')
            if typ:
                total_checks += 1
                check_id = f"{contract_id}.{field_name}.type"
                column_name = field_name
                if column_name not in df.columns:
                    status = STATUS_ERROR
                    severity = SEVERITY_CRITICAL
                    message = f"column {column_name} missing for type check"
                    records_failing = 0
                    sample_failing = []
                    errored += 1
                else:
                    # vectorized type check: count rows failing
                    col = df[column_name]
                    # Avoid late-binding lambda closure issues by creating a dedicated checker function
                    def make_type_checker(expected_type):
                        def checker(v):
                            try:
                                return not is_type(v, expected_type)
                            except Exception:
                                # treat errors as failures to be conservative
                                return True
                        return checker
                    try:
                        mask_fail = col.apply(make_type_checker(typ))
                        failing = int(mask_fail.sum())
                        if failing > 0:
                            status = STATUS_FAIL
                            severity = SEVERITY_CRITICAL
                            records_failing = failing
                            sample_failing = list(col[mask_fail].dropna().astype(str).unique()[:5])
                            failed += 1
                            message = f"{failing} rows with type != {typ}"
                        else:
                            status = STATUS_PASS
                            severity = SEVERITY_CRITICAL
                            records_failing = 0
                            sample_failing = []
                            passed += 1
                    except Exception as e:
                        # never crash validation runner; emit ERROR for this check
                        status = STATUS_ERROR
                        severity = SEVERITY_CRITICAL
                        records_failing = 0
                        sample_failing = []
                        message = f"type check failed with exception: {e}"
                        errored += 1

                    # If this is an array type and has item definitions, also validate the items' types
                    if typ == 'array' and isinstance(field_def, dict):
                        items_def = field_def.get('items', {}) or {}
                        if isinstance(items_def, dict):
                            for subfield, subdef in items_def.items():
                                # run type check for nested item field using expand_nested
                                nested_expr = f"{field_name}[*].{subfield}"
                                nested_df = expand_nested(df, nested_expr)
                                if nested_df.empty:
                                    continue
                                expected_subtype = subdef.get('type')
                                if not expected_subtype:
                                    continue
                                # create checker for subtype
                                checker = make_type_checker(expected_subtype)
                                try:
                                    nested_mask_fail = nested_df['value'].apply(checker)
                                    nested_failing = int(nested_mask_fail.sum())
                                    total_checks += 1
                                    chk_id = f"{contract_id}.{field_name}.items.{subfield}.type"
                                    if nested_failing > 0:
                                        failed += 1
                                        results.append({
                                            "check_id": chk_id,
                                            "column_name": nested_expr,
                                            "check_type": "type",
                                            "status": STATUS_FAIL,
                                            "actual_value": f"failing={nested_failing}",
                                            "expected": f"type=={expected_subtype}",
                                            "severity": SEVERITY_CRITICAL,
                                            "records_failing": nested_failing,
                                            "sample_failing": list(nested_df.loc[nested_mask_fail, 'fact_id'].dropna().astype(str).unique()[:5]),
                                            "message": f"{nested_failing} nested rows with type != {expected_subtype}"
                                        })
                                    else:
                                        passed += 1
                                        results.append({
                                            "check_id": chk_id,
                                            "column_name": nested_expr,
                                            "check_type": "type",
                                            "status": STATUS_PASS,
                                            "actual_value": "failing=0",
                                            "expected": f"type=={expected_subtype}",
                                            "severity": SEVERITY_CRITICAL,
                                            "records_failing": 0,
                                            "sample_failing": [],
                                            "message": "nested items type ok"
                                        })
                                except Exception as e:
                                    total_checks += 1
                                    errored += 1
                                    results.append({
                                        "check_id": f"{contract_id}.{field_name}.items.{subfield}.type.error",
                                        "column_name": nested_expr,
                                        "check_type": "type",
                                        "status": STATUS_ERROR,
                                        "actual_value": "error",
                                        "expected": f"type=={expected_subtype}",
                                        "severity": SEVERITY_CRITICAL,
                                        "records_failing": 0,
                                        "sample_failing": [],
                                        "message": f"error checking nested item types: {e}"
                                    })

                results.append({
                    "check_id": check_id,
                    "column_name": column_name,
                    "check_type": "type",
                    "status": status,
                    "actual_value": f"failing={records_failing}",
                    "expected": f"type=={typ}",
                    "severity": severity,
                    "records_failing": records_failing,
                    "sample_failing": sample_failing,
                    "message": message
                })

            # format
            fmt = field_def.get('format') or field_def.get('pattern')
            if fmt:
                total_checks += 1
                check_id = f"{contract_id}.{field_name}.format"
                column_name = field_name
                if column_name not in df.columns:
                    status = STATUS_ERROR
                    severity = SEVERITY_CRITICAL
                    message = f"column {column_name} missing for format check"
                    records_failing = 0
                    sample_failing = []
                    errored += 1
                else:
                    col = df[column_name]
                    mask_fail = col.apply(lambda v: not check_format(v, 'uuid') if fmt == 'uuid' else not check_format(v, 'date-time'))
                    failing = int(mask_fail.sum())
                    if failing > 0:
                        status = STATUS_FAIL
                        severity = SEVERITY_CRITICAL
                        records_failing = failing
                        sample_failing = list(col[mask_fail].dropna().astype(str).unique()[:5])
                        failed += 1
                        message = f"{failing} rows failing format {fmt}"
                    else:
                        status = STATUS_PASS
                        severity = SEVERITY_CRITICAL
                        records_failing = 0
                        sample_failing = []
                        passed += 1
                results.append({
                    "check_id": check_id,
                    "column_name": column_name,
                    "check_type": "format",
                    "status": status,
                    "actual_value": f"failing={records_failing}",
                    "expected": f"format={fmt}",
                    "severity": severity,
                    "records_failing": records_failing,
                    "sample_failing": sample_failing,
                    "message": message
                })

            # accepted_values (enum)
            if 'enum' in field_def:
                total_checks += 1
                check_id = f"{contract_id}.{field_name}.accepted_values"
                column_name = field_name
                allowed = set(field_def.get('enum', []))
                if column_name not in df.columns:
                    status = STATUS_ERROR
                    severity = SEVERITY_CRITICAL
                    message = f"column {column_name} missing for accepted_values check"
                    records_failing = 0
                    sample_failing = []
                    errored += 1
                else:
                    col = df[column_name].astype(str)
                    mask_fail = ~col.isin(allowed)
                    failing = int(mask_fail.sum())
                    if failing > 0:
                        status = STATUS_FAIL
                        severity = SEVERITY_CRITICAL
                        records_failing = failing
                        sample_failing = list(col[mask_fail].unique()[:5])
                        failed += 1
                        message = f"{failing} rows with values not in accepted set"
                    else:
                        status = STATUS_PASS
                        severity = SEVERITY_CRITICAL
                        records_failing = 0
                        sample_failing = []
                        passed += 1
                results.append({
                    "check_id": check_id,
                    "column_name": column_name,
                    "check_type": "accepted_values",
                    "status": status,
                    "actual_value": f"failing={records_failing}",
                    "expected": f"accepted_values={list(allowed)}",
                    "severity": severity,
                    "records_failing": records_failing,
                    "sample_failing": sample_failing,
                    "message": message
                })

            # range for top-level numeric
            if field_def.get('type') in ('number', 'integer') and ('minimum' in field_def or 'maximum' in field_def):
                total_checks += 1
                check_id = f"{contract_id}.{field_name}.range"
                column_name = field_name
                if column_name not in df.columns:
                    status = STATUS_ERROR
                    severity = SEVERITY_CRITICAL
                    message = f"column {column_name} missing for range check"
                    records_failing = 0
                    sample_failing = []
                    errored += 1
                else:
                    col = pd.to_numeric(df[column_name], errors='coerce')
                    minv = field_def.get('minimum')
                    maxv = field_def.get('maximum')
                    mask_fail = pd.Series(False, index=col.index)
                    if minv is not None:
                        mask_fail = mask_fail | (col < float(minv))
                    if maxv is not None:
                        mask_fail = mask_fail | (col > float(maxv))
                    failing = int(mask_fail.sum())
                    if failing > 0:
                        status = STATUS_FAIL
                        severity = SEVERITY_CRITICAL
                        records_failing = failing
                        sample_failing = list(df.loc[mask_fail].head(5).index.astype(str))
                        failed += 1
                        message = f"{failing} rows outside range [{minv},{maxv}]"
                    else:
                        status = STATUS_PASS
                        severity = SEVERITY_CRITICAL
                        records_failing = 0
                        sample_failing = []
                        passed += 1
                results.append({
                    "check_id": check_id,
                    "column_name": column_name,
                    "check_type": "range",
                    "status": status,
                    "actual_value": f"failing={records_failing}",
                    "expected": f"min={field_def.get('minimum')} max={field_def.get('maximum')}",
                    "severity": severity,
                    "records_failing": records_failing,
                    "sample_failing": sample_failing,
                    "message": message
                })

        except Exception as e:
            # never crash on one field
            total_checks += 1
            errored += 1
            results.append({
                "check_id": f"{contract_id}.{field_name}.error",
                "column_name": field_name,
                "check_type": "error",
                "status": STATUS_ERROR,
                "actual_value": "error",
                "expected": "n/a",
                "severity": SEVERITY_CRITICAL,
                "records_failing": 0,
                "sample_failing": [],
                "message": f"internal error profiling field: {e}: {traceback.format_exc()}"
            })

    # 2) Quality checks specified in contract. Handle extracted_facts[*].confidence and others
    checks = contract.get('quality', {}).get('specification', {}).get('checks', [])
    for chk in checks:
        try:
            c_id = chk.get('check_id') or chk.get('name') or 'unnamed_check'
            col = chk.get('column')
            check_type = chk.get('check')
            severity = chk.get('severity', SEVERITY_HIGH)
            total_checks += 1
            # support range check on nested fields
            if isinstance(col, str) and '[*].' in col and check_type == 'range':
                # expand nested
                nested = expand_nested(df, col)
                values = pd.to_numeric(nested['value'], errors='coerce')
                minv = chk.get('expected', {}).get('minimum')
                maxv = chk.get('expected', {}).get('maximum')
                mask_fail = pd.Series(False, index=values.index)
                if minv is not None:
                    mask_fail = mask_fail | (values < float(minv))
                if maxv is not None:
                    mask_fail = mask_fail | (values > float(maxv))
                failing = int(mask_fail.sum())
                if failing > 0:
                    status = STATUS_FAIL
                    records_failing = failing
                    sample_failing = list(nested.loc[mask_fail, 'fact_id'].dropna().astype(str).unique()[:5])
                    message = f"{failing} nested values outside range [{minv},{maxv}]"
                    failed += 1
                else:
                    status = STATUS_PASS
                    records_failing = 0
                    sample_failing = []
                    passed += 1
                results.append({
                    "check_id": c_id,
                    "column_name": col,
                    "check_type": check_type,
                    "status": status,
                    "actual_value": f"failing={records_failing}",
                    "expected": f"min={minv} max={maxv}",
                    "severity": severity,
                    "records_failing": records_failing,
                    "sample_failing": sample_failing,
                    "message": message
                })
                # special confidence rule
                if 'confidence' in col.lower():
                    try:
                        meanv = float(values.mean()) if not values.empty else 0.0
                        maxv_obs = float(values.max()) if not values.empty else 0.0
                    except Exception:
                        meanv = 0.0
                        maxv_obs = 0.0
                    if meanv > 1.0 or maxv_obs > 1.0:
                        # escalate
                        results[-1]['status'] = STATUS_FAIL
                        results[-1]['severity'] = SEVERITY_CRITICAL
                        results[-1]['actual_value'] = f"max={maxv_obs}, mean={meanv}"
                        results[-1]['message'] = "confidence is in 0-100 range or otherwise >1.0; Breaking change detected."
                continue

            # generic expression check (e.g., total_tokens == prompt_tokens + completion_tokens)
            if check_type == 'expression':
                expr = chk.get('expression')
                # try evaluate row-wise, build mask of failures
                try:
                    # create local DataFrame context
                    mask_fail = ~df.eval(expr)
                    failing = int(mask_fail.sum())
                    if failing > 0:
                        status = STATUS_FAIL
                        records_failing = failing
                        sample_failing = list(df.loc[mask_fail].head(5).index.astype(str))
                        failed += 1
                        message = f"{failing} rows where {expr} is False"
                    else:
                        status = STATUS_PASS
                        records_failing = 0
                        sample_failing = []
                        passed += 1
                except Exception as e:
                    status = STATUS_ERROR
                    records_failing = 0
                    sample_failing = []
                    message = f"failed to evaluate expression: {e}"
                    errored += 1
                results.append({
                    "check_id": c_id,
                    "column_name": col or '',
                    "check_type": check_type,
                    "status": status,
                    "actual_value": f"failing={records_failing}",
                    "expected": expr,
                    "severity": severity,
                    "records_failing": records_failing,
                    "sample_failing": sample_failing,
                    "message": message
                })
                continue

            # time_order check (end_time > start_time)
            if check_type == 'time_order':
                # column may list two columns in check definition
                try:
                    # assume expected contains mapping
                    end_col = chk.get('column').split(',')[0].strip() if chk.get('column') else None
                    start_col = chk.get('column').split(',')[1].strip() if chk.get('column') and ',' in chk.get('column') else None
                except Exception:
                    end_col = None
                    start_col = None
                if not end_col or not start_col or end_col not in df.columns or start_col not in df.columns:
                    status = STATUS_ERROR
                    records_failing = 0
                    sample_failing = []
                    message = f"time_order check columns missing: {start_col},{end_col}"
                    errored += 1
                else:
                    # parse ISO datetimes
                    try:
                        start_vals = pd.to_datetime(df[start_col], errors='coerce')
                        end_vals = pd.to_datetime(df[end_col], errors='coerce')
                        mask_fail = end_vals <= start_vals
                        failing = int(mask_fail.sum())
                        if failing > 0:
                            status = STATUS_FAIL
                            records_failing = failing
                            sample_failing = list(df.loc[mask_fail].head(5).index.astype(str))
                            failed += 1
                            message = f"{failing} rows where {end_col} <= {start_col}"
                        else:
                            status = STATUS_PASS
                            records_failing = 0
                            sample_failing = []
                            passed += 1
                    except Exception as e:
                        status = STATUS_ERROR
                        records_failing = 0
                        sample_failing = []
                        message = f"time_order evaluation error: {e}"
                        errored += 1
                results.append({
                    "check_id": c_id,
                    "column_name": chk.get('column'),
                    "check_type": check_type,
                    "status": status,
                    "actual_value": f"failing={records_failing}",
                    "expected": "end_time > start_time",
                    "severity": severity,
                    "records_failing": records_failing,
                    "sample_failing": sample_failing,
                    "message": message
                })
                continue

            # fallback: unsupported check
            results.append({
                "check_id": c_id,
                "column_name": chk.get('column', ''),
                "check_type": chk.get('check', ''),
                "status": STATUS_ERROR,
                "actual_value": "unsupported_check",
                "expected": json.dumps(chk.get('expected', {})),
                "severity": SEVERITY_LOW,
                "records_failing": 0,
                "sample_failing": [],
                "message": "unsupported check type by validation runner"
            })
            errored += 1

        except Exception as e:
            total_checks += 1
            errored += 1
            results.append({
                "check_id": chk.get('check_id', 'unknown'),
                "column_name": chk.get('column', ''),
                "check_type": chk.get('check', ''),
                "status": STATUS_ERROR,
                "actual_value": "error",
                "expected": json.dumps(chk.get('expected', {})),
                "severity": SEVERITY_CRITICAL,
                "records_failing": 0,
                "sample_failing": [],
                "message": f"exception running check: {e}: {traceback.format_exc()}"
            })

    # 3) Statistical drift detection for numeric columns with baselines
    # build numeric columns list from contract (including nested expanded names)
    numeric_columns = []
    # top-level numeric
    for col, defn in contract.get('schema', {}).items():
        if isinstance(defn, dict) and defn.get('type') in ('number', 'integer'):
            numeric_columns.append((col, col))
        # nested arrays
        if isinstance(defn, dict) and defn.get('type') == 'array':
            items = defn.get('items', {})
            for subfield, subdef in items.items():
                if isinstance(subdef, dict) and subdef.get('type') in ('number', 'integer'):
                    numeric_columns.append((f"{col}[*].{subfield}", f"{col}.{subfield}"))

    for col_expr, plain_name in numeric_columns:
        try:
            # obtain series of values, expand nested if needed
            if '[*]' in col_expr:
                nested = expand_nested(df, col_expr)
                values = pd.to_numeric(nested['value'], errors='coerce').dropna()
            else:
                values = pd.to_numeric(df[col_expr], errors='coerce').dropna()
            if values.empty:
                continue
            mean_curr = float(values.mean())
            std_curr = float(values.std()) if not np.isnan(values.std()) else 0.0
            baseline = contract_baselines.get(plain_name)
            if baseline:
                mean_base = baseline.get('mean')
                std_base = baseline.get('stddev')
                if std_base is None or std_base == 0:
                    # cannot compute zscore reliably
                    continue
                z = abs(mean_curr - mean_base) / float(std_base)
                total_checks += 1
                check_id = f"{contract_id}.{plain_name}.statistical_drift"
                if z > 3.0:
                    status = STATUS_FAIL
                    severity = SEVERITY_HIGH
                    message = f"mean deviation {z:.2f} stddevs (>3)"
                    failed += 1
                elif z > 2.0:
                    status = STATUS_WARN
                    severity = SEVERITY_MEDIUM
                    message = f"mean deviation {z:.2f} stddevs (2-3)"
                    warned += 1
                else:
                    status = STATUS_PASS
                    severity = SEVERITY_LOW
                    message = f"mean deviation {z:.2f} stddevs"
                    passed += 1
                results.append({
                    "check_id": check_id,
                    "column_name": col_expr,
                    "check_type": "statistical_drift",
                    "status": status,
                    "actual_value": f"mean={mean_curr:.6g}, stddev={std_curr:.6g}",
                    "expected": f"baseline_mean={mean_base}, baseline_stddev={std_base}",
                    "severity": severity,
                    "records_failing": 0,
                    "sample_failing": [],
                    "message": message
                })
        except Exception as e:
            total_checks += 1
            errored += 1
            results.append({
                "check_id": f"{contract_id}.{plain_name}.statistical_drift",
                "column_name": col_expr,
                "check_type": "statistical_drift",
                "status": STATUS_ERROR,
                "actual_value": "error",
                "expected": "has baseline",
                "severity": SEVERITY_LOW,
                "records_failing": 0,
                "sample_failing": [],
                "message": f"error computing statistical drift: {e}: {traceback.format_exc()}"
            })

    # 4) Special rule: any column name containing 'confidence' -> fail if mean>1.0 or max>1.0
    # find nested and top-level
    conf_checks = []
    for col, defn in contract.get('schema', {}).items():
        if 'confidence' in col.lower():
            conf_checks.append(col)
        if isinstance(defn, dict) and defn.get('type') == 'array':
            items = defn.get('items', {})
            for subfield in items.keys():
                if 'confidence' in subfield.lower():
                    conf_checks.append(f"{col}[*].{subfield}")

    for col_expr in conf_checks:
        try:
            if '[*]' in col_expr:
                nested = expand_nested(df, col_expr)
                values = pd.to_numeric(nested['value'], errors='coerce').dropna()
            else:
                values = pd.to_numeric(df[col_expr], errors='coerce').dropna()
            if values.empty:
                continue
            meanv = float(values.mean())
            maxv = float(values.max())
            total_checks += 1
            check_id = f"{contract_id}.{col_expr}.confidence_range"
            if meanv > 1.0 or maxv > 1.0:
                status = STATUS_FAIL
                severity = SEVERITY_CRITICAL
                records_failing = int((values > 1.0).sum())
                sample_failing = list(nested.loc[nested['value'].astype(float) > 1.0, 'fact_id'].dropna().astype(str).unique()[:5]) if '[*]' in col_expr else list(df.loc[pd.to_numeric(df[col_expr], errors='coerce') > 1.0].head(5).index.astype(str))
                failed += 1
                message = f"confidence is in 0-100 or otherwise >1.0: max={maxv}, mean={meanv}"
            else:
                status = STATUS_PASS
                severity = SEVERITY_CRITICAL
                records_failing = 0
                sample_failing = []
                passed += 1
                message = f"confidence within 0.0-1.0: max={maxv}, mean={meanv}"
            results.append({
                "check_id": check_id,
                "column_name": col_expr,
                "check_type": "range",
                "status": status,
                "actual_value": f"max={maxv}, mean={meanv}",
                "expected": "max<=1.0, min>=0.0",
                "severity": severity,
                "records_failing": records_failing,
                "sample_failing": sample_failing,
                "message": message
            })
        except Exception as e:
            total_checks += 1
            errored += 1
            results.append({
                "check_id": f"{contract_id}.{col_expr}.confidence_range",
                "column_name": col_expr,
                "check_type": "range",
                "status": STATUS_ERROR,
                "actual_value": "error",
                "expected": "max<=1.0",
                "severity": SEVERITY_CRITICAL,
                "records_failing": 0,
                "sample_failing": [],
                "message": f"error evaluating confidence check: {e}: {traceback.format_exc()}"
            })

    # 5) After completing checks, update baselines for numeric columns (store mean/stddev)
    # Only update if run produced no ERROR statuses (or optionally always update latest baseline and let diffing detect change)
    # We'll update baselines for numeric columns observed in this run.
    updated_baselines = baselines.copy()
    updated_contract_baseline = contract_baselines.copy()
    for col_expr, plain_name in numeric_columns:
        try:
            if '[*]' in col_expr:
                nested = expand_nested(df, col_expr)
                vals = pd.to_numeric(nested['value'], errors='coerce').dropna()
            else:
                vals = pd.to_numeric(df[col_expr], errors='coerce').dropna()
            if vals.empty:
                continue
            updated_contract_baseline[plain_name] = {"mean": float(vals.mean()), "stddev": float(vals.std()) if not np.isnan(vals.std()) else 0.0}
        except Exception:
            continue
    if updated_contract_baseline:
        updated_baselines[contract_id] = updated_contract_baseline
        try:
            save_baselines(updated_baselines)
        except Exception:
            # do not fail the validation because baseline saving failed
            pass

    report = {
        "report_id": str(uuid.uuid4()),
        "contract_id": contract_id,
        "snapshot_id": snapshot_id,
        "run_timestamp": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "total_checks": total_checks,
        "passed": passed,
        "failed": failed,
        "warned": warned,
        "errored": errored,
        "results": results
    }
    return report


def main():
    parser = argparse.ArgumentParser(description="ValidationRunner: run contract checks against JSONL data")
    parser.add_argument("--contract", required=True, help="Path to contract YAML (Bitol)")
    parser.add_argument("--data", required=True, help="Path to data JSONL file")
    parser.add_argument("--output", required=True, help="Path to write validation report JSON")
    args = parser.parse_args()

    contract_path = args.contract
    data_path = args.data
    out_path = args.output

    try:
        contract = load_contract(contract_path)
    except Exception as e:
        print(json.dumps({"error": f"failed to load contract: {e}"}))
        sys.exit(2)

    try:
        df = safe_load_jsonl_to_df(data_path)
    except Exception as e:
        print(json.dumps({"error": f"failed to load data: {e}"}))
        sys.exit(3)

    try:
        snapshot_id = sha256_of_file(data_path)
    except Exception:
        snapshot_id = ""

    report = run_checks(contract, df, snapshot_id)

    # write output
    safe_dir = os.path.dirname(out_path)
    if safe_dir and not os.path.exists(safe_dir):
        os.makedirs(safe_dir, exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as fh:
        json.dump(report, fh, indent=2, sort_keys=False)

    print(f"Wrote validation report: {out_path}")

if __name__ == '__main__':
    main()
