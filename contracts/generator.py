#!/usr/bin/env python3
"""
contracts/generator.py

ContractGenerator: infer Bitol DataContract YAML and dbt tests from a JSONL source.

Usage (example for Week 3):
  python contracts/generator.py --source outputs/week3/extractions.jsonl --output-dir generated_contracts

Key features implemented (per challenge):
 - Structural profiling (including explode of extracted_facts[])
 - Statistical profiling for numeric columns (min/max/mean/pctiles/stddev)
 - Special handling for any field containing "confidence" (enforce 0.0..1.0 range & breaking warning)
 - Lineage context injection using outputs/week4/lineage_snapshots.jsonl (graceful if missing)
 - Snapshot saved to schema_snapshots/{contract_id}/{timestamp}.yaml
 - dbt-style YAML companion file generated
"""

import argparse
import os
import sys
import json
import datetime
import pathlib
import pandas as pd
import yaml
import math
from collections import defaultdict

# -------------------------
# Helpers
# -------------------------
UUID_REGEX = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
SHA256_REGEX = r'^[a-f0-9]{64}$'
ISO_DATETIME_SAMPLE = '2025-01-15T14:23:00Z'

def load_jsonl(path):
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    return pd.read_json(path, lines=True)

def safe_mkdir(path):
    os.makedirs(path, exist_ok=True)

def iso_ts_now():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def detect_string_pattern(values):
    # simple heuristics to detect uuid / sha256 / iso-datetime / url
    values = [v for v in values if isinstance(v, str) and v]
    if not values:
        return None
    sample = values[:10]
    try:
        if all(pd.Series(sample).str.match(UUID_REGEX, na=False)):
            return "uuid"
        if all(pd.Series(sample).str.match(SHA256_REGEX, na=False)):
            return "sha256"
    except Exception:
        pass
    if all(('T' in s and s.endswith('Z') and len(s) >= 20) for s in sample):
        return "iso_datetime"
    if all(s.startswith("http://") or s.startswith("https://") for s in sample):
        return "url"
    return None

def numeric_stats(series):
    ser = series.dropna()
    # try numeric coercion
    try:
        ser = ser.astype(float)
    except Exception:
        return None
    if ser.empty:
        return None
    desc = ser.quantile([0.0, 0.25, 0.5, 0.75, 0.95, 0.99]).to_dict()
    return {
        "min": float(ser.min()),
        "max": float(ser.max()),
        "mean": float(ser.mean()),
        "p25": float(desc.get(0.25, math.nan)),
        "p50": float(desc.get(0.5, math.nan)),
        "p75": float(desc.get(0.75, math.nan)),
        "p95": float(desc.get(0.95, math.nan)),
        "p99": float(desc.get(0.99, math.nan)),
        "stddev": float(ser.std()) if not math.isnan(float(ser.std())) else 0.0,
    }

def _safe_serialize_for_uniqueness(val):
    # Convert unhashable types (list/dict) to a stable JSON string for uniqueness checks
    if isinstance(val, (list, dict)):
        try:
            return json.dumps(val, sort_keys=True)
        except Exception:
            return str(val)
    return val

def summarize_column(series):
    total = len(series)
    nulls = int(series.isnull().sum()) if total else 0
    null_frac = float(nulls) / total if total else 0.0
    # compute unique safely: handle unhashable list/dict elements by serializing them
    try:
        unique = int(series.nunique(dropna=True))
    except TypeError:
        # fallback: map values to serialized representation
        safe_vals = series.dropna().apply(_safe_serialize_for_uniqueness)
        unique = int(safe_vals.nunique(dropna=True))
    # sample values: convert complex objects to readable form
    # sample values: take up to first 5 non-null rows and serialize complex objects for readability
    sample_vals = []
    for v in series.dropna().head(5):
        if isinstance(v, (list, dict)):
            try:
                sample_vals.append(json.dumps(v, sort_keys=True))
            except Exception:
                sample_vals.append(str(v))
        else:
            sample_vals.append(v)
    dtype = str(series.dtype)
    pattern = None
    if dtype == 'object':
        # take up to 50 string representations to detect pattern
        string_candidates = []
        for v in series.dropna().head(200):
            if isinstance(v, str):
                string_candidates.append(v)
            elif isinstance(v, (list, dict)):
                # try convert to str
                try:
                    s = json.dumps(v, sort_keys=True)
                    string_candidates.append(s)
                except Exception:
                    continue
        pattern = detect_string_pattern(string_candidates)
    return {
        "dtype": dtype,
        "null_fraction": round(null_frac, 6),
        "cardinality": int(unique),
        "sample_values": sample_vals,
        "dominant_pattern": pattern
    }

def explode_array_column(df, array_col, prefix):
    """
    If array_col exists and contains lists of dicts, explode into a DataFrame where
    nested fields are available as prefix_field.
    Returns exploded_df and a mapping of new column names.
    """
    if array_col not in df.columns:
        return None, {}
    rows = []
    for _, row in df.iterrows():
        base = {k: row[k] for k in df.columns if k != array_col}
        arr = row.get(array_col)
        if isinstance(arr, list) and arr:
            for item in arr:
                if isinstance(item, dict):
                    new = base.copy()
                    for k, v in item.items():
                        new[f"{prefix}{k}"] = v
                    rows.append(new)
                else:
                    new = base.copy()
                    new[f"{prefix}item"] = item
                    rows.append(new)
        else:
            # preserve an empty item row to keep shape (fields will be NaN)
            new = base.copy()
            rows.append(new)
    if not rows:
        return None, {}
    exploded = pd.DataFrame(rows)
    mapping = {c: c for c in exploded.columns if c.startswith(prefix)}
    return exploded, mapping

# -------------------------
# Lineage loading
# -------------------------
def load_latest_lineage(lineage_path):
    if not os.path.exists(lineage_path):
        return None
    snapshots = []
    try:
        with open(lineage_path, 'r', encoding='utf-8') as fh:
            for line in fh:
                if not line.strip():
                    continue
                try:
                    snapshots.append(json.loads(line))
                except Exception:
                    continue
    except Exception:
        return None
    if not snapshots:
        return None
    snapshots_sorted = sorted(snapshots, key=lambda s: s.get('captured_at', ''), reverse=True)
    return snapshots_sorted[0]

def find_downstream_consumers(lineage_snapshot, source_hint=None):
    if not lineage_snapshot:
        return []
    nodes = lineage_snapshot.get('nodes', [])
    edges = lineage_snapshot.get('edges', [])
    node_map = {n['node_id']: n for n in nodes}
    consumers = []
    for e in edges:
        rel = e.get('relationship', '')
        tgt = e.get('target')
        target_node = node_map.get(tgt)
        if not target_node:
            continue
        md = target_node.get('metadata', {})
        path = md.get('path', '') or ''
        label = target_node.get('label', '') or ''
        candidate = False
        if 'cartograph' in path.lower() or 'cartographer' in path.lower() or 'week4' in path.lower() or 'cartograph' in label.lower():
            candidate = True
        if rel in ('CONSUMES', 'READS') and ('week3' in (node_map.get(e.get('source'), {}).get('metadata', {}).get('path','') or '') or 'extraction' in (node_map.get(e.get('source'), {}).get('label','') or '').lower()):
            candidate = True
        if candidate:
            consumers.append({
                "id": target_node.get('node_id'),
                "description": f"{target_node.get('label')} ({path})",
                "fields_consumed": []
            })
    if not consumers:
        for n in nodes:
            path = n.get('metadata', {}).get('path', '') or ''
            if 'week4' in path.lower() or 'cartograph' in (n.get('label','') or '').lower():
                consumers.append({
                    "id": n['node_id'], "description": f"{n.get('label')} ({path})", "fields_consumed": []
                })
    return consumers

# -------------------------
# Contract generation
# -------------------------
def build_bitol_contract(contract_id, info_title, source_path, profiles, numeric_profiles, confidence_fields, lineage_consumers):
    now = iso_ts_now()
    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": contract_id,
        "info": {
            "title": info_title,
            "version": "1.0.0",
            "owner": contract_id.split('_')[0] if '_' in contract_id else "owner",
            "description": f"Auto-generated contract for {source_path}"
        },
        "servers": {
            "local": {"type": "local", "path": source_path, "format": "jsonl"}
        },
        "terms": {
            "usage": "Internal inter-system data contract. Do not publish.",
            "limitations": "See schema clauses for field-level constraints. Any change to fields listed in lineage.breaking_if_changed is breaking."
        },
        "schema": {},
        "quality": {
            "type": "SodaChecks",
            "specification": {"checks": []}
        },
        "lineage": {
            "upstream": [],
            "downstream": lineage_consumers or []
        },
        "generated_at": now
    }

    # top-level fields
    for col, meta in profiles.items():
        dtype = meta.get("dtype", "object")
        if dtype.startswith('int'):
            t = "integer"
        elif dtype.startswith('float') or dtype.startswith('double') or dtype.startswith('number'):
            t = "number"
        else:
            t = "string" if dtype.startswith('object') else dtype
        entry = {
            "type": t,
            "required": meta["null_fraction"] < 1.0,
            "description": f"auto-profiled dtype={meta['dtype']}, cardinality={meta['cardinality']}"
        }
        if meta.get("dominant_pattern") == "uuid":
            entry["format"] = "uuid"
        if meta.get("dominant_pattern") == "sha256":
            entry["pattern"] = "^[a-f0-9]{64}$"
        contract["schema"][col] = entry

    # handle exploded nested numeric profiles (items)
    if numeric_profiles:
        for key, stats in numeric_profiles.items():
            parts = key.split('.')
            if len(parts) < 2:
                # top-level numeric column
                contract["schema"].setdefault(parts[0], {})
                contract["schema"][parts[0]].update({
                    "type": "number",
                    "minimum": stats.get("min"),
                    "maximum": stats.get("max"),
                    "description": f"Numeric profile: mean={stats.get('mean'):.6g}, stddev={stats.get('stddev'):.6g}"
                })
                if "confidence" in parts[0].lower():
                    contract["schema"][parts[0]]["minimum"] = 0.0
                    contract["schema"][parts[0]]["maximum"] = 1.0
                    contract["schema"][parts[0]]["description"] += " IMPORTANT: MUST be float in 0.0–1.0. BREAKING if changed to 0-100."
                    contract["quality"]["specification"]["checks"].append({
                        "check_id": f"{contract_id}.{parts[0]}.range",
                        "column": parts[0],
                        "check": "range",
                        "expected": {"minimum": 0.0, "maximum": 1.0},
                        "severity": "CRITICAL"
                    })
                continue

            arr_name = parts[0]
            field_name = parts[1]
            if arr_name not in contract["schema"]:
                contract["schema"][arr_name] = {"type": "array", "items": {}}
            items = contract["schema"][arr_name].get("items", {})
            items.setdefault(field_name, {})
            items[field_name].update({
                "type": "number",
                "minimum": stats.get("min"),
                "maximum": stats.get("max"),
                "description": f"Numeric profile: mean={stats.get('mean'):.6g}, stddev={stats.get('stddev'):.6g}"
            })
            contract["schema"][arr_name]["items"] = items

            if "confidence" in field_name.lower():
                items[field_name]["minimum"] = 0.0
                items[field_name]["maximum"] = 1.0
                items[field_name]["description"] = items[field_name]["description"] + " IMPORTANT: MUST be float in 0.0–1.0. BREAKING if changed to 0-100."
                contract["quality"]["specification"]["checks"].append({
                    "check_id": f"{contract_id}.{arr_name}.{field_name}.range",
                    "column": f"{arr_name}[*].{field_name}",
                    "check": "range",
                    "expected": {"minimum": 0.0, "maximum": 1.0},
                    "severity": "CRITICAL"
                })
                if "breaking_if_changed" not in contract["lineage"]:
                    contract["lineage"]["breaking_if_changed"] = []
                contract["lineage"]["breaking_if_changed"].append(f"{arr_name}.{field_name}")

    contract["quality"]["specification"]["checks"].append({
        "check_id": f"{contract_id}.row_count",
        "check": "row_count",
        "expected": {"minimum": 1},
        "severity": "HIGH"
    })
    return contract

def generate_dbt_tests(contract, dbt_outpath):
    model = {"version": 2, "models": [{"name": contract["id"], "description": contract["info"].get("description",""), "columns": []}]}
    for col_name, col_def in contract.get("schema", {}).items():
        col_entry = {"name": col_name, "description": col_def.get("description","")}
        tests = []
        if col_def.get("required"):
            tests.append("not_null")
        if isinstance(col_def.get("enum"), list):
            tests.append({"accepted_values": {"values": col_def["enum"]}})
        if tests:
            col_entry["tests"] = tests
        model["models"][0]["columns"].append(col_entry)
    with open(dbt_outpath, "w", encoding="utf-8") as fh:
        yaml.safe_dump(model, fh, sort_keys=False)

# -------------------------
# Main flow
# -------------------------
def main():
    parser = argparse.ArgumentParser(description="ContractGenerator: generate Bitol contract + dbt tests from JSONL")
    parser.add_argument("--source", required=True, help="Path to source JSONL (e.g. outputs/week3/extractions.jsonl)")
    parser.add_argument("--output-dir", default="generated_contracts", help="Directory to write generated contracts")
    parser.add_argument("--lineage", default="outputs/week4/lineage_snapshots.jsonl", help="Path to week4 lineage snapshots JSONL (optional)")
    args = parser.parse_args()

    source = args.source
    output_dir = args.output_dir
    lineage_path = args.lineage

    if not os.path.exists(source):
        print(f"[ERROR] source not found: {source}", file=sys.stderr)
        sys.exit(2)

    safe_mkdir(output_dir)

    basename = pathlib.Path(source).stem
    parts = pathlib.Path(source).parts
    week_token = None
    for p in parts:
        if p.lower().startswith("week"):
            week_token = p
            break
    contract_id = f"{week_token}_{basename}" if week_token else f"{basename}"
    contract_id = contract_id.replace("-", "_")

    try:
        df = load_jsonl(source)
    except Exception as e:
        print(f"[ERROR] failed to read JSONL {source}: {e}", file=sys.stderr)
        sys.exit(3)

    profiles = {}
    for col in df.columns:
        try:
            profiles[col] = summarize_column(df[col])
        except Exception:
            profiles[col] = {"dtype": str(df[col].dtype), "null_fraction": 1.0, "cardinality": 0, "sample_values": [], "dominant_pattern": None}

    numeric_profiles = {}
    confidence_fields = []
    exploded_map = {}
    if 'extracted_facts' in df.columns:
        exploded_df, mapping = explode_array_column(df, 'extracted_facts', 'extracted_facts.')
        if exploded_df is not None:
            for col in exploded_df.columns:
                if col.startswith('extracted_facts.'):
                    short = col.replace('extracted_facts.', '')
                    exploded_map[short] = summarize_column(exploded_df[col])
                    if pd.api.types.is_numeric_dtype(exploded_df[col]) or short.lower().find("confidence") >= 0:
                        stats = numeric_stats(exploded_df[col])
                        if stats:
                            numeric_profiles[f"extracted_facts.{short}"] = stats
                            if "confidence" in short.lower():
                                confidence_fields.append(f"extracted_facts.{short}")

    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            stats = numeric_stats(df[col])
            if stats:
                numeric_profiles[col] = stats
                if "confidence" in col.lower():
                    confidence_fields.append(col)

    lineage_snapshot = None
    try:
        lineage_snapshot = load_latest_lineage(lineage_path)
    except Exception:
        lineage_snapshot = None
    lineage_consumers = find_downstream_consumers(lineage_snapshot, source)

    info_title = f"Auto-generated contract for {basename}"
    contract = build_bitol_contract(contract_id, info_title, source, profiles, numeric_profiles, confidence_fields, lineage_consumers)

    if lineage_consumers:
        contract["lineage"]["downstream"] = lineage_consumers
    else:
        if 'week3' in contract_id.lower():
            contract["lineage"]["downstream"] = [{
                "id": "week4-cartographer",
                "description": "Cartographer ingests doc_id and extracted_facts as node metadata",
                "fields_consumed": ["doc_id", "extracted_facts", "extraction_model"]
            }]
            contract["lineage"].setdefault("breaking_if_changed", []).extend(["extracted_facts.confidence", "doc_id"])

    safe_mkdir(output_dir)
    out_path = os.path.join(output_dir, f"{contract_id}.yaml")
    with open(out_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(contract, fh, sort_keys=False)
    print(f"[INFO] wrote contract: {out_path}")

    snapshot_dir = os.path.join("schema_snapshots", contract_id)
    safe_mkdir(snapshot_dir)
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    snapshot_path = os.path.join(snapshot_dir, f"{timestamp}.yaml")
    with open(snapshot_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(contract, fh, sort_keys=False)
    print(f"[INFO] saved schema snapshot: {snapshot_path}")

    dbt_outpath = os.path.join(output_dir, f"{contract_id}_dbt.yml")
    try:
        generate_dbt_tests(contract, dbt_outpath)
        print(f"[INFO] wrote dbt tests: {dbt_outpath}")
    except Exception as e:
        print(f"[WARN] dbt test generation failed: {e}", file=sys.stderr)

    print(f"[SUMMARY] columns profiled: {len(profiles)}; nested numeric profiles: {len(numeric_profiles)}; confidence_fields: {confidence_fields}")
    if lineage_snapshot:
        print("[INFO] lineage snapshot loaded and downstream consumers injected where detected.")
    else:
        print("[WARN] lineage snapshot missing or could not be parsed; fallback downstream hints applied.")

if __name__ == "__main__":
    main()