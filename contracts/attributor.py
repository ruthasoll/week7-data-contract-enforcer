#!/usr/bin/env python3
"""
contracts/attributor.py

Violation attributor with registry-first blast radius.
"""

import argparse
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import yaml


DEFAULT_REGISTRY = "contracts/subscriptions_registry.json"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def load_latest_lineage(lineage_path: str) -> Dict[str, Any] | None:
    try:
        with open(lineage_path, "r", encoding="utf-8") as fh:
            lines = [line.strip() for line in fh if line.strip()]
        if not lines:
            return None
        return json.loads(lines[-1])
    except Exception:
        return None


def load_contract_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        obj = yaml.safe_load(fh)
    return obj if isinstance(obj, dict) else {}


def load_registry(path: str) -> List[Dict[str, Any]]:
    try:
        payload = load_json(path)
    except Exception:
        return []
    subs = payload.get("subscriptions", [])
    return subs if isinstance(subs, list) else []


def choose_failed_check(results: List[Dict[str, Any]]) -> Dict[str, Any] | None:
    # confidence FAIL first
    for row in results:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status", "")).upper()
        cid = str(row.get("check_id", "")).lower()
        col = str(row.get("column_name", "")).lower()
        if status == "FAIL" and ("confidence" in cid or "confidence" in col):
            return row
    # then critical FAIL
    for row in results:
        if not isinstance(row, dict):
            continue
        if str(row.get("status", "")).upper() == "FAIL" and str(row.get("severity", "")).upper() == "CRITICAL":
            return row
    # then any FAIL/ERROR
    for row in results:
        if not isinstance(row, dict):
            continue
        if str(row.get("status", "")).upper() in {"FAIL", "ERROR"}:
            return row
    return None


def registry_blast_radius(registry: List[Dict[str, Any]], contract_id: str) -> Dict[str, List[str]]:
    """
    Use registry first for blast radius.
    """
    if not contract_id:
        return {"affected_nodes": [], "affected_pipelines": []}

    cid = contract_id.lower().replace("_", "-")
    sources = {cid}
    if "week3" in cid:
        sources.add("week3-document-refinery-extractions")
    if "week5" in cid or "event" in cid:
        sources.add("week5-events")

    affected_nodes = []
    affected_pipelines = []
    for sub in registry:
        if not isinstance(sub, dict):
            continue
        source = str(sub.get("source", "")).lower()
        if source in sources:
            consumer = str(sub.get("consumer", "")).strip()
            pipeline = str(sub.get("pipeline", "")).strip()
            if consumer:
                affected_nodes.append(consumer)
            if pipeline:
                affected_pipelines.append(pipeline)

    # Always include other canonical upstream links that feed Week7.
    for source_name in ("week4-lineage-snapshots", "langsmith-verdicts"):
        for sub in registry:
            if str(sub.get("source", "")).lower() == source_name:
                consumer = str(sub.get("consumer", "")).strip()
                pipeline = str(sub.get("pipeline", "")).strip()
                if consumer:
                    affected_nodes.append(consumer)
                if pipeline:
                    affected_pipelines.append(pipeline)

    return {
        "affected_nodes": sorted(set(affected_nodes)),
        "affected_pipelines": sorted(set(affected_pipelines)),
    }


def fallback_blast_radius_from_lineage(lineage_snapshot: Dict[str, Any] | None, contract_yaml: Dict[str, Any]) -> Dict[str, List[str]]:
    affected_nodes = []
    affected_pipelines = []
    downstream = contract_yaml.get("lineage", {}).get("downstream", [])
    if isinstance(downstream, list):
        for d in downstream:
            if isinstance(d, dict):
                nid = str(d.get("id", "")).strip()
            else:
                nid = str(d).strip()
            if nid:
                affected_nodes.append(nid)
                if "week" in nid.lower() or "cartograph" in nid.lower():
                    affected_pipelines.append(nid)

    if not affected_nodes and lineage_snapshot:
        nodes = lineage_snapshot.get("nodes", [])
        if isinstance(nodes, list):
            for n in nodes:
                if not isinstance(n, dict):
                    continue
                nid = str(n.get("node_id") or n.get("id") or "").strip()
                label = str(n.get("label", "")).lower()
                if nid and ("week7" in label or "cartograph" in label):
                    affected_nodes.append(nid)
                    affected_pipelines.append(nid)

    return {
        "affected_nodes": sorted(set(affected_nodes)),
        "affected_pipelines": sorted(set(affected_pipelines)),
    }


def simple_blame_chain(failed_check: Dict[str, Any]) -> List[Dict[str, Any]]:
    check_id = str(failed_check.get("check_id", "")).lower()
    files = ["src/week3/extractor.py", "src/week3/document_refinery.py"]
    if "event" in check_id:
        files = ["src/week5/events.py", "src/week7/enforcer.py"]
    chain = []
    for idx, fp in enumerate(files, start=1):
        chain.append(
            {
                "rank": idx,
                "file_path": fp,
                "commit_hash": "unknown",
                "author": "unknown",
                "commit_timestamp": now_iso(),
                "commit_message": "registry-guided attribution fallback",
                "confidence_score": round(max(0.3, 0.8 - (idx - 1) * 0.2), 2),
            }
        )
    return chain


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--violation", required=True, help="Validation report JSON path")
    parser.add_argument("--lineage", required=True, help="Lineage snapshots JSONL path")
    parser.add_argument("--contract", required=True, help="Contract YAML path")
    parser.add_argument("--output", default="violation_log/violations.jsonl", help="Output JSONL for violation entries")
    parser.add_argument("--registry", default=DEFAULT_REGISTRY, help="Subscription registry JSON path")
    args = parser.parse_args()

    Path("violation_log").mkdir(exist_ok=True)

    try:
        report = load_json(args.violation)
    except Exception:
        report = {"results": []}
    results = report.get("results", [])
    if not isinstance(results, list):
        results = []

    failed_check = choose_failed_check(results)
    if not failed_check:
        failed_check = {
            "check_id": None,
            "records_failing": 0,
            "status": "PASS",
            "message": "no failed checks in report",
        }

    contract_yaml = load_contract_yaml(args.contract)
    contract_id = str(contract_yaml.get("id", "") or "")
    lineage_snapshot = load_latest_lineage(args.lineage)
    registry = load_registry(args.registry)

    blast = registry_blast_radius(registry, contract_id)
    if not blast["affected_nodes"]:
        blast = fallback_blast_radius_from_lineage(lineage_snapshot, contract_yaml)

    violation_entry = {
        "violation_id": str(uuid.uuid4()),
        "check_id": failed_check.get("check_id"),
        "detected_at": now_iso(),
        "blame_chain": simple_blame_chain(failed_check),
        "blast_radius": {
            "affected_nodes": blast["affected_nodes"],
            "affected_pipelines": blast["affected_pipelines"],
            "estimated_records": int(failed_check.get("records_failing", 0) or 0),
        },
    }

    with open(args.output, "a", encoding="utf-8") as out_f:
        out_f.write(json.dumps(violation_entry) + os.linesep)

    print(f"Wrote violation to {args.output}")
    print(f"check_id={violation_entry.get('check_id')}")
    print(f"affected_pipelines={violation_entry['blast_radius']['affected_pipelines']}")


if __name__ == "__main__":
    main()

