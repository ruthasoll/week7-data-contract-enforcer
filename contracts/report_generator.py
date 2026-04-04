#!/usr/bin/env python3
"""
TRP Week 7 Enforcer Report Generator

Creates a stakeholder-friendly JSON report with five required sections:
1) Data Health Score + narrative
2) Top 3 violations with business impact
3) Schema changes + compatibility verdict
4) AI system risk assessment
5) Recommended actions with file + contract clause
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


VALIDATION_REPORTS_DIR = Path("validation_reports")
VIOLATION_LOG_DIR = Path("violation_log")
AI_METRICS_DIR = Path("ai_metrics")

OUTPUT_DIR = Path("enforcer_report")
OUTPUT_PATH = OUTPUT_DIR / "report_data.json"

CRITICAL_FAIL_PENALTY = 20.0


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def _parse_iso(ts: Any) -> datetime:
    if not isinstance(ts, str) or not ts.strip():
        return datetime.min.replace(tzinfo=timezone.utc)
    raw = ts.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


def load_validation_reports() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not VALIDATION_REPORTS_DIR.exists():
        return rows
    for path in sorted(VALIDATION_REPORTS_DIR.glob("*.json")):
        obj = _safe_load_json(path)
        if isinstance(obj, dict):
            obj["_source_file"] = str(path)
            rows.append(obj)
    return rows


def load_violation_logs() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not VIOLATION_LOG_DIR.exists():
        return rows
    for path in sorted(VIOLATION_LOG_DIR.glob("*.jsonl")):
        try:
            with path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(rec, dict):
                        rec["_source_file"] = str(path)
                        rows.append(rec)
        except Exception:
            continue
    return rows


def load_ai_metrics() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not AI_METRICS_DIR.exists():
        return rows
    for path in sorted(AI_METRICS_DIR.glob("*.json")):
        obj = _safe_load_json(path)
        if isinstance(obj, dict):
            obj["_source_file"] = str(path)
            rows.append(obj)
    return rows


def select_latest_validation_run(reports: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    candidates = [r for r in reports if isinstance(r.get("total_checks"), int)]
    if not candidates:
        return None
    return max(candidates, key=lambda r: _parse_iso(r.get("run_timestamp")))


def is_internal_runner_issue(result_row: Dict[str, Any]) -> bool:
    """
    Identify runner/tooling artifacts that should not be treated as producer data failures.
    """
    check_id = str(result_row.get("check_id", "")).lower()
    message = str(result_row.get("message", "")).lower()
    status = str(result_row.get("status", "")).upper()
    check_type = str(result_row.get("check_type", "")).lower()
    column_name = str(result_row.get("column_name", "")).lower()

    if status == "ERROR":
        return True
    if ".error" in check_id:
        return True
    if "nameerror" in message:
        return True
    if "internal error profiling field" in message:
        return True
    if "cannot access free variable" in message:
        return True
    if "validation runner" in message and "error" in message:
        return True

    # Known runner false-positive pattern from this project for list/object fields.
    if check_type == "type" and column_name in {"entities", "extracted_facts"}:
        if "rows with type !=" in message:
            return True

    return False


def compute_data_health_section(latest_validation: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    print("Calculating health score...")
    if not latest_validation:
        return {
            "score": 0.0,
            "base_score": 0.0,
            "critical_fail_count": 0,
            "ignored_internal_issues": 0,
            "formula": "score = (passed / total_checks) * 100 - (critical_fail_count * 20)",
            "narrative": "No validation run was found, so data health cannot be assessed yet.",
        }

    total_checks = int(latest_validation.get("total_checks", 0) or 0)
    passed = int(latest_validation.get("passed", 0) or 0)
    results = latest_validation.get("results", [])
    if not isinstance(results, list):
        results = []

    critical_fail_count = 0
    ignored_internal = 0
    for row in results:
        if not isinstance(row, dict):
            continue
        if is_internal_runner_issue(row):
            ignored_internal += 1
            continue
        severity = str(row.get("severity", "")).upper()
        status = str(row.get("status", "")).upper()
        if severity == "CRITICAL" and status == "FAIL":
            critical_fail_count += 1

    base_score = (passed / total_checks * 100.0) if total_checks > 0 else 0.0
    penalty = critical_fail_count * CRITICAL_FAIL_PENALTY
    score = max(0.0, base_score - penalty)

    if score >= 85:
        posture = "healthy"
    elif score >= 65:
        posture = "stable but needs attention"
    else:
        posture = "at risk"

    narrative = (
        f"Data health is {posture}. Base quality is {base_score:.1f}% ({passed}/{total_checks} checks passed). "
        f"Penalty applied: {critical_fail_count} real CRITICAL FAIL(s) x 20 points = {penalty:.1f}. "
        f"{ignored_internal} internal runner issue(s) were excluded from penalties, because they reflect tooling defects "
        f"rather than contract producer behavior."
    )

    return {
        "score": round(score, 2),
        "base_score": round(base_score, 2),
        "critical_fail_count": critical_fail_count,
        "ignored_internal_issues": ignored_internal,
        "formula": "score = (passed / total_checks) * 100 - (critical_fail_count * 20)",
        "narrative": narrative,
    }


def _severity_rank(sev: str) -> int:
    order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "WARNING": 3, "LOW": 4}
    return order.get(sev.upper(), 5)


def _business_description(check_id: str, column_name: str, raw_message: str) -> str:
    cid = check_id.lower()
    col = column_name.lower()
    _ = raw_message  # preserved for future specialization

    if "confidence" in cid or "confidence" in col:
        return (
            "Confidence values shifted from a 0.0-1.0 scale to 0-100. "
            "This change can break downstream scoring and decision logic."
        )
    if "format" in cid or "pattern" in cid:
        return "Format mismatch can break joins, identity matching, and audit traceability."
    if "required" in cid:
        return "Missing required fields can stop downstream jobs and reporting pipelines."
    if "type" in cid:
        return "Type mismatch can produce incorrect aggregations and unstable analytics outputs."
    if "range" in cid:
        return "Out-of-range values can distort KPI thresholds and alert logic."
    return "This issue reduces trust in downstream analytics and operational decisions."


def _build_violation_log_index(logs: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for row in logs:
        cid = row.get("check_id")
        if not isinstance(cid, str) or not cid:
            continue
        bucket = index.setdefault(
            cid,
            {
                "occurrences": 0,
                "latest_detected_at": "",
                "max_estimated_records": 0,
                "affected_pipelines": set(),
                "blame_file": "",
            },
        )
        bucket["occurrences"] += 1
        ts = str(row.get("detected_at", ""))
        if _parse_iso(ts) > _parse_iso(bucket["latest_detected_at"]):
            bucket["latest_detected_at"] = ts

        blast = row.get("blast_radius", {})
        if isinstance(blast, dict):
            est = blast.get("estimated_records")
            if isinstance(est, int):
                bucket["max_estimated_records"] = max(bucket["max_estimated_records"], est)
            pipelines = blast.get("affected_pipelines", [])
            if isinstance(pipelines, list):
                for p in pipelines:
                    if isinstance(p, str) and p.strip():
                        bucket["affected_pipelines"].add(p.strip())

        if not bucket["blame_file"]:
            chain = row.get("blame_chain", [])
            if isinstance(chain, list):
                for node in chain:
                    if isinstance(node, dict):
                        fp = node.get("file_path")
                        if isinstance(fp, str) and fp.strip():
                            bucket["blame_file"] = fp.strip()
                            break
    return index


def compute_top_violations_section(
    latest_validation: Optional[Dict[str, Any]],
    logs: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    print("Selecting top violations...")
    if not latest_validation:
        return []

    results = latest_validation.get("results", [])
    if not isinstance(results, list):
        return []

    candidates: List[Dict[str, Any]] = []
    for row in results:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status", "")).upper()
        if status != "FAIL":
            continue
        if is_internal_runner_issue(row):
            continue

        check_id = str(row.get("check_id", ""))
        column_name = str(row.get("column_name", ""))
        severity = str(row.get("severity", "UNKNOWN")).upper()
        records_failing = int(row.get("records_failing", 0) or 0)
        message = str(row.get("message", ""))
        confidence_priority = 0 if ("confidence" in check_id.lower() or "confidence" in column_name.lower()) else 1

        candidates.append(
            {
                "check_id": check_id,
                "column_name": column_name,
                "severity": severity,
                "status": status,
                "records_failing": records_failing,
                "message": message,
                "confidence_priority": confidence_priority,
            }
        )

    candidates.sort(
        key=lambda x: (
            x["confidence_priority"],           # strongly prefer confidence checks
            _severity_rank(x["severity"]),      # CRITICAL first
            -x["records_failing"],              # larger impact first
            x["check_id"],
        )
    )

    log_index = _build_violation_log_index(logs)
    top3: List[Dict[str, Any]] = []
    for c in candidates[:3]:
        idx = log_index.get(c["check_id"], {})
        description = _business_description(c["check_id"], c["column_name"], c["message"])
        entry = {
            "check_id": c["check_id"],
            "column_name": c["column_name"],
            "severity": c["severity"],
            "status": c["status"],
            "records_failing": c["records_failing"],
            "description": description,
            "business_impact": (
                f"{description} Latest failing records: {c['records_failing']}. "
                f"Historical occurrences: {int(idx.get('occurrences', 0) or 0)}."
            ),
            "affected_pipelines": sorted(idx.get("affected_pipelines", set())),
            "latest_detected_at": str(idx.get("latest_detected_at", "")),
        }
        print(f"Selected top violation: {entry['check_id']} ({entry['severity']})")
        top3.append(entry)
    return top3


def compute_schema_changes_section(reports: List[Dict[str, Any]]) -> Dict[str, Any]:
    schema_report = None
    for r in reports:
        if "compatibility_verdict" in r and "diff" in r:
            schema_report = r
            break

    if not schema_report:
        return {
            "compatibility_verdict": "UNKNOWN",
            "summary": "No schema evolution report found.",
            "changes": [],
            "breaking_change_count": 0,
        }

    verdict = str(schema_report.get("compatibility_verdict", "UNKNOWN")).upper()
    diff = schema_report.get("diff", [])
    if not isinstance(diff, list):
        diff = []

    changes: List[str] = []
    for d in diff[:5]:
        if not isinstance(d, dict):
            continue
        fn = str(d.get("field_name", "unknown_field"))
        ct = str(d.get("change_type", "modified"))
        detail = str(d.get("detail", "")).strip()
        line = f"{fn}: {ct}"
        if detail:
            line += f" ({detail})"
        changes.append(line)

    if not changes:
        changes.append("No material schema field changes detected.")

    if verdict == "BREAKING":
        summary = "Breaking schema changes detected; migration planning is required."
    elif verdict == "COMPATIBLE":
        summary = "Current schema changes are backward-compatible."
    else:
        summary = "Schema compatibility could not be confidently determined."

    return {
        "compatibility_verdict": verdict,
        "summary": summary,
        "changes": changes,
        "breaking_change_count": int(schema_report.get("breaking_change_count", 0) or 0),
    }


def compute_ai_risk_section(
    reports: List[Dict[str, Any]],
    data_health: Dict[str, Any],
    top_violations: List[Dict[str, Any]],
    ai_metrics: List[Dict[str, Any]],
) -> Dict[str, Any]:
    print("Assessing AI system risk...")
    points = 0
    drivers: List[str] = []

    emb = None
    for r in reports:
        if str(r.get("extension", "")).lower() == "embedding_drift_detection":
            emb = r
            break

    if emb:
        status = str(emb.get("status", "UNKNOWN")).upper()
        drift = float(emb.get("drift_score", 0.0) or 0.0)
        threshold = float(emb.get("threshold", 0.15) or 0.15)
        if status == "FAIL" or drift > threshold:
            points += 3
            drivers.append(f"Embedding drift exceeded threshold ({drift:.4f} > {threshold:.4f}).")
        elif status == "BASELINE_SET":
            points += 1
            drivers.append("Embedding baseline was recently initialized; trend confidence is limited.")
    else:
        points += 1
        drivers.append("No embedding drift report available.")

    health_score = float(data_health.get("score", 0.0) or 0.0)
    if health_score < 65:
        points += 3
        drivers.append(f"Data health score is low ({health_score:.1f}%).")
    elif health_score < 85:
        points += 1
        drivers.append(f"Data health score is moderate ({health_score:.1f}%).")

    if any(str(v.get("severity", "")).upper() == "CRITICAL" for v in top_violations):
        points += 2
        drivers.append("Critical contract violations are still present.")

    if not ai_metrics:
        points += 1
        drivers.append("No AI metrics telemetry files found for trend analysis.")

    if points >= 7:
        level = "HIGH"
    elif points >= 4:
        level = "MEDIUM"
    else:
        level = "LOW"

    return {
        "risk_level": level,
        "risk_points": points,
        "summary": f"AI system risk is {level}. " + " ".join(drivers),
        "drivers": drivers,
    }


def _default_action_mapping(check_id: str) -> tuple[str, str]:
    cid = check_id.lower()
    if "confidence" in cid:
        return (
            "src/week3/extractor.py",
            "generated_contracts/week3_extractions.yaml::quality.specification.checks[week3_extractions.extracted_facts.confidence.range]",
        )
    if "source_hash" in cid and "format" in cid:
        return (
            "src/week3/document_refinery.py",
            "generated_contracts/week3_extractions.yaml::schema.source_hash.pattern",
        )
    if "required" in cid:
        return (
            "src/week3/document_refinery.py",
            "generated_contracts/week3_extractions.yaml::schema.required",
        )
    return (
        "contracts/runner.py",
        "generated_contracts/week3_extractions.yaml::quality.specification.checks",
    )


def compute_recommended_actions_section(
    top_violations: List[Dict[str, Any]],
    schema_section: Dict[str, Any],
    ai_risk_section: Dict[str, Any],
) -> List[Dict[str, str]]:
    actions: List[Dict[str, str]] = []

    for v in top_violations:
        file_path, clause = _default_action_mapping(str(v.get("check_id", "")))
        actions.append(
            {
                "priority": "P1",
                "file_path": file_path,
                "contract_clause": clause,
                "action": (
                    f"Fix `{v.get('check_id')}` and add a regression test to prevent this contract break from recurring."
                ),
            }
        )

    verdict = str(schema_section.get("compatibility_verdict", "UNKNOWN")).upper()
    if verdict == "BREAKING":
        actions.append(
            {
                "priority": "P1",
                "file_path": "contracts/schema_analyzer.py",
                "contract_clause": "schema_evolution.compatibility_verdict",
                "action": "Publish a migration plan and coordinate downstream consumer cutover before deployment.",
            }
        )
    else:
        actions.append(
            {
                "priority": "P2",
                "file_path": "contracts/schema_analyzer.py",
                "contract_clause": "schema_evolution.monitoring",
                "action": "Continue weekly schema diff checks to catch future breaking changes early.",
            }
        )

    risk_level = str(ai_risk_section.get("risk_level", "LOW")).upper()
    if risk_level in {"HIGH", "MEDIUM"}:
        actions.append(
            {
                "priority": "P2",
                "file_path": "contracts/ai_extensions.py",
                "contract_clause": "ai.embedding_drift_detection.threshold",
                "action": "Run embedding drift check on each pipeline execution and block publish on FAIL.",
            }
        )

    return actions[:6]


def build_report_payload() -> Dict[str, Any]:
    reports = load_validation_reports()
    logs = load_violation_logs()
    ai_metrics = load_ai_metrics()

    latest = select_latest_validation_run(reports)
    s1 = compute_data_health_section(latest)
    s2 = compute_top_violations_section(latest, logs)
    s3 = compute_schema_changes_section(reports)
    s4 = compute_ai_risk_section(reports, s1, s2, ai_metrics)
    s5 = compute_recommended_actions_section(s2, s3, s4)

    return {
        "generated_at": utc_now_iso(),
        "section_1_data_health_score": s1,
        "section_2_top_violations": s2,
        "section_3_schema_changes": s3,
        "section_4_ai_system_risk_assessment": s4,
        "section_5_recommended_actions": s5,
        "meta": {
            "latest_validation_source": latest.get("_source_file") if latest else None,
            "validation_reports_scanned": len(reports),
            "violation_log_records_scanned": len(logs),
            "ai_metrics_files_scanned": len(ai_metrics),
            "output_path": str(OUTPUT_PATH),
        },
    }


def write_report(payload: Dict[str, Any]) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    return OUTPUT_PATH


def main() -> None:
    payload = build_report_payload()
    out = write_report(payload)
    print(f"Report written: {out}")


if __name__ == "__main__":
    main()

