#!/usr/bin/env python3
"""
contracts/ai_extensions.py

TRP Week 7 Phase 4 AI Extensions:
1) Embedding drift detection
2) Prompt input schema validation + quarantine
3) LLM output schema violation-rate tracking
"""

from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
from dotenv import load_dotenv
from google import genai

# Environment load
load_dotenv()
REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=REPO_ROOT / ".env")

# Shared paths
EXTRACTIONS_PATH = Path("outputs/week3/extractions.jsonl")
VERDICTS_PATH = Path("outputs/week2/verdicts.jsonl")
EMBEDDING_BASELINE_PATH = Path("schema_snapshots/embedding_baselines.npz")
EMBEDDING_REPORT_PATH = Path("validation_reports/embedding_drift_report.json")
PROMPT_SCHEMA_REPORT_PATH = Path("validation_reports/prompt_input_schema_report.json")
LLM_VIOLATION_RATE_REPORT_PATH = Path("validation_reports/llm_output_schema_violation_rate_report.json")
QUARANTINE_DIR = Path("outputs/quarantine")
VIOLATION_LOG_PATH = Path("violation_log/violations.jsonl")

# Embedding settings
EMBEDDING_MODEL = os.getenv("GEMINI_EMBEDDING_MODEL", "gemini-embedding-2-preview")
FALLBACK_EMBEDDING_MODEL = "gemini-embedding-001"
DRIFT_THRESHOLD = 0.15
MAX_TEXT_SAMPLES = 200

# LLM output schema rule
VALID_VERDICTS = {"PASS", "FAIL", "WARN"}
VIOLATION_RATE_THRESHOLD = 0.02


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"JSONL file not found: {path}")
    records: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                records.append(row)
    return records


def append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row) + os.linesep)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)


def get_gemini_client() -> genai.Client:
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise EnvironmentError("GEMINI_API_KEY missing. Add GEMINI_API_KEY=<key> in .env.")
    return genai.Client(api_key=api_key)


# -------------------------
# Extension 1: Embedding Drift
# -------------------------
def collect_extracted_fact_texts(records: List[Dict[str, Any]]) -> List[str]:
    texts: List[str] = []
    for record in records:
        facts = record.get("extracted_facts", [])
        if not isinstance(facts, list):
            continue
        for fact in facts:
            if not isinstance(fact, dict):
                continue
            text = fact.get("text")
            if isinstance(text, str) and text.strip():
                texts.append(text.strip())
    if len(texts) > MAX_TEXT_SAMPLES:
        return texts[:MAX_TEXT_SAMPLES]
    return texts


def _extract_vectors(embed_response: Any) -> List[List[float]]:
    embeddings = getattr(embed_response, "embeddings", None)
    if isinstance(embeddings, list) and embeddings:
        out = []
        for item in embeddings:
            values = getattr(item, "values", None)
            if isinstance(values, list):
                out.append(values)
                continue
            if isinstance(item, dict) and isinstance(item.get("values"), list):
                out.append(item["values"])
                continue
            raise ValueError("Unexpected embedding item format.")
        return out
    raise ValueError("Unexpected embedding response format.")


def get_embeddings(client: genai.Client, texts: List[str], model: str = EMBEDDING_MODEL) -> Tuple[np.ndarray, str]:
    if not texts:
        raise ValueError("No texts provided for embedding generation.")
    try:
        resp = client.models.embed_content(model=model, contents=texts)
        vectors = _extract_vectors(resp)
        return np.array(vectors, dtype=float), model
    except Exception:
        resp = client.models.embed_content(model=FALLBACK_EMBEDDING_MODEL, contents=texts)
        vectors = _extract_vectors(resp)
        return np.array(vectors, dtype=float), FALLBACK_EMBEDDING_MODEL


def cosine_distance(v1: np.ndarray, v2: np.ndarray) -> float:
    n1 = np.linalg.norm(v1)
    n2 = np.linalg.norm(v2)
    if n1 == 0 or n2 == 0:
        raise ValueError("Cannot compute cosine distance for zero vector.")
    return float(1.0 - float(np.dot(v1, v2) / (n1 * n2)))


def save_embedding_baseline(path: Path, centroid: np.ndarray, model: str, source_count: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    np.savez(
        path,
        centroid=centroid,
        model=np.array([model]),
        source_count=np.array([source_count], dtype=np.int64),
        created_at=np.array([utc_now_iso()]),
    )


def load_embedding_baseline(path: Path) -> Dict[str, Any]:
    with np.load(path, allow_pickle=True) as archive:
        if "centroid" not in archive.files:
            raise ValueError("embedding baseline missing centroid.")
        return {
            "centroid": archive["centroid"],
            "model": str(archive["model"][0]) if "model" in archive.files else None,
            "source_count": int(archive["source_count"][0]) if "source_count" in archive.files else None,
            "created_at": str(archive["created_at"][0]) if "created_at" in archive.files else None,
        }


def embedding_drift_detection() -> Dict[str, Any]:
    client = get_gemini_client()
    records = read_jsonl(EXTRACTIONS_PATH)
    texts = collect_extracted_fact_texts(records)
    if not texts:
        raise ValueError("No extracted_facts[*].text values found.")

    embeddings, used_model = get_embeddings(client, texts)
    centroid = embeddings.mean(axis=0)

    if not EMBEDDING_BASELINE_PATH.exists():
        save_embedding_baseline(EMBEDDING_BASELINE_PATH, centroid, used_model, len(texts))
        report = {
            "extension": "embedding_drift_detection",
            "status": "BASELINE_SET",
            "threshold": DRIFT_THRESHOLD,
            "drift_score": 0.0,
            "model": used_model,
            "sample_size": len(texts),
            "baseline_path": str(EMBEDDING_BASELINE_PATH),
            "report_generated_at": utc_now_iso(),
            "message": "No baseline found; baseline created successfully.",
        }
        write_json(EMBEDDING_REPORT_PATH, report)
        return report

    baseline = load_embedding_baseline(EMBEDDING_BASELINE_PATH)
    drift_score = cosine_distance(centroid, baseline["centroid"])
    status = "FAIL" if drift_score > DRIFT_THRESHOLD else "PASS"
    report = {
        "extension": "embedding_drift_detection",
        "status": status,
        "threshold": DRIFT_THRESHOLD,
        "drift_score": float(drift_score),
        "model": used_model,
        "sample_size": len(texts),
        "baseline_path": str(EMBEDDING_BASELINE_PATH),
        "baseline_model": baseline.get("model"),
        "baseline_source_count": baseline.get("source_count"),
        "baseline_created_at": baseline.get("created_at"),
        "report_generated_at": utc_now_iso(),
        "message": "Embedding drift exceeds threshold." if status == "FAIL" else "Embedding drift within threshold.",
    }
    write_json(EMBEDDING_REPORT_PATH, report)
    return report


# -------------------------
# Extension 2: Prompt Input Schema Validation
# -------------------------
PROMPT_INPUT_SCHEMA = {
    "type": "object",
    "required": ["doc_id", "source_path", "content_preview"],
    "properties": {
        "doc_id": {"type": "string", "minLength": 1},
        "source_path": {"type": "string", "minLength": 1},
        "content_preview": {"type": "string", "minLength": 1, "maxLength": 2000},
    },
}


def to_prompt_input(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize extraction records into prompt-input schema fields.
    """
    preview = record.get("content_preview")
    if not isinstance(preview, str) or not preview.strip():
        facts = record.get("extracted_facts", [])
        if isinstance(facts, list) and facts:
            first = facts[0]
            if isinstance(first, dict):
                preview = first.get("source_excerpt") or first.get("text")
    if not isinstance(preview, str):
        preview = ""
    return {
        "doc_id": record.get("doc_id"),
        "source_path": record.get("source_path"),
        "content_preview": preview.strip(),
    }


def validate_prompt_input(item: Dict[str, Any]) -> List[str]:
    errors = []
    for req in PROMPT_INPUT_SCHEMA["required"]:
        if req not in item:
            errors.append(f"missing required field: {req}")
    for field, spec in PROMPT_INPUT_SCHEMA["properties"].items():
        value = item.get(field)
        if spec["type"] == "string":
            if not isinstance(value, str):
                errors.append(f"{field} must be string")
                continue
            if len(value) < spec.get("minLength", 0):
                errors.append(f"{field} too short")
            if "maxLength" in spec and len(value) > spec["maxLength"]:
                errors.append(f"{field} too long")
    return errors


def prompt_input_schema_validation(input_path: Path = EXTRACTIONS_PATH) -> Dict[str, Any]:
    records = read_jsonl(input_path)
    valid_count = 0
    invalid_count = 0
    quarantine_rows: List[Dict[str, Any]] = []
    invalid_examples = []

    for idx, record in enumerate(records):
        candidate = to_prompt_input(record)
        errors = validate_prompt_input(candidate)
        if errors:
            invalid_count += 1
            quarantine_rows.append(
                {
                    "line_index": idx,
                    "errors": errors,
                    "record": candidate,
                    "raw_record": record,
                }
            )
            if len(invalid_examples) < 5:
                invalid_examples.append({"line_index": idx, "errors": errors})
        else:
            valid_count += 1

    quarantine_path = None
    if quarantine_rows:
        QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
        quarantine_path = QUARANTINE_DIR / f"prompt_inputs_quarantine_{datetime.now().strftime('%Y%m%dT%H%M%SZ')}.jsonl"
        with quarantine_path.open("w", encoding="utf-8") as fh:
            for row in quarantine_rows:
                fh.write(json.dumps(row) + os.linesep)

    status = "FAIL" if invalid_count > 0 else "PASS"
    report = {
        "extension": "prompt_input_schema_validation",
        "status": status,
        "schema": PROMPT_INPUT_SCHEMA,
        "input_path": str(input_path),
        "valid_records": valid_count,
        "invalid_records": invalid_count,
        "quarantine_path": str(quarantine_path) if quarantine_path else None,
        "invalid_examples": invalid_examples,
        "report_generated_at": utc_now_iso(),
        "message": (
            f"{invalid_count} invalid prompt input record(s) quarantined."
            if invalid_count > 0
            else "All prompt input records satisfy schema."
        ),
    }
    write_json(PROMPT_SCHEMA_REPORT_PATH, report)
    return report


# -------------------------
# Extension 3: LLM Output Schema Violation Rate
# -------------------------
def llm_output_schema_violation_rate(verdicts_path: Path = VERDICTS_PATH) -> Dict[str, Any]:
    records = read_jsonl(verdicts_path)
    total = len(records)
    invalid = 0
    invalid_rows = []
    for idx, row in enumerate(records):
        verdict = row.get("overall_verdict")
        if not isinstance(verdict, str) or verdict.upper() not in VALID_VERDICTS:
            invalid += 1
            if len(invalid_rows) < 5:
                invalid_rows.append({"line_index": idx, "overall_verdict": verdict})

    rate = (invalid / total) if total else 0.0

    # Determine trend from previous report if available.
    prev_rate = None
    trend = "stable"
    if LLM_VIOLATION_RATE_REPORT_PATH.exists():
        prev = _safe_load_json(LLM_VIOLATION_RATE_REPORT_PATH)
        if isinstance(prev, dict):
            try:
                prev_rate = float(prev.get("violation_rate"))
            except Exception:
                prev_rate = None
    if prev_rate is not None:
        trend = "rising" if rate > prev_rate else "stable"

    status = "WARN" if rate > VIOLATION_RATE_THRESHOLD else "PASS"
    report = {
        "extension": "llm_output_schema_violation_rate",
        "status": status,
        "verdicts_path": str(verdicts_path),
        "valid_verdicts": sorted(VALID_VERDICTS),
        "total_records": total,
        "invalid_records": invalid,
        "violation_rate": round(rate, 6),
        "threshold": VIOLATION_RATE_THRESHOLD,
        "trend": trend,
        "invalid_examples": invalid_rows,
        "report_generated_at": utc_now_iso(),
        "message": (
            "Violation rate above threshold; WARN logged to violation log."
            if status == "WARN"
            else "Violation rate within threshold."
        ),
    }
    write_json(LLM_VIOLATION_RATE_REPORT_PATH, report)

    if status == "WARN":
        warn_entry = {
            "violation_id": str(uuid.uuid4()),
            "check_id": "ai_extensions.llm_output_schema_violation_rate",
            "detected_at": utc_now_iso(),
            "severity": "WARN",
            "details": {
                "violation_rate": rate,
                "threshold": VIOLATION_RATE_THRESHOLD,
                "trend": trend,
                "invalid_records": invalid,
                "total_records": total,
            },
            "blast_radius": {
                "affected_nodes": ["langsmith-verdicts", "week7-data-contract-enforcer"],
                "affected_pipelines": ["week7-enforcer"],
                "estimated_records": invalid,
            },
        }
        append_jsonl(VIOLATION_LOG_PATH, warn_entry)

    return report


def _safe_load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            obj = json.load(fh)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="TRP Week 7 Phase 4 AI Extensions")
    parser.add_argument(
        "--mode",
        choices=["embedding_drift", "prompt_input_schema", "llm_output_rate", "all"],
        required=True,
        help="AI extension mode to run.",
    )
    parser.add_argument(
        "--input",
        default=str(EXTRACTIONS_PATH),
        help="Input JSONL for prompt_input_schema mode.",
    )
    parser.add_argument(
        "--verdicts",
        default=str(VERDICTS_PATH),
        help="Verdicts JSONL path for llm_output_rate mode.",
    )
    args = parser.parse_args()

    try:
        if args.mode == "embedding_drift":
            result = embedding_drift_detection()
            print(json.dumps(result, indent=2))
            return
        if args.mode == "prompt_input_schema":
            result = prompt_input_schema_validation(Path(args.input))
            print(json.dumps(result, indent=2))
            return
        if args.mode == "llm_output_rate":
            result = llm_output_schema_violation_rate(Path(args.verdicts))
            print(json.dumps(result, indent=2))
            return
        # all
        aggregate = {
            "embedding_drift": embedding_drift_detection(),
            "prompt_input_schema": prompt_input_schema_validation(Path(args.input)),
            "llm_output_rate": llm_output_schema_violation_rate(Path(args.verdicts)),
        }
        print(json.dumps(aggregate, indent=2))
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "message": str(exc),
                    "report_generated_at": utc_now_iso(),
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()

