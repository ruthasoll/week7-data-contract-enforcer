#!/usr/bin/env python3
"""
contracts/ai_extensions.py

TRP Week 7 Phase 4 AI contract extensions.
Currently implemented:
 - Extension 1: Embedding Drift Detection (Gemini via google-genai SDK)

Install requirements:
  pip install google-genai python-dotenv numpy
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
from dotenv import load_dotenv
from google import genai

# Load .env from both CWD and repository root for reliability.
load_dotenv()
REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=REPO_ROOT / ".env")

# Shared paths
EXTRACTIONS_PATH = Path("outputs/week3/extractions.jsonl")
EMBEDDING_BASELINE_PATH = Path("schema_snapshots/embedding_baselines.npz")
EMBEDDING_REPORT_PATH = Path("validation_reports/embedding_drift_report.json")

# Extension 1 settings
EMBEDDING_MODEL = os.getenv("GEMINI_EMBEDDING_MODEL", "gemini-embedding-2-preview")
FALLBACK_EMBEDDING_MODEL = "gemini-embedding-001"
DRIFT_THRESHOLD = 0.15
MAX_TEXT_SAMPLES = 200


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def get_gemini_api_key() -> str:
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise EnvironmentError(
            "GEMINI_API_KEY is missing. Add GEMINI_API_KEY=<your_key> to .env and rerun."
        )
    return api_key


def create_gemini_client() -> genai.Client:
    api_key = get_gemini_api_key()
    return genai.Client(api_key=api_key)


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
                records.append(json.loads(line))
            except json.JSONDecodeError:
                # Skip malformed lines so one bad record does not block the run.
                continue
    return records


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
    return texts


def sample_texts(texts: List[str], max_samples: int = MAX_TEXT_SAMPLES) -> List[str]:
    # Deterministic sampling for stable baseline behavior.
    if len(texts) <= max_samples:
        return texts
    return texts[:max_samples]


def _extract_vector_from_item(item: Any) -> List[float]:
    """Extract vector values from one embedding item in a tolerant way."""
    if item is None:
        raise ValueError("Embedding item is None.")

    # Dict-like SDK response
    if isinstance(item, dict):
        if isinstance(item.get("values"), list):
            return item["values"]
        if isinstance(item.get("embedding"), list):
            return item["embedding"]

    # Object-like SDK response
    values = getattr(item, "values", None)
    if isinstance(values, list):
        return values
    embedding = getattr(item, "embedding", None)
    if isinstance(embedding, list):
        return embedding

    raise ValueError("Could not parse embedding vector from response item.")


def _extract_vectors(embed_response: Any) -> List[List[float]]:
    """
    Extract vectors from google-genai embed_content response.
    Supports current shape (.embeddings) plus tolerant fallbacks.
    """
    if embed_response is None:
        raise ValueError("Empty embed response from Gemini.")

    # Common shape for batch calls: response.embeddings
    embeddings = getattr(embed_response, "embeddings", None)
    if isinstance(embeddings, list) and embeddings:
        return [_extract_vector_from_item(item) for item in embeddings]

    # Possible single-embedding shape: response.embedding
    single = getattr(embed_response, "embedding", None)
    if single is not None:
        return [_extract_vector_from_item(single)]

    # Dict-like fallbacks
    if isinstance(embed_response, dict):
        if isinstance(embed_response.get("embeddings"), list):
            return [_extract_vector_from_item(item) for item in embed_response["embeddings"]]
        if embed_response.get("embedding") is not None:
            return [_extract_vector_from_item(embed_response["embedding"])]

    raise ValueError("Unexpected embed_content response shape.")


def get_text_embeddings(
    client: genai.Client,
    texts: List[str],
    model: str = EMBEDDING_MODEL,
) -> tuple[np.ndarray, str]:
    """
    Generate embeddings for text inputs.

    Text-only for now, while keeping the `contents` call shape that can later
    accept multimodal content objects in the same API.
    """
    if not texts:
        raise ValueError("No texts provided for embedding generation.")

    # Use batch embed call for efficiency.
    try:
        response = client.models.embed_content(
            model=model,
            contents=texts,
        )
        vectors = _extract_vectors(response)
        return np.array(vectors, dtype=float), model
    except Exception as primary_error:
        # Optional fallback for environments where preview model is unavailable.
        if model == EMBEDDING_MODEL and FALLBACK_EMBEDDING_MODEL and FALLBACK_EMBEDDING_MODEL != model:
            response = client.models.embed_content(
                model=FALLBACK_EMBEDDING_MODEL,
                contents=texts,
            )
            vectors = _extract_vectors(response)
            return np.array(vectors, dtype=float), FALLBACK_EMBEDDING_MODEL
        raise primary_error


def compute_centroid(embeddings: np.ndarray) -> np.ndarray:
    if embeddings.size == 0:
        raise ValueError("Cannot compute centroid from empty embeddings.")
    return embeddings.mean(axis=0)


def cosine_distance(vec1: np.ndarray, vec2: np.ndarray) -> float:
    norm1 = np.linalg.norm(vec1)
    norm2 = np.linalg.norm(vec2)
    if norm1 == 0 or norm2 == 0:
        raise ValueError("Cosine distance undefined for zero vector.")
    cosine_similarity = float(np.dot(vec1, vec2) / (norm1 * norm2))
    return float(1.0 - cosine_similarity)


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
    if not path.exists():
        raise FileNotFoundError(f"Embedding baseline file not found: {path}")

    with np.load(path, allow_pickle=True) as archive:
        if "centroid" not in archive.files:
            raise ValueError(f"Invalid baseline file: {path} (missing centroid key)")
        payload: Dict[str, Any] = {"centroid": archive["centroid"]}
        if "model" in archive.files:
            payload["model"] = str(archive["model"][0])
        if "source_count" in archive.files:
            payload["source_count"] = int(archive["source_count"][0])
        if "created_at" in archive.files:
            payload["created_at"] = str(archive["created_at"][0])
    return payload


def write_json_report(path: Path, report: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)


def embedding_drift_detection(
    extractions_path: Path = EXTRACTIONS_PATH,
    baseline_path: Path = EMBEDDING_BASELINE_PATH,
    report_path: Path = EMBEDDING_REPORT_PATH,
    threshold: float = DRIFT_THRESHOLD,
) -> Dict[str, Any]:
    client = create_gemini_client()

    records = read_jsonl(extractions_path)
    texts = collect_extracted_fact_texts(records)
    sampled_texts = sample_texts(texts, max_samples=MAX_TEXT_SAMPLES)
    if not sampled_texts:
        raise ValueError("No extracted_facts[*].text values found in input data.")

    embeddings, used_model = get_text_embeddings(client, sampled_texts, model=EMBEDDING_MODEL)
    current_centroid = compute_centroid(embeddings)

    if not baseline_path.exists():
        save_embedding_baseline(
            path=baseline_path,
            centroid=current_centroid,
            model=used_model,
            source_count=len(sampled_texts),
        )
        report = {
            "extension": "embedding_drift_detection",
            "status": "BASELINE_SET",
            "threshold": threshold,
            "drift_score": 0.0,
            "model": used_model,
            "sample_size": len(sampled_texts),
            "baseline_path": str(baseline_path),
            "report_generated_at": utc_now_iso(),
            "message": "No baseline found; baseline created successfully.",
        }
        write_json_report(report_path, report)
        return report

    baseline = load_embedding_baseline(baseline_path)
    baseline_centroid = baseline["centroid"]
    drift_score = cosine_distance(current_centroid, baseline_centroid)
    status = "FAIL" if drift_score > threshold else "PASS"

    report = {
        "extension": "embedding_drift_detection",
        "status": status,
        "threshold": threshold,
        "drift_score": float(drift_score),
        "model": used_model,
        "sample_size": len(sampled_texts),
        "baseline_path": str(baseline_path),
        "baseline_model": baseline.get("model"),
        "baseline_source_count": baseline.get("source_count"),
        "baseline_created_at": baseline.get("created_at"),
        "report_generated_at": utc_now_iso(),
        "message": (
            "Embedding drift exceeds threshold; investigate extraction/prompt changes."
            if status == "FAIL"
            else "Embedding drift is within acceptable threshold."
        ),
    }
    write_json_report(report_path, report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="TRP Week 7 Phase 4 AI Extensions")
    parser.add_argument(
        "--mode",
        choices=["embedding_drift"],
        required=True,
        help="AI extension mode to run.",
    )
    args = parser.parse_args()

    if args.mode == "embedding_drift":
        try:
            result = embedding_drift_detection()
            print(json.dumps(result, indent=2))
        except Exception as exc:
            error_payload = {
                "extension": "embedding_drift_detection",
                "status": "ERROR",
                "message": str(exc),
                "report_generated_at": utc_now_iso(),
            }
            print(json.dumps(error_payload, indent=2))


if __name__ == "__main__":
    main()

