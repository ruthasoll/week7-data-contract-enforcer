import json
import uuid
import subprocess
import re
from datetime import datetime, timezone
from pathlib import Path
import argparse
import yaml
import os

# Utilities

def run_git_command(cmd, cwd=None):
    """Run git command and return output safely (stdout) or None on failure."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd, timeout=15)
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            print(f"Git command failed: {' '.join(cmd)}")
            if result.stderr:
                print(f"Git stderr: {result.stderr.strip()}")
            return None
    except Exception as e:
        print(f"Error running git command: {e}")
        return None


def parse_git_log_lines(log_output):
    """Parse git log pretty format lines into structured commits."""
    commits = []
    lines = log_output.splitlines()
    for i, line in enumerate(lines):
        if '|' in line:
            parts = line.split('|', 4)
            if len(parts) >= 5:
                commit_hash, author, email, timestamp, message = parts
                commits.append({
                    'commit_hash': commit_hash,
                    'author': author,
                    'email': email,
                    'timestamp': timestamp,
                    'message': message.strip()
                })
    return commits


def get_commits_for_file(file_path, limit=5):
    """Get recent commits touching a file. Returns list of commits dicts or []"""
    cmd = ['git', 'log', '--follow', f'--pretty=format:%H|%an|%ae|%ai|%s', f'-n{limit}', '--', file_path]
    out = run_git_command(cmd)
    if not out:
        return []
    return parse_git_log_lines(out)


def get_first_blame_commit(file_path):
    """Attempt to get a most-recent commit from git blame for the first non-empty line.
    Returns commit_hash or None.
    """
    cmd = ['git', 'blame', '--line-porcelain', file_path]
    out = run_git_command(cmd)
    if not out:
        return None
    # blame output repeats commit headers; find first commit hash occurrence
    m = re.search(r'^([0-9a-f]{7,40})\b', out)
    if m:
        return m.group(1)
    return None


def days_since_iso(ts_iso):
    try:
        # parse common iso formats
        dt = datetime.fromisoformat(ts_iso.split('+')[0].replace('Z', '+00:00'))
        now = datetime.now(timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0, (now - dt).days)
    except Exception:
        return 999


def score_candidate(days_since, lineage_hops):
    score = 1.0 - (days_since * 0.1) - (lineage_hops * 0.2)
    return round(max(0.0, min(1.0, score)), 2)


def ensure_defaults_in_chain(chain, defaults):
    """Ensure that at least the important default files are present in the chain.
    Defaults is an iterable of file paths.
    """
    existing_paths = {entry.get('file_path') for entry in chain}
    rank = len(chain) + 1
    for d in defaults:
        if d not in existing_paths:
            chain.append({
                'rank': rank,
                'file_path': d,
                'commit_hash': 'unknown',
                'author': 'unknown',
                'commit_timestamp': datetime.now(timezone.utc).isoformat(),
                'commit_message': 'fallback - git unavailable or no history',
                'confidence_score': 0.5 if rank == 1 else 0.4
            })
            rank += 1
    return chain


# Main attribution logic

def attribute_failure(failed_check, lineage_snapshot, contract_yaml):
    """Given a failed check dict, lineage snapshot (dict) and contract YAML dict,
    produce a violation entry with blame_chain and blast_radius.

    Key improvements made:
    - Prioritize checks that mention 'confidence' in check_id or column_name.
    - Use lineage snapshot to find candidate producer files heuristically.
    - Robust git log + git blame fallbacks, always include Week3 extractor and Week4 cartographer
    - Use timezone-aware timestamps (datetime.now(timezone.utc))
    - Print debug lines for easier triage
    """
    check_id = failed_check.get('check_id')
    column_name = (failed_check.get('column_name') or '').lower()
    print(f"Detected failure: {check_id}")

    # Try to find candidate files from lineage snapshot first
    candidates = []  # tuples of (file_path, hops)
    if lineage_snapshot:
        nodes = {n.get('id'): n for n in lineage_snapshot.get('nodes', [])}
        edges = lineage_snapshot.get('edges', [])
        # Heuristic: if any node metadata.path or id mentions week3 or extractor/confidence, prefer it
        for nid, node in nodes.items():
            meta = node.get('metadata', {}) or {}
            path = meta.get('path') or meta.get('file') or nid
            lname = str(path).lower()
            if 'week3' in lname or 'extract' in lname or 'refinery' in lname or 'document' in lname:
                candidates.append((path, 0))
            if 'cartograph' in lname or 'week4' in lname:
                candidates.append((path, 1))
    # Also include contract lineage.downstream as possible affected nodes
    downstream_nodes = []
    try:
        downstream_nodes = contract_yaml.get('lineage', {}).get('downstream', []) if contract_yaml else []
    except Exception:
        downstream_nodes = []

    # If confidence appears in check id or column_name, prioritize files likely responsible
    priority_keywords = ['confidence']
    prioritized = []
    if any(k in (check_id or '').lower() for k in priority_keywords) or any(k in column_name for k in priority_keywords):
        print("Prioritizing confidence-related failure")
        # prefer candidates with 'week3' or 'extract' in path
        prioritized = [c for c in candidates if any(k in str(c[0]).lower() for k in ['week3', 'extract', 'refinery'])]

    # Build final candidate file list; include defaults if empty
    final_candidates = []
    if prioritized:
        final_candidates = prioritized
    elif candidates:
        final_candidates = candidates
    else:
        # fallback defaults (ensure these exist in repository layout)
        final_candidates = [
            ("src/week3/extractor.py", 0),
            ("src/week4/cartographer.py", 1)
        ]

    # De-duplicate while preserving order
    seen = set()
    uniq_final = []
    for fp, hops in final_candidates:
        p = str(fp)
        if p not in seen:
            seen.add(p)
            uniq_final.append((p, hops))

    # Collect blame entries
    blame_entries = []
    now_iso = datetime.now(timezone.utc).isoformat()
    for idx, (file_path, hops) in enumerate(uniq_final):
        print(f"Using file: {file_path}")
        commits = get_commits_for_file(file_path, limit=5)
        if commits:
            for i, c in enumerate(commits):
                days = days_since_iso(c.get('timestamp') or c.get('commit_timestamp') or now_iso)
                score = score_candidate(days, hops)
                blame_entries.append({
                    'rank': len(blame_entries) + 1,
                    'file_path': file_path,
                    'commit_hash': c.get('commit_hash'),
                    'author': c.get('author'),
                    'commit_timestamp': c.get('timestamp'),
                    'commit_message': c.get('message'),
                    'confidence_score': score
                })
        else:
            # try blame fallback to at least get a commit hash
            blame_hash = get_first_blame_commit(file_path)
            if blame_hash:
                # lookup that commit for metadata
                cmd = ['git', 'show', '--quiet', '--pretty=format:%H|%an|%ae|%ai|%s', blame_hash]
                out = run_git_command(cmd)
                if out and '|' in out:
                    parts = out.split('|', 4)
                    if len(parts) >= 5:
                        commit_hash, author, email, timestamp, message = parts
                        days = days_since_iso(timestamp)
                        score = score_candidate(days, hops)
                        blame_entries.append({
                            'rank': len(blame_entries) + 1,
                            'file_path': file_path,
                            'commit_hash': commit_hash,
                            'author': author,
                            'commit_timestamp': timestamp,
                            'commit_message': message,
                            'confidence_score': score
                        })
                        continue
            # Generic fallback entry
            blame_entries.append({
                'rank': len(blame_entries) + 1,
                'file_path': file_path,
                'commit_hash': 'unknown',
                'author': 'unknown',
                'commit_timestamp': now_iso,
                'commit_message': 'no git info available',
                'confidence_score': 0.5
            })

    # Ensure defaults are present if git failed to return useful info
    blame_entries = ensure_defaults_in_chain(blame_entries, ["src/week3/extractor.py", "src/week4/cartographer.py"])[:5]

    # Blast radius computation using contract lineage downstream if available
    affected_nodes = []
    affected_pipelines = []
    try:
        for d in downstream_nodes:
            nid = d.get('id') if isinstance(d, dict) else str(d)
            affected_nodes.append(nid)
            if 'cartograph' in str(nid).lower() or 'week5' in str(nid).lower() or 'event' in str(nid).lower():
                affected_pipelines.append(nid)
    except Exception:
        affected_nodes = ["file::src/week4/cartographer.py"]
        affected_pipelines = [n for n in affected_nodes if 'cartographer' in n.lower()]

    violation = {
        'violation_id': str(uuid.uuid4()),
        'check_id': failed_check.get('check_id'),
        'detected_at': datetime.now(timezone.utc).isoformat(),
        'blame_chain': blame_entries,
        'blast_radius': {
            'affected_nodes': affected_nodes,
            'affected_pipelines': affected_pipelines,
            'estimated_records': int(failed_check.get('records_failing', 0) or 0)
        }
    }
    return violation


def load_latest_lineage(lineage_path):
    """Load the last JSON object from a JSONL lineage snapshot file."""
    try:
        with open(lineage_path, 'r', encoding='utf-8') as f:
            lines = [l for l in f.read().splitlines() if l.strip()]
            if not lines:
                return None
            last = lines[-1]
            return json.loads(last)
    except Exception as e:
        print(f"Failed to load lineage snapshot: {e}")
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--violation', required=True)
    parser.add_argument('--lineage', required=True)
    parser.add_argument('--contract', required=True)
    parser.add_argument('--output', default='violation_log/violations.jsonl')
    args = parser.parse_args()

    Path('violation_log').mkdir(exist_ok=True)

    # Load validation report
    try:
        with open(args.violation, 'r', encoding='utf-8') as f:
            report = json.load(f)
    except Exception as e:
        print(f"Failed to load violation report: {e}")
        report = {'results': []}

    # Load contract YAML
    contract_yaml = None
    try:
        with open(args.contract, 'r', encoding='utf-8') as f:
            contract_yaml = yaml.safe_load(f)
    except Exception as e:
        print(f"Failed to load contract YAML: {e}")
        contract_yaml = None

    # Load lineage
    lineage_snapshot = load_latest_lineage(args.lineage)

    # Select the failed check to attribute
    results = report.get('results', [])
    failed_check = None

    # 1) Prefer a FAIL that mentions confidence
    for r in results:
        if r.get('status') == 'FAIL' and ('confidence' in (r.get('check_id') or '').lower() or 'confidence' in (r.get('column_name') or '').lower()):
            failed_check = r
            break

    # 2) If none, fall back to first CRITICAL FAIL
    if not failed_check:
        for r in results:
            if r.get('status') == 'FAIL' and str(r.get('severity', '')).upper() == 'CRITICAL':
                failed_check = r
                break

    # 3) Otherwise take first FAIL
    if not failed_check:
        for r in results:
            if r.get('status') == 'FAIL':
                failed_check = r
                break

    # 4) Otherwise first ERROR
    if not failed_check and results:
        failed_check = results[0]

    if not failed_check:
        print('No failing checks to attribute.')
        return

    violation_entry = attribute_failure(failed_check, lineage_snapshot, contract_yaml)

    # Append to output JSONL
    try:
        with open(args.output, 'a', encoding='utf-8') as out_f:
            out_f.write(json.dumps(violation_entry) + os.linesep)
        print(f"Wrote violation to {args.output}")
    except Exception as e:
        print(f"Failed to write violation entry: {e}")

    print(f"Violation ID: {violation_entry.get('violation_id')}")
    print(f"Blame chain length: {len(violation_entry.get('blame_chain', []))}")


if __name__ == '__main__':
    main()
