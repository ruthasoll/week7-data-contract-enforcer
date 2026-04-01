# week7-data-contract-enforcer

# TRP Week 7: Data Contract Enforcer

This repository implements the **Data Contract Enforcer** — a system that generates, validates, and attributes violations of data contracts across the five previous TRP systems.

## Project Overview

The Data Contract Enforcer turns implicit promises between systems into explicit, machine-checkable contracts. It detects breaking changes (e.g., confidence scale from 0.0–1.0 to 0–100), traces them to the responsible code commit, and calculates the blast radius using the Week 4 lineage graph.

## Repository Structure

```bash
.
├── contracts/
│   ├── generator.py           # Generates Bitol + dbt contracts
│   ├── runner.py              # Validates data against contracts
│   ├── attributor.py          # Traces violations to git commits + blast radius
│   └── ...                    # (schema_analyzer.py and ai_extensions.py coming in Phase 3/4)
├── generated_contracts/       # Auto-generated contracts
│   ├── week3_extractions.yaml
│   └── week3_extractions_dbt.yml
├── validation_reports/        # Validation results (JSON)
├── violation_log/             # Attributed violations (JSONL)
├── schema_snapshots/          # Historical contract snapshots
├── outputs/                   # Input data from Weeks 1–5
│   ├── week1/
│   ├── week2/
│   ├── week3/extractions.jsonl
│   ├── week4/lineage_snapshots.jsonl
│   └── week5/events.jsonl
├── DOMAIN_NOTES.md
├── create_violation.py        # Utility to inject confidence scale violation
└── README.md


## How to Run (Step-by-Step)


pip install pandas ydata-profiling pyyaml jsonschema gitpython

1. Generate Contracts

python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/

2. Run Validation

# Clean data
python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/clean_run.json

# Violated data (to test failure detection)
python create_violation.py
python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions_violated.jsonl \
  --output validation_reports/violated_run.json

3. Run ViolationAttributor

python contracts/attributor.py \
  --violation validation_reports/violated_run.json \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --contract generated_contracts/week3_extractions.yaml \
  --output violation_log/violations.jsonl
