DOMAIN_NOTES.md
2026-04-01

I am the developer of this platform and I wrote these notes during Phase 0 — Domain Reconnaissance for the Data Contract Enforcer. Below I answer the five required questions exactly, using concrete examples from my own Weeks 1–5 outputs (the canonical schemas in the challenge document). I reference Bitol Open Data Contract Standard, Confluent Schema Registry compatibility concepts, and dbt tests where relevant.

What is the difference between a backward-compatible and a breaking schema change? Give three examples of each, drawn from your own week 1–5 output schemas defined above.
Short definition (my working interpretation, aligned with Confluent): a backward-compatible change allows existing consumers to continue working without modification; a breaking change forces consumer updates or will cause incorrect behaviour. Confluent Schema Registry classifies changes as backward/forward/full compatible depending on whether new readers can read old writers and vice versa — I apply that taxonomy to each example.

Three backward-compatible changes (examples from my weeks):

Week 3 (extractions.jsonl) — Add nullable field to extracted_facts.items: add "confidence_notes" as nullable string. Rationale: downstream consumers that read extracted_facts[*].confidence continue to work; dbt test mapping: no new not_null test required. This is additive/nullable and therefore backward-compatible.
Week 5 (events.jsonl) — Add a new optional metadata field metadata.deploy_tag (nullable string). Event consumers that validate event_type/payload still succeed; Confluent: additive optional fields are typically backward-compatible.
Week 4 (lineage_snapshots.jsonl) — Add node.metadata.operational_tags: [] (nullable array). Cartographer consumers that require nodes[].node_id still work; dbt: accepted_values unaffected. This is additive metadata only.
Three breaking (incompatible) changes (examples from my weeks):

Week 3 — Change extracted_facts[*].confidence from float in 0.0–1.0 to integer 0–100 (narrowing / semantic change). This is a type/scale change and per Confluent it is a breaking (not backward-compatible) change because existing readers expect 0.0–1.0 floats and calculations (means, thresholds) will break.
Week 5 — Change event_record.sequence_number semantics to allow non-monotonic sequence_number or to change it from int to string. Consumers rely on monotonic int per aggregate_id; changing type or monotonicity breaks downstream idempotency and ordering guarantees (breaking).
Week 4 — Remove nodes[].node_id or rename it (node_id → id). The Cartographer and ViolationAttributor use node_id as stable identifier (file::...), so renaming/removing is structural and breaking. dbt mapping: removal of a foreign-key-like field would fail relationships tests.
Mapping to contract enforcement and dbt: for backward-compatible additions I map to dbt tests as "no new not_null/unique required", and for breaking changes I map to failing "not_null/accepted_values/relationships" or explicit SchemaEvolutionAnalyzer classification (rename, type narrowing) and produce migration impact reports.

The Week 3 Document Refinery's confidence field is float 0.0–1.0. An update changes it to integer 0–100. Trace the failure this causes in the Week 4 Cartographer. Write the data contract clause that would catch this change before it propagates, in Bitol YAML format. (Include a full valid Bitol YAML snippet for the confidence field.)
Failure trace (concrete, stepwise, using my data and tools):

Week 3 extractor (outputs/week3/extractions.jsonl) starts producing extracted_facts[*].confidence values like 43, 85, 0, 100 (integer percentages) instead of floats 0.43, 0.85, etc.
ContractGenerator had produced a Bitol contract for week3-document-refinery-extractions with a confidence clause (minimum 0.0, maximum 1.0). ValidationRunner reads outputs/week3/extractions.jsonl and runs the range check on extracted_facts[*].confidence. Statistical profiling shows max=100, mean≈43.2, which violates expected max<=1.0.
ValidationRunner emits a structured validation report entry:
check_id: week3.extracted_facts.confidence.range
column_name: extracted_facts[*].confidence
status: FAIL
actual_value: "max=100, mean=43.2"
severity: CRITICAL
ViolationAttributor loads latest outputs/week4/lineage_snapshots.jsonl (the Cartographer snapshot that lists the node representing doc_id and metadata fields created from extracted_facts). The lineage snapshot has an edge where week3 doc node -> file::src/week4/cartographer.py; blast radius includes file::src/week4/cartographer.py and downstream consumers.
The Cartographer (which consumes extracted_facts to produce node metadata and may aggregate confidences into edges confidence) now computes edge.confidence using the invalid 0–100 scale. Downstream services reading lineage_snapshot.edges[].confidence see values outside 0.0–1.0 and produce misleading confidence metrics; event_record metadata.source_service = "week3-document-refinery" (Week 5) may record events with incorrect source confidence context, causing analytics and decisions to be wrong.
ViolationAttributor uses BFS upstream traversal (see Q3) and git blame to find the commit in src/week3/extractor.py that introduced the change and writes a violation_log/ entry pointing at that commit with estimated blast_radius. The statistical drift rule (3 stddev fail) also triggers if type check passes but distribution shifts.
Full valid Bitol YAML snippet that would catch the change (this is the confidence clause in the Week 3 contract; it is syntactically consistent with the example in the challenge document and Bitol style):

kind: DataContract
apiVersion: v3.0.0
id: week3-document-refinery-extractions
info:
title: Week 3 Document Refinery — Extraction Records
version: 1.0.0
owner: week3-team
schema:
extracted_facts:
type: array
items:
confidence:
type: number
minimum: 0.0
maximum: 1.0
description: |
Confidence score returned by the extraction model. MUST be a float between 0.0 and 1.0.
required: true
quality:
type: SodaChecks
specification:
checks:
- check_id: week3.extracted_facts.confidence.range
column: extracted_facts[*].confidence
check: range
expected:
minimum: 0.0
maximum: 1.0
severity: CRITICAL

Practical note: the Bitol clause above maps neatly to a dbt test: not_null for extracted_facts[*].confidence, and a custom test asserting min>=0.0 and max<=1.0. Under Confluent compatibility taxonomy, changing float->int and changing value semantics is a breaking "type narrowing/semantic change" that must be blocked or require a migration.
The Cartographer (Week 4) produced a lineage graph. Explain, step by step, how the Data Contract Enforcer uses that graph to produce a blame chain when a contract violation is detected. Include the specific graph traversal logic (BFS, upstream traversal, etc.).
I use the Week 4 lineage snapshot and the following deterministic algorithm in the ViolationAttributor:

Input: a validation failure (example: check_id = week3.extracted_facts.confidence.range, failing element = extracted_facts[*].confidence).
Map failing schema element to lineage node(s): ContractGenerator placed doc_id → node (node_id "file::outputs/week3/extraction::doc_id" or similar). I locate all nodes whose metadata lists fields_consumed containing extracted_facts or doc_id.
Upstream traversal strategy: breadth-first search (BFS) on the directed lineage graph, traversing upstream edges (edges where edge.target == current_node; step to edge.source), because we want the minimal-hop producers that can be blamed. BFS ensures we find nearest upstream producers first (minimal lineage hops), which matches the Confluent-inspired "stop at first external boundary" rule.
Stopping conditions: stop BFS when (a) node.type == EXTERNAL or SERVICE boundary, (b) node.type == FILE and path is inside our repo root but we hit a file that has modifications in recent git history, or (c) reach a configured maximum hop depth (I use 5).
Candidate file extraction: for each upstream FILE node discovered by BFS, extract metadata.path (e.g., src/week3/extractor.py) and gather git history with git log --follow --since="14 days ago" to find candidate commits that touched code producing the failing field. Then run git blame -L line_start,line_end to get exact lines if we have line mapping (ContractGenerator stores code_refs where available).
Rank candidates: compute confidence_score = base (1.0 − days_since_commit*0.1) minus 0.2 per lineage hop. Rank by score and include up to 5 candidates.
Produce blame chain JSON: for top-ranked candidates include file_path, commit_hash, author, commit_timestamp, commit_message, and confidence_score. Include blast_radius by querying lineage graph downstream from the blamed node (BFS downstream) to list affected nodes (Cartographer file::src/week4/cartographer.py, downstream event consumers, etc.).
Write violation_log/violations.jsonl entry and return to ReportGenerator.
This process is deterministic, relies on BFS upstream traversal to prefer closest producers, and uses git metadata to attribute the likely commit. It is the approach required by the challenge (breadth-first upstream traversal + git blame integration).

Write a data contract for the LangSmith trace_record schema defined in the challenge. Include at least one structural clause, one statistical clause, and one AI-specific clause. Show it in full Bitol-compatible YAML format.
I create a full Bitol-compatible contract for trace_record (outputs/traces/runs.jsonl). The contract enforces structure, statistics (token consistency), and an AI-specific clause (total_cost sanity and run_type accepted values). It also maps to dbt tests (not_null, accepted_values, relationships) when generated.
kind: DataContract
apiVersion: v3.0.0
id: langsmith-trace-records
info:
title: LangSmith Trace Records
version: 1.0.0
owner: ai-team
description: >
LangSmith run traces exported to outputs/traces/runs.jsonl.
servers:
local:
type: local
path: outputs/traces/runs.jsonl
format: jsonl
schema:
id:
type: string
format: uuid
required: true
unique: true
run_type:
type: string
required: true
enum: [llm, chain, tool, retriever, embedding]
description: Run classification
start_time:
type: string
format: date-time
required: true
end_time:
type: string
format: date-time
required: true
prompt_tokens:
type: integer
minimum: 0
required: true
completion_tokens:
type: integer
minimum: 0
required: true
total_tokens:
type: integer
minimum: 0
required: true
total_cost:
type: number
minimum: 0.0
required: true
quality:
type: SodaChecks
specification:
checks:
- check_id: traces.time.order
column: end_time, start_time
check: time_order
expected: { end_after_start: true }
severity: CRITICAL
- check_id: traces.tokens.consistency
column: total_tokens
check: expression
expression: "total_tokens == prompt_tokens + completion_tokens"
severity: CRITICAL
- check_id: traces.run_type.enum
column: run_type
check: accepted_values
expected: [llm, chain, tool, retriever, embedding]
severity: HIGH
- check_id: traces.cost.sanity
column: total_cost
check: range
expected: { minimum: 0.0 }
severity: MEDIUM
ai_extensions:
embedding_related:
note: >
If run_type == "embedding", validation_runner should link to embedding baseline checks (embedding centroid drift).
lineage:
upstream: []
downstream: []
terms:
usage: AI trace validation for LLM/chain/tool diagnostics.

Structural clause example: run_type enum and id: uuid required (maps to dbt accepted_values and not_null/unique tests).
Statistical clause example: token consistency (total_tokens == prompt_tokens + completion_tokens) — mapped to an expression check and Soda/dbt custom test.
AI-specific clause example: if run_type == "embedding" the ai_extensions block instructs the ValidationRunner to run embedding-drift checks using stored centroids (this is the AI contract extension described in Phase 4).
What is the most common failure mode of contract enforcement systems in production? Why do contracts get stale? How does your architecture (ContractGenerator + snapshotting + lineage injection + ValidationRunner) prevent this?
Most common failure mode (experience): contracts become stale or unenforced because the contract owner is not in the loop for code changes, and enforcement is limited to structural checks only. Two concrete causes I have seen in my own platform:

Upstream code changes (src/week3/extractor.py) that alter units/scale (confidence float->percentage) without updating contract files and without CI gate checks — silent corruption occurs because consumer code still runs but uses wrong semantics.
Contracts expressed only structurally (field name/type) miss statistical/semantic drift (mean shift from 0.87 to 43.2). Those pass type-checking but produce wrong business results.
Why contracts get stale:

Developers change output serialization (types/units) but don't update contract YAML or schema registry; the change bypasses CI if contract checks are not integrated into pre-merge pipelines.
Contracts are not snapshot/versioned; without temporal snapshots, it's impossible to know when a change landed, so attribution and rollback are manual and slow.
How my architecture prevents this (concrete mechanisms using the components in the repo):

ContractGenerator: runs automatically on real outputs/ (Week 3 and Week 5 are required) and produces Bitol contracts and dbt schema.yml. Because it reads live JSONL and lineage, it generates contracts that reflect actual baseline structure and statistical profiles (min/max/mean/stddev).
Schema snapshotting: every run writes schema_snapshots/{contract_id}/{timestamp}.yaml and stores baselines.json (means/stddevs). This enforces temporal discipline so changes are diffable and attributable.
Lineage injection: ContractGenerator injects downstream_consumers[] from outputs/week4/lineage_snapshots.jsonl into the generated contract. When ValidationRunner fails a clause, ViolationAttributor can immediately compute blast radius and contact owners of affected nodes, reducing silent breakage.
ValidationRunner: executes structural and statistical checks (range, expression, token consistency, embedding drift hook). The Statistical Drift Rule (baseline mean/stddev) catches semantic shifts like 0.0–1.0 → 0–100 even if a naive type test passes.
CI / dbt integration: generated dbt schema.yml contains not_null/accepted_values/relationships tests that can be run in pre-merge pipelines. Combined with the ValidationRunner producing structured validation_reports/, failing CI prevents merges that would introduce breaking changes undetected.
In short: by (a) generating contracts from real outputs, (b) snapshotting schema and baselines, (c) injecting lineage context so we know affected consumers, and (d) running both structural and statistical checks automatically, the architecture reduces the window in which a contract can go stale and provides immediate attribution when it does.

Compounding Architecture Note

This week's violation_log/violations.jsonl is designed to be a first-class data source for Week 8. Each violation record contains metadata: violation_id, check_id, detected_at, blame_chain (ranked file/commit candidates), blast_radius (affected_nodes and estimated_records), and severity. The Week 8 Sentinel will ingest violation_log/ alongside LangSmith trace quality metrics to correlate data-quality incidents with LLM-run failures and trigger alerts and remediation workflows automatically.