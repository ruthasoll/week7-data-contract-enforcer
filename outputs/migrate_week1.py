import json
import uuid
from datetime import datetime
from pathlib import Path

def migrate_week1():
    # UPDATE THIS PATH to your actual Week 1 file
    input_path = Path(r'C:\Users\ruths\Desktop\TRP1\Roo-Code-week1\.orchestration\agent_trace.jsonl')
    
    output_path = Path('outputs/week1/intent_records.jsonl')
    output_path.parent.mkdir(parents=True, exist_ok=True)

    migrated = []

    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        print(f"Read {len(lines)} lines from Week 1 trace file.")

        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            try:
                # Try to fix common issues (remove comments, fix single quotes, etc.)
                if '//' in line:
                    line = line.split('//')[0].strip() + '}'
                if line.endswith(','):
                    line = line[:-1]
                if line.startswith('{') and not line.endswith('}'):
                    line += '}'

                trace = json.loads(line)

                # Extract intent records
                for file_entry in trace.get("files", []):
                    for conv in file_entry.get("conversations", []):
                        for ref in conv.get("ranges", []):
                            intent_record = {
                                "intent_id": str(uuid.uuid4()),
                                "description": f"AI-assisted implementation of {file_entry.get('relative_path', 'unknown file')}",
                                "code_refs": [{
                                    "file": file_entry.get("relative_path", ""),
                                    "line_start": ref.get("start_line", 1),
                                    "line_end": ref.get("end_line", ref.get("start_line", 1)),
                                    "symbol": "ai_generated_code",
                                    "confidence": 0.85
                                }],
                                "governance_tags": ["ai-assisted", "code-generation"],
                                "created_at": trace.get("timestamp", datetime.utcnow().isoformat() + "Z")
                            }
                            migrated.append(intent_record)

            except json.JSONDecodeError as e:
                print(f"Warning: Skipped line {i+1} due to JSON error: {e}")
                continue

    except FileNotFoundError:
        print(f"ERROR: Could not find input file at {input_path}")
        print("Please update the input_path in the script with the correct full path.")
        return

    # Write output
    with open(output_path, 'w', encoding='utf-8') as f:
        for record in migrated:
            f.write(json.dumps(record) + '\n')

    print(f"✅ Week 1 migration complete!")
    print(f"Created {len(migrated)} intent_records in outputs/week1/intent_records.jsonl")

if __name__ == "__main__":
    migrate_week1()