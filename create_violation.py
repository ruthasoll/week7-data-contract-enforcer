import json
from pathlib import Path

def inject_confidence_violation():
    input_path = Path('outputs/week3/extractions.jsonl')
    output_path = Path('outputs/week3/extractions_violated.jsonl')
    
    if not input_path.exists():
        print("Error: Week 3 data not found")
        return
    
    records = []
    with open(input_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                record = json.loads(line)
                # Change confidence from 0.0-1.0 to 0-100 scale
                for fact in record.get('extracted_facts', []):
                    if 'confidence' in fact:
                        fact['confidence'] = round(fact['confidence'] * 100, 1)
                records.append(record)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        for record in records:
            f.write(json.dumps(record) + '\n')
    
    print(f"✅ Violation injected! Created {len(records)} violated records")
    print(f"Saved to: {output_path}")
    print("Now run the runner against this violated file to generate a FAIL report.")

if __name__ == "__main__":
    inject_confidence_violation()