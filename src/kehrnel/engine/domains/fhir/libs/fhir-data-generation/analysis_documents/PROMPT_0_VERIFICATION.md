# Prompt 0 — Prerequisites verification

Completed: 2026-05-27  
Status: **PASS** — ready for Prompt 1

## Required input files

| File | Status | Notes |
|------|--------|-------|
| `INSTRUCTIONS.txt` | OK | 14 product requirements |
| `fhir.schema.v5.json` | OK | Created from `fhir.schema.v5.json` (FHIR R5, 158 resources) |
| `FHIR_DATATYPES.txt` | OK | Primitives, complex, metadata, special types |
| `FHIR_RESOURCE_URLS.txt` | OK | 157 HL7 resource spec URLs |
| `hl7_codes/healthcare_codes.yaml` | OK | 95 terminology sections |
| `PROMPTS_FHIR_DATA_GENERATION.md` | OK | Implementation prompts 0–18 |
| `requirements.txt` | OK | Runtime + dev dependencies |

### Schema variants in repo

| File | Definitions | Resources | Use |
|------|-------------|-----------|-----|
| `fhir.schema.v5.json` | 857 | **158** | **Default** for `fhir-gen` (R5) |
| `fhir.schema.v5.json` | 857 | 158 | Source copy for R5 |
| `fhir.schema.v6.json` | 704 | 127 | R6 preview only; not the default |

## Requirements summary (`INSTRUCTIONS.txt`)

1. Generic generator for all FHIR resources → Prompts 2, 8, 15  
2. All datatypes in `FHIR_DATATYPES.txt` → Prompts 4, 5, 6  
3. Correct field datatypes → Schema parser + generators  
4–5. Reference linking; dependencies first → Prompts 7, 8  
6. Optional custom schema path → Settings + CLI `--schema-path`  
7. Polymorphic variant documents → `generate_variants()` + `--variants`  
8. Healthcare-standard data → `healthcare_codes.yaml` + enrichers 9–13  
9–10. Static HL7 codes → `hl7_codes/healthcare_codes.yaml`  
11. MongoDB persistence → Prompt 14  
12. Installable library + CLI → Prompts 1, 15, 16  
13. Searchable cross-resource data → Mongo indexes + ReferenceStore  
14. Full test coverage → Prompts 17, 18  

## Datatype coverage (`FHIR_DATATYPES.txt`)

| Section | Count | Generator prompt |
|---------|-------|------------------|
| Primitives | 20 types | 4 |
| General-purpose complex | 27 types | 5 |
| Metadata | 10 types | 6 |
| Special | 6 types | 6 |
| Backbone/nested | Via schema engine | 8 |

## Resource scope

- **158** resources must be generatable with schema-only engine (Prompt 8).  
- **~55** high-value types get optional enrichers (Prompts 9–13).  
- **157** URLs in `FHIR_RESOURCE_URLS.txt` for binding research.

## Environment (recommended before Prompt 1)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Re-run checks

```powershell
.\.venv\Scripts\python.exe -c "
import json, yaml
from pathlib import Path
root = Path('.')
assert (root/'fhir.schema.v5.json').exists()
d = json.load(open(root/'fhir.schema.v5.json', encoding='utf-8'))
res = sum(1 for v in d['definitions'].values() if 'const' in v.get('properties',{}).get('resourceType',{}))
assert res == 158, res
yaml.safe_load(open(root/'hl7_codes/healthcare_codes.yaml', encoding='utf-8'))
print('Prompt 0 checks OK:', res, 'resources')
"
```

## Next step

**Prompt 1** — Create `fhir_gen/` scaffold, `pyproject.toml`, `config.py`, copy `fhir.schema.v5.json` → `fhir_gen/schema/fhir.schema.v5.json`.
