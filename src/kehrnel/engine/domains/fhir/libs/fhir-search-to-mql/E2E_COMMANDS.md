# E2E commands — fhir-search-to-mql

End-to-end tests for **CLI_COMMANDS.md** `convert` / `search` / `denormalize` scenarios.  
Pipeline tests use the **same MongoDB database** as the matching fhir-gen scenario (`fhir_e2e_gen_<id>`).  
Combined runner: [E2E_COMBINED.md](E2E_COMBINED.md).

---

## Prerequisites

| Requirement | Default |
|-------------|---------|
| Python | 3.9+ (3.11+ recommended) |
| Virtual env | `.venv` in this repo root |
| MongoDB | `mongodb://localhost:27017/` (pipeline tests) |
| fhir-gen | `pip install -e "..\fhir-data-generation"` |

```powershell
cd D:\Users\desh.bandhu\Desktop\MongoDB_FHIR\code_repositories\fhir-search-to-mql
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
pip install -e "..\fhir-data-generation"
```

---

## Environment variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `E2E_MONGODB_URI` | MongoDB connection | `mongodb://localhost:27017/` |
| `E2E_DB_PREFIX` | Per-scenario DB prefix (shared with fhir-gen) | `fhir_e2e_gen_` |
| `E2E_FULL_COUNTS` | Gen volumes when loading data | off |

```powershell
$env:E2E_MONGODB_URI = "mongodb://localhost:27017/"
$env:E2E_DB_PREFIX = "fhir_e2e_gen_"
```

Pipeline tests target the same DB as generation — e.g. `fhir_e2e_gen_hc01` for healthcare §1 (not a separate pipeline database).

---

## Run tests (pytest)

### Convert only — no MongoDB

```powershell
.venv\Scripts\python.exe -m pytest tests/e2e/test_convert_e2e.py -m e2e --no-cov -q
```

### Single pipeline scenario

```powershell
.venv\Scripts\python.exe -m pytest tests/e2e/test_cli_commands_e2e.py::TestPipelineScenarios -m "e2e and mongodb" -k hc01 --no-cov -v
```

Each test: load gen data → `fhir_e2e_gen_hc01` → index / denormalize / search on **that** DB.

### All pipeline scenarios

```powershell
.venv\Scripts\python.exe -m pytest tests/e2e/ -m "e2e and mongodb" --no-cov -q
```

### FHIR search query testing (saved results)

Each scenario runs **convert** + **search** for every bundled resource, extra queries where defined, and **compartment** searches (Patient / Practitioner / Device / Encounter) when ids exist in the DB.

Results are saved for review:

```
tests/e2e/results/<scenario_id>/search_results.json
```

```powershell
# Plan-only tests (no MongoDB)
.venv\Scripts\python.exe -m pytest tests/e2e/test_search_plan.py -m e2e --no-cov -q

# After pipeline run
Get-Content tests\e2e\results\hc01\search_results.json -Head 60
```

Disable artifact writes: `$env:E2E_SAVE_RESULTS = "0"`

---

## Remove E2E databases

```powershell
# Drops MongoDB fhir_e2e_* AND clears tests/e2e/results/
cd ..\fhir-data-generation
.venv\Scripts\python.exe scripts\drop_e2e_databases.py
# Or from this repo:
.venv\Scripts\python.exe scripts\drop_e2e_databases.py

# One scenario (mongosh)
mongosh mongodb://localhost:27017/ --eval "db.getSiblingDB('fhir_e2e_gen_hc01').dropDatabase()"

# Python (this repo venv)
.venv\Scripts\python.exe -c "from pymongo import MongoClient; c=MongoClient('mongodb://localhost:27017/'); c.drop_database('fhir_e2e_gen_hc01'); print('dropped')"
```

Bulk remove (mongosh):

```powershell
mongosh mongodb://localhost:27017/ --eval "db.adminCommand('listDatabases').databases.filter(d => d.name.startsWith('fhir_e2e_')).forEach(d => { db.getSiblingDB(d.name).dropDatabase(); print('dropped ' + d.name); })"
```

---

## Manual CLI (one scenario)

```powershell
$URI = "mongodb://localhost:27017/"
$DB  = "fhir_e2e_gen_hc01"   # same DB for gen and mql

# 1) Load data (fhir-data-generation)
cd ..\fhir-data-generation
fhir-gen --seed 2001 --mongo-uri $URI --db $DB generate-many Patient Person RelatedPerson Organization --save

# 2) Index / denormalize / search (this repo)
cd ..\fhir-search-to-mql
fhir-mql indexes Patient Organization Person RelatedPerson --uri $URI --db $DB
fhir-mql denormalize Patient Organization Person RelatedPerson --uri $URI --db $DB
fhir-mql search Patient "active=true" --uri $URI --db $DB --limit 10
```

---

## Related

- [E2E_COMBINED.md](E2E_COMBINED.md) — `scripts/run_cli_e2e.py`
- [fhir-data-generation E2E_COMMANDS.md](../fhir-data-generation/E2E_COMMANDS.md)
- [CLI_COMMANDS.md](CLI_COMMANDS.md)
