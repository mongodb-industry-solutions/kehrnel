# E2E commands — fhir-data-generation

End-to-end tests for **CLI_COMMANDS.md** `generate-many` scenarios (healthcare 1–21, industrial A–K).  
For the **combined** fhir-gen + fhir-mql runner, see [E2E_COMBINED.md](E2E_COMBINED.md).

---

## Prerequisites

| Requirement | Default |
|-------------|---------|
| Python | 3.11+ |
| Virtual env | `.venv` in this repo root |
| MongoDB | `mongodb://localhost:27017/` (only for `@mongodb` tests) |
| Install | `pip install -e ".[dev]"` from repo root |

Verify MongoDB:

```powershell
.venv\Scripts\python.exe -c "from pymongo import MongoClient; print(MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000).server_info()['version'])"
```

---

## Environment variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `E2E_MONGODB_URI` | MongoDB connection | `mongodb://localhost:27017/` |
| `E2E_DB_PREFIX` | Prefix for per-scenario DBs | `fhir_e2e_gen_` |
| `E2E_FULL_COUNTS` | Use documented volumes from CLI_COMMANDS (`1` / `true`) | off (minimal: 1 per type) |

Example:

```powershell
$env:E2E_MONGODB_URI = "mongodb://localhost:27017/"
$env:E2E_DB_PREFIX = "fhir_e2e_gen_"
```

Each scenario uses **one** database for both generation and fhir-mql pipeline, e.g. `fhir_e2e_gen_hc01`, `fhir_e2e_gen_ind_hospital`.

---

## Remove E2E databases

After tests or manual runs, drop scenario databases to reclaim space.

### Script (all `fhir_e2e_*` databases)

```powershell
cd fhir-data-generation
.venv\Scripts\python.exe scripts\drop_e2e_databases.py
.venv\Scripts\python.exe scripts\drop_e2e_databases.py --dry-run   # list only
```

### One scenario

```powershell
$URI = "mongodb://localhost:27017/"

# mongosh
mongosh $URI --eval "db.getSiblingDB('fhir_e2e_gen_hc01').dropDatabase()"

# Python
.venv\Scripts\python.exe -c "from pymongo import MongoClient; MongoClient('$URI').drop_database('fhir_e2e_gen_hc01')"
```

### All E2E databases (mongosh)

```powershell
mongosh mongodb://localhost:27017/ --eval "db.adminCommand('listDatabases').databases.filter(d => d.name.startsWith('fhir_e2e_')).forEach(d => { db.getSiblingDB(d.name).dropDatabase(); print('dropped ' + d.name); })"
```

Legacy `fhir_e2e_pipeline_*` databases from older runs are removed by the same commands (prefix `fhir_e2e_`).

Also clears **fhir-mql** search artifacts:

```powershell
.venv\Scripts\python.exe scripts\drop_e2e_databases.py
# Databases only:
.venv\Scripts\python.exe scripts\drop_e2e_databases.py --no-clear-results
```

Removes `fhir-search-to-mql/tests/e2e/results/<scenario_id>/search_results.json`.

---

## Activate venv

```powershell
cd D:\Users\desh.bandhu\Desktop\MongoDB_FHIR\code_repositories\fhir-data-generation
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

---

## Run tests (pytest)

### Fast — in-memory (no MongoDB)

All 32 `generate-many` scenarios via the Python API:

```powershell
.venv\Scripts\python.exe -m pytest tests/e2e/test_cli_commands_e2e.py::TestGenerateManyScenariosApi -m e2e --no-cov -q
```

### Single scenario — MongoDB + CLI

```powershell
.venv\Scripts\python.exe -m pytest tests/e2e/test_cli_commands_e2e.py::TestGenerateManyScenariosCli -m "e2e and mongodb" -k hc01 --no-cov -v
```

### All generation scenarios — MongoDB

```powershell
.venv\Scripts\python.exe -m pytest tests/e2e/ -m "e2e and mongodb" --no-cov -q
```

Heavy scenarios: `hc20`, `ind_full84` (all 84 resource types). Expect long runtimes.

### Documented volumes (slow)

```powershell
$env:E2E_FULL_COUNTS = "1"
.venv\Scripts\python.exe -m pytest tests/e2e/ -m "e2e and mongodb" --no-cov -q
```

---

## Scenario inventory

Defined in `tests/e2e/cli_scenarios_gen.py`:

| Section | IDs | Count |
|---------|-----|-------|
| Healthcare | `hc01` … `hc21` | 21 |
| Industrial | `ind_hospital`, `ind_revcycle`, … `ind_full84` | 11 |

Aligned with [CLI_COMMANDS.md](CLI_COMMANDS.md) § Healthcare workflow scenarios and § Industrial & enterprise scenarios.

**fhir-search-to-mql** imports this file as the source of truth for scenario **id**,
**title**, and **resources** (`tests/e2e/cli_scenarios_mql.py` in that repo).

---

## Manual CLI (one scenario)

Example — healthcare §1:

```powershell
$URI = "mongodb://localhost:27017/"
$DB  = "fhir_e2e_gen_hc01_manual"

fhir-gen --seed 2001 --mongo-uri $URI --db $DB generate-many Patient Person RelatedPerson Organization `
  --count Patient=5 --count Organization=2 --save

fhir-gen --seed 2001 --mongo-uri $URI --db $DB db-stats
```

---

## Related

- [E2E_COMBINED.md](E2E_COMBINED.md) — `scripts/run_cli_e2e.py` (gen + mql on the **same** DB per scenario)
- `scripts/drop_e2e_databases.py` — remove all `fhir_e2e_*` databases
- [fhir-search-to-mql E2E_COMMANDS.md](../fhir-search-to-mql/E2E_COMMANDS.md) — search / denormalize E2E
- [CLI_COMMANDS.md](CLI_COMMANDS.md) — full command reference
