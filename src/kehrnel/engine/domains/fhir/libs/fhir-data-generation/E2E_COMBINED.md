# E2E combined — `run_cli_e2e.py`

Runs **both** repositories end-to-end: **fhir-gen** loads MongoDB, then **fhir-mql** indexes, denormalizes, and searches on the **same database** per scenario.

Script location: **`fhir-data-generation/scripts/run_cli_e2e.py`**

Per-repo pytest: [E2E_COMMANDS.md](E2E_COMMANDS.md) and [fhir-search-to-mql E2E_COMMANDS.md](../fhir-search-to-mql/E2E_COMMANDS.md).

---

## Prerequisites

1. **MongoDB** on `localhost:27017`
2. **Sibling repos** under `code_repositories/` (`fhir-data-generation` + `fhir-search-to-mql`)
3. **Editable installs** in each `.venv` (see [E2E_COMMANDS.md](E2E_COMMANDS.md))

---

## Environment variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `E2E_MONGODB_URI` | MongoDB URI | `mongodb://localhost:27017/` |
| `E2E_DB_PREFIX` | Per-scenario DB prefix | `fhir_e2e_gen_` |

**One database per scenario** — e.g. healthcare §1 uses only `fhir_e2e_gen_hc01` for generation and for fhir-mql. There is no separate `fhir_e2e_pipeline_*` database.

---

## Run combined E2E

From **fhir-data-generation**:

```powershell
cd D:\Users\desh.bandhu\Desktop\MongoDB_FHIR\code_repositories\fhir-data-generation
.\.venv\Scripts\Activate.ps1

# Full run: gen → mql on same DB for each scenario
.venv\Scripts\python.exe scripts\run_cli_e2e.py

.venv\Scripts\python.exe scripts\run_cli_e2e.py --gen-only
.venv\Scripts\python.exe scripts\run_cli_e2e.py --pipeline-only
.venv\Scripts\python.exe scripts\run_cli_e2e.py --section healthcare
.venv\Scripts\python.exe scripts\run_cli_e2e.py --section industrial
.venv\Scripts\python.exe scripts\run_cli_e2e.py --full-counts
```

### CLI flags

| Flag | Effect |
|------|--------|
| *(default)* | For each scenario: **gen** into `fhir_e2e_gen_<id>`, then **mql** on that same DB (no reload) |
| `--gen-only` | Only `fhir-gen generate-many` |
| `--pipeline-only` | Drop DB, reload gen data, then fhir-mql (standalone pipeline test) |
| `--section healthcare` \| `industrial` | Subset of scenarios |
| `--full-counts` | CLI_COMMANDS document volumes (slow) |
| `--quiet` | Only `[GEN OK]` / `[PIPELINE OK]` lines (no status updates) |
| `--uri URL` | MongoDB connection |

By default each scenario prints **short phase status** (e.g. “Generating FHIR data…”, “Running search tests…”). Steps that run longer than ~30s also print a **still running** line with elapsed time (and search progress as `n/total`). Use `--quiet` to hide these.

---

## What each phase does

### Phase 1 — Generation

1. Drop `fhir_e2e_gen_<id>` (if present)
2. `fhir-gen generate-many … --save` into that database

### Phase 2 — Pipeline (same database)

On the **existing** `fhir_e2e_gen_<id>` from phase 1 (full run does **not** drop or reload):

1. `fhir-mql indexes` + `fhir-mql denormalize` (with dependency expansion)
2. `fhir-mql search` for each query in the scenario

`--pipeline-only` drops and reloads gen data first, then runs mql on the same DB name.

Special case: `hc20_compartment` pipeline uses `fhir_e2e_gen_hc20` (same as healthcare §20).

---

## Scenario databases

| Section | Example DB names |
|---------|------------------|
| Healthcare 1–21 | `fhir_e2e_gen_hc01` … `fhir_e2e_gen_hc21` |
| Industrial | `fhir_e2e_gen_ind_hospital` … `fhir_e2e_gen_ind_full84` |
| Go-live pytest | `fhir_e2e_gen_go_live` |

---

## Remove E2E databases

```powershell
# Drop all fhir_e2e_* databases
.venv\Scripts\python.exe scripts\drop_e2e_databases.py

# Preview
.venv\Scripts\python.exe scripts\drop_e2e_databases.py --dry-run

# One scenario
mongosh mongodb://localhost:27017/ --eval "db.getSiblingDB('fhir_e2e_gen_hc01').dropDatabase()"
```

See [E2E_COMMANDS.md](E2E_COMMANDS.md) for more cleanup options (Python one-liner, mongosh bulk).

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Pipeline sees empty collections | Run full `run_cli_e2e.py` (not `--pipeline-only` without prior gen), or use `--pipeline-only` to reload |
| Stale data | `scripts/drop_e2e_databases.py` then re-run |
| `MongoDB unavailable` | Start MongoDB on port 27017 |

---

## See also

- [E2E_COMMANDS.md](E2E_COMMANDS.md) — pytest (this repo)
- [fhir-search-to-mql E2E_COMBINED.md](../fhir-search-to-mql/E2E_COMBINED.md)
