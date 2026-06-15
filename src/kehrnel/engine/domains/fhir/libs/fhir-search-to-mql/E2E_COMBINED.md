# E2E combined — `run_cli_e2e.py`

Cross-repo runner: **fhir-gen** → MongoDB → **fhir-mql** on the **same database** per scenario.

Script: **`fhir-data-generation/scripts/run_cli_e2e.py`** (run from that repo).

Canonical doc: [fhir-data-generation/E2E_COMBINED.md](../fhir-data-generation/E2E_COMBINED.md)

---

## Database model

| Step | Database |
|------|----------|
| `fhir-gen generate-many` | `fhir_e2e_gen_<scenario_id>` |
| `fhir-mql` index / denormalize / search | **Same** `fhir_e2e_gen_<scenario_id>` |

Example: healthcare §13 → only `fhir_e2e_gen_hc13` (no `fhir_e2e_pipeline_*`).

Full combined run: phase 2 does **not** drop or reload the database created in phase 1.

---

## Quick start

```powershell
cd D:\Users\desh.bandhu\Desktop\MongoDB_FHIR\code_repositories\fhir-data-generation
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
cd ..\fhir-search-to-mql
pip install -e ".[dev]"
pip install -e "..\fhir-data-generation"

cd ..\fhir-data-generation
.venv\Scripts\python.exe scripts\run_cli_e2e.py
```

| Flag | Effect |
|------|--------|
| `--gen-only` | Generation only |
| `--pipeline-only` | Drop/reload gen data, then fhir-mql |
| `--section healthcare` \| `industrial` | Subset |
| `--full-counts` | CLI_COMMANDS volumes (slow) |
| `--quiet` | Pass/fail only (no phase status) |
| `--uri` | MongoDB URI (`E2E_MONGODB_URI`) |

Default run prints **phase status** and **still running** heartbeats (~30s) for long steps. See [fhir-data-generation/E2E_COMBINED.md](../fhir-data-generation/E2E_COMBINED.md).

---

## Remove E2E databases

```powershell
cd ..\fhir-data-generation
.venv\Scripts\python.exe scripts\drop_e2e_databases.py
```

Drops every database whose name starts with `fhir_e2e_` (includes legacy `fhir_e2e_pipeline_*` from older runs).

Single DB:

```powershell
mongosh mongodb://localhost:27017/ --eval "db.getSiblingDB('fhir_e2e_gen_hc01').dropDatabase()"
```

---

## Pytest (this repo)

```powershell
cd fhir-search-to-mql
.venv\Scripts\python.exe -m pytest tests/e2e/ -m "e2e and mongodb" --no-cov -q
```

Uses the same `fhir_e2e_gen_*` naming as fhir-gen.

---

## See also

- [E2E_COMMANDS.md](E2E_COMMANDS.md) — pytest for fhir-mql only
- [fhir-data-generation E2E_COMBINED.md](../fhir-data-generation/E2E_COMBINED.md) — full combined reference
