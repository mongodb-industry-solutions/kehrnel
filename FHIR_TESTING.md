# FHIR testing guide (Kehrnel + vendored libraries)

End-to-end commands to install, test, and run **fhir.rps_canonical** with **fhir-gen** and **fhir-mql**.

**Vendored library root** (all paths below are relative to the kehrnel repo root):

| Path | Package |
|------|---------|
| `src/kehrnel/engine/domains/fhir/libs/fhir-data-generation/` | **fhir-gen** (`fhir_gen`, CLI `fhir-gen`) |
| `src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql/` | **fhir-mql** (`fhir_search_to_mql`, CLI `fhir-mql`) |

The former top-level `libs/` folder was removed; FHIR libraries live under the **FHIR domain** package.

**Related:** [src/kehrnel/engine/domains/fhir/README.md](src/kehrnel/engine/domains/fhir/README.md) · [src/kehrnel/engine/domains/fhir/libs/README.md](src/kehrnel/engine/domains/fhir/libs/README.md) · [rps_canonical specification](src/kehrnel/engine/strategies/fhir/rps_canonical/specification/README.md) · [docs/strategies/fhir/rps-canonical/](docs/website/docs/strategies/fhir/rps-canonical/index.md)

---

## Prerequisites

### MongoDB

```powershell
# Quick check (kehrnel root, venv active)
python -c "from pymongo import MongoClient; MongoClient('mongodb://localhost:27017', serverSelectionTimeoutMS=3000).admin.command('ping'); print('Mongo OK')"
```

If MongoDB is not running:

```powershell
docker run -d --name mongo -p 27017:27017 mongo:7
```

**Git Bash:**

```bash
python -c "from pymongo import MongoClient; MongoClient('mongodb://localhost:27017', serverSelectionTimeoutMS=3000).admin.command('ping'); print('Mongo OK')"
```

---

## 1. One-time setup

From the **kehrnel** repository root:

### PowerShell

```powershell
cd D:\Users\desh.bandhu\Desktop\MongoDB_FHIR\code_repositories\datalab\kehrnel

python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Vendored FHIR libraries first (required for pip; uv uses [tool.uv.sources])
python -m pip install -e src/kehrnel/engine/domains/fhir/libs/fhir-data-generation -e src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql
pip install -e ".[api,mongo,fhir,test]"

fhir-gen version
fhir-mql --help
```

### Git Bash

```bash
cd kehrnel
python -m venv .venv
source .venv/Scripts/activate   # or: source .venv/bin/activate on Linux/macOS

pip install -e src/kehrnel/engine/domains/fhir/libs/fhir-data-generation -e src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql
pip install -e ".[api,mongo,fhir,test]"

fhir-gen version
fhir-mql --help
```

**Notes:**

- Core Kehrnel (`pip install -e .`) does **not** install FHIR; use the `[fhir]` extra.
- `./startKehrnel` uses **uv** and installs `.[all]`; path deps are wired via `[tool.uv.sources]` in `pyproject.toml`.
- After changing `src/kehrnel/engine/domains/fhir/libs/fhir-data-generation`, reinstall: `pip install -e src/kehrnel/engine/domains/fhir/libs/fhir-data-generation`
- `fhir-gen` loads `.env` only from `src/kehrnel/engine/domains/fhir/libs/fhir-data-generation/.env`, not kehrnel’s root `.env`.

---

## 2. Automated tests (no API server)

### PowerShell

```powershell
cd kehrnel
$env:KEHRNEL_AUTH_ENABLED = "false"
$env:FHIR_CONTRACT_MONGO = "1"   # optional: force Mongo-backed execute tests

python -m pytest (Get-ChildItem tests\contract\test_fhir_rps*.py).FullName -v
```

### Git Bash

```bash
cd kehrnel
export KEHRNEL_AUTH_ENABLED=false
export FHIR_CONTRACT_MONGO=1

pytest tests/contract/test_fhir_rps*.py -v
```

### FHIR library spike (no Kehrnel API)

```powershell
python src\kehrnel\engine\strategies\fhir\rps_canonical\scripts\spike_generate_and_search.py --db fhir_kehrnel_spike
```

Options: `--uri`, `--db`, `--seed` (default `1`).

Expected: ends with `[spike] OK` and exit code `0`.

---

## 3. Start Kehrnel API

Install `[fhir]` **before** starting the server. If the API was started earlier without FHIR, restart it after `pip install -e ".[fhir]"`.

### Option A — `./startKehrnel` (port **8080**)

```powershell
cd kehrnel
.\startKehrnel
# .\startKehrnel --no-reload --port 8080
# .\startKehrnel --build-docs
```

### Option B — manual

```powershell
$env:KEHRNEL_AUTH_ENABLED = "false"
$env:KEHRNEL_API_PORT = "8080"
$env:KEHRNEL_API_RELOAD = "false"
$env:KEHRNEL_INIT_INGESTION_RUNTIME = "false"
python -m kehrnel.api.app
```

**Git Bash:**

```bash
export KEHRNEL_AUTH_ENABLED=false
export KEHRNEL_API_PORT=8080
export KEHRNEL_API_RELOAD=false
python -m kehrnel.api.app
```

### URLs

| URL | Purpose |
|-----|---------|
| http://localhost:8080/health | Liveness |
| http://localhost:8080/docs | Swagger UI |
| http://localhost:8080/redoc | ReDoc |
| http://localhost:8080/guide | Docusaurus (if built) |
| http://localhost:8080/strategies | Strategy catalog |

### Smoke check

```powershell
Invoke-RestMethod http://localhost:8080/health
Invoke-RestMethod http://localhost:8080/strategies
```

```bash
curl -sS http://localhost:8080/health
curl -sS http://localhost:8080/strategies
```

Set base URL for later steps:

```powershell
$base = "http://localhost:8080"
```

```bash
export RUNTIME_URL=http://localhost:8080
```

---

## 4. FHIR end-to-end (HTTP API)

### 4.1 Activate `fhir.rps_canonical`

**PowerShell:**

```powershell
Invoke-RestMethod -Method POST -Uri "$base/environments/dev/activate" `
  -ContentType "application/json" `
  -Body (Get-Content src\kehrnel\engine\strategies\fhir\rps_canonical\specification\activate_dev.json -Raw)
```

**curl:**

```bash
curl -sS -X POST "$RUNTIME_URL/environments/dev/activate" \
  -H "Content-Type: application/json" \
  -d @src/kehrnel/engine/strategies/fhir/rps_canonical/specification/activate_dev.json
```

### 4.2 Synthetic generation job

The API returns the job under a **`job`** object. Use `$job.job.job_id`, not `$job.job_id`.

**Recommended payload** (includes denormalize for search):

```powershell
$jobJson = @'
{
  "domain": "fhir",
  "op": "synthetic_generate_batch",
  "payload": {
    "seed": 42,
    "resources": { "Patient": 5, "Observation": 10 },
    "store_canonical": true,
    "denormalize_after": true,
    "denormalize_resource_types": ["Patient", "Observation"]
  }
}
'@

$job = Invoke-RestMethod -Method POST -Uri "$base/environments/dev/synthetic/jobs" `
  -ContentType "application/json" -Body $jobJson

$jid = $job.job.job_id
Write-Host "Job id:" $jid "initial status:" $job.job.status
```

**Poll until complete:**

```powershell
do {
  Start-Sleep -Seconds 2
  $st = Invoke-RestMethod -Uri "$base/environments/dev/synthetic/jobs/$jid"
  $status = $st.job.status
  Write-Host "status:" $status
} until ($status -in @("completed", "failed", "cancelled"))

$st.job | ConvertTo-Json -Depth 6
```

**Small packaged job** (`src/kehrnel/engine/strategies/fhir/rps_canonical/specification/job_generate_small.json`) — no `denormalize_after`; run denormalize separately (§4.3) before search.

```powershell
$job = Invoke-RestMethod -Method POST -Uri "$base/environments/dev/synthetic/jobs" `
  -ContentType "application/json" `
  -Body (Get-Content src\kehrnel\engine\strategies\fhir\rps_canonical\specification\job_generate_small.json -Raw)
$jid = $job.job.job_id
```

**curl (with denormalize):**

```bash
curl -sS -X POST "$RUNTIME_URL/environments/dev/synthetic/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "fhir",
    "op": "synthetic_generate_batch",
    "payload": {
      "seed": 42,
      "resources": {"Patient": 5, "Observation": 10},
      "denormalize_after": true
    }
  }'

# Poll (replace JOB_ID)
curl -sS "$RUNTIME_URL/environments/dev/synthetic/jobs/JOB_ID"
```

### 4.3 Denormalize (if job did not use `denormalize_after`)

```powershell
Invoke-RestMethod -Method POST -Uri "$base/environments/dev/run" `
  -ContentType "application/json" `
  -Body '{"domain":"fhir","operation":"fhir_denormalize","payload":{"resource_types":["Patient","Observation"]}}'
```

```bash
curl -sS -X POST "$RUNTIME_URL/environments/dev/run" \
  -H "Content-Type: application/json" \
  -d '{"domain":"fhir","operation":"fhir_denormalize","payload":{"resource_types":["Patient"]}}'
```

### 4.4 Ensure indexes

```powershell
Invoke-RestMethod -Method POST -Uri "$base/environments/dev/run" `
  -ContentType "application/json" `
  -Body '{"domain":"fhir","operation":"fhir_ensure_indexes","payload":{"resource_types":["Patient"]}}'
```

### 4.5 FHIR domain search (Bundle)

Requires active `fhir.rps_canonical` on `dev` and denormalized data.

**Recommended — structured `criteria`** (use `resource_type` + param map):

```powershell
Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" `
  -Headers @{ "x-active-env" = "dev" } `
  -Body '{"resource_type":"Patient","criteria":{"gender":"male"},"limit":10}'
```

```bash
curl -sS -X POST "$RUNTIME_URL/api/domains/fhir/search" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{"resource_type":"Patient","criteria":{"gender":"male"},"limit":10}'
```

**Alternative — `fhir_search` string** must include the resource type and `?` before parameters (FHIR REST style).  
Do **not** pass only `gender=male` in `fhir_search` when `resource_type` is separate — that returns `INVALID_INPUT`.

```powershell
# Correct: ResourceType?param=value
Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" `
  -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"Patient?gender=male","limit":10}'

# Wrong (fails validation):
# '{"resource_type":"Patient","fhir_search":"gender=male","limit":10}'
```

```bash
curl -sS -X POST "$RUNTIME_URL/api/domains/fhir/search" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{"fhir_search":"Patient?gender=male","limit":10}'
```

Full URL form also works: `"fhir_search":"http://localhost/fhir/Patient?gender=male"`.

**View the full JSON in PowerShell** — `Invoke-RestMethod` parses JSON into nested `PSCustomObject` values. The default table view often shows `entry : @{resource=; search=}` even when `total` is correct. Assign the result and print JSON:

```powershell
$bundle = Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" `
  -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"Patient?gender=male","limit":10}'

# Full Bundle as JSON (increase -Depth if nested arrays are truncated)
$bundle | ConvertTo-Json -Depth 30

# First Patient resource only
$bundle.entry[0].resource | ConvertTo-Json -Depth 30

# Raw response string (no PSCustomObject conversion)
(Invoke-WebRequest -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" `
  -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"Patient?gender=male","limit":10}').Content
```

### 4.6 Strategy ops via `/run`

**Stats:**

```powershell
Invoke-RestMethod -Method POST -Uri "$base/environments/dev/run" `
  -ContentType "application/json" `
  -Body '{"domain":"fhir","operation":"fhir_stats","payload":{}}'
```

**Compile + execute search (`fhir_search` op):**

```powershell
Invoke-RestMethod -Method POST -Uri "$base/environments/dev/run" `
  -ContentType "application/json" `
  -Body '{"domain":"fhir","operation":"fhir_search","payload":{"resource_type":"Patient","criteria":{"gender":"male"},"_count":5}}'
```

**List search parameters:**

```powershell
Invoke-RestMethod -Method POST -Uri "$base/environments/dev/run" `
  -ContentType "application/json" `
  -Body '{"domain":"fhir","operation":"fhir_list_search_params","payload":{"resource_type":"Patient"}}'
```

**Universal query** (nest input under `query`):

```powershell
Invoke-RestMethod -Method POST -Uri "$base/environments/dev/run" `
  -ContentType "application/json" `
  -Body '{"domain":"fhir","operation":"query","payload":{"query":{"resource_type":"Patient","criteria":{"gender":"male"},"_count":10}}}'
```

---

## 5. Standalone vendored libraries

Run **fhir-gen** / **fhir-mql** without the Kehrnel API:

```powershell
pip install -e src\kehrnel\engine\domains\fhir\libs\fhir-data-generation
pip install -e src\kehrnel\engine\domains\fhir\libs\fhir-search-to-mql

fhir-gen version
fhir-gen generate --help
fhir-mql --help
```

Run each library’s tests:

```powershell
cd src\kehrnel\engine\domains\fhir\libs\fhir-data-generation
python -m pytest -q

cd ..\fhir-search-to-mql
python -m pytest -q
```

---

## 6. Sync vendored libraries from standalone repos

Scripts copy upstream repos into `src/kehrnel/engine/domains/fhir/libs/` (not the old root `libs/` folder).

If you still edit `code_repositories/fhir-data-generation` or `fhir-search-to-mql` outside kehrnel:

```powershell
.\src\kehrnel\engine\domains\fhir\scripts\sync-fhir-libs.ps1
pip install -e src/kehrnel/engine/domains/fhir/libs/fhir-data-generation -e src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql
pip install -e ".[fhir]"
```

```bash
./src/kehrnel/engine/domains/fhir/scripts/sync-fhir-libs.sh
pip install -e src/kehrnel/engine/domains/fhir/libs/fhir-data-generation -e src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql
pip install -e ".[fhir]"
```

---

## 7. Docker

From kehrnel root:

```powershell
# API without FHIR (openEHR / core only)
docker compose up kehrnel-api

# API with FHIR (includes engine/domains/fhir/libs/)
docker compose --profile fhir up kehrnel-fhir-api

# All-in-one (legacy Dockerfile.backend, .[all])
docker compose --profile all up kehrnel-all

# Standalone library images
docker compose --profile fhir build fhir-gen fhir-mql
```

| Image | Dockerfile |
|-------|------------|
| API (no FHIR) | `docker/Dockerfile.kehrnel-api` |
| API + FHIR | `docker/Dockerfile.kehrnel-fhir` |
| All-in-one | `Dockerfile.backend` |
| fhir-gen only | `docker/Dockerfile.fhir-gen` |
| fhir-mql only | `docker/Dockerfile.fhir-mql` |

---

## 8. Kehrnel CLI (optional)

```powershell
$env:RUNTIME_URL = "http://localhost:8080"

kehrnel setup --runtime-url $env:RUNTIME_URL --env dev --domain fhir --strategy fhir.rps_canonical
kehrnel core health
kehrnel strategy list --domain fhir
kehrnel core env show --env dev
```

---

## 9. Minimal smoke sequence

```powershell
cd kehrnel
.\.venv\Scripts\Activate.ps1
pip install -e src/kehrnel/engine/domains/fhir/libs/fhir-data-generation -e src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql
pip install -e ".[api,mongo,fhir,test]"

python -m pytest (Get-ChildItem tests\contract\test_fhir_rps*.py).FullName -q
python src\kehrnel\engine\strategies\fhir\rps_canonical\scripts\spike_generate_and_search.py --db fhir_kehrnel_spike

$env:KEHRNEL_AUTH_ENABLED = "false"
$env:KEHRNEL_API_PORT = "8080"
python -m kehrnel.api.app
```

New terminal:

```powershell
Invoke-RestMethod http://localhost:8080/health
Invoke-RestMethod -Method POST -Uri "http://localhost:8080/environments/dev/activate" `
  -ContentType "application/json" -Body (Get-Content src\kehrnel\engine\strategies\fhir\rps_canonical\specification\activate_dev.json -Raw)
```

---

## Troubleshooting

### Port 8080 stuck (`Port 8080 is already in use`)

`./startKehrnel` only supports port **8080**. A previous API run (or a background `python -m kehrnel.api.app`) may still be listening.

**PowerShell — find what is using 8080:**

```powershell
Get-NetTCPConnection -LocalPort 8080 -ErrorAction SilentlyContinue |
  Select-Object LocalAddress, LocalPort, State, OwningProcess |
  Format-Table -AutoSize

Get-NetTCPConnection -LocalPort 8080 -ErrorAction SilentlyContinue |
  Select-Object -ExpandProperty OwningProcess -Unique |
  ForEach-Object { Get-Process -Id $_ | Select-Object Id, ProcessName, Path }
```

**PowerShell — free the port (replace `<PID>` or use the one-liner):**

```powershell
Stop-Process -Id <PID> -Force

# Or stop every process bound to 8080:
Get-NetTCPConnection -LocalPort 8080 -ErrorAction SilentlyContinue |
  Select-Object -ExpandProperty OwningProcess -Unique |
  ForEach-Object { Stop-Process -Id $_ -Force }

# Verify:
Get-NetTCPConnection -LocalPort 8080 -ErrorAction SilentlyContinue
# (no output = port is free)
```

**Git Bash / CMD:**

```bash
netstat -ano | findstr :8080
taskkill //PID <pid> //F
```

Then start again:

```powershell
cd kehrnel
./startKehrnel
```

Prefer stopping the server with **Ctrl+C** in the terminal where `./startKehrnel` is running instead of leaving a background Python process.

---

### Other issues

| Issue | Action |
|-------|--------|
| `No module named fhir_gen` or `fhir_search_to_mql` | `pip install -e src/kehrnel/engine/domains/fhir/libs/fhir-data-generation -e src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql` then `pip install -e ".[fhir]"`; **restart** the API |
| `fhir-gen` fails from kehrnel root with pydantic `.env` errors | `pip install -e src/kehrnel/engine/domains/fhir/libs/fhir-data-generation` (uses package-root `.env` only) |
| Search returns 400 or empty | Run `fhir_denormalize` or set `denormalize_after: true` on the synthetic job |
| `INVALID_INPUT` + `Internal server error` on `/api/domains/fhir/search` (criteria or `fhir_search` both fail) | Usually BSON `ObjectId` in Mongo rows could not be JSON-encoded; fixed in `routes.py` via `_json_safe` — **restart** the API after pulling the fix |
| `fhir_search must include resource type` (e.g. with `gender=male` only) | Use `criteria`: `{"resource_type":"Patient","criteria":{"gender":"male"}}` **or** `fhir_search":"Patient?gender=male"` (not `"fhir_search":"gender=male"` alone) |
| Synthetic poll shows blank status | Use `$job.job.job_id` and `$st.job.status` (status is under `.job`) |
| `FHIR_LIBS_NOT_INSTALLED` on search | API started before `[fhir]` install — restart server after `pip install` |
| `uv` / `startKehrnel`: `relative path without a working directory` | No `file:src/kehrnel/engine/domains/fhir/libs/...` in `pyproject.toml` extras; use `[tool.uv.sources]`; delete `.venv/.startKehrnel-pyproject.sha256`; `./startKehrnel --force-sync` |
| `Index already exists with a different name: birthDate_1` | Stale indexes from older fhir-gen (index-on-save removed). Once per DB: `db.Patient.dropIndexes()` in mongosh, then re-run job with `denormalize_after: true` |
| pytest `test_fhir_rps*.py` not found (PowerShell) | `python -m pytest (Get-ChildItem tests\contract\test_fhir_rps*.py).FullName` |
| Port 8080 stuck | See [Port 8080 stuck](#port-8080-stuck-port-8080-is-already-in-use) above |
| `kehrnel-api` on port 8000 vs `./startKehrnel` on 8080 | Set `KEHRNEL_API_PORT=8080` and `RUNTIME_URL=http://localhost:8080`, or use `./startKehrnel` only |

---

## Environment variables (reference)

| Variable | Typical value | Purpose |
|----------|---------------|---------|
| `KEHRNEL_AUTH_ENABLED` | `false` (local dev) | Disable API key checks |
| `KEHRNEL_API_PORT` | `8080` | API listen port (`startKehrnel` default) |
| `FHIR_CONTRACT_MONGO` | `1` | Force Mongo in contract tests |
| `FHIR_GEN_MONGODB_URI` | `mongodb://localhost:27017` | fhir-gen settings (optional) |
| `MONGODB_URI` | `mongodb://localhost:27017` | Used by some tests / bindings |
