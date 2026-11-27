# FHIR testing guide (Kehrnel + vendored libraries)

End-to-end commands to install, test, and run **fhir.clinical_cdr** with **fhir-gen** and **fhir-mql**.

**Vendored library root** (all paths below are relative to the kehrnel repo root):


| Path                                                         | Package                                             |
| ------------------------------------------------------------ | --------------------------------------------------- |
| `src/kehrnel/engine/domains/fhir/libs/fhir-data-generation/` | **fhir-gen** (`fhir_gen`, CLI `fhir-gen`)           |
| `src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql/`   | **fhir-mql** (`fhir_search_to_mql`, CLI `fhir-mql`) |


The former top-level `libs/` folder was removed; FHIR libraries live under the **FHIR domain** package.

**Related:** [clinical_cdr strategy pack](src/kehrnel/engine/strategies/fhir/clinical_cdr/README.md) (config JSON, recipes, ops) · [FHIR_STRATEGIES_DOCUMENT.md](FHIR_STRATEGIES_DOCUMENT.md) · [domains/fhir](src/kehrnel/engine/domains/fhir/README.md) · [vendored libs](src/kehrnel/engine/domains/fhir/libs/README.md) · [Docusaurus clinical-cdr](docs/website/docs/strategies/fhir/clinical-cdr/index.md)

**Strategy ID:** `fhir.clinical_cdr` (not `fhir.rps_canonical`). **Default dev MongoDB:** `fhir_synthetic_clinical_cdr` (see `specification/activate_dev.json`).

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
cd <kehrnel-repo-root>   # e.g. code_repositories\datalab_forked\kehrnel

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
cd <kehrnel-repo-root>
$env:KEHRNEL_AUTH_ENABLED = "false"
$env:MONGODB_URI = "mongodb://localhost:27017"
$env:FHIR_CONTRACT_MONGO = "1"   # force Mongo-backed golden search execute tests

python -m pytest tests\contract\clinical_cdr -v
```

### Git Bash

```bash
cd <kehrnel-repo-root>
export KEHRNEL_AUTH_ENABLED=false
export MONGODB_URI=mongodb://localhost:27017
export FHIR_CONTRACT_MONGO=1

pytest tests/contract/clinical_cdr -v
```

Golden FHIR search fixtures: `tests/contract/clinical_cdr/fixtures/fhir_golden_queries.json`.

### FHIR library spike (no Kehrnel API)

```powershell
python src\kehrnel\engine\strategies\fhir\clinical_cdr\scripts\spike_generate_and_search.py --db fhir_kehrnel_spike
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


| URL                                                                  | Purpose               |
| -------------------------------------------------------------------- | --------------------- |
| [http://localhost:8080/health](http://localhost:8080/health)         | Liveness              |
| [http://localhost:8080/docs](http://localhost:8080/docs)             | Swagger UI            |
| [http://localhost:8080/redoc](http://localhost:8080/redoc)           | ReDoc                 |
| [http://localhost:8080/guide](http://localhost:8080/guide)           | Docusaurus (if built) |
| [http://localhost:8080/strategies](http://localhost:8080/strategies) | Strategy catalog      |


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

### 4.1 Activate `fhir.clinical_cdr`

Activation binds the environment to strategy `fhir.clinical_cdr`, merges pack defaults (`defaults.json` + `recipes.json`), and stores a **manifest digest**. After a real `manifest.json` version bump you must re-activate; routine code reload / pack hydration drift is auto-healed on the next op (see [§Troubleshooting — `ACTIVATION_STRATEGY_MISMATCH](#activation_strategy_mismatch)`).

Sample body: `src/kehrnel/engine/strategies/fhir/clinical_cdr/specification/activate_dev.json`


| Field             | Sample value                  |
| ----------------- | ----------------------------- |
| `strategy_id`     | `fhir.clinical_cdr`           |
| `domain`          | `fhir`                        |
| `config.database` | `fhir_synthetic_clinical_cdr` |
| `bindings.db.uri` | `mongodb://localhost:27017`   |


**PowerShell:**

```powershell
$spec = "src\kehrnel\engine\strategies\fhir\clinical_cdr\specification"

Invoke-RestMethod -Method POST -Uri "$base/environments/dev/activate" `
  -ContentType "application/json" `
  -Body (Get-Content "$spec\activate_dev.json" -Raw)

# Verify activation
Invoke-RestMethod -Uri "$base/environments/dev/activations"
```

**curl:**

```bash
curl -sS -X POST "$RUNTIME_URL/environments/dev/activate" \
  -H "Content-Type: application/json" \
  -d @src/kehrnel/engine/strategies/fhir/clinical_cdr/specification/activate_dev.json

curl -sS "$RUNTIME_URL/environments/dev/activations"
```

**Refresh manifest digest without changing config** (optional):

```powershell
Invoke-RestMethod -Method POST -Uri "$base/environments/dev/activations/fhir/upgrade"
```

### 4.2 Synthetic generation job

`POST /environments/{env_id}/synthetic/jobs` is **asynchronous**: it returns **HTTP 202** immediately with a `job_id` while generation runs in the background. It does **not** wait for resources to be created.

The response nests the job under `**job`**. Use `$job.job.job_id`, not `$job.job_id`. Poll `GET .../synthetic/jobs/{job_id}` until `status` is `completed`, `failed`, or `cancelled`.

Do **not** use `POST /environments/{env_id}/run` with `synthetic_generate_batch` for long-running generation — that endpoint runs the op **synchronously** and blocks until finished.

#### Packaged job files (`specification/`)


| File                       | Corpus                                | Notes                                                                          |
| -------------------------- | ------------------------------------- | ------------------------------------------------------------------------------ |
| `job_generate_dev.json`    | **~535 docs**, 10 types               | `recipe: clinical_dev` + `denormalize_after: true` — **recommended for demos** |
| `job_generate_full84.json` | **~1,000+ docs**, 52 types            | `recipe: clinical_full84` — soak / regression                                  |
| `job_generate_small.json`  | **7 docs** (2 Patient, 5 Observation) | No denormalize — run §4.3 before search                                        |


Recipes are defined in `specification/recipes.json`. Override counts in the job payload without editing recipes:

```json
{ "recipe": "clinical_dev", "resources": { "Patient": 10 }, "denormalize_after": true }
```

**PowerShell — dev corpus (recommended):**

```powershell
$spec = "src\kehrnel\engine\strategies\fhir\clinical_cdr\specification"

$job = Invoke-RestMethod -Method POST -Uri "$base/environments/dev/synthetic/jobs" `
  -ContentType "application/json" `
  -Body (Get-Content "$spec\job_generate_dev.json" -Raw)

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

**Small / inline payload** (quick test):

```powershell
$jobJson = @'
{
  "domain": "fhir",
  "op": "synthetic_generate_batch",
  "payload": {
    "seed": 42,
    "resources": { "Patient": 5, "Observation": 10 },
    "store_canonical": true,
    "denormalize_after": true
  }
}
'@

$job = Invoke-RestMethod -Method POST -Uri "$base/environments/dev/synthetic/jobs" `
  -ContentType "application/json" -Body $jobJson
$jid = $job.job.job_id
```

**curl — dev recipe job:**

```bash
curl -sS -X POST "$RUNTIME_URL/environments/dev/synthetic/jobs" \
  -H "Content-Type: application/json" \
  -d @src/kehrnel/engine/strategies/fhir/clinical_cdr/specification/job_generate_dev.json

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

Requires active `fhir.clinical_cdr` on `dev` and denormalized data. Run `**job_generate_dev.json**` (§4.2) so the `clinical_dev` recipe populates the ten searchable types below.

For a full **FHIR REST → Kehrnel** query catalog (multi-param searches, reference params, compartment matrix), see [§4.5.8](#458-fhir-search-query-catalog-rest-perspective).

#### 4.5.1 Search endpoints


| Endpoint                                        | Compartment                                          | Best for                                     |
| ----------------------------------------------- | ---------------------------------------------------- | -------------------------------------------- |
| `POST /api/domains/fhir/search`                 | **Yes** via `fhir_search` REST path                  | Bundle responses — preferred for compartment |
| `POST /environments/dev/run` (`fhir_search` op) | **Yes** (`fhir_search` path or `compartment` object) | Raw rows, explain-only                       |
| `POST /environments/dev/run` (`query` op)       | **Yes** (nest under `query`)                         | Same as compile+execute universal path       |


All domain searches need header `**x-active-env: dev`** (or `KEHRNEL_DEFAULT_ENV_ID=dev`).

#### 4.5.2 `clinical_dev` resource scenarios

After `job_generate_dev.json` completes with `denormalize_after: true`, these types exist in `fhir_synthetic_clinical_cdr` with fhir-mql YAML configs. Example criteria match [golden query fixtures](tests/contract/clinical_cdr/fixtures/fhir_golden_queries.json) and fhir-mql E2E defaults.


| Resource               | Example criteria                             | Notes                                            |
| ---------------------- | -------------------------------------------- | ------------------------------------------------ |
| **Patient**            | `gender=female`, `name=Smith`, `active=true` | Demographics; `name` matches family/given tokens |
| **Practitioner**       | `active=true`                                | Care-team actors                                 |
| **Organization**       | `active=true`                                | Facilities / payers                              |
| **Encounter**          | `status=finished`                            | Visits linked to patients                        |
| **Observation**        | `status=final`                               | Labs, vitals (largest collection in dev corpus)  |
| **Condition**          | `clinical-status=active`                     | Problems / diagnoses                             |
| **Procedure**          | `status=completed`                           | Completed procedures                             |
| **MedicationRequest**  | `status=active`                              | Active orders                                    |
| **DiagnosticReport**   | `status=final`                               | Result reports                                   |
| **AllergyIntolerance** | `clinical-status=active`                     | Active allergies                                 |


**Scheduling types** (`Schedule`, `Slot`, `Appointment`) are **not** in `clinical_dev`. Generate them inline (§4.5.5) or use contract-test seeds before searching.

**Domain API — structured `criteria`** (recommended):

```powershell
$hdr = @{ "x-active-env" = "dev" }
$search = "$base/api/domains/fhir/search"

# Patient — gender
Invoke-RestMethod -Method POST -Uri $search -ContentType "application/json" -Headers $hdr `
  -Body '{"resource_type":"Patient","criteria":{"gender":"female"},"limit":10}'

# Observation — status
Invoke-RestMethod -Method POST -Uri $search -ContentType "application/json" -Headers $hdr `
  -Body '{"resource_type":"Observation","criteria":{"status":"final"},"limit":20}'

# Encounter — status
Invoke-RestMethod -Method POST -Uri $search -ContentType "application/json" -Headers $hdr `
  -Body '{"resource_type":"Encounter","criteria":{"status":"finished"},"limit":10}'

# Condition — clinical status
Invoke-RestMethod -Method POST -Uri $search -ContentType "application/json" -Headers $hdr `
  -Body '{"resource_type":"Condition","criteria":{"clinical-status":"active"},"limit":10}'

# Procedure
Invoke-RestMethod -Method POST -Uri $search -ContentType "application/json" -Headers $hdr `
  -Body '{"resource_type":"Procedure","criteria":{"status":"completed"},"limit":10}'

# MedicationRequest
Invoke-RestMethod -Method POST -Uri $search -ContentType "application/json" -Headers $hdr `
  -Body '{"resource_type":"MedicationRequest","criteria":{"status":"active"},"limit":10}'

# DiagnosticReport
Invoke-RestMethod -Method POST -Uri $search -ContentType "application/json" -Headers $hdr `
  -Body '{"resource_type":"DiagnosticReport","criteria":{"status":"final"},"limit":10}'

# AllergyIntolerance
Invoke-RestMethod -Method POST -Uri $search -ContentType "application/json" -Headers $hdr `
  -Body '{"resource_type":"AllergyIntolerance","criteria":{"clinical-status":"active"},"limit":10}'

# Organization / Practitioner
Invoke-RestMethod -Method POST -Uri $search -ContentType "application/json" -Headers $hdr `
  -Body '{"resource_type":"Organization","criteria":{"active":"true"},"limit":10}'
Invoke-RestMethod -Method POST -Uri $search -ContentType "application/json" -Headers $hdr `
  -Body '{"resource_type":"Practitioner","criteria":{"active":"true"},"limit":10}'
```

**curl equivalents:**

```bash
HDR='-H "Content-Type: application/json" -H "x-active-env: dev"'

curl -sS -X POST "$RUNTIME_URL/api/domains/fhir/search" $HDR \
  -d '{"resource_type":"Observation","criteria":{"status":"final"},"limit":20}'

curl -sS -X POST "$RUNTIME_URL/api/domains/fhir/search" $HDR \
  -d '{"resource_type":"Condition","criteria":{"clinical-status":"active"},"limit":10}'

curl -sS -X POST "$RUNTIME_URL/api/domains/fhir/search" $HDR \
  -d '{"resource_type":"Encounter","criteria":{"status":"finished"},"limit":10}'
```

#### 4.5.3 `fhir_search` string form

`fhir_search` must include the resource type and `?` before parameters (FHIR REST style).  
Do **not** pass only `gender=male` when `resource_type` is separate — that returns `INVALID_INPUT`.

```powershell
# Correct: ResourceType?param=value
Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" `
  -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"Patient?gender=male","limit":10}'

Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" `
  -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"Observation?status=final","limit":20}'

# Wrong (fails validation):
# '{"resource_type":"Patient","fhir_search":"gender=male","limit":10}'
```

```bash
curl -sS -X POST "$RUNTIME_URL/api/domains/fhir/search" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{"fhir_search":"Observation?status=final","limit":20}'
```

Full URL form also works: `"fhir_search":"http://localhost/fhir/Patient?gender=male"`.

**View the full JSON in PowerShell** — `Invoke-RestMethod` parses JSON into nested `PSCustomObject` values. The default table view often shows `entry : @{resource=; search=}` even when `total` is correct:

```powershell
$bundle = Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" `
  -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"Patient?gender=male","limit":10}'

$bundle | ConvertTo-Json -Depth 30
$bundle.entry[0].resource | ConvertTo-Json -Depth 30
```

#### 4.5.4 Compartment search

Compartment search scopes results to resources linked to a compartment instance (FHIR REST: `GET /{compartmentType}/{id}/{searchType}?…`). Kehrnel parses that REST shape from `**fhir_search**` internally — you do **not** need separate `resource_type` / `criteria` / `compartment` fields unless you prefer structured JSON.

**Preferred — FHIR REST path in `fhir_search`** (same style as type-level `Patient?gender=female`):


| FHIR REST                                                         | `fhir_search` value                                                   |
| ----------------------------------------------------------------- | --------------------------------------------------------------------- |
| `GET /Patient/{id}/Observation?status=final`                      | `Patient/{id}/Observation?status=final`                               |
| `GET /Patient/{id}/Observation?category=vital-signs&status=final` | `Patient/{id}/Observation?category=vital-signs&status=final`          |
| Full URL                                                          | `http://localhost/fhir/Patient/{id}/Condition?clinical-status=active` |


**Domain API (Bundle)** — works on `/api/domains/fhir/search`:

```powershell
# Replace PATIENT_ID
Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" `
  -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"Patient/PATIENT_ID/Observation?category=vital-signs&status=final","limit":50}'
```

`**/run` op** — same string, or structured fallback:

```powershell
Invoke-RestMethod -Method POST -Uri "$base/environments/dev/run" `
  -ContentType "application/json" `
  -Body (@{
    domain = "fhir"
    operation = "fhir_search"
    payload = @{
      fhir_search = "Patient/$patientId/Observation?status=final&category=vital-signs"
      _count = 50
    }
  } | ConvertTo-Json -Depth 5)
```

**Alternative — structured payload** (still supported):

```json
{
  "domain": "fhir",
  "operation": "fhir_search",
  "payload": {
    "resource_type": "Observation",
    "criteria": { "status": "final" },
    "compartment": { "type": "Patient", "id": "<patient-id>" },
    "_count": 25
  }
}
```

**Supported compartment types (fhir-mql):** `Patient`, `Practitioner`, `Encounter`, `Device`, and dynamic resolution for some types (e.g. `RelatedPerson`).

**Step 1 — pick a Patient id from your corpus:**

```powershell
$p = Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" `
  -Headers @{ "x-active-env" = "dev" } `
  -Body '{"resource_type":"Patient","criteria":{"active":"true"},"limit":1}'
$patientId = $p.entry[0].resource.id
Write-Host "Patient id:" $patientId
```

**Step 2 — compartment scenarios (`fhir_search` REST path):**

```powershell
$run = "$base/environments/dev/run"
$hdr = @{ "x-active-env" = "dev" }
$search = "$base/api/domains/fhir/search"

# GET /Patient/{id}/Observation?status=final&category=vital-signs
Invoke-RestMethod -Method POST -Uri $search -ContentType "application/json" -Headers $hdr `
  -Body "{`"fhir_search`":`"Patient/$patientId/Observation?category=laboratory&status=amended`",`"limit`":50}"

# GET /Patient/{id}/Condition?clinical-status=active
Invoke-RestMethod -Method POST -Uri $search -ContentType "application/json" -Headers $hdr `
  -Body "{`"fhir_search`":`"Patient/$patientId/Condition?clinical-status=active`",`"limit`":20}"

# GET /Patient/{id}/Encounter?status=finished
Invoke-RestMethod -Method POST -Uri $run -ContentType "application/json" -Body (@{
  domain = "fhir"; operation = "fhir_search"
  payload = @{ fhir_search = "Patient/$patientId/Encounter?status=entered-in-error"; _count = 10 }
} | ConvertTo-Json -Depth 5)

# GET /Patient/{id}/MedicationRequest?status=active
Invoke-RestMethod -Method POST -Uri $run -ContentType "application/json" -Body (@{
  domain = "fhir"; operation = "fhir_search"
  payload = @{ fhir_search = "Patient/$patientId/MedicationRequest?status=active"; _count = 10 }
} | ConvertTo-Json -Depth 5)

# GET /Patient/{id}/DiagnosticReport?status=final
Invoke-RestMethod -Method POST -Uri $run -ContentType "application/json" -Body (@{
  domain = "fhir"; operation = "fhir_search"
  payload = @{ fhir_search = "Patient/$patientId/DiagnosticReport?status=final"; _count = 10 }
} | ConvertTo-Json -Depth 5)

# GET /Patient/{id}/AllergyIntolerance?clinical-status=active
Invoke-RestMethod -Method POST -Uri $run -ContentType "application/json" -Body (@{
  domain = "fhir"; operation = "fhir_search"
  payload = @{ fhir_search = "Patient/$patientId/AllergyIntolerance?clinical-status=active"; _count = 10 }
} | ConvertTo-Json -Depth 5)
```

**Practitioner compartment** (after you have a Practitioner id — search `Practitioner?active=true` and take `.id`):

```powershell
$pr = Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"resource_type":"Practitioner","criteria":{"active":"true"},"limit":1}'
$practitionerId = $pr.entry[0].resource.id

# Practitioner compartment → Schedule (scheduling corridor; generate §4.5.5 first)
Invoke-RestMethod -Method POST -Uri "$base/environments/dev/run" -ContentType "application/json" -Body (@{
  domain = "fhir"
  operation = "fhir_search"
  payload = @{
    resource_type = "Schedule"
    criteria = @{ active = "true" }
    compartment = @{ type = "Practitioner"; id = $practitionerId }
    _count = 10
  }
} | ConvertTo-Json -Depth 6)
```

**Encounter compartment** (self-scoped encounter search):

```powershell
$enc = Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"resource_type":"Encounter","criteria":{"status":"discontinued"},"limit":1}'
$encounterId = $enc.entry[0].resource.id
Write-Host "Encounter id:" $encounterId

Invoke-RestMethod -Method POST -Uri "$base/environments/dev/run" -ContentType "application/json" -Body (@{
  domain = "fhir"
  operation = "fhir_search"
  payload = @{
    resource_type = "Encounter"
    criteria = @{ status = "finished" }
    compartment = @{ type = "Encounter"; id = $encounterId }
    _count = 5
  }
} | ConvertTo-Json -Depth 6)
```

**curl — Patient compartment Observation search** (replace `PATIENT_ID`):

```bash
curl -sS -X POST "$RUNTIME_URL/environments/dev/run" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "fhir",
    "operation": "fhir_search",
    "payload": {
      "resource_type": "Observation",
      "criteria": { "status": "final" },
      "compartment": { "type": "Patient", "id": "PATIENT_ID" },
      "_count": 25
    }
  }'
```

**Explain-only** (compile MQL, no Mongo execute) — add `"explain_only": true` to the payload.

#### 4.5.5 Scheduling resource search (optional)

Golden tests seed `Schedule`, `Slot`, and `Appointment` separately. Generate a small scheduling corridor, denormalize, then search:

```powershell
$schedJob = @'
{
  "domain": "fhir",
  "op": "synthetic_generate_batch",
  "payload": {
    "seed": 42,
    "resources": {
      "Patient": 4,
      "Practitioner": 2,
      "Schedule": 3,
      "Slot": 8,
      "Appointment": 5
    },
    "store_canonical": true,
    "denormalize_after": true
  }
}
'@
$job = Invoke-RestMethod -Method POST -Uri "$base/environments/dev/synthetic/jobs" `
  -ContentType "application/json" -Body $schedJob
# Poll $job.job.job_id until completed (§4.2)
```


| Resource        | Example criteria | Golden fixture id    |
| --------------- | ---------------- | -------------------- |
| **Schedule**    | `active=true`    | `schedule_active`    |
| **Slot**        | `status=free`    | `slot_status_free`   |
| **Appointment** | `status=booked`  | `appointment_status` |


```powershell
Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"resource_type":"Slot","criteria":{"status":"free"},"limit":10}'

Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"resource_type":"Appointment","criteria":{"status":"booked"},"limit":10}'
```

**Patient compartment → Appointments** (after scheduling generation; use `$patientId` from §4.5.4):

```powershell
Invoke-RestMethod -Method POST -Uri "$base/environments/dev/run" -ContentType "application/json" -Body (@{
  domain = "fhir"
  operation = "fhir_search"
  payload = @{
    resource_type = "Appointment"
    criteria = @{ status = "booked" }
    compartment = @{ type = "Patient"; id = $patientId }
    _count = 10
  }
} | ConvertTo-Json -Depth 6)
```

#### 4.5.6 Discover search parameters

Before trying a new param, list what fhir-mql supports for a type:

```powershell
Invoke-RestMethod -Method POST -Uri "$base/environments/dev/run" `
  -ContentType "application/json" `
  -Body '{"domain":"fhir","operation":"fhir_list_search_params","payload":{"resource_type":"Observation"}}'
```

Swap `Observation` for any shipped type (`Patient`, `Encounter`, `Condition`, …). For the full 84-type list see [FHIR_STRATEGIES_DOCUMENT.md](FHIR_STRATEGIES_DOCUMENT.md) §2.1; `clinical_full84` recipe (§4.2) generates 52 searchable types for soak testing.

#### 4.5.7 `clinical_full84` spot checks

After `job_generate_full84.json` completes, try additional types not in `clinical_dev`:

```powershell
Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"resource_type":"CarePlan","criteria":{"status":"active"},"limit":10}'

Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"resource_type":"DocumentReference","criteria":{"status":"current"},"limit":10}'

Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"resource_type":"ServiceRequest","criteria":{"status":"active"},"limit":10}'
```

#### 4.5.8 FHIR search query catalog (REST perspective)

FHIR search is always `**[ResourceType]?[param]=[value]&…**`. Kehrnel accepts that string as `fhir_search`, or the same params as a `criteria` map on `resource_type`. Pagination maps to FHIR `_count` / `_offset` (`limit` / `offset` on the domain API).

**Three equivalent shapes** (pick one):


| FHIR REST (mental model)                           | Domain API `fhir_search`                                      | Domain API `criteria`                                      | Structured `/run` fallback                  |
| -------------------------------------------------- | ------------------------------------------------------------- | ---------------------------------------------------------- | ------------------------------------------- |
| `GET /Patient?gender=female`                       | `"fhir_search":"Patient?gender=female"`                       | `"resource_type":"Patient","criteria":{"gender":"female"}` | same `fhir_search` on `/run`                |
| `GET /Observation?patient=Patient/p1&status=final` | `"fhir_search":"Observation?patient=Patient/p1&status=final"` | `"criteria":{"patient":"Patient/p1","status":"final"}`     | linking-param search (not compartment)      |
| `GET /Patient/p1/Observation?status=final`         | `"fhir_search":"Patient/p1/Observation?status=final"`         | use `fhir_search` (parsed internally)                      | `"compartment":{...}` + `criteria` optional |


**Date / prefix modifiers** (`ge`, `le`, `gt`, `lt`, `eq`) belong in the query string, e.g. `date=ge2024-01-01`. Use `fhir_search` for multi-valued or prefixed params:

```powershell
# Patient — combined demographics
Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"Patient?name=Smith&gender=female&active=true","limit":20}'

# Encounter — class + date range (synthetic data may return 0 rows if no match)
Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"Encounter?status=finished&class=AMB&date=ge2020-01-01","limit":25}'

# Observation — category + status
Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"Observation?category=vital-signs&status=final","limit":50}'

# Condition — code token (ICD/SNOMED); use a code present in your corpus or expect empty Bundle
Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"Condition?clinical-status=active&category=problem-list-item","limit":20}'

# MedicationRequest — intent + status
Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"MedicationRequest?status=active&intent=order","limit":30}'

# Procedure — date lower bound
Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"Procedure?status=completed&date=ge2020-01-01","limit":20}'

# DiagnosticReport — category LAB
Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"DiagnosticReport?status=final&category=LAB","limit":25}'

# AllergyIntolerance — criticality
Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"AllergyIntolerance?clinical-status=active&criticality=high","limit":15}'
```

##### Reference searches (`patient=`, `subject=`, `encounter=`)

Type-level search with a **reference parameter** (FHIR linking param) — no compartment block:

```powershell
# Replace PATIENT_ID with a real id from §4.5.4
Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"Observation?patient=Patient/PATIENT_ID&status=final","limit":50}'

Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"Encounter?patient=Patient/PATIENT_ID&status=finished","limit":20}'

Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"Condition?patient=Patient/PATIENT_ID&clinical-status=active","limit":30}'
```

**Scheduling** (after §4.5.5 generation; replace ids):

```powershell
Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"Schedule?active=true&actor=Practitioner/PRACTITIONER_ID","limit":10}'

Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"Slot?status=free&schedule=Schedule/SCHEDULE_ID","limit":50}'

Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"Appointment?patient=Patient/PATIENT_ID&status=booked","limit":20}'
```

##### `clinical_full84` — additional type-level queries

Requires `job_generate_full84.json`. Examples mirror [fhir-mql healthcare workflows](src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql/CLI_COMMANDS.md#healthcare-workflow-scenarios):


| Workflow          | `fhir_search` string                        |
| ----------------- | ------------------------------------------- |
| Care plan         | `CarePlan?status=active`                    |
| Care team         | `CareTeam?status=active`                    |
| Goals             | `Goal?lifecycle-status=active`              |
| Tasks             | `Task?status=in-progress`                   |
| Service orders    | `ServiceRequest?status=active&intent=order` |
| Documents         | `DocumentReference?status=current`          |
| Compositions      | `Composition?status=final`                  |
| Specimens         | `Specimen?status=available`                 |
| Device usage      | `DeviceUsage?status=active`                 |
| Provenance        | `Provenance?activity=UPDATE`                |
| Communication     | `Communication?status=completed`            |
| Episode of care   | `EpisodeOfCare?status=active`               |
| Location          | `Location?status=active`                    |
| Practitioner role | `PractitionerRole?active=true`              |


```powershell
Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"CarePlan?status=active","limit":10}'

Invoke-RestMethod -Method POST -Uri "$base/api/domains/fhir/search" `
  -ContentType "application/json" -Headers @{ "x-active-env" = "dev" } `
  -Body '{"fhir_search":"Goal?lifecycle-status=active","limit":15}'
```

##### Compartment query catalog

Compartment REST pattern: `**GET /{compartmentType}/{compartmentId}/{searchType}?{params}**`  
Kehrnel: `POST /environments/dev/run` with `"operation":"fhir_search"` and `"compartment":{"type":"…","id":"…"}`.


| FHIR REST compartment URL                  | `resource_type`            | Example `criteria` / query string                                   |
| ------------------------------------------ | -------------------------- | ------------------------------------------------------------------- |
| `/Patient/{id}/Observation?…`              | `Observation`              | `status=final`, `category=vital-signs`, `code=8480-6` (systolic BP) |
| `/Patient/{id}/Condition?…`                | `Condition`                | `clinical-status=active`, `category=problem-list-item`              |
| `/Patient/{id}/Encounter?…`                | `Encounter`                | `status=finished`, `class=AMB`                                      |
| `/Patient/{id}/Procedure?…`                | `Procedure`                | `status=completed`, `date=ge2020-01-01`                             |
| `/Patient/{id}/MedicationRequest?…`        | `MedicationRequest`        | `status=active`, `intent=order`                                     |
| `/Patient/{id}/DiagnosticReport?…`         | `DiagnosticReport`         | `status=final`, `category=LAB`                                      |
| `/Patient/{id}/AllergyIntolerance?…`       | `AllergyIntolerance`       | `clinical-status=active`, `criticality=high`                        |
| `/Patient/{id}/Appointment?…`              | `Appointment`              | `status=booked`, `date=ge2024-01-01`                                |
| `/Patient/{id}/CarePlan?…`                 | `CarePlan`                 | `status=active`                                                     |
| `/Patient/{id}/ServiceRequest?…`           | `ServiceRequest`           | `status=active`                                                     |
| `/Patient/{id}/DocumentReference?…`        | `DocumentReference`        | `status=current`                                                    |
| `/Patient/{id}/Composition?…`              | `Composition`              | `status=final`                                                      |
| `/Patient/{id}/MedicationAdministration?…` | `MedicationAdministration` | `status=completed`                                                  |
| `/Patient/{id}/Immunization?…`             | `Immunization`             | `status=completed`                                                  |
| `/Practitioner/{id}/Schedule?…`            | `Schedule`                 | `active=true`                                                       |
| `/Practitioner/{id}/Appointment?…`         | `Appointment`              | `status=booked`                                                     |
| `/Encounter/{id}/Condition?…`              | `Condition`                | `clinical-status=active`                                            |
| `/Encounter/{id}/Observation?…`            | `Observation`              | `status=final`                                                      |
| `/Encounter/{id}/Encounter?…`              | `Encounter`                | `status=in-progress` (self compartment)                             |
| `/Device/{id}/Observation?…`               | `Observation`              | `status=final`, `code=8480-6`                                       |


**Patient compartment — vitals and labs** (`$patientId` from §4.5.4):

```powershell
$run = "$base/environments/dev/run"

# GET /Patient/{id}/Observation?category=vital-signs&status=final
Invoke-RestMethod -Method POST -Uri $run -ContentType "application/json" -Body (@{
  domain = "fhir"; operation = "fhir_search"
  payload = @{
    resource_type = "Observation"
    criteria = @{ category = "vital-signs"; status = "final" }
    compartment = @{ type = "Patient"; id = $patientId }
    _count = 50
  }
} | ConvertTo-Json -Depth 6)

# GET /Patient/{id}/Observation?code=http://loinc.org|8480-6  (systolic BP — token form in fhir_search)
Invoke-RestMethod -Method POST -Uri $run -ContentType "application/json" -Body (@{
  domain = "fhir"; operation = "fhir_search"
  payload = @{
    fhir_search = "Observation?code=http://loinc.org|8480-6&status=final"
    compartment = @{ type = "Patient"; id = $patientId }
    _count = 25
  }
} | ConvertTo-Json -Depth 6)

# GET /Patient/{id}/Procedure?status=completed
Invoke-RestMethod -Method POST -Uri $run -ContentType "application/json" -Body (@{
  domain = "fhir"; operation = "fhir_search"
  payload = @{
    resource_type = "Procedure"
    criteria = @{ status = "completed" }
    compartment = @{ type = "Patient"; id = $patientId }
    _count = 20
  }
} | ConvertTo-Json -Depth 6)
```

**Encounter compartment** — resources during a visit (`$encounterId` from §4.5.4):

```powershell
# GET /Encounter/{id}/Condition?clinical-status=active
Invoke-RestMethod -Method POST -Uri "$base/environments/dev/run" -ContentType "application/json" -Body (@{
  domain = "fhir"; operation = "fhir_search"
  payload = @{
    resource_type = "Condition"
    criteria = @{ "clinical-status" = "active" }
    compartment = @{ type = "Encounter"; id = $encounterId }
    _count = 30
  }
} | ConvertTo-Json -Depth 6)

# GET /Encounter/{id}/Observation?status=final
Invoke-RestMethod -Method POST -Uri "$base/environments/dev/run" -ContentType "application/json" -Body (@{
  domain = "fhir"; operation = "fhir_search"
  payload = @{
    resource_type = "Observation"
    criteria = @{ status = "final" }
    compartment = @{ type = "Encounter"; id = $encounterId }
    _count = 50
  }
} | ConvertTo-Json -Depth 6)
```

**Practitioner compartment** — schedules and appointments (`$practitionerId` from §4.5.4; scheduling data §4.5.5):

```powershell
# GET /Practitioner/{id}/Schedule?active=true
Invoke-RestMethod -Method POST -Uri "$base/environments/dev/run" -ContentType "application/json" -Body (@{
  domain = "fhir"; operation = "fhir_search"
  payload = @{
    resource_type = "Schedule"
    criteria = @{ active = "true" }
    compartment = @{ type = "Practitioner"; id = $practitionerId }
    _count = 10
  }
} | ConvertTo-Json -Depth 6)

# GET /Practitioner/{id}/Appointment?status=booked
Invoke-RestMethod -Method POST -Uri "$base/environments/dev/run" -ContentType "application/json" -Body (@{
  domain = "fhir"; operation = "fhir_search"
  payload = @{
    resource_type = "Appointment"
    criteria = @{ status = "booked" }
    compartment = @{ type = "Practitioner"; id = $practitionerId }
    _count = 20
  }
} | ConvertTo-Json -Depth 6)
```

**Device compartment** (when `Device` resources exist — `clinical_full84` or inline generation):

```powershell
# Pick a Device id first, then:
# GET /Device/{id}/Observation?status=final
Invoke-RestMethod -Method POST -Uri "$base/environments/dev/run" -ContentType "application/json" -Body (@{
  domain = "fhir"; operation = "fhir_search"
  payload = @{
    resource_type = "Observation"
    criteria = @{ status = "final" }
    compartment = @{ type = "Device"; id = "DEVICE_ID" }
    _count = 25
  }
} | ConvertTo-Json -Depth 6)
```

**curl — Patient compartment with LOINC code** (replace `PATIENT_ID`):

```bash
curl -sS -X POST "$RUNTIME_URL/environments/dev/run" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "fhir",
    "operation": "fhir_search",
    "payload": {
      "fhir_search": "Observation?code=http://loinc.org|8480-6&status=final",
      "compartment": { "type": "Patient", "id": "PATIENT_ID" },
      "_count": 25
    }
  }'
```

**Compartment vs `patient=` reference:** Both express “resources for this patient.” Compartment search uses precomputed `_compartments.Patient` (fast path after denormalize). Reference `patient=Patient/{id}` uses the linking search parameter. Prefer **compartment** for `GET /Patient/{id}/[type]` semantics; use `**patient=`** when mirroring `GET /[type]?patient=…` on the domain Bundle API.

##### Reusable PowerShell helper

```powershell
function Invoke-FhirCompartmentSearch {
  param(
    [string]$BaseUrl = $base,
    [string]$EnvId = "dev",
    [string]$CompartmentType,   # Patient | Practitioner | Encounter | Device
    [string]$CompartmentId,
    [string]$ResourceType,
    [hashtable]$Criteria = @{},
    [string]$FhirSearch,          # optional; overrides Criteria when set
    [int]$Count = 25
  )
  $payload = @{
    resource_type = $ResourceType
    compartment = @{ type = $CompartmentType; id = $CompartmentId }
    _count = $Count
  }
  if ($FhirSearch) { $payload.fhir_search = $FhirSearch; $payload.Remove("resource_type") }
  elseif ($Criteria.Count) { $payload.criteria = $Criteria }
  Invoke-RestMethod -Method POST -Uri "$BaseUrl/environments/$EnvId/run" `
    -ContentType "application/json" `
    -Body (@{ domain = "fhir"; operation = "fhir_search"; payload = $payload } | ConvertTo-Json -Depth 8)
}

# Usage:
# Invoke-FhirCompartmentSearch -CompartmentType Patient -CompartmentId $patientId `
#   -ResourceType Observation -Criteria @{ status = "final"; category = "vital-signs" }
```

Standalone **fhir-mql CLI** equivalents (same query strings, no API):

```powershell
fhir-mql search Patient "gender=female&active=true" --limit 20
fhir-mql search Observation "category=vital-signs&status=final" --limit 50
fhir-mql search Observation "code=8480-6" --compartment-type Patient --compartment-id $patientId --limit 25
fhir-mql search Condition "clinical-status=active" --compartment-type Patient --compartment-id $patientId --limit 30
fhir-mql search Schedule "active=true" --compartment-type Practitioner --compartment-id $practitionerId --limit 10
```

See [fhir-mql CLI_COMMANDS.md](src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql/CLI_COMMANDS.md) for the full 84-type workflow list.

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


| Image         | Dockerfile                       |
| ------------- | -------------------------------- |
| API (no FHIR) | `docker/Dockerfile.kehrnel-api`  |
| API + FHIR    | `docker/Dockerfile.kehrnel-fhir` |
| All-in-one    | `Dockerfile.backend`             |
| fhir-gen only | `docker/Dockerfile.fhir-gen`     |
| fhir-mql only | `docker/Dockerfile.fhir-mql`     |


---

## 8. Kehrnel CLI (optional)

```powershell
$env:RUNTIME_URL = "http://localhost:8080"

kehrnel setup --runtime-url $env:RUNTIME_URL --env dev --domain fhir --strategy fhir.clinical_cdr
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

python -m pytest tests\contract\clinical_cdr -q
python src\kehrnel\engine\strategies\fhir\clinical_cdr\scripts\spike_generate_and_search.py --db fhir_kehrnel_spike

$env:KEHRNEL_AUTH_ENABLED = "false"
$env:KEHRNEL_API_PORT = "8080"
python -m kehrnel.api.app
```

New terminal:

```powershell
$base = "http://localhost:8080"
$spec = "src\kehrnel\engine\strategies\fhir\clinical_cdr\specification"

Invoke-RestMethod "$base/health"
Invoke-RestMethod -Method POST -Uri "$base/environments/dev/activate" `
  -ContentType "application/json" -Body (Get-Content "$spec\activate_dev.json" -Raw)

$job = Invoke-RestMethod -Method POST -Uri "$base/environments/dev/synthetic/jobs" `
  -ContentType "application/json" -Body (Get-Content "$spec\job_generate_dev.json" -Raw)
# Poll $job.job.job_id until completed, then search (§4.5)
```

---

## Troubleshooting

### `ACTIVATION_STRATEGY_MISMATCH`

**Symptom:** `"code": "ACTIVATION_STRATEGY_MISMATCH"`, `"message": "Active strategy differs from current manifest"`.

**Cause:** Environment `dev` was activated against a different strategy manifest identity (e.g. before rename to `fhir.clinical_cdr`, or after a real `manifest.json` **version** bump). Stored `manifest_digest` no longer matches.

**Auto-heal:** If `strategy_id` and manifest **version** still match, the next op auto-updates the digest (no action needed). You only need the steps below when the version changed or strategy id differs.

**Fix:** Re-activate (§4.1) or upgrade:

```powershell
Invoke-RestMethod -Method POST -Uri "$base/environments/dev/activate" `
  -ContentType "application/json" `
  -Body (Get-Content src\kehrnel\engine\strategies\fhir\clinical_cdr\specification\activate_dev.json -Raw)

# Or:
Invoke-RestMethod -Method POST -Uri "$base/environments/dev/activations/fhir/upgrade"
```

Restart the API after pulling pack changes, then activate again. Use a fresh env name (`dev2`) if the registry is stuck.

---

### FHIR search / compartment search feels slow (3–10+ seconds)

**Not MongoDB** — on `mongodb://localhost:27017` with denormalized data, `find` + `count` are typically **<10 ms** per collection (~50–500 docs).

**Main cause (fixed in current code):** each search used to construct a new `FHIRSearchConverter` / `ConfigLoader`, reloading **80+ fhir-mql YAML files** (~**3–5 s per request**). Kehrnel now caches the converter and Mongo client per process — **restart the API** after pulling this fix. The **first** search after restart may still take a few seconds (cold cache); later searches should be sub-second.

**Other contributors:**


| Factor                            | Effect                                                                |
| --------------------------------- | --------------------------------------------------------------------- |
| `count_documents` on every search | Extra Mongo round-trip for `Bundle.total` (usually small on local DB) |
| Compartment queries               | `$and` + `$or` filters; ensure `fhir_ensure_indexes` ran (§4.4)       |
| Missing `_search` / denormalize   | Full collection scans — run job with `denormalize_after: true`        |
| Large `limit`                     | Returns full canonical FHIR JSON for every row                        |


**Verify Mongo is fast directly:**

```powershell
mongosh mongodb://localhost:27017/fhir_synthetic_clinical_cdr --eval "db.Patient.getIndexes().length"
```

---

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


| Issue                                                                                           | Action                                                                                                                                                                                     |
| ----------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `ACTIVATION_STRATEGY_MISMATCH`                                                                  | Re-activate or `POST .../activations/fhir/upgrade` — see [above](#activation_strategy_mismatch)                                                                                            |
| `strategy_id` `fhir.rps_canonical` not found                                                    | Use `fhir.clinical_cdr` in activate body                                                                                                                                                   |
| `No module named fhir_gen` or `fhir_search_to_mql`                                              | `pip install -e src/kehrnel/engine/domains/fhir/libs/fhir-data-generation -e src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql` then `pip install -e ".[fhir]"`; **restart** the API |
| `fhir-gen` fails from kehrnel root with pydantic `.env` errors                                  | `pip install -e src/kehrnel/engine/domains/fhir/libs/fhir-data-generation` (uses package-root `.env` only)                                                                                 |
| Search returns 400 or empty                                                                     | Run `fhir_denormalize` or set `denormalize_after: true` on the synthetic job                                                                                                               |
| `INVALID_INPUT` + `Internal server error` on `/api/domains/fhir/search`                         | BSON `ObjectId` in Mongo rows could not be JSON-encoded — **restart** the API after pulling the `_json_safe` fix in domain search routes                                                   |
| Same error on `POST /environments/dev/run` (`fhir_search` with **compartment**) when rows match | Same root cause on `/run` responses — fixed via `_json_safe` on run endpoints; **restart** the API                                                                                         |
| `fhir_search must include resource type` (e.g. with `gender=male` only)                         | Use `criteria`: `{"resource_type":"Patient","criteria":{"gender":"male"}}` **or** `fhir_search":"Patient?gender=male"` (not `"fhir_search":"gender=male"` alone)                           |
| Synthetic poll shows blank status                                                               | Use `$job.job.job_id` and `$st.job.status` (status is under `.job`)                                                                                                                        |
| `FHIR_LIBS_NOT_INSTALLED` on search                                                             | API started before `[fhir]` install — restart server after `pip install`                                                                                                                   |
| `uv` / `startKehrnel`: `relative path without a working directory`                              | No `file:src/kehrnel/engine/domains/fhir/libs/...` in `pyproject.toml` extras; use `[tool.uv.sources]`; delete `.venv/.startKehrnel-pyproject.sha256`; `./startKehrnel --force-sync`       |
| `Index already exists with a different name: birthDate_1`                                       | Stale indexes from older fhir-gen (index-on-save removed). Once per DB: `db.Patient.dropIndexes()` in mongosh, then re-run job with `denormalize_after: true`                              |
| pytest clinical_cdr tests not found                                                             | `python -m pytest tests\contract\clinical_cdr`                                                                                                                                             |
| Port 8080 stuck                                                                                 | See [Port 8080 stuck](#port-8080-stuck-port-8080-is-already-in-use) above                                                                                                                  |
| `kehrnel-api` on port 8000 vs `./startKehrnel` on 8080                                          | Set `KEHRNEL_API_PORT=8080` and `RUNTIME_URL=http://localhost:8080`, or use `./startKehrnel` only                                                                                          |


---

## Environment variables (reference)


| Variable                      | Typical value               | Purpose                                                     |
| ----------------------------- | --------------------------- | ----------------------------------------------------------- |
| `KEHRNEL_AUTH_ENABLED`        | `false` (local dev)         | Disable API key checks                                      |
| `KEHRNEL_API_PORT`            | `8080`                      | API listen port (`startKehrnel` default)                    |
| `FHIR_CONTRACT_MONGO`         | `1`                         | Enable Mongo execute tests in `tests/contract/clinical_cdr` |
| `FHIR_GEN_MONGODB_URI`        | `mongodb://localhost:27017` | fhir-gen settings (optional)                                |
| `MONGODB_URI`                 | `mongodb://localhost:27017` | Contract tests + Mongo bindings                             |
| `FHIR_GENERATION_INTEGRATION` | `1`                         | Optional: persist test in `test_generation.py`              |
| `FHIR_BRIDGE_INTEGRATION`     | `1`                         | Optional: live fhir-mql client smoke in `test_bridge.py`    |


