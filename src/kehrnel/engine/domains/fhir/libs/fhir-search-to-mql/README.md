# FHIR Search to MQL Conversion Library

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Version](https://img.shields.io/badge/version-1.2.0-green.svg)](pyproject.toml)

A production-ready Python library and CLI that converts FHIR (Fast
Healthcare Interoperability Resources) search queries into MongoDB
Query Language (MQL) and manages the matching denormalization layer
for high-performance healthcare data search.

The library ships **bundled YAML configurations for 84 FHIR R5
resources** — clinical, administrative, financial, devices, quality
reporting, genomics, and interoperability — validated against FHIR R5
search parameters and compartment definitions. A fresh `pip install`
is fully usable without extra setup.

Pair with **[fhir-data-generation](../fhir-data-generation/)** (`fhir-gen`)
to synthesize interlinked test data for the same 84 types, then
`fhir-mql denormalize` and `fhir-mql search` on one MongoDB database.

| | fhir-data-generation | fhir-search-to-mql |
|--|----------------------|---------------------|
| **Role** | Generate synthetic FHIR JSON | FHIR search → MQL + `_search` / `_compartments` |
| **CLI** | `fhir-gen` | `fhir-mql` |
| **Shared set** | `MQL_SHIPPED_RESOURCES` (84) | `src/fhir_search_to_mql/configs/*.yaml` (84) |
| **Default DB** | `fhir_synthetic` | `fhir_synthetic` |

**Command cookbooks:** [CLI_COMMANDS.md](CLI_COMMANDS.md) (this repo) · [fhir-gen CLI_COMMANDS.md](../fhir-data-generation/CLI_COMMANDS.md)  
**Combined E2E:** [E2E_COMBINED.md](E2E_COMBINED.md) · canonical doc in [fhir-data-generation/E2E_COMBINED.md](../fhir-data-generation/E2E_COMBINED.md)

---

## Table of contents

1. [Getting started (full setup walkthrough)](#getting-started-full-setup-walkthrough)
   * [1.1 Prerequisites](#11-prerequisites)
   * [1.2 Get the source code](#12-get-the-source-code)
   * [1.3 Create and activate a virtual environment](#13-create-and-activate-a-virtual-environment)
   * [1.4 Install the package](#14-install-the-package)
   * [1.5 Verify the install](#15-verify-the-install)
   * [1.6 Set up MongoDB (optional but typical)](#16-set-up-mongodb-optional-but-typical)
   * [1.7 First run — denormalize and search](#17-first-run--denormalize-and-search)
   * [1.8 Run the test suite](#18-run-the-test-suite)
2. [What's new — 84 shipped resources](#whats-new--84-shipped-resources)
3. [Highlights](#highlights)
4. [Bundled FHIR resources (84 shipped)](#bundled-fhir-resources-84-shipped)
5. [End-to-end with fhir-gen](#end-to-end-with-fhir-gen)
6. [Combined E2E testing](#combined-e2e-testing)
7. [Library quick start](#library-quick-start)
8. [Command-line interface (`fhir-mql`)](#command-line-interface-fhir-mql)
   * [CLI command reference (full scenarios)](#cli-command-reference-full-scenarios)
9. [Denormalization and search parameters](#denormalization-and-search-parameters)
10. [Performance](#performance)
11. [FHIR schema tooling](#fhir-schema-tooling)
12. [Using the library as a plugin in another project](#using-the-library-as-a-plugin-in-another-project)
13. [Compartment search (hybrid fast-path)](#compartment-search-hybrid-fast-path)
14. [Configuration](#configuration)
15. [Architecture](#architecture)
16. [Build and distribution](#build-and-distribution)
17. [Testing](#testing)
18. [Troubleshooting](#troubleshooting)
19. [License](#license)

---

## Getting started (full setup walkthrough)

Follow these steps once and the rest of the README is reference
material you can dip into as needed. All commands below are PowerShell
on Windows; bash equivalents are shown where the syntax differs.

### 1.1 Prerequisites

| Requirement | Version | Why |
|-------------|---------|-----|
| Python      | 3.9+    | Library + CLI runtime. 3.11 / 3.12 also officially tested. |
| MongoDB     | 4.0+    | Only needed for `fhir-mql search / denormalize / indexes / reset / stats` and the integration tests. The pure `convert` workflow has no DB dependency. |
| pip         | recent  | `pip install --upgrade pip` if you hit resolver warnings. |
| Git         | any     | To clone the repo (skip if you already have a tarball). |

Confirm before starting:

```powershell
python --version          # >= 3.9
pip --version
git --version             # optional; only for clone
```

### 1.2 Get the source code

The package is **not** on PyPI — `pip install fhir-search-to-mql`
fails by design today. Clone the repo (or unpack the source archive
you were given) into a working directory:

```powershell
# PowerShell / Windows
git clone <your-fork-or-internal-url>/fhir-search-to-mql.git
cd fhir-search-to-mql
```

```bash
# bash / macOS / Linux
git clone <your-fork-or-internal-url>/fhir-search-to-mql.git
cd fhir-search-to-mql
```

If you only have the source folder on disk (no Git), simply `cd` into
that folder.

### 1.3 Create and activate a virtual environment

A dedicated venv keeps `fhir-mql` and its dependencies isolated from
your system Python. Pick the right activation command for your shell:

```powershell
# PowerShell (Windows)
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

```cmd
:: cmd.exe (Windows, legacy)
python -m venv .venv
.\.venv\Scripts\activate.bat
```

```bash
# bash / zsh (macOS / Linux / Git Bash)
python -m venv .venv
source .venv/bin/activate
```

Your prompt should now begin with `(.venv)`. From here on, every
`python`, `pip`, and `fhir-mql` invocation refers to the venv's
copy.

> If PowerShell refuses to run `Activate.ps1` with a security
> error, allow signed scripts for the current user once:
> `Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned`.

### 1.4 Install the package

For local development (recommended) install in **editable** mode so
changes to the source are picked up immediately, with the dev /
docs extras for tests and tooling:

```powershell
pip install --upgrade pip
pip install -e ".[dev,docs]"
```

If you only need the runtime (no tests / linters / docs build):

```powershell
pip install -e .
```

For installing into a **different** project's venv (i.e. consuming
this library elsewhere), build a wheel here and install it there:

```powershell
# in this repo's venv
pip install build
python -m build
# → dist/fhir_search_to_mql-1.2.0-py3-none-any.whl

# in the other project's venv
pip install path\to\fhir_search_to_mql-1.2.0-py3-none-any.whl
```

You can also `pip install` directly from a Git URL the consumer can
reach (no wheel needed):

```powershell
pip install "git+https://your-git-host/<org>/fhir-search-to-mql.git@main"
```

### 1.5 Verify the install

The console script and Python API should both work after the install:

```powershell
fhir-mql --version
# fhir-mql 1.2.0

fhir-mql resources
# Resource           FHIR   Params  Denorm  Indexes
# --------------------------------------------------
# (84 rows — Patient, Observation, Composition, DeviceRequest, MeasureReport, …)
```

```powershell
python -c "from fhir_search_to_mql import ConfigLoader; print(len(ConfigLoader().list_resources()))"
# 84
```

If either command fails, jump to [Troubleshooting](#troubleshooting).

### 1.6 Set up MongoDB (optional but typical)

The library is fully usable for FHIR-search → MQL conversion without
a database (`fhir-mql convert`, `FHIRSearchConverter().convert(...)`).
The bulk subcommands and integration tests, however, need MongoDB
reachable at `mongodb://localhost:27017/`.

The fastest way is Docker:

```powershell
docker run -d --name mongo-fhir -p 27017:27017 mongo:7
```

Or install MongoDB Community Server natively
(<https://www.mongodb.com/try/download/community>). Confirm it's
running:

```powershell
python -c "from pymongo import MongoClient; print(MongoClient('mongodb://localhost:27017/').server_info()['version'])"
```

Set the defaults the CLI will pick up if you don't pass `--uri` /
`--db`:

```powershell
$env:MONGODB_URI = "mongodb://localhost:27017/"
$env:MONGODB_DB  = "fhir_synthetic"
```

```bash
export MONGODB_URI="mongodb://localhost:27017/"
export MONGODB_DB="fhir_synthetic"
```

### 1.7 First run — denormalize and search

Drop a few Patient docs into your demo DB and run the full pipeline:

```powershell
python -c @"
from pymongo import MongoClient
db = MongoClient('mongodb://localhost:27017/')['fhir_synthetic']
db.Patient.delete_many({})
db.Patient.insert_many([
    {'resourceType': 'Patient', 'id': 'p1',
     'name': [{'family': 'Smith', 'given': ['John']}],
     'gender': 'male', 'birthDate': '1980-05-15'},
    {'resourceType': 'Patient', 'id': 'p2',
     'name': [{'family': 'Smyth', 'given': ['Jane']}],
     'gender': 'female', 'birthDate': '1990-02-01'},
])
print('inserted', db.Patient.count_documents({}))
"@
```

Then:

```powershell
# Build the indexes declared in Patient.yaml
fhir-mql indexes Patient

# Populate _search and _compartments on every Patient doc
fhir-mql denormalize Patient

# Convert + run a search query (no DB write needed)
fhir-mql search Patient "name=Smith&gender=male" --format json

# Coverage report — confirms every doc has _search populated
fhir-mql stats Patient
```

You're done. The same pattern works for every bundled resource — just
pass any resource name or `--all`.

### 1.8 Run the test suite

The full suite (~2,100 passing) takes about 5 minutes and exercises
the CLI, denormalizer, converter, extractors, and a live MongoDB:

```powershell
python -m pytest tests/ -q
```

Pure-unit-only (no MongoDB required, ~15 seconds):

```powershell
python -m pytest tests/unit/ -q
```

Skip the CLI integration tests (requires MongoDB + installed CLI):

```powershell
python -m pytest --ignore=tests/integration/test_cli_integration.py -q
```

A single resource's integration suite:

```powershell
python -m pytest tests/integration/test_slot_comprehensive.py -v
```

If MongoDB isn't running, all `@pytest.mark.mongodb` tests are skipped
automatically — the rest of the suite still runs to completion.

---

## What's new — 84 shipped resources

The bundled config set grew from the original scheduling/clinical core
to **84 FHIR R5 resources** covering practical healthcare and
enterprise use cases:

| Area | Examples |
|------|----------|
| **Clinical & docs** | Composition, AdverseEvent, BodyStructure, ClinicalImpression, DiagnosticReport, DocumentReference |
| **Orders & workflow** | DeviceRequest, RequestOrchestration, SupplyRequest, SupplyDelivery, Task, ServiceRequest |
| **Financial / RCM** | ExplanationOfBenefit, CoverageEligibilityRequest/Response, ChargeItemDefinition, PaymentNotice, PaymentReconciliation |
| **Payer** | EnrollmentRequest/Response, InsurancePlan |
| **Quality & safety** | Measure, MeasureReport, DetectedIssue, ImmunizationRecommendation |
| **Interop** | Endpoint, OrganizationAffiliation, Provenance, Basic |
| **Specialty** | GenomicStudy, BiologicallyDerivedProduct, VisionPrescription, NutritionIntake, Questionnaire |

Each resource has YAML under `src/fhir_search_to_mql/configs/`, integration
tests (`tests/integration/test_<resource>_comprehensive.py`), and coverage
in `tests/integration/test_config_audit_regressions.py`.

**Alignment with fhir-gen:** the same 84 type names are listed in
`fhir_gen/resolvers/dependency.py` → `MQL_SHIPPED_RESOURCES` with matching
enrichers and `CORE_DEPENDENCIES`.

**E2E alignment:** industrial scenario databases use descriptive IDs (`fhir_e2e_gen_ind_hospital`, `fhir_e2e_gen_ind_full84`, …) matching fhir-gen. Search plans use **`resource_search_queries.py`** so each resource type is queried with parameters it actually supports.

---

## Highlights

- **Configuration-driven**: Only fields explicitly listed in YAML
  are denormalized. No magic.
- **21 generic extractors**: Cover every searchable FHIR R4/R5
  datatype (HumanName, CodeableConcept, Identifier, Reference,
  Address, ContactPoint, Quantity, Period, Timing, Range, Ratio,
  Coding, Extension, Money, Age/Duration, Dosage, Availability) plus
  generic helpers (PhoneticExtractor, TextExtractor,
  CompartmentMembershipExtractor).
- **Range-query strings, no regex**: String prefixes use
  `_lower` fields with `$gte/$lt` ranges (~5 ms) instead of regex
  scans (~15 s). Up to 3000× faster.
- **Hybrid compartment strategy**: High-frequency compartments
  (Patient, Practitioner, Device) are precomputed into
  `_compartments.<Type>` for single-field indexed lookups; other
  compartments (RelatedPerson, Encounter) fall back to dynamic
  translation through the relevant search parameter fields.
- **Sparse output**: Denormalized fields are only written when the
  source exists and yields values — no empty `[]` or `null` fields
  appear in `_search` or `_compartments`.
- **Multi-version FHIR** (R4 / R5) and multiple input sources
  (in-memory dicts, files, folders, MongoDB collections).

---

## Bundled FHIR resources (84 shipped)

```powershell
fhir-mql resources
fhir-mql resources --format json
python -c "from fhir_search_to_mql import ConfigLoader; print(len(ConfigLoader().list_resources()))"
```

### By domain

| Domain | Resources |
|--------|-----------|
| **Identity & directory** | Patient, Person, Practitioner, PractitionerRole, RelatedPerson, Organization, OrganizationAffiliation, Location, Endpoint, HealthcareService, Group |
| **Scheduling & access** | Appointment, Schedule, Slot, Encounter, EpisodeOfCare, Account |
| **Clinical record** | Condition, AllergyIntolerance, Observation, DiagnosticReport, ImagingStudy, Specimen, ClinicalImpression, FamilyMemberHistory, BodyStructure, Composition, DocumentReference |
| **Medications** | Medication, MedicationRequest, MedicationAdministration, MedicationDispense, MedicationStatement, Substance |
| **Orders & care** | ServiceRequest, Procedure, DeviceRequest, RequestOrchestration, CarePlan, CareTeam, Goal, Task, NutritionOrder, NutritionIntake, VisionPrescription |
| **Devices & supplies** | Device, DeviceUsage, DeviceDispense, SupplyRequest, SupplyDelivery, BiologicallyDerivedProduct |
| **Immunizations** | Immunization, ImmunizationRecommendation |
| **Safety & quality** | AdverseEvent, DetectedIssue, Flag, RiskAssessment, Measure, MeasureReport |
| **Financial / RCM** | Coverage, CoverageEligibilityRequest, CoverageEligibilityResponse, Claim, ClaimResponse, ExplanationOfBenefit, Invoice, ChargeItem, ChargeItemDefinition, PaymentNotice, PaymentReconciliation |
| **Payer & enrollment** | EnrollmentRequest, EnrollmentResponse, InsurancePlan |
| **Research & genomics** | ResearchStudy, ResearchSubject, GenomicStudy |
| **Forms & PROs** | Questionnaire, QuestionnaireResponse |
| **Privacy & legal** | Consent, Contract |
| **Interop & audit** | AuditEvent, Provenance, Communication, Basic |

Alphabetical list and search-parameter counts: **[CLI_COMMANDS.md — Resource inventory](CLI_COMMANDS.md#resource-inventory-84-shipped)**.

**Compartments:** many resources precompute `_compartments` for Patient / Practitioner / Device (hybrid fast-path). Resources without FHIR compartments (e.g. Slot, Organization) use direct field or dynamic resolution — see [Compartment search](#compartment-search-hybrid-fast-path).

---

## End-to-end with fhir-gen

```powershell
$URI = "mongodb://localhost:27017/"
$DB  = "fhir_synthetic"

# 1) Generate synthetic FHIR (fhir-data-generation repo)
cd ..\fhir-data-generation
fhir-gen --seed 4000 --db $DB generate-many Patient Encounter Observation Composition DeviceRequest MeasureReport `
  Account Endpoint Provenance ExplanationOfBenefit --count Patient=50 --count Encounter=80 --save
# Or load all 84 types — see fhir-gen CLI_COMMANDS.md scenario 20

# 2) Denormalize + search (this repo)
cd ..\fhir-search-to-mql
fhir-mql indexes --all --uri $URI --db $DB
fhir-mql denormalize --all --uri $URI --db $DB --batch-size 500
fhir-mql search Patient "name=Smith&active=true" --uri $URI --db $DB --limit 10
fhir-mql search DeviceRequest "status=active" --uri $URI --db $DB --limit 10
fhir-mql stats --all --uri $URI --db $DB
```

**21 healthcare** and **11 industrial** workflow scenarios (search side): [CLI_COMMANDS.md](CLI_COMMANDS.md#healthcare-workflow-scenarios).  
**Matching generate commands:** [fhir-gen CLI_COMMANDS.md](../fhir-data-generation/CLI_COMMANDS.md#healthcare-workflow-scenarios).

---

## Combined E2E testing

The **fhir-data-generation** repo owns the cross-repo driver **`scripts/run_cli_e2e.py`**. It runs **fhir-gen** then **fhir-mql** on one MongoDB database per scenario (`fhir_e2e_gen_<id>`). Full reference: **[fhir-data-generation/E2E_COMBINED.md](../fhir-data-generation/E2E_COMBINED.md)**.

```powershell
cd ..\fhir-data-generation
.\.venv\Scripts\Activate.ps1
.venv\Scripts\python.exe scripts\run_cli_e2e.py
.venv\Scripts\python.exe scripts\run_cli_e2e.py --section healthcare
.venv\Scripts\python.exe scripts\run_cli_e2e.py --pipeline-only
.venv\Scripts\python.exe scripts\run_cli_e2e.py --quiet
```

| Phase (per scenario) | What runs |
|----------------------|-----------|
| Prepare DB | Drop `fhir_e2e_gen_<id>` when generating or `--pipeline-only` |
| Generate | `fhir-gen generate-many` (healthcare `hc01`–`hc21`, industrial `ind_*`) |
| Index / denormalize | `fhir-mql indexes` + `fhir-mql denormalize` |
| Search | Planned FHIR queries → MQL → `fhir-mql search`; results under `tests/e2e/results/<id>/` |

**Status output (default):** one line per phase (`Generating FHIR data…`, `Running search tests (N queries)…`). Long steps emit a **still running** heartbeat every ~30s (with search progress `n/total`). Use `--quiet` for pass/fail only.

### E2E package (`tests/e2e/`)

| Module | Role |
|--------|------|
| `cli_scenarios_mql.py` | Pipeline scenarios aligned with fhir-gen `cli_scenarios_gen.py` |
| `resource_search_queries.py` | Per-resource valid search query strings (avoids invalid params like `status=active` on types without `status`) |
| `search_plan.py` | Builds convert + search + compartment steps; `compartment_query()` for compartment-safe queries |
| `search_runner.py` | Executes the plan, writes `search_results.json` |
| `e2e_runner.py` | Subprocess helpers for `fhir-mql` / `fhir-gen` CLIs |
| `test_cli_commands_e2e.py` | Pytest E2E (`-m "e2e and mongodb"`) |

```powershell
# This repo only (after data exists in fhir_e2e_gen_*)
python -m pytest tests/e2e/ -m "e2e and mongodb" --no-cov -q
```

**Denormalization note:** string fields that are FHIR `string` in the spec but sometimes appear as Reference-shaped JSON from generators are coerced where needed (e.g. `ChargeItemDefinition.publisher` → plain string for `_search`).

---

## Library quick start

### 1. Denormalize a FHIR resource

```python
from fhir_search_to_mql import ResourceDenormalizer

denormalizer = ResourceDenormalizer()  # uses bundled configs

patient = {
    "resourceType": "Patient",
    "id": "example",
    "name": [{"family": "Smith", "given": ["John"]}],
    "gender": "male",
    "birthDate": "1980-05-15",
}

result = denormalizer.denormalize(patient)
# result["_search"]["familyName_lower"] == ["smith"]
# result["_compartments"]["Patient"]   == ["example"]
```

The original dict is never mutated. Fields with no matching source
in the resource are silently omitted from `_search` (sparse output).

### 2. Convert a FHIR search query to MQL

```python
from fhir_search_to_mql import FHIRSearchConverter

converter = FHIRSearchConverter()  # uses bundled configs

mql = converter.convert(
    resource_type="Patient",
    query_string="name=Smith&gender=male&birthdate=ge1980-01-01",
)

from pymongo import MongoClient
patients = list(MongoClient()["fhir_synthetic"].Patient.find(mql))
```

All FHIR R5 search modifiers and prefixes are supported:
`:exact`, `:contains`, `:not`, `:missing`, `:text`, `:of-type`,
typed-resource modifiers (`:Patient`, `:Device`, …) and date prefixes
`eq`, `ne`, `gt`, `lt`, `ge`, `le`, `sa`, `eb`, `ap`.

### 3. Bulk operate on a MongoDB collection

```python
from fhir_search_to_mql import MongoDBHandler, ResourceDenormalizer
from pymongo import MongoClient

denormalizer = ResourceDenormalizer()
db = MongoClient()["fhir_synthetic"]

stats = MongoDBHandler.update_search_fields(
    collection=db.Observation,
    processor=denormalizer.denormalize,
    batch_size=500,
)
print(stats)
# {
#   'processed': 50000, 'updated': 50000, 'failed': 0,
#   'field_failures': 0, 'documents_with_field_failures': 0,
# }
```

`update_search_fields`, `batch_process(update_in_place=True)`, and
`remove_search_fields` all persist (or clear) **both** `_search` and
`_compartments` together. The compartment fast-path stays consistent
across re-denormalization runs without extra calls.

The stats dict distinguishes:
- `failed` — entire documents that crashed
- `field_failures` / `documents_with_field_failures` — partial
  successes where one denorm rule was skipped (e.g. extractor typo).
  The `Completed:` log line appends this count only when non-zero.

### 4. Convert a compartment-scoped query

```python
# GET [base]/Patient/p-123/Observation?code=8480-6
mql = converter.convert_with_compartment(
    compartment_type="Patient",
    compartment_id="p-123",
    resource_type="Observation",
    query_string="code=8480-6&date=ge2024-01-01",
)
# {"$and": [{"_compartments.Patient": "p-123"}, ...code+date clauses...]}
```

Compartments that are precomputed (Patient, Practitioner, Device on
supported resources) resolve as a single-field equality on the
indexed `_compartments.*` field. Other compartment types fall back
to dynamic resolution through the resource's reference search params.

### 5. Layer custom configs on top of bundled configs

```python
from fhir_search_to_mql import FHIRSearchConverter, ConfigLoader

bundled = ConfigLoader().config_dir  # path to bundled YAMLs
converter = FHIRSearchConverter(
    config_dir=[bundled, "/opt/myapp/fhir-config"]
)

# Encounter.yaml from /opt/myapp/fhir-config is now available
mql = converter.convert("Encounter", "status=finished&date=ge2024-01-01")
```

---

## Command-line interface (`fhir-mql`)

```text
usage: fhir-mql [-h] [--version] COMMAND ...

positional arguments:
  COMMAND
    resources    List configured resource types and their feature counts.
    convert      Convert a FHIR search query string to MQL (no DB access).
    search       Convert a FHIR search query and execute it against MongoDB.
    denormalize  Recompute and persist _search and _compartments fields for
                 one or more resources.
    indexes      Create the indexes declared in each resource's YAML config.
    reset        Clear _search and _compartments fields for one or more
                 resources (keeps the source FHIR documents intact).
    stats        Show document counts and denormalization coverage per
                 resource.
```

### Common flags

| Flag                                | Purpose |
|-------------------------------------|---------|
| `--config-dir DIR`                  | Override or layer config directories. Repeat to layer; later wins. Defaults to bundled package configs. |
| `--compartment-definitions-dir DIR` | Custom CompartmentDefinition JSON dir. Defaults to bundled. |
| `--format {json,table}`             | Output shape. `json` is machine-parseable on stdout; `table` is human-readable. |
| `--uri URI`                         | MongoDB connection URI. Falls back to `$MONGODB_URI` then `mongodb://localhost:27017/`. |
| `--db NAME`                         | MongoDB database. Falls back to `$MONGODB_DB` then `fhir_synthetic`. |
| `--collection-prefix PREFIX`        | Prepended to the resource type name to derive the collection (e.g. `fhir_Patient`). |
| `--dry-run`                         | (Bulk subcommands) Print the plan without touching MongoDB. |
| `--limit N`                         | (search/denormalize) Cap the number of docs processed. |

### Subcommands

#### `resources` — list loaded resources

```powershell
fhir-mql resources
fhir-mql resources --format json
```

#### `convert` — pure FHIR → MQL conversion (no DB needed)

```powershell
# Basic token + string + date
fhir-mql convert Patient "name=Smith&gender=male&birthdate=ge1980-01-01"

# Date prefixes
fhir-mql convert Slot "start=ge2024-07-01&start=le2024-07-31&status=free"

# Reference
fhir-mql convert Slot "schedule=Schedule/sched-1&status=free"

# CodeableReference (concept arm vs reference arm)
fhir-mql convert Schedule "service-type=11429006"
fhir-mql convert Schedule "service-type-reference=HealthcareService/hs-1"

# Compartment-scoped conversion
fhir-mql convert Observation "code=8480-6" `
    --compartment-type Patient --compartment-id patient-123

# Modifiers
fhir-mql convert Patient "name:exact=Smith"
fhir-mql convert Patient "identifier:missing=true"
fhir-mql convert Appointment "status:not=cancelled"
```

#### `search` — convert AND execute against MongoDB

```powershell
$env:MONGODB_URI = "mongodb://localhost:27017/"
$env:MONGODB_DB  = "fhir_synthetic"

fhir-mql search Patient "name=Smith&gender=male" --limit 10
fhir-mql search Observation "code=http://loinc.org|8480-6" `
    --compartment-type Patient --compartment-id patient-123 `
    --format json
fhir-mql search Slot "status=free&schedule=sched-1&start=ge2024-07-01"
fhir-mql search Schedule "active=true&specialty=394814009"
```

- `--explain` prints the generated MQL but skips execution.
- `--format json` emits one JSON object on stdout containing the
  query string, generated MQL, result count, and matched documents.

#### `denormalize` — bulk re-denormalization

Bulk subcommands (`denormalize`, `indexes`, `reset`, `stats`) expand each
named resource to transitive **dependencies** from the same graph as
fhir-gen (`fhir_search_to_mql/resolvers/dependency.py`), processing anchors
before dependents. Pass `--no-with-deps` to target only the types on the
command line.

```powershell
# Single resource
fhir-mql denormalize Patient

# Dependent + anchors (e.g. MeasureReport → Measure, Patient, …)
fhir-mql denormalize MeasureReport

# Multiple resources
fhir-mql denormalize Patient Observation Appointment

# All 84 bundled resources
fhir-mql denormalize --all --batch-size 500

# Preview without writing
fhir-mql denormalize --all --dry-run

# Limit to first 100 docs per resource (staging / smoke test)
fhir-mql denormalize --all --limit 100

# Custom batch size
fhir-mql denormalize Observation --batch-size 1000
```

Writes both `_search` and `_compartments` on every document processed.

#### `indexes` — create indexes declared in each YAML

```powershell
fhir-mql indexes --all
fhir-mql indexes Patient Observation
fhir-mql indexes Slot Schedule --dry-run
```

#### `reset` — clear `_search` and `_compartments`

```powershell
fhir-mql reset Patient
fhir-mql reset Patient Observation Appointment
fhir-mql reset --all
```

Keeps source FHIR documents intact; only unsets the two denormalization
buckets.

#### `stats` — coverage report

```powershell
fhir-mql stats Patient
fhir-mql stats Patient Observation Appointment
fhir-mql stats --all
fhir-mql stats --all --format json
```

Reports total document count and how many have `_search` populated —
invaluable when checking whether denormalization is fully applied.

### Multi-resource invocations

Every bulk subcommand takes one or more positional resource types
**or** the `--all` switch:

```powershell
fhir-mql denormalize Patient Observation Appointment Organization Location \
    Practitioner PractitionerRole Device Group Schedule Slot

fhir-mql indexes --all
fhir-mql stats --all --format json
```

Unknown resource types fail fast before any DB call:

```powershell
fhir-mql denormalize Patient Bogus
# Error: No configuration found for: Bogus.
# (error lists all 84 configured resource type names)
```

### Environment variables

| Variable      | Purpose                                               | Default                      |
|---------------|-------------------------------------------------------|------------------------------|
| `MONGODB_URI` | Connection string used when `--uri` is omitted.       | `mongodb://localhost:27017/` |
| `MONGODB_DB`  | Database used when `--db` is omitted.                 | `fhir_synthetic`             |

CLI flags always take precedence over environment variables.

### CLI command reference (full scenarios)

**[CLI_COMMANDS.md](CLI_COMMANDS.md)** is the canonical command cookbook. It includes:

- Every `fhir-mql` subcommand with flags
- Convert/search examples for all **84** bundled resources
- Compartment-scoped queries (Patient, Practitioner, Device, Encounter)
- Healthcare workflows (scheduling, vitals, problem list, cohorts, reindex)
- Multi-database presets (`fhir_schedule_appointment_hybrid`, `fhir_synthetic`)
- Python one-liners, schema tooling, and pytest slices

---

## Denormalization and search parameters

### What denormalization does

FHIR resources are stored as documents. Searchable values are extracted into
`_search` (and compartment membership into `_compartments`) so MongoDB can use
B-tree indexes instead of scanning nested JSON.

**Original:**

```json
{
  "resourceType": "Patient",
  "name": [{"family": "Smith", "given": ["John"]}]
}
```

**After `denormalize`:**

```json
{
  "resourceType": "Patient",
  "name": [{"family": "Smith", "given": ["John"]}],
  "_search": {
    "familyName_lower": ["smith"],
    "givenNames_lower": ["john"]
  },
  "_compartments": {
    "Patient": ["example-id"]
  }
}
```

Only fields declared in the resource YAML are written. Missing source data
produces **no** `_search` key (sparse output — no empty `[]` or `null`).

### Python API — files and folders

```python
from fhir_search_to_mql import ResourceDenormalizer

denormalizer = ResourceDenormalizer()  # bundled configs by default

denormalizer.denormalize(patient_dict)
denormalizer.denormalize_from_file("patient.json")
denormalizer.denormalize_from_folder(
    "fhir_data/",
    resource_type="Patient",  # optional filter
    pattern="*.json",
    recursive=True,
)
```

Bulk persistence uses `MongoDBHandler.update_search_fields()` (writes both
`_search` and `_compartments`). See [Library quick start](#library-quick-start).

### Search parameter types

| Type | Example | MQL shape (typical) |
|------|---------|---------------------|
| **string** | `name=Smith` | Range on `_search.*_lower` (prefix, case-insensitive) |
| **string** `:exact` | `name:exact=Smith` | Equality on `_search.*` |
| **string** `:contains` | `name:contains=mit` | Token / text fields |
| **token** | `gender=male` | `_search.*_codes` or root scalar (`status`, `active`) |
| **token** | `code=http://loinc.org\|8480-6` | `system\|code` composite field |
| **reference** | `patient=p1` | `_search.patientId` or `$or` across fields |
| **reference** `:Patient` | `subject:Patient=p1` | Type-filtered id bucket |
| **date** | `birthdate=ge1980-01-01` | `$gte` / `$lte` on date or Period |
| **quantity** | `length=1` | Quantity field or `_search` quantity |
| **composite** | *(resource-specific)* | Multiple sub-fields AND-ed |

### Modifiers and prefixes

**String:** `:exact`, `:contains`, `:missing`, `:text` (phonetic where configured)

**Token:** `:not`, `:text`, `:in`, `:not-in`, `:missing`

**Reference:** `:identifier`, `:missing`, type modifiers (`:Patient`, `:Practitioner`, …)

**Date / number / quantity prefixes:** `eq` (default), `ne`, `gt`, `ge`, `lt`, `le`, `sa`, `eb`, `ap`

```python
from fhir_search_to_mql import FHIRSearchConverter

converter = FHIRSearchConverter()
converter.convert("Patient", "name=Smith&gender=male&birthdate=ge1980-01-01")
converter.convert("Observation", "code=http://loinc.org|8480-6&status=final")
converter.convert("Encounter", "status=in-progress&date-start=ge2024-07-01")
```

Configuration format: [Configuration](#configuration). Per-resource YAML:
`src/fhir_search_to_mql/configs/`.

---

## Performance

### No-regex policy for string prefix search

Avoid regex on large collections — use denormalized lowercase fields with
**range queries** (`$gte` / `$lt` with `\uffff` upper bound):

| Approach | Typical query time |
|----------|-------------------|
| Regex on raw JSON | ~15,000 ms |
| Collation | ~200 ms |
| Denormalized range (`_search.*_lower`) | **~5 ms** |

### Operational checklist

1. Run `fhir-mql indexes <Resource>` (or `--all`) after deploy.
2. Run `fhir-mql denormalize` whenever FHIR documents change.
3. Use `fhir-mql stats` to confirm `_search` coverage before go-live.
4. Prefer compartment fast-path (`_compartments.*`) for Patient/Practitioner/Device scoped lists.
5. Use `collection.find(mql).explain("executionStats")` to validate index use.

---

## FHIR schema tooling

Local FHIR R5/R6 schema and HL7 search-parameter packages live under
`schema/` (not shipped inside the wheel). Python helpers are in
`src/fhir_search_to_mql/schema/`.

```powershell
# Regenerate indexes (resources, search params, shipped YAML cross-ref)
python -m fhir_search_to_mql.schema.build_indexes

# Inspect one resource before authoring YAML
python -m fhir_search_to_mql.schema.resource_spec Encounter
python -m fhir_search_to_mql.schema.resource_spec Condition
```

Optional: set `FHIR_SCHEMA_ROOT` if the schema tree is relocated. See
[schema/README.md](schema/README.md) and
[.cursor/skills/fhir-resource-config/](.cursor/skills/fhir-resource-config/)
for adding new resources.

---

## Using the library as a plugin in another project

### Pattern A — library dependency (recommended)

```python
# my_app/services/search.py
from fhir_search_to_mql import (
    FHIRSearchConverter,
    ResourceDenormalizer,
    MongoDBHandler,
)

class FHIRSearchService:
    def __init__(self, db, *, custom_config_dir=None):
        config_dir = [custom_config_dir] if custom_config_dir else None
        self.converter    = FHIRSearchConverter(config_dir=config_dir)
        self.denormalizer = ResourceDenormalizer(config_dir=config_dir)
        self.db = db

    def find(self, resource: str, query: str, limit: int = 50):
        mql = self.converter.convert(resource, query)
        return list(self.db[resource].find(mql).limit(limit))

    def find_by_compartment(self, compartment_type, compartment_id,
                             resource, query=""):
        mql = self.converter.convert_with_compartment(
            compartment_type, compartment_id, resource, query
        )
        return list(self.db[resource].find(mql))

    def reindex(self, resource: str):
        return MongoDBHandler.update_search_fields(
            collection=self.db[resource],
            processor=self.denormalizer.denormalize,
        )
```

### Pattern B — CLI subprocess (zero coupling)

```bash
# scripts/reindex.sh
fhir-mql denormalize --all --batch-size 500
fhir-mql indexes --all
```

Exit codes:

| Code | Meaning |
|-----:|---------|
| 0    | Success |
| 2    | Usage error (bad CLI args) |
| 3    | Configuration error |
| 4    | Runtime error (bubbled-up library exception) |

JSON output → **stdout**; progress and errors → **stderr**.

### Pattern C — layered configs (advanced)

Ship your own YAMLs inside a package and layer them on top of the
bundled set. Only your overriding resources are replaced; the other
Other bundled resources remain available when you layer configs:

```python
from fhir_search_to_mql import ConfigLoader, FHIRSearchConverter

bundled = ConfigLoader().config_dir
converter = FHIRSearchConverter(config_dir=[bundled, "my_configs/"])
```

---

## Compartment search (hybrid fast-path)

The library uses a hybrid strategy to balance query performance with
operational simplicity:

### How it works

1. **Precompute** at denormalization time. The
   `CompartmentMembershipExtractor` runs inside
   `ResourceDenormalizer.denormalize()` and writes
   `_compartments.<Type>` arrays containing the IDs of matching
   compartment resources. For example, a Schedule with
   `actor: [Patient/p-1, Practitioner/pr-1, Device/dev-1]` produces:

   ```json
   "_compartments": {
     "Patient":      ["p-1"],
     "Practitioner": ["pr-1"],
     "Device":       ["dev-1"]
   }
   ```

2. **Fast-path query** for precomputed compartments:
   `GET [base]/Patient/p-1/Schedule` → `{"_compartments.Patient": "p-1"}`.
   A single indexed equality — no joins, no pipeline.

3. **Dynamic fallback** for compartments not precomputed (RelatedPerson,
   Encounter). The resolver translates the compartment query into the
   resource's regular reference search parameter (e.g.
   `{"_search.actorIds": "rp-1"}` for Schedule's RelatedPerson compartment).

### Precomputed compartments by resource

| Resource         | Patient | Practitioner | Device | Dynamic fallback |
|------------------|:-------:|:------------:|:------:|-----------------|
| Patient          | ✓ (self)| ✓            | —      | —               |
| Observation      | ✓       | ✓            | ✓      | —               |
| Appointment      | ✓       | ✓            | ✓      | —               |
| Practitioner     | —       | ✓ (self)     | —      | —               |
| PractitionerRole | —       | ✓            | —      | —               |
| Device           | —       | —            | ✓ (self)| —              |
| Group            | ✓       | ✓            | ✓      | —               |
| Condition        | ✓       | ✓            | ✓      | Encounter       |
| Encounter        | ✓       | ✓            | ✓      | Encounter (self + partOf) |
| Schedule         | ✓       | ✓            | ✓      | RelatedPerson   |
| Slot             | —       | —            | —      | *(no compartments)* |

### Python API

```python
# Precomputed fast-path (Patient compartment)
mql = converter.convert_with_compartment("Patient", "p-1", "Observation")
# → {"_compartments.Patient": "p-1"}

# With additional query filters
mql = converter.convert_with_compartment(
    "Patient", "p-1", "Observation", "code=8480-6&date=ge2024-01-01"
)
# → {"$and": [{"_compartments.Patient": "p-1"}, <code>, <date>]}

# Precomputed — Practitioner
mql = converter.convert_with_compartment("Practitioner", "pr-1", "Schedule")
# → {"_compartments.Practitioner": "pr-1"}

# Dynamic fallback — RelatedPerson (Schedule)
mql = converter.convert_with_compartment("RelatedPerson", "rp-1", "Schedule")
# → {"_search.actorIds": "rp-1"}

# Raises — Slot has no FHIR R5 compartments
converter.convert_with_compartment("Patient", "p-1", "Slot")
# InvalidCompartmentQuery: Resource type 'Slot' is not in compartment 'Patient'
```

### CLI

```powershell
fhir-mql convert Observation "code=8480-6" `
    --compartment-type Patient --compartment-id patient-123

fhir-mql search Schedule "" `
    --compartment-type Practitioner --compartment-id prac-1
```

---

## Configuration

Each resource is described by a YAML file. The bundled set lives at
`src/fhir_search_to_mql/configs/`. Minimal example showing all major
sections:

```yaml
resource: Slot
fhir_version: R5

search_parameters:
  status:
    type: token
    description: "busy | free | busy-unavailable | busy-tentative | entered-in-error"
    fields:
      - field: "status"
        tokenType: code       # queried directly from FHIR root — no _search needed

  start:
    type: date
    description: "Slot.start (instant) queried directly"
    fields:
      - field: "start"
        type: date

  schedule:
    type: reference
    fields:
      - field: "_search.scheduleId"
        referenceType: id
      - field: "_search.scheduleType"
        referenceType: type
      - field: "schedule.reference"
        referenceType: full

  service-type:
    type: token
    fields:
      - field: "_search.serviceType_systemCode"
        tokenType: systemCode
      - field: "_search.serviceType_codes"
        tokenType: code

denormalization:
  # Source field absent → the rule is skipped; no empty field is written.
  schedule:
    source: schedule
    target: _search
    extractor: ReferenceExtractor
    field_mappings:
      - source_path: schedule.reference
        target_field: scheduleId
        datatype: string
        extractType: id
      - source_path: schedule.reference
        target_field: scheduleType
        datatype: string
        extractType: type

  serviceType_concept:
    source: $resource          # extractor receives the full FHIR document
    target: _search
    extractor: CodeableConceptExtractor
    field_mappings:
      - source_path: serviceType[*].concept.coding[*].code
        target_field: serviceType_codes
        datatype: array[string]
      - source_path: serviceType[*].concept.coding[*]
        target_field: serviceType_systemCode
        datatype: array[string]

  # No compartment_membership rule — Slot has no FHIR R5 compartments

indexes:
  - fields:
      - "status": 1
    options:
      name: idx_status
  - fields:
      - "start": 1
    options:
      name: idx_start
  - fields:
      - "status": 1
      - "start": 1
    options:
      name: idx_status_start_compound
  - fields:
      - "_search.scheduleId": 1
    options:
      name: idx_schedule_id
      sparse: true
```

### Bundled vs. custom configs

| Use case | How |
|----------|-----|
| Library defaults, plug-and-play | `ResourceDenormalizer()` / `FHIRSearchConverter()` (no args). |
| Add resources beyond the 84 bundled | `config_dir=[bundled_dir, my_dir]` — your dir wins on conflicts. |
| Replace a bundled resource entirely | Same as above; your YAML for that resource takes precedence. |
| Use only your own configs | `config_dir="my_dir"` (single string). |
| Single-resource config file | `config_path="path/to/Patient.yaml"`. |

### Extractor reference

| Extractor | FHIR datatype(s) | Notes |
|-----------|-----------------|-------|
| `HumanNameExtractor` | HumanName | Family, given, prefix, suffix |
| `CodeableConceptExtractor` | CodeableConcept, CodeableConcept[] | Splits into `_systemCode` and `_codes` arrays |
| `IdentifierExtractor` | Identifier[] | `system\|value` pairs + bare values |
| `ReferenceExtractor` | Reference, Reference[], CodeableReference | Extracts bare id, resource type, or full ref |
| `ContactPointExtractor` | ContactPoint[] | Phone, email, fax with system filter |
| `AddressExtractor` | Address[] | Line, city, state, postal, country |
| `PeriodExtractor` | Period | start/end as BSON ISODate for range queries |
| `QuantityExtractor` | Quantity | value + system\|code |
| `TimingExtractor` | Timing | event dates |
| `RangeExtractor` | Range | low/high bounds |
| `RatioExtractor` | Ratio | numerator/denominator |
| `RatioRangeExtractor` | RatioRange | low/high ratio bounds |
| `CodingExtractor` | Coding | system\|code pair |
| `ExtensionExtractor` | Extension[] | URL-keyed extensions |
| `MoneyExtractor` | Money | value + currency |
| `AgeDurationExtractor` | Age, Duration | value + unit |
| `DosageExtractor` | Dosage | timing + route + method |
| `AvailabilityExtractor` | Availability | FHIR R5 availability |
| `PhoneticExtractor` | HumanName | Soundex for `:text` phonetic search |
| `TextExtractor` | any | Free-text concat / lowercase for `:text` |
| `CompartmentMembershipExtractor` | full resource | Precomputes `_compartments.<Type>` arrays |

---

## Architecture

```
                       ┌──────────────────────────────────┐
                       │            CLI: fhir-mql         │
                       │  resources / convert / search    │
                       │  denormalize / indexes / reset   │
                       │  stats                           │
                       └──────────────┬───────────────────┘
                                      │
                                      ▼
   ┌─────────────────────────────────────────────────────────┐
   │                Public Python API                         │
   │  ConfigLoader · ResourceDenormalizer ·                  │
   │  FHIRSearchConverter · MongoDBHandler · QueryParser     │
   └────┬───────────────────┬────────────────────────────────┘
        │                   │
        ▼                   ▼
┌────────────────┐   ┌──────────────────────────────────┐
│ Denormalizer   │   │ Converter                        │
│ (21 Extractors)│   │  Parser → Converters → MQL       │
│                │   │                                  │
│ • HumanName    │   │  • String / Token / Date /       │
│ • CodeableConcept   │    Reference / Number / Quantity │
│ • Identifier   │   │  • Composite                    │
│ • Reference    │   │  • Special (_id, _lastUpdated…)  │
│ • Period       │   │  • Compartment resolver          │
│ • Address      │   │    (precomputed fast-path +      │
│ • …            │   │     dynamic fallback)            │
│ • CompartmentMembership│                              │
└──┬─────────────┘   └──────────────────────────────────┘
   │
   ▼
┌────────────────────────────────┐
│  MongoDB documents             │
│  ─ _search.<denormalized>      │  ← sparse; no empty [] or None
│  ─ _compartments.<Type>        │  ← only populated types present
└────────────────────────────────┘
```

---

## Build and distribution

### Repository layout (relevant parts)

```
fhir-search-to-mql/
├── src/fhir_search_to_mql/
│   ├── __init__.py
│   ├── cli.py                               # `fhir-mql` console script
│   ├── configs/                             # bundled FHIR R5 YAMLs (84 resources)
│   │   ├── Patient.yaml … VisionPrescription.yaml
│   │   └── (one YAML per shipped resource type)
│   ├── schema/                              # build_indexes, resource_spec (see schema/)
│   ├── core/
│   │   └── config_loader.py                # importlib.resources default
│   ├── compartments/
│   │   └── definitions/*.json              # FHIR R5 CompartmentDefinitions
│   ├── converters/                         # FHIR-search → MQL
│   ├── denormalizer/
│   │   ├── extractors/                     # 21 generic extractors
│   │   ├── mongodb_handler.py              # _search + _compartments sync
│   │   └── resource_denormalizer.py
│   ├── parser/                             # FHIR query parser
│   └── fhir_search_converter.py
├── tests/
│   ├── e2e/                               # Combined E2E with fhir-gen (see E2E_COMBINED.md)
│   │   ├── cli_scenarios_mql.py
│   │   ├── resource_search_queries.py
│   │   ├── search_plan.py / search_runner.py
│   │   └── results/                       # search_results.json per scenario (gitignored)
│   ├── unit/
│   │   ├── test_cli.py
│   │   └── ...
│   └── integration/
│       ├── test_cli_integration.py
│       ├── test_config_audit_regressions.py   # cross-resource quality gates
│       ├── test_patient_comprehensive.py
│       ├── test_observation_comprehensive.py
│       ├── test_appointment_comprehensive.py
│       ├── test_organization_comprehensive.py
│       ├── test_location_comprehensive.py
│       ├── test_practitioner_comprehensive.py
│       ├── test_practitionerrole_comprehensive.py
│       ├── test_device_comprehensive.py
│       ├── test_group_comprehensive.py
│       ├── test_schedule_comprehensive.py
│       ├── test_slot_comprehensive.py
│       ├── test_condition_comprehensive.py
│       └── test_encounter_comprehensive.py
├── schema/                                  # FHIR JSON + generated indexes
├── CLI_COMMANDS.md                          # CLI & workflow command cookbook
├── E2E_COMMANDS.md                          # Pytest E2E for this repo
├── E2E_COMBINED.md                          # Pointer to fhir-gen combined runner
├── pyproject.toml                           # [project.scripts] fhir-mql
└── README.md
```

### Build a wheel + sdist

```powershell
pip install build
python -m build
# → dist/fhir_search_to_mql-1.2.0-py3-none-any.whl
# → dist/fhir_search_to_mql-1.2.0.tar.gz
```

Verify the wheel is self-contained (all 84 YAML configs bundled):

```powershell
pip install dist/fhir_search_to_mql-1.2.0-py3-none-any.whl
python -c "from fhir_search_to_mql import ConfigLoader; print(len(ConfigLoader().list_resources()))"
# 84

fhir-mql --version
# fhir-mql 1.2.0
```

### Code quality

```powershell
black src/ tests/
flake8 src/ tests/
mypy src/
```

---

## Testing

### Run everything

```powershell
.\.venv\Scripts\Activate.ps1   # PowerShell
# source .venv/bin/activate    # bash

python -m pytest tests/ -q
```

### Useful slices

```powershell
# Pure unit tests (no MongoDB required, ~15 s)
python -m pytest tests/unit/ -q

# All integration tests except the CLI live-DB suite
python -m pytest --ignore=tests/integration/test_cli_integration.py -q

# All unit tests verbosely
python -m pytest tests/unit/ -v

# CLI unit tests only
python -m pytest tests/unit/test_cli.py -v

# CLI integration tests (require MongoDB at localhost:27017)
python -m pytest tests/integration/test_cli_integration.py -v -m mongodb

# Cross-resource quality-gate audit (all 84 shipped resources)
python -m pytest tests/integration/test_config_audit_regressions.py -v

# Combined E2E with fhir-gen (MongoDB + data in fhir_e2e_gen_*)
python -m pytest tests/e2e/ -m "e2e and mongodb" --no-cov -q

# Per-resource comprehensive suites (examples)
python -m pytest tests/integration/test_patient_comprehensive.py -v
python -m pytest tests/integration/test_composition_comprehensive.py -v
python -m pytest tests/integration/test_device_request_comprehensive.py -v
python -m pytest tests/integration/test_measure_report_comprehensive.py -v
python -m pytest tests/integration/test_observation_comprehensive.py -v
python -m pytest tests/integration/test_appointment_comprehensive.py -v
python -m pytest tests/integration/test_organization_comprehensive.py -v
python -m pytest tests/integration/test_location_comprehensive.py -v
python -m pytest tests/integration/test_practitioner_comprehensive.py -v
python -m pytest tests/integration/test_practitionerrole_comprehensive.py -v
python -m pytest tests/integration/test_device_comprehensive.py -v
python -m pytest tests/integration/test_group_comprehensive.py -v
python -m pytest tests/integration/test_schedule_comprehensive.py -v
python -m pytest tests/integration/test_slot_comprehensive.py -v
python -m pytest tests/integration/test_condition_comprehensive.py -v
python -m pytest tests/integration/test_encounter_comprehensive.py -v

# Run only MongoDB E2E tests (tagged @pytest.mark.mongodb)
python -m pytest -m mongodb -v

# Coverage report
python -m pytest tests/ --cov=fhir_search_to_mql --cov-report=term-missing

# HTML coverage report
python -m pytest tests/ --cov=fhir_search_to_mql --cov-report=html
# open htmlcov/index.html

# Run with benchmark output
python -m pytest tests/ --benchmark-only -v
```

### Current numbers

| Metric | Value |
|--------|-------|
| Tests passing | **~3,000+** (varies by MongoDB availability) |
| Skipped (MongoDB not running) | 8 |
| Failing | 0 |
| Overall coverage | 85 %+ |
| Shipped resource configs | **84** |
| Per-resource comprehensive suites | `tests/integration/test_*_comprehensive.py` |
| Cross-config audit | `tests/integration/test_config_audit_regressions.py` |

MongoDB integration tests pass against a stock
`mongodb://localhost:27017/` deployment (Docker or native).

---

## Troubleshooting

### `pip install fhir-search-to-mql` fails with "No matching distribution found"

Expected — this package is not on PyPI. Use editable install from
your local checkout or a wheel:

```powershell
# editable from this repo
pip install -e .

# or a wheel built elsewhere
pip install path\to\fhir_search_to_mql-1.2.0-py3-none-any.whl

# or directly from a Git URL
pip install "git+https://your-git-host/<org>/fhir-search-to-mql.git@main"
```

### `fhir-mql` not found after install

Almost always means the venv isn't active:

```powershell
.\.venv\Scripts\Activate.ps1   # PowerShell
# source .venv/bin/activate    # bash
fhir-mql --version
```

If active and still missing:

```powershell
where.exe python
where.exe fhir-mql
pip show fhir-search-to-mql | Select-String -Pattern '^(Location|Editable)'
```

The `Location:` line should point inside `.venv\Lib\site-packages`.

### `Activate.ps1 cannot be loaded because running scripts is disabled`

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

### `ConnectionError` / `ServerSelectionTimeoutError` on bulk subcommands

Confirm MongoDB is running and reachable:

```powershell
python -c "from pymongo import MongoClient; print(MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000).server_info()['version'])"
```

Then either pass `--uri mongodb://host:port/` or set `$env:MONGODB_URI`.

### `Invalid date format 'ge<date>'` warning in the logs

The FHIR date prefix was not stripped. This usually means a custom
resource config has a date search parameter whose name is not in the
parser's allowlist. Ensure the YAML specifies `type: date` for the
parameter, or add the parameter name to the `_infer_parameter_type`
date allowlist in `parameter_parser.py`. Common names already in the
allowlist: `birthdate`, `date`, `date-start`, `end-date`, `period`,
`_lastUpdated`, `start`, `death-date`, `expiration-date`,
`manufacture-date`, etc.

### Configs not found after editing in place

`ResourceDenormalizer()` and friends pick up bundled configs via
`importlib.resources`. If you edited a YAML inside
`src/fhir_search_to_mql/configs/` and changes don't take, you likely
installed via wheel rather than `-e`. Reinstall editable:

```powershell
pip install -e .
```

### "0 failed" but you saw warnings on stderr

That is the per-**document** failure count. The per-**field** counters
(`field_failures`, `documents_with_field_failures`) live alongside it
in the stats dict and are appended to the `Completed:` log line when
non-zero. A field failure means one denorm rule was skipped on a
document (e.g. an unknown extractor name in the YAML) without
aborting the whole document.

---

## Related projects

| Project | Role |
|---------|------|
| [fhir-data-generation](../fhir-data-generation/) | `fhir-gen` — synthetic FHIR R5 data for the same 84 types |
| [fhir-data-generation E2E_COMBINED.md](../fhir-data-generation/E2E_COMBINED.md) | `run_cli_e2e.py` — gen → mql on one DB per scenario |
| [FHIR-GEN](../FHIR-GEN/) | Parent monorepo / related generators (if applicable in your layout) |

---

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.

## Citation

```bibtex
@software{fhir_search_to_mql,
  title  = {FHIR Search to MQL Conversion Library},
  author = {FHIR-GEN Team},
  year   = {2026},
  url    = {https://github.com/fhir-gen/fhir-search-to-mql}
}
```
