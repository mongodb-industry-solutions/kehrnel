# CLI & command reference

Practical commands for **fhir-mql**, Python APIs, schema tooling, and common
healthcare search workflows. All examples assume an activated venv and
`pip install -e ".[dev]"` from the repo root.

**Shell:** PowerShell on Windows; use `export VAR=value` instead of `$env:VAR` on bash.

---

## Table of contents

1. [Environment & connection](#environment--connection)
2. [Install & verify](#install--verify)
3. [Resource inventory](#resource-inventory)
4. [Convert only (no MongoDB)](#convert-only-no-mongodb)
5. [Search (convert + execute)](#search-convert--execute)
6. [Bulk operations](#bulk-operations)
7. [Compartment-scoped queries](#compartment-scoped-queries)
8. [Healthcare workflow scenarios](#healthcare-workflow-scenarios)
9. [Multi-database / project presets](#multi-database--project-presets)
10. [Python API one-liners](#python-api-one-liners)
11. [FHIR schema tooling](#fhir-schema-tooling)
12. [Testing commands](#testing-commands)
13. [Troubleshooting commands](#troubleshooting-commands)

---

## Environment & connection

```powershell
# Defaults used when flags are omitted
$env:MONGODB_URI = "mongodb://localhost:27017/"
$env:MONGODB_DB  = "fhir_synthetic"

# Shorthand for repeated commands
$URI = "mongodb://localhost:27017/"
$DB  = "fhir_synthetic"
```

| Flag / variable | Purpose | Default |
|-----------------|---------|---------|
| `--uri` / `MONGODB_URI` | MongoDB connection string | `mongodb://localhost:27017/` |
| `--db` / `MONGODB_DB` | Database name | `fhir_synthetic` |
| `--config-dir DIR` | Layer YAML configs (repeatable) | Bundled `src/fhir_search_to_mql/configs/` |
| `--compartment-definitions-dir DIR` | CompartmentDefinition JSON | Bundled definitions |
| `--collection-prefix PREFIX` | Collection name = `PREFIX` + resource type | *(none)* |
| `--format {json,table}` | Output shape | `table` |
| `--dry-run` | Plan only (bulk subcommands) | off |
| `--limit N` | Cap documents (search / denormalize) | unlimited |
| `--batch-size N` | Denormalize batch size | 500 |

**Docker MongoDB (local dev):**

```powershell
docker run -d --name mongo-fhir -p 27017:27017 mongo:7
python -c "from pymongo import MongoClient; print(MongoClient('mongodb://localhost:27017/').server_info()['version'])"
```

---

## Install & verify

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -e ".[dev,docs]"

fhir-mql --version
fhir-mql resources
python -c "from fhir_search_to_mql import ConfigLoader; print(sorted(ConfigLoader().list_resources()))"
```

---

## Resource inventory

```powershell
fhir-mql resources
fhir-mql resources --format json
```

**Shipped resources (13):** Appointment, Condition, Device, Encounter, Group,
Location, Observation, Organization, Patient, Practitioner, PractitionerRole,
Schedule, Slot.

---

## Convert only (no MongoDB)

Pure FHIR search → MQL JSON. Use for query review, CI, or app integration without DB.

### Common parameters

```powershell
# _id / _lastUpdated (all resources)
fhir-mql convert Patient "_id=p1"
fhir-mql convert Observation "_lastUpdated=ge2024-01-01"

# Modifiers
fhir-mql convert Patient "name:exact=Smith"
fhir-mql convert Patient "identifier:missing=false"
fhir-mql convert Appointment "status:not=cancelled"
```

### Patient — identity & demographics

```powershell
fhir-mql convert Patient "name=Smith&gender=male"
fhir-mql convert Patient "birthdate=ge1980-01-01&birthdate=le1990-12-31"
fhir-mql convert Patient "identifier=http://hospital.org/mrn|MRN-1001"
fhir-mql convert Patient "active=true&address-city=Springfield"
fhir-mql convert Patient "telecom=555-0100"
fhir-mql convert Patient "deceased=false"
fhir-mql convert Patient "language=en-US"
fhir-mql convert Patient "organization=org-1"
```

### Practitioner & PractitionerRole — workforce

```powershell
fhir-mql convert Practitioner "name=Jones&active=true"
fhir-mql convert Practitioner "identifier=http://npi|1234567890"
fhir-mql convert PractitionerRole "practitioner=pr-1&organization=org-1"
fhir-mql convert PractitionerRole "location=loc-er&service=hs-cardiology"
fhir-mql convert PractitionerRole "specialty=394814009"
```

### Organization & Location — facilities

```powershell
fhir-mql convert Organization "name=General Hospital&active=true"
fhir-mql convert Organization "identifier=urn:oid:2.16.840.1.113883.4.6|123"
fhir-mql convert Location "name=ER&status=active"
fhir-mql convert Location "address-city=Boston&organization=org-1"
```

### Observation — vitals, labs, clinical data

```powershell
fhir-mql convert Observation "code=http://loinc.org|8480-6"
fhir-mql convert Observation "patient=p1&status=final"
fhir-mql convert Observation "date=ge2024-06-01&code=8480-6"
fhir-mql convert Observation "value-quantity=120"
fhir-mql convert Observation "category=vital-signs"
fhir-mql convert Observation "encounter=enc-1"
```

### Appointment, Schedule, Slot — scheduling

```powershell
fhir-mql convert Appointment "status=booked&patient=p1"
fhir-mql convert Appointment "date=ge2024-07-01&actor=Practitioner/pr-1"
fhir-mql convert Appointment "reason-code=185345009"
fhir-mql convert Appointment "reason-reference=Condition/cond-1"
fhir-mql convert Schedule "active=true&actor=Practitioner/pr-1"
fhir-mql convert Schedule "service-type=11429006"
fhir-mql convert Schedule "service-type-reference=HealthcareService/hs-1"
fhir-mql convert Schedule "date=ge2024-07-01"
fhir-mql convert Slot "status=free&schedule=sched-1&start=ge2024-07-15"
```

### Encounter — visits & episodes

```powershell
fhir-mql convert Encounter "status=in-progress&patient=p1"
fhir-mql convert Encounter "class=AMB&type=185349003"
fhir-mql convert Encounter "date=ge2024-07-01&practitioner=pr-1"
fhir-mql convert Encounter "date-start=ge2024-07-01&end-date=le2024-07-31"
fhir-mql convert Encounter "location=loc-1&service-provider=org-1"
fhir-mql convert Encounter "diagnosis-code=44054006"
fhir-mql convert Encounter "part-of=enc-parent"
```

### Condition — problem list & diagnoses

```powershell
fhir-mql convert Condition "clinical-status=active&patient=p1"
fhir-mql convert Condition "code=44054006&verification-status=confirmed"
fhir-mql convert Condition "encounter=enc-1&onset-date=ge2020-01-01"
fhir-mql convert Condition "category=problem-list-item"
```

### Device & Group — assets & cohorts

```powershell
fhir-mql convert Device "status=active&organization=org-1"
fhir-mql convert Device "type=182722004&manufacturer=Acme"
fhir-mql convert Device "expiration-date=le2025-12-31"
fhir-mql convert Group "name=Cohort-A&type=person"
fhir-mql convert Group "member=Patient/p1&membership=enumerated"
fhir-mql convert Group "characteristic=73211009"
```

### Combined / AND queries

```powershell
fhir-mql convert Patient "name=Smith&gender=male&birthdate=ge1980-01-01"
fhir-mql convert Observation "patient=p1&code=8480-6&date=ge2024-01-01&status=final"
fhir-mql convert Appointment "status=booked&patient=p1&date=ge2024-07-01"
```

---

## Search (convert + execute)

Requires MongoDB. Documents should be denormalized first (`fhir-mql denormalize`).

```powershell
fhir-mql search Patient "name=Smith&gender=male" --limit 10
fhir-mql search Patient "name=Smith" --uri $URI --db $DB --format json
fhir-mql search Observation "code=http://loinc.org|8480-6&status=final" --limit 50
fhir-mql search Slot "status=free&start=ge2024-07-01" --limit 20

# Explain MQL without running find
fhir-mql search Patient "name=Smith" --explain
```

---

## Bulk operations

### Indexes (from YAML)

```powershell
fhir-mql indexes Patient
fhir-mql indexes Patient Observation Encounter --uri $URI --db $DB
fhir-mql indexes --all
fhir-mql indexes Slot Schedule --dry-run
```

### Denormalize (`_search` + `_compartments`)

```powershell
fhir-mql denormalize Patient
fhir-mql denormalize Patient Observation Appointment --batch-size 1000
fhir-mql denormalize --all --uri $URI --db $DB
fhir-mql denormalize --all --limit 100          # smoke / staging
fhir-mql denormalize --all --dry-run
```

### Reset (clear denorm fields only)

```powershell
fhir-mql reset Patient
fhir-mql reset Patient Observation --uri $URI --db $DB
fhir-mql reset --all
```

### Stats (coverage)

```powershell
fhir-mql stats Patient
fhir-mql stats --all --format json
fhir-mql stats Patient Observation Encounter Appointment
```

### Full reindex pipeline (typical deploy)

```powershell
fhir-mql indexes --all --uri $URI --db $DB
fhir-mql denormalize --all --uri $URI --db $DB --batch-size 500
fhir-mql stats --all --uri $URI --db $DB
```

---

## Compartment-scoped queries

Precomputed fast-path: Patient, Practitioner, Device (and Encounter self on
Encounter). RelatedPerson / some Encounter links use dynamic resolution.

```powershell
# Patient compartment → Observations for one patient
fhir-mql convert Observation "code=8480-6" `
  --compartment-type Patient --compartment-id p1

fhir-mql search Observation "status=final" `
  --compartment-type Patient --compartment-id p1 --limit 25

# Practitioner compartment → Schedules
fhir-mql convert Schedule "" `
  --compartment-type Practitioner --compartment-id pr-1

fhir-mql search Schedule "active=true" `
  --compartment-type Practitioner --compartment-id pr-1

# Device compartment
fhir-mql convert Observation "code=8480-6" `
  --compartment-type Device --compartment-id dev-1

# Encounter compartment (precomputed on Encounter resource)
fhir-mql convert Encounter "status=in-progress" `
  --compartment-type Encounter --compartment-id enc-1

# Dynamic: RelatedPerson → Schedule (no _compartments.RelatedPerson)
fhir-mql convert Schedule "" `
  --compartment-type RelatedPerson --compartment-id rp-1
```

**REST equivalent mental model:**

| HTTP pattern | CLI |
|--------------|-----|
| `GET /Patient/{id}/Observation?code=…` | `search Observation … --compartment-type Patient --compartment-id {id}` |
| `GET /Practitioner/{id}/Schedule` | `search Schedule … --compartment-type Practitioner --compartment-id {id}` |
| `GET /Encounter/{id}/Condition?…` | `convert Condition … --compartment-type Encounter --compartment-id {id}` *(dynamic)* |

---

## Healthcare workflow scenarios

### 1. Patient registration & MPI lookup

```powershell
fhir-mql search Patient "identifier=http://hospital.org/mrn|MRN-1001" --limit 5
fhir-mql search Patient "name=Smith&birthdate=1980-05-15" --limit 10
fhir-mql search Patient "name:exact=Smith&gender=male" --limit 5
```

### 2. Provider directory & credentialing

```powershell
fhir-mql search Practitioner "name=Jones&active=true" --limit 20
fhir-mql search PractitionerRole "organization=org-1&active=true" --limit 50
fhir-mql search PractitionerRole "practitioner=pr-1" --limit 5
```

### 3. Facility & location discovery

```powershell
fhir-mql search Organization "name=Hospital&active=true" --limit 10
fhir-mql search Location "name=ER&status=active" --limit 10
fhir-mql search Location "organization=org-1" --limit 25
```

### 4. Outpatient scheduling (find open slots)

```powershell
fhir-mql search Schedule "active=true&actor=Practitioner/pr-1" --limit 10
fhir-mql search Slot "status=free&schedule=sched-1&start=ge2024-07-15&start=le2024-07-31" --limit 100
fhir-mql search Appointment "status=booked&patient=p1&date=ge2024-07-01" --limit 20
```

### 5. In-progress encounters & ward board

```powershell
fhir-mql search Encounter "status=in-progress&location=loc-er" --limit 50
fhir-mql search Encounter "patient=p1&status=in-progress" --limit 5
fhir-mql search Encounter "practitioner=pr-1&date=ge2024-07-01" --limit 30
```

### 6. Problem list & active conditions

```powershell
fhir-mql search Condition "patient=p1&clinical-status=active" --limit 50
fhir-mql search Condition "code=44054006&verification-status=confirmed" --limit 10

fhir-mql search Condition "clinical-status=active" `
  --compartment-type Patient --compartment-id p1 --limit 50
```

### 7. Vitals & laboratory results

```powershell
fhir-mql search Observation "patient=p1&category=vital-signs&date=ge2024-06-01" --limit 100
fhir-mql search Observation "code=http://loinc.org|8480-6&status=final" --limit 20

fhir-mql search Observation "status=final" `
  --compartment-type Patient --compartment-id p1 --limit 25
```

### 8. Device asset management

```powershell
fhir-mql search Device "status=active" --limit 20
fhir-mql search Device "manufacturer=Acme&expiration-date=le2025-12-31" --limit 50
fhir-mql search Device "identifier=DEV-001" --limit 5
```

### 9. Research cohorts & population health

```powershell
fhir-mql search Group "name=Diabetes-Cohort&type=person" --limit 10
fhir-mql search Group "member=Patient/p1" --limit 5
fhir-mql search Group "type=person&characteristic=73211009" --limit 20
```

### 10. Care-team attribution (Appointment / Encounter participants)

```powershell
fhir-mql search Appointment "actor=Practitioner/pr-1&status=booked" --limit 30
fhir-mql search Encounter "participant=Practitioner/pr-1&status=completed" --limit 30
fhir-mql search Encounter "careteam=ct-1" --limit 20
```

### 11. Audit: denormalization coverage before go-live

```powershell
fhir-mql stats --all --uri $URI --db $DB --format json
fhir-mql denormalize --all --dry-run --uri $URI --db $DB
```

### 12. Disaster recovery — reindex without losing FHIR documents

```powershell
fhir-mql reset --all --uri $URI --db $DB
fhir-mql indexes --all --uri $URI --db $DB
fhir-mql denormalize --all --uri $URI --db $DB --batch-size 500
fhir-mql stats --all --uri $URI --db $DB
```

---

## Multi-database / project presets

Commands copied from real hybrid / synthetic data loads:

### Schedule-appointment hybrid database

```powershell
$URI = "mongodb://localhost:27017/"
$DB  = "fhir_schedule_appointment_hybrid"

fhir-mql denormalize --uri $URI --db $DB `
  Appointment Device Group Location Organization Patient Practitioner PractitionerRole Schedule Slot

fhir-mql reset --uri $URI --db $DB `
  Appointment Device Group Location Organization Patient Practitioner PractitionerRole Schedule Slot

fhir-mql search Patient "name=Taylor" --limit 10 --uri $URI --db $DB
```

### Synthetic demographics database

```powershell
$URI = "mongodb://localhost:27017/"
$DB  = "fhir_synthetic"

fhir-mql denormalize --uri $URI --db $DB Location Organization Patient Practitioner
fhir-mql reset --uri $URI --db $DB --all
```

### Full 13-resource denormalize

```powershell
fhir-mql denormalize --uri $URI --db $DB `
  Appointment Condition Device Encounter Group Location Observation Organization `
  Patient Practitioner PractitionerRole Schedule Slot
```

---

## Python API one-liners

```powershell
# Convert
python -c "from fhir_search_to_mql import FHIRSearchConverter as C; print(C().convert('Patient','name=Smith'))"

# Denormalize
python -c "from fhir_search_to_mql import ResourceDenormalizer as D; r={'resourceType':'Patient','id':'x','name':[{'family':'Smith'}]}; print(D().denormalize(r).get('_search',{}))"

# Compartment query
python -c "from fhir_search_to_mql import FHIRSearchConverter as C; print(C().convert_with_compartment('Patient','p1','Observation','code=8480-6'))"

# Denormalize from file / folder
python -c "
from fhir_search_to_mql import ResourceDenormalizer
d = ResourceDenormalizer()
# d.denormalize_from_file('patient.json')
# d.denormalize_from_folder('fhir_data/', resource_type='Patient', recursive=True)
"

# Bulk MongoDB update
python -c "
from pymongo import MongoClient
from fhir_search_to_mql import ResourceDenormalizer, MongoDBHandler
db = MongoClient('mongodb://localhost:27017/')['fhir_synthetic']
print(MongoDBHandler.update_search_fields(db.Patient, ResourceDenormalizer().denormalize, batch_size=500))
"
```

---

## FHIR schema tooling

Local spec indexes under `schema/` (not installed with the wheel). See
[schema/README.md](schema/README.md).

```powershell
# Regenerate resources.r5.json, search-parameters.r5.json, shipped index
python -m fhir_search_to_mql.schema.build_indexes

# Per-resource spec summary (search params, elements, compartments)
python -m fhir_search_to_mql.schema.resource_spec Encounter
python -m fhir_search_to_mql.schema.resource_spec Condition
python -m fhir_search_to_mql.schema.resource_spec Patient

# Optional: point at a relocated schema tree
$env:FHIR_SCHEMA_ROOT = "D:\path\to\schema"
python -m fhir_search_to_mql.schema.build_indexes
```

---

## Testing commands

```powershell
# Full suite (~5 min; skips @mongodb if DB down)
python -m pytest tests/ -q --no-cov --ignore=tests/integration/test_performance.py

# Unit only (no MongoDB)
python -m pytest tests/unit/ -q

# Skip CLI live-DB integration
python -m pytest --ignore=tests/integration/test_cli_integration.py -q

# Cross-config audit harness (all 13 resources)
python -m pytest tests/integration/test_config_audit_regressions.py -v

# Per-resource comprehensive suites
python -m pytest tests/integration/test_encounter_comprehensive.py -v
python -m pytest tests/integration/test_condition_comprehensive.py -v
python -m pytest tests/integration/test_patient_comprehensive.py -v

# MongoDB-tagged E2E only
python -m pytest -m mongodb -v

# Coverage
python -m pytest tests/ --cov=fhir_search_to_mql --cov-report=term-missing
```

---

## Troubleshooting commands

```powershell
# CLI on PATH?
where.exe fhir-mql
pip show fhir-search-to-mql

# MongoDB reachable?
python -c "from pymongo import MongoClient; print(MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000).server_info())"

# Unknown resource type
fhir-mql denormalize Bogus
# → lists configured resources in error message

# Re-pick bundled YAML after local edits (editable install)
pip install -e .
```

---

## Related documentation

- [README.md](README.md) — setup, architecture, configuration, API patterns
- [schema/README.md](schema/README.md) — FHIR schema assets and indexes
- [.cursor/skills/fhir-resource-config/](.cursor/skills/fhir-resource-config/) — adding new resources
