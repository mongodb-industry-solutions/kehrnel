# fhir-gen

**FHIR R5 synthetic healthcare data generator** — schema-driven, interlinked, terminology-aware synthetic FHIR resources with optional MongoDB persistence and a full CLI.

Designed to pair with **[fhir-search-to-mql](../fhir-search-to-mql/)** for the same **84** resource types: generate interlinked data here, then denormalize and run FHIR search queries there.

| Capability | Details |
|------------|---------|
| **Schema coverage** | **158** FHIR R5 resource types (`fhir_gen/schema/fhir.schema.v5.json`) |
| **MQL-aligned enrichers** | **84** shipped types with YAML terminology + realistic fields (`MQL_SHIPPED_RESOURCES`) |
| **Terminology** | `healthcare_codes.yaml` (**146** sections); `pick_code` / `codeable_from_section` helpers |
| **Scenarios** | Named Patient/Practitioner/Person lifecycle + schema `poly_*` choice coverage (~49 types) |
| **Dependencies** | `CORE_DEPENDENCIES` + topological `resolve_order()` for referential integrity |
| **Persistence** | MongoDB per resource type (`Patient`, `Observation`, …) or JSON export |

**Requirements:** Python **3.11+**, MongoDB **optional** (for `--save`, `search`, `db-stats`)

**Command cookbook:** [CLI_COMMANDS.md](CLI_COMMANDS.md) — all 84 resources, 21 healthcare + 11 industrial `generate-many` scenarios, fhir-mql pipeline.

---

## Installation

### From source (development)

```bash
python -m venv .venv
# Windows: .\.venv\Scripts\Activate.ps1
# Windows bash: source .venv/Scripts/activate
# Linux/macOS: source .venv/bin/activate

pip install --upgrade pip
pip install -e ".[dev]"
```

### Dependencies only

```bash
pip install -r requirements.txt
```

### Published package (when published)

```bash
pip install fhir-gen
```

Copy environment defaults:

```bash
cp .env.example .env   # Unix
Copy-Item .env.example .env   # Windows PowerShell
```

---

## Quick start (Python API)

```python
from fhir_gen import ResourceGenerator
from fhir_gen.persistence import FHIRMongoStore

# Generate with automatic dependencies (Patient, etc.)
gen = ResourceGenerator(seed=42)
patients = gen.generate("Patient", count=5)
encounters = gen.generate("Encounter", count=10)

# Multi-type bundle in dependency order
bundle = gen.generate_many(
    ["Patient", "Practitioner", "Organization", "Encounter", "Observation"],
    counts={"Patient": 10, "Encounter": 20, "Observation": 50},
)

# Polymorphic variants (one Observation per value[x] choice)
variants = gen.generate_variants("Observation", variant_fields=["value"])

# Named lifecycle scenario (e.g. deceased patient with deceasedDateTime)
deceased = gen.generate_scenario("Patient", "deceased_datetime")

# All Patient named scenarios (9 records)
cohort = gen.generate_scenarios("Patient", named_only=True)

# Persist to MongoDB
store = FHIRMongoStore()
store.save_many(patients + encounters)
print(store.search_patient(family="Smith"))
store.close()
```

Custom schema (INSTRUCTIONS #6):

```python
from pathlib import Path

gen.generate("Patient", count=1, schema_version="R6")
```

---

## End-to-end with fhir-search-to-mql

```powershell
# 1) Generate (this repo) — full 84-type sandbox
fhir-gen --seed 4000 --db fhir_synthetic generate-many Patient Encounter Observation Composition DeviceRequest MeasureReport `
  Account Endpoint Provenance ExplanationOfBenefit Questionnaire `
  --count Patient=50 --count Encounter=80 --count Observation=200 --save

# See CLI_COMMANDS.md for the explicit all-84 generate-many block

# 2) Index + denormalize + search (fhir-search-to-mql repo)
fhir-mql indexes --all --db fhir_synthetic
fhir-mql denormalize --all --db fhir_synthetic --batch-size 500
fhir-mql search Patient "name=Smith&active=true" --db fhir_synthetic --limit 10
fhir-mql search Composition "status=final&type=18842-5" --db fhir_synthetic --limit 10
```

Use the same `FHIR_GEN_MONGODB_DB` / `MONGODB_DB` name (default `fhir_synthetic`) so both tools share one database.

### Combined E2E (both repos)

**[E2E_COMBINED.md](E2E_COMBINED.md)** documents the cross-repo runner: **`scripts/run_cli_e2e.py`** loads each scenario into `fhir_e2e_gen_<id>`, then runs **fhir-mql** indexes, denormalize, and search on the **same** database.

```powershell
cd fhir-data-generation
.\.venv\Scripts\Activate.ps1
# Editable installs in both repos (see E2E_COMBINED.md)
.venv\Scripts\python.exe scripts\run_cli_e2e.py
.venv\Scripts\python.exe scripts\run_cli_e2e.py --section industrial
.venv\Scripts\python.exe scripts\run_cli_e2e.py --gen-only
.venv\Scripts\python.exe scripts\run_cli_e2e.py --quiet   # pass/fail only
```

| Flag | Effect |
|------|--------|
| `--gen-only` | Generation scenarios only |
| `--pipeline-only` | Drop/reload data, then fhir-mql per scenario |
| `--section healthcare` \| `industrial` | Subset of [CLI_COMMANDS.md](CLI_COMMANDS.md) scenarios |
| `--full-counts` | Documented volumes (slow) |
| `--quiet` | Hide phase status; only `[GEN OK]` / `[PIPELINE OK]` |

By default each scenario prints **short phase status** (preparing DB, generating data, indexes, denormalize, search tests). Steps running longer than ~30s print a **still running** line with elapsed time (search progress as `n/total`).

**Industrial scenario IDs** use descriptive slugs (e.g. `ind_hospital`, `ind_full84`) — databases are `fhir_e2e_gen_ind_<slug>`. Cleanup: `scripts/drop_e2e_databases.py`. Optional validation after a run: `scripts/validate_e2e_database.py`.

Per-repo pytest: [E2E_COMMANDS.md](E2E_COMMANDS.md).

---

## CLI

**Full command cookbook:** [CLI_COMMANDS.md](CLI_COMMANDS.md) — resource inventory (84), healthcare & industrial scenarios, `generate-many` bundles, PowerShell examples, MongoDB search.

Global options: `--seed`, `--schema-version`, `--mongo-uri`, `--db`


| Command                             | Description                                                            |
| ----------------------------------- | ---------------------------------------------------------------------- |
| `fhir-gen version`                  | Package version                                                        |
| `fhir-gen list-resources`           | All 158 resource type names                                            |
| `fhir-gen list-scenarios [Type]`    | Named lifecycle + `poly_`* scenario catalog                            |
| `fhir-gen list-poly-groups [Type]`  | Schema choice groups (`value[x]`, `onset[x]`, …)                       |
| `fhir-gen generate <Type>`          | Generate one resource type (`--scenario`, `--scenarios`, `--variants`) |
| `fhir-gen generate-many <Types...>` | Multiple types in dependency order                                     |
| `fhir-gen schema-info <Type>`       | Schema fields and polymorphic groups                                   |
| `fhir-gen db-stats`                 | Document counts per collection                                         |
| `fhir-gen search <Type>`            | Query MongoDB                                                          |
| `fhir-gen clear [Type]`             | Drop collection(s)                                                     |


### Examples

```bash
# List types
fhir-gen list-resources

# Generate to stdout (no MongoDB)
fhir-gen --seed 42 generate Patient --count 3 --no-save

# Save to MongoDB (includes dependency resources in session store)
fhir-gen generate Encounter --count 5 --save

# Write JSON file
fhir-gen generate Observation --count 10 --no-save --output observations.json

# Polymorphic variants (all value[x] branches for Observation)
fhir-gen generate Observation --variants --no-save

# Named lifecycle scenario (deceasedDateTime, not deceasedBoolean)
fhir-gen generate Patient --scenario deceased_datetime --no-save

# All Patient named scenarios (9 records)
fhir-gen generate Patient --scenarios --no-save

# Skip auto-dependencies (schema-only single type)
fhir-gen generate Patient --count 1 --no-deps --no-save

# Clinical bundle (bash)
fhir-gen generate-many Patient Practitioner Organization Encounter Observation --counts '{"Patient":10,"Encounter":20,"Observation":50}' --save

# PowerShell: use one line, backtick continuation, or --count (recommended)
fhir-gen generate-many Patient Practitioner Organization Encounter Observation `
  --count Patient=10 --count Encounter=20 --count Observation=50 --save

# Schema inspection
fhir-gen schema-info MedicationRequest

# MongoDB
fhir-gen generate Patient --count 100 --save
fhir-gen db-stats
fhir-gen search Patient --limit 5
fhir-gen search Observation --patient-id <uuid> --code 8867-4
fhir-gen clear --yes
```

---

## MongoDB setup

1. Start MongoDB (default: `mongodb://localhost:27017`).
2. Configure `.env` (see [Environment variables](#environment-variables)).
3. Generate with `--save` (default). The CLI saves all resources accumulated in the generator session store (including dependencies).

Collections use the FHIR resource type name by default (`Patient`, `Observation`, …). Set `FHIR_GEN_MONGODB_COLLECTION_PREFIX` (e.g. `fhir_`) to get `fhir_Patient`, `fhir_Observation`, etc. Indexes are created on common search paths (`id`, `subject.reference`, `identifier.value`, etc.).

```bash
fhir-gen generate Patient --count 50 --save
fhir-gen generate Observation --count 200 --save
fhir-gen db-stats
```

---

## Supported resources

All **158** resources in `fhir_gen/schema/fhir.schema.v5.json` can be generated via the schema engine:

```bash
fhir-gen list-resources
```

### MQL-aligned enrichers (84 types)

These match **`MQL_SHIPPED_RESOURCES`** in `fhir_gen/resolvers/dependency.py` and have enrichers + `CORE_DEPENDENCIES` entries (aligned with [fhir-search-to-mql](../fhir-search-to-mql/) YAML configs):

| Module | Count | Examples |
|--------|-------|----------|
| **clinical** | 20 | Patient, Composition, AdverseEvent, BodyStructure, Person, ImmunizationRecommendation, Encounter, Observation, … |
| **medication** | 6 | Medication, MedicationRequest, MedicationStatement, MedicationAdministration, … |
| **workflow** | 23 | Appointment, Questionnaire, DeviceRequest, SupplyRequest, Provenance, RequestOrchestration, … |
| **financial** | 15 | Coverage, Claim, ExplanationOfBenefit, CoverageEligibilityResponse, PaymentReconciliation, InsurancePlan, … |
| **specialized** | 21 | DeviceUsage, Endpoint, Measure, MeasureReport, GenomicStudy, BiologicallyDerivedProduct, … |

```bash
python -c "from fhir_gen.resolvers.dependency import MQL_SHIPPED_RESOURCES; print(len(MQL_SHIPPED_RESOURCES))"
# 84
```

**Domain inventory** (identity, clinical, RCM, devices, quality, research, …): see [CLI_COMMANDS.md — Resource inventory](CLI_COMMANDS.md#resource-inventory-84-mql-aligned-enrichers).

**Also enriched (not in the 84 MQL set):** `MedicationKnowledge`.

**Schema-only:** remaining types among the 158 (e.g. `Subscription`) — valid FHIR without the clinical enricher layer.

### Terminology & CodeSystems

- Canonical **R5 default schema** ships at `fhir_gen/schema/fhir.schema.v5.json` (optional R6: `fhir_gen/schema/fhir.schema.v6.json`).
- Codes load from `fhir_gen/hl7_codes/healthcare_codes.yaml` via `fhir_gen.codes.loader`.
- `fhir_gen.codes.validation` checks absolute `Coding.system` URIs, rejects ValueSet URLs, and validates codes against the YAML catalog (with pattern checks for SNOMED, LOINC, RxNorm).
- **Condition verification status** uses the HL7 canonical CodeSystem  
`http://terminology.hl7.org/CodeSystem/condition-ver-status` (not a ValueSet URL and not a relative `CodeSystem/...` path).
- Enrichers bind status/category fields to YAML sections (e.g. `condition_clinical_status`, `condition_verification_status`, `observation_categories`, `service_type`).
- Regenerate YAML after editing `_build_healthcare_codes.py`:  
`python fhir_gen/hl7_codes/_build_healthcare_codes.py`

---

## Architecture

```mermaid
flowchart LR
  schema[fhir_gen/schema/*.json] --> parser[SchemaParser / Registry]
  codes[healthcare_codes.yaml] --> loader[Codes Loader]
  loader --> validation[Terminology Validation]
  parser --> engine[ResourceGenerator]
  loader --> enrichers[Resource Enrichers]
  scenarios[scenarios.py + poly_catalog] --> engine
  enrichers --> engine
  field_fill[field_fill.py] --> engine
  engine --> store[ReferenceStore]
  store --> engine
  engine --> mongo[FHIRMongoStore]
```



1. **Schema** — `FHIRSchemaParser` reads JSON Schema definitions; `SchemaRegistry` caches `ResourceDef` (fields, required, polymorphic groups).
2. **Generators** — Primitives → complex types → special types (Meta, Reference, Dosage). Bare `CodeableConcept` calls without `system`/`code` emit text-only (no random SNOMED).
3. **Engine** — `ResourceGenerator` fills fields, resolves references via `ReferenceStore`, applies scenarios and one variant per `value[x]` group when requested.
4. **Field fill** — `fill_schema_gaps` / backbone fillers add contact, telecom, address, identifiers after enrichers.
5. **Dependencies** — `resolve_order()` topological sort from `CORE_DEPENDENCIES` + schema references.
6. **Enrichers** — Per-resource functions in `fhir_gen/generators/resources/` override fields with YAML-backed terminology.
7. **Scenarios** — Named lifecycle (`scenarios.py`) and schema polymorphic catalog (`poly_catalog.py`); CLI `--scenario` / `--scenarios` / `--variants`.
8. **Persistence** — `FHIRMongoStore` upserts to per-type collections with search indexes.
9. **Healthcare text** — `fhir_gen/generators/healthcare_text.py` fills narrative and string fields with clinically relevant templates (resource+field → field templates → generic), instead of random Faker prose.
10. **Canonical metadata** — `canonical_resource.py` sets publisher, version, and related administrative strings on definition-style resources (e.g. ChargeItemDefinition, Measure) with correct primitive types.

---

## Environment variables

Loaded from `.env` with prefix `FHIR_GEN_` (see `fhir_gen/config.py`).


| Variable                             | Default                                    | Description                                                      |
| ------------------------------------ | ------------------------------------------ | ---------------------------------------------------------------- |
| `FHIR_GEN_MONGODB_URI`               | `mongodb://localhost:27017`                | MongoDB connection string                                        |
| `FHIR_GEN_MONGODB_DB`                | `fhir_synthetic`                           | Database name                                                    |
| `FHIR_GEN_MONGODB_COLLECTION_PREFIX` | *(empty)*                                  | Prefix for collection names; empty → `Patient`, `Observation`, … |
| `FHIR_GEN_SEED`                      | *(none)*                                   | Default random seed (CLI `--seed` overrides)                     |
| `FHIR_GEN_SCHEMA_VERSION`            | `R5`                                         | FHIR release: `R5` (default) or `R6` (CLI `--schema-version`)    |
| `FHIR_GEN_SCHEMA_PATH`               | *(unset)*                                    | Optional advanced override of bundled schema JSON file           |
| `FHIR_GEN_CODES_PATH`                | `fhir_gen/hl7_codes/healthcare_codes.yaml` | Terminology YAML (override for custom code sets)                 |
| `FHIR_GEN_LOG_LEVEL`                 | `INFO`                                     | Logging level                                                    |


CLI flags `--mongo-uri`, `--db`, and `--schema-version` override settings for that invocation.

---

## Repository layout


| Path                                            | Purpose                                                         |
| ----------------------------------------------- | --------------------------------------------------------------- |
| `fhir_gen/`                                     | Python package (generators, schema parser, CLI, persistence)    |
| `fhir_gen/schema/fhir.schema.v5.json`           | Default FHIR R5 schema (packaged in wheel)                      |
| `fhir_gen/schema/fhir.schema.v6.json`           | Optional FHIR R6 preview schema                                 |
| `fhir_gen/codes/`                               | Terminology loader, validation, `codeable_from_section` helpers |
| `fhir_gen/hl7_codes/healthcare_codes.yaml`      | HL7/FHIR terminology (146 sections)                             |
| `fhir_gen/hl7_codes/_build_healthcare_codes.py` | Regenerate/merge YAML                                           |
| `tests/`                                        | Pytest suite (`test_mql_shipped_resources`, integration, terminology) |
| `tests/e2e/`                                    | E2E scenario defs (`cli_scenarios_gen.py`), runner helpers, `e2e_log.py` |
| `scripts/run_cli_e2e.py`                        | Combined fhir-gen + fhir-mql E2E driver                           |
| `scripts/drop_e2e_databases.py`                 | Drop all `fhir_e2e_*` MongoDB databases                           |
| `scripts/validate_e2e_database.py`              | Post-run schema/field checks on an E2E database                    |
| `E2E_COMBINED.md`                               | Combined E2E runner (status logging, flags, DB naming)            |
| `E2E_COMMANDS.md`                               | Per-repo E2E pytest commands                                      |
| `CLI_COMMANDS.md`                               | Full CLI cookbook (84 resources, healthcare & industrial scenarios) |


---

## Development

```bash
# Activate venv, then:
pip install -e ".[dev]"
pytest tests/
ruff check fhir_gen tests
```

Focused test suites:

```bash
pytest tests/test_mql_shipped_resources.py -q --no-cov    # All 84 MQL types (generate, deps, codings)
pytest tests/test_mql_integration.py -q --no-cov          # Batch + dependency chains
pytest tests/test_terminology_validation.py -v --no-cov   # CodeSystem / coding validation
pytest tests/test_scenarios.py -v --no-cov                # Named + poly scenarios
pytest tests/test_schema_reference_integrity.py -v --no-cov
pytest tests/test_schema_field_validation.py -v --no-cov
pytest tests/test_clinical.py tests/test_workflow.py tests/test_financial.py tests/test_specialized.py -q --no-cov
pytest tests/test_canonical_resource.py tests/test_schema_parser.py -q --no-cov
```

Combined E2E (requires MongoDB + sibling **fhir-search-to-mql** install):

```bash
python scripts/run_cli_e2e.py --section healthcare
python -m pytest tests/e2e/ -m "e2e and mongodb" --no-cov -q
```

Regenerate terminology:

```bash
python fhir_gen/hl7_codes/_build_healthcare_codes.py
```

---

## Contributing

1. Use a virtual environment and `pip install -e ".[dev]"`.
2. Follow existing patterns in `fhir_gen/` (minimal scope, schema-first).
3. Add tests for new behavior; keep `pytest` coverage ≥ 75%.
4. Enrich `healthcare_codes.yaml` via `_build_healthcare_codes.py` rather than full rewrites.
5. New MQL-shipped enrichers: add to `fhir_gen/generators/resources/*.py`, register in `ENRICHERS`, update `MQL_SHIPPED_RESOURCES` and `CORE_DEPENDENCIES`, and add a matching YAML in fhir-search-to-mql.

Implementation is guided by the Cursor skill `.cursor/skills/fhir-data-generation/SKILL.md` (standalone fhir-data-generation repo for full prompt docs).

---

## Troubleshooting


| Issue                              | Fix                                                                                                         |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `fhir-gen` not found               | Activate `.venv`; `pip install -e ".[dev]"`                                                                 |
| `ModuleNotFoundError: fhir_gen`    | Install from repo root with venv active                                                                     |
| MongoDB connection failed          | Check `FHIR_GEN_MONGODB_URI`; start `mongod`                                                                |
| PowerShell venv activation blocked | `Set-ExecutionPolicy RemoteSigned -Scope CurrentUser`                                                       |
| No `deceasedDateTime` on Patient   | Use `--scenario deceased_datetime` or `--scenarios`, not `generate Patient -n 1` alone                      |
| Wrong `Coding.system` on Condition | Enriched Condition uses `http://terminology.hl7.org/CodeSystem/condition-ver-status`; run terminology tests |
| Custom schema not found            | Use `fhir_gen/schema/fhir.schema.v6.json` or set `FHIR_GEN_SCHEMA_PATH`                                     |


---

## See also

- [fhir-search-to-mql README](../fhir-search-to-mql/README.md) — denormalize and search the same 84 resources
- [fhir-search-to-mql CLI_COMMANDS.md](../fhir-search-to-mql/CLI_COMMANDS.md)
- [E2E_COMBINED.md](E2E_COMBINED.md) — `run_cli_e2e.py` (gen → mql on one DB per scenario)
- [E2E_COMMANDS.md](E2E_COMMANDS.md) — pytest E2E for this repo
- [CLI_COMMANDS.md](CLI_COMMANDS.md) — generate commands and scenario catalog

---

## License

MIT (see `pyproject.toml`).