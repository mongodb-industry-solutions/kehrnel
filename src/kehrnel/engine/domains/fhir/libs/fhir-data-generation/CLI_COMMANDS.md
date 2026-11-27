# fhir-gen CLI command reference

Practical commands for generating **interlinked, terminology-aware** FHIR R5 synthetic data suitable for integration testing, search demos, and MongoDB-backed sandboxes.

**Prerequisites**

```powershell
# From repo root (Windows PowerShell)
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
Copy-Item .env.example .env
```

Ensure MongoDB is running when using `--save` (default). Use `--no-save` with `--output` or stdout for JSON only.

Pair with **[fhir-search-to-mql](../fhir-search-to-mql/CLI_COMMANDS.md)** to denormalize and search the same 84 resource types after generation.

---

## Table of contents

1. [Global options](#global-options-prefix-every-command)
2. [Discovery & inspection](#discovery--inspection)
3. [Resource inventory (84 MQL-aligned enrichers)](#resource-inventory-84-mql-aligned-enrichers)
4. [Recent improvements](#recent-improvements-data-quality--testing)
5. [Generation scenarios](#generation-scenarios-full-coverage)
6. [Healthcare workflow scenarios](#healthcare-workflow-scenarios)
7. [Industrial & enterprise scenarios](#industrial--enterprise-scenarios)
8. [Terminology & CodeSystems](#terminology--codesystems)
9. [Single-resource generation](#single-resource-generation-generate)
10. [Multi-resource bundles](#multi-resource-bundles-generate-many)
11. [Generate → search pipeline (fhir-mql)](#generate--search-pipeline-fhir-mql)
12. [Polymorphic variants](#polymorphic-variants---variants)
13. [MongoDB operations](#mongodb-operations)
14. [Enriched vs schema-only](#enriched-vs-schema-only-resources)
15. [Development & test commands](#development--test-commands)

---

## Global options (prefix every command)

| Option | Env variable | Purpose |
|--------|----------------|---------|
| `--seed N` | `FHIR_GEN_SEED` | Reproducible IDs, codes, and references |
| `--mongo-uri URI` | `FHIR_GEN_MONGODB_URI` | MongoDB connection |
| `--db NAME` | `FHIR_GEN_MONGODB_DB` | Database name |
| `--schema-version {R5,R6}` | `FHIR_GEN_SCHEMA_VERSION` | FHIR release for JSON Schema (**default: R5**) |
| `--schema-path PATH` | `FHIR_GEN_SCHEMA_PATH` | Advanced override of bundled schema file (optional) |

**PowerShell tips**

- Use **backtick** `` ` `` for line continuation, not `\`.
- Prefer **`--count Type=N`** over JSON for `--counts` (PowerShell strips JSON quotes).
- Brace form also works: `--counts '{Patient:10,Encounter:20}'`.

**Bash tips**

- Line continuation with `\`.
- JSON counts: `--counts '{"Patient":10,"Encounter":20}'`.

### `generate` command options

| Option | Short | Purpose |
|--------|-------|---------|
| `--count N` | `-n` | Number of resources (default 1) |
| `--save` / `--no-save` | | MongoDB persist (default: save) |
| `--output PATH` | | Write JSON to a file; use `--output -` for stdout |
| `--pretty` / `--no-pretty` | | Pretty-print JSON (default: pretty) |
| `--scenario ID` | | One named or `poly_*` scenario |
| `--scenarios` | | All scenarios for the type (named + poly) |
| `--variants` | | Schema polymorphic variants only |
| `--with-deps` / `--no-deps` | | Auto-generate dependencies (default: on) |

There is **no** `-o` short flag — always use **`--output`**.

---

## Discovery & inspection

```powershell
fhir-gen version
fhir-gen list-resources
fhir-gen list-scenarios --all-types
fhir-gen list-scenarios Patient
fhir-gen list-scenarios Observation
fhir-gen list-poly-groups
fhir-gen list-poly-groups Condition
fhir-gen schema-info Observation
fhir-gen schema-info Patient
fhir-gen schema-info Claim
fhir-gen schema-info MedicationRequest
```

---

## Resource inventory (84 MQL-aligned enrichers)

These **84** types match `fhir_gen/resolvers/dependency.py` → `MQL_SHIPPED_RESOURCES` and have **terminology-aware enrichers** in `fhir_gen/generators/resources/`. They align with fhir-search-to-mql YAML configs for end-to-end generate → denormalize → search demos.

```powershell
# Count enrichers vs MQL list (should be 84 / 84, no missing)
python -c "from fhir_gen.resolvers.dependency import MQL_SHIPPED_RESOURCES; from fhir_gen.generators.resources import ENRICHERS; m=set(MQL_SHIPPED_RESOURCES)-set(ENRICHERS); print(len(MQL_SHIPPED_RESOURCES), len(ENRICHERS), m or 'ok')"

fhir-gen list-resources
```

### By healthcare / industrial domain

| Domain | Resources | Typical use |
|--------|-----------|-------------|
| **Identity & directory** | Patient, Person, Practitioner, PractitionerRole, RelatedPerson, Organization, OrganizationAffiliation, Location, Endpoint, HealthcareService, Group | MPI, provider directory, facility registry, FHIR endpoints |
| **Scheduling & access** | Appointment, Schedule, Slot, Encounter, EpisodeOfCare, Account | booking, capacity, visits, care episodes, patient accounts |
| **Clinical record** | Condition, AllergyIntolerance, Observation, DiagnosticReport, ImagingStudy, Specimen, ClinicalImpression, FamilyMemberHistory, BodyStructure, Composition, DocumentReference | problems, labs, imaging, assessments, anatomy, discharge summaries |
| **Medications** | Medication, MedicationRequest, MedicationAdministration, MedicationDispense, MedicationStatement, Substance | e-prescribing, MAR, dispensing, home med list |
| **Orders & care delivery** | ServiceRequest, Procedure, DeviceRequest, RequestOrchestration, CarePlan, CareTeam, Goal, Task, NutritionOrder, NutritionIntake, VisionPrescription | orders, procedures, devices, orchestration, care plans |
| **Devices & supplies** | Device, DeviceUsage, DeviceDispense, SupplyRequest, SupplyDelivery, BiologicallyDerivedProduct | asset tracking, utilization, inventory, blood/tissue |
| **Immunizations** | Immunization, ImmunizationRecommendation | IIS, forecast, clinic campaigns |
| **Safety & quality** | AdverseEvent, DetectedIssue, Flag, RiskAssessment, Measure, MeasureReport | pharmacovigilance, CDS, alerts, eCQM/HEDIS |
| **Financial / RCM** | Coverage, CoverageEligibilityRequest, CoverageEligibilityResponse, Claim, ClaimResponse, ExplanationOfBenefit, Account, Invoice, ChargeItem, ChargeItemDefinition, PaymentNotice, PaymentReconciliation | eligibility, claims, EOB, billing definitions |
| **Payer & enrollment** | EnrollmentRequest, EnrollmentResponse, InsurancePlan | member enrollment, plan catalog |
| **Research & genomics** | ResearchStudy, ResearchSubject, GenomicStudy | trials, biobank, molecular results |
| **Forms & PROs** | Questionnaire, QuestionnaireResponse | assessments, PROMs, screening |
| **Privacy & legal** | Consent, Contract | consent directives, agreements |
| **Interop & audit** | AuditEvent, Provenance, Communication, Basic | security audit, lineage, messaging, extensions |

### Alphabetical index (all 84)

`Account`, `AdverseEvent`, `AllergyIntolerance`, `Appointment`, `AuditEvent`, `Basic`, `BiologicallyDerivedProduct`, `BodyStructure`, `CarePlan`, `CareTeam`, `ChargeItem`, `ChargeItemDefinition`, `Claim`, `ClaimResponse`, `ClinicalImpression`, `Communication`, `Composition`, `Condition`, `Consent`, `Contract`, `Coverage`, `CoverageEligibilityRequest`, `CoverageEligibilityResponse`, `DetectedIssue`, `Device`, `DeviceDispense`, `DeviceRequest`, `DeviceUsage`, `DiagnosticReport`, `DocumentReference`, `Encounter`, `Endpoint`, `EnrollmentRequest`, `EnrollmentResponse`, `EpisodeOfCare`, `ExplanationOfBenefit`, `FamilyMemberHistory`, `Flag`, `GenomicStudy`, `Goal`, `Group`, `HealthcareService`, `Immunization`, `ImmunizationRecommendation`, `ImagingStudy`, `InsurancePlan`, `Invoice`, `Location`, `Measure`, `MeasureReport`, `Medication`, `MedicationAdministration`, `MedicationDispense`, `MedicationRequest`, `MedicationStatement`, `NutritionIntake`, `NutritionOrder`, `Observation`, `Organization`, `OrganizationAffiliation`, `Patient`, `PaymentNotice`, `PaymentReconciliation`, `Person`, `Practitioner`, `PractitionerRole`, `Procedure`, `Provenance`, `Questionnaire`, `QuestionnaireResponse`, `RelatedPerson`, `RequestOrchestration`, `ResearchStudy`, `ResearchSubject`, `RiskAssessment`, `Schedule`, `ServiceRequest`, `Slot`, `Specimen`, `Substance`, `SupplyDelivery`, `SupplyRequest`, `Task`, `VisionPrescription`

**Schema-only:** all **158** R5 types remain available via `fhir-gen generate <Type>`; types outside the 84 list use schema fill without the clinical enricher layer.

---

## Recent improvements (data quality & testing)

| Area | What changed |
|------|----------------|
| **Field coverage** | Higher optional-field fill rate; schema-aware `field_fill` after enrichers (contact, telecom, address, identifiers, etc.) |
| **Patient `contact`** | Emergency contacts with relationship, name, telecom, address, optional organization reference |
| **Datatypes** | Primitives and complex types validated (CodeableConcept, ContactPoint, HumanName, Reference, instant/dateTime with fractional seconds) |
| **Backbone generation** | Fixed empty `{}` nested elements; required backbone children forced; recursion stack bug fixed |
| **References** | `fill_missing_references` for actor/party/coverage; full session saved on `generate-many --save`; generation priority (`Organization` before `Patient`) |
| **No garbage codings** | Bare `gen_CodeableConcept()` without `system`/`code` emits text-only (no random SNOMED + faker codes) |
| **Terminology** | `healthcare_codes.yaml` (**146** sections); gap resources use `pick_code` / `codeable_from_section`; rebuild via `_build_healthcare_codes.py` |
| **MQL alignment** | **84** shipped types in `MQL_SHIPPED_RESOURCES` with `CORE_DEPENDENCIES` + enrichers (`clinical`, `workflow`, `financial`, `specialized`, `medication`) |
| **Scenarios** | Named lifecycle (Patient/Practitioner/Person) + **49 resources** with schema choice variants (`poly_*`) — see below |
| **Tests** | `test_mql_shipped_resources.py` (per-resource regression); `test_mql_integration.py` (batch + chains); module tests per enricher file |

---

## Generation scenarios (full coverage)

FHIR uses **choice groups** (only one branch allowed): `deceasedBoolean` *or* `deceasedDateTime`, `valueQuantity` *or* `valueString`, `onsetDateTime` *or* `onsetAge`, etc. fhir-gen covers these in two layers:

| Layer | Mechanism | CLI | Count |
|-------|-----------|-----|-------|
| **Named lifecycle** | Hand-crafted enricher logic (active/deceased, contact, …) | `--scenario <id>` | 17 scenarios across 3 types |
| **Schema polymorphic** | One document per choice from JSON Schema | `--scenarios` or `--variants` | **49 resource types**, **~200 variants** total |

```powershell
# Overview: named + poly counts for every resource that has scenarios
fhir-gen list-scenarios --all-types

# Detail for one resource (named + poly_* ids)
fhir-gen list-scenarios Observation
fhir-gen list-scenarios Patient --named-only

# Raw schema choice groups (no enricher semantics)
fhir-gen list-poly-groups
fhir-gen list-poly-groups FamilyMemberHistory
```

### Command cheat sheet

| Goal | Command |
|------|---------|
| All scenarios for a type (named + poly) | `fhir-gen generate Observation --scenarios --no-save` |
| Named lifecycle only (9 Patient, no poly ids) | `fhir-gen generate Patient --scenarios --scenarios-named-only --save` |
| One named scenario | `fhir-gen generate Patient --scenario deceased_datetime --save` |
| One poly variant by id | `fhir-gen generate Condition --scenario poly_onset_onsetAge --no-save` |
| Schema variants only (same as poly loop) | `fhir-gen generate Observation --variants --no-save` |
| Rotate scenarios in a batch | `fhir-gen generate Patient -n 27 --save` (cycles catalog) |

**Do not combine** `--scenarios`, `--scenarios-named-only`, `--variants`, and `--scenario` on one command.

---

### Named lifecycle scenarios (hand-crafted)

These apply **enricher post-processing** (realistic codes, references, clinical defaults) in addition to schema choices.

| Resource | Named count | Poly added by `--scenarios` | Total with `--scenarios` |
|----------|-------------|-----------------------------|--------------------------|
| `Patient` | 9 | 0 (deceased choice already in named set) | **9** |
| `Practitioner` | 4 | 0 | **4** |
| `Person` | 4 | 0 | **4** |

### Patient named scenarios (all 9)

| Scenario ID | Fields populated | Notes |
|-------------|------------------|-------|
| `alive_active` | `active: true` | No `deceasedBoolean` / `deceasedDateTime` |
| `alive_inactive` | `active: false` | Living, inactive account |
| **`deceased_boolean`** | **`deceasedBoolean: true`**, `active: false` | Mutually exclusive with `deceasedDateTime` |
| **`deceased_datetime`** | **`deceasedDateTime`** (after `birthDate`), `active: false` | Death timestamp; no `deceasedBoolean` |
| `multiple_birth_boolean` | `multipleBirthBoolean: true` | |
| `multiple_birth_integer` | `multipleBirthInteger` (2–4) | Birth order |
| `with_photo` | `photo[]` | Attachment |
| `with_link` | `link[]` | Requires another Patient in session |
| `with_communication` | `communication[]` | Preferred language |

### Practitioner / Person scenarios (4 each)

| Scenario ID | Fields populated |
|-------------|------------------|
| `active` | `active: true` |
| `inactive` | `active: false` |
| `deceased_boolean` | `deceasedBoolean: true`, `active: false` |
| `deceased_datetime` | `deceasedDateTime`, `active: false` |

### Important: `generate Patient -n 1` vs deceased scenarios

`fhir-gen generate Patient -n 1` uses **scenario index 0** (`alive_active`) only. It does **not** produce `deceasedDateTime` or `deceasedBoolean: true`.

To get a **deceased date/time** patient:

```powershell
fhir-gen --seed 42 generate Patient --scenario deceased_datetime --no-save --output deceased-dt-patient.json
```

To get **every** Patient named scenario (9 records; same as `--scenarios` for Patient):

```powershell
fhir-gen --seed 42 generate Patient --scenarios --no-save --output all-patient-scenarios.json
```

To rotate scenarios in a batch (patient 0 = alive_active, 3 = deceased_datetime, …):

```powershell
fhir-gen --seed 42 generate Patient -n 9 --save
```

### Scenario CLI reference

```powershell
# List catalogs
fhir-gen list-scenarios
fhir-gen list-scenarios Patient

# One specific scenario (repeat -n for copies)
fhir-gen --seed 100 generate Patient --scenario deceased_boolean -n 1 --save
fhir-gen --seed 101 generate Patient --scenario deceased_datetime -n 1 --save
fhir-gen --seed 102 generate Patient --scenario alive_inactive -n 5 --save

# Full Patient scenario set (9 resources)
fhir-gen --seed 200 generate Patient --scenarios --save

# Practitioner deceased variants
fhir-gen --seed 300 generate Practitioner --scenario deceased_datetime --no-save
fhir-gen --seed 301 generate Practitioner --scenarios --no-save --output practitioner-scenarios.json
```

### Python API (scenarios)

```python
from fhir_gen import ResourceGenerator

gen = ResourceGenerator(seed=42)
gen.generate("Organization", count=1)

# Single named scenario
deceased = gen.generate_scenario("Patient", "deceased_datetime")

# All Patient named scenarios
all_patients = gen.generate_scenarios("Patient", named_only=True)

# Observation: all 19 value/effective/instantiates variants
obs_variants = gen.generate_scenarios("Observation")

# Single poly variant (schema id)
cond = gen.generate_scenario("Condition", "poly_onset_onsetDateTime")

# Batch rotation (index i uses catalog[i % len(catalog)])
cohort = gen.generate("Patient", count=27)
```

---

### Schema polymorphic scenarios (49 resource types)

Any resource with a `value[x]`, `onset[x]`, `deceased[x]`, `effective[x]`, etc. group in the R5 schema gets **one scenario per variant** when you use `--scenarios` (poly ids look like `poly_value_valueQuantity`).

**Clinical / enriched types** (also have terminology-aware enrichers — recommended for demos):

| Resource | Choice groups | Variants | Example poly scenario IDs |
|----------|---------------|----------|---------------------------|
| `Observation` | `instantiates`, `effective`, `value` | **19** | `poly_value_valueQuantity`, `poly_value_valueString`, `poly_effective_effectivePeriod`, … |
| `FamilyMemberHistory` | `instantiates`, `born`, `age`, `deceased` | **13** | `poly_deceased_deceasedDate`, `poly_age_ageRange`, … |
| `Condition` | `onset`, `abatement` | **10** | `poly_onset_onsetDateTime`, `poly_abatement_abatementString`, … |
| `Procedure` | `instantiates`, `occurrence`, `reported` | **10** | `poly_occurrence_occurrenceDateTime`, … |
| `ServiceRequest` | `instantiates`, `quantity`, `occurrence` | **8** | `poly_quantity_quantityQuantity`, … |
| `AllergyIntolerance` | `onset` | **5** | `poly_onset_onsetDateTime`, … |
| `ClinicalImpression` | `effective`, `prognosis` | **4** | `poly_effective_effectivePeriod`, … |
| `MedicationAdministration` | `occurence` | **3** | `poly_occurence_occurenceDateTime`, … |
| `MedicationStatement` | `effective` | **3** | `poly_effective_effectivePeriod`, … |
| `DiagnosticReport` | `effective` | **2** | `poly_effective_effectiveDateTime`, … |
| `Immunization` | `occurrence` | **2** | `poly_occurrence_occurrenceDateTime`, … |
| `Goal` | `start` | **2** | `poly_start_startDate`, … |
| `RiskAssessment` | `occurrence` | **2** | `poly_occurrence_occurrenceDateTime`, … |
| `Task` | `instantiates` | **2** | `poly_instantiates_instantiatesCanonical`, … |
| `CarePlan` | `instantiates` | **2** | … |
| `Consent` | `source` | **2** | … |
| `ChargeItem` | `definition`, `occurrence` | **5** | … |
| `AuditEvent` | `occurred` | **2** | … |
| `DetectedIssue` | `identified` | **2** | … |
| `Invoice` | `period` | **2** | … |
| `NutritionOrder` | `instantiates` | **2** | … |
| `CoverageEligibilityRequest` | `serviced` | **2** | … |

**Full list (all 49 types with choice groups):**

| Resource | Groups | Variants | Enriched? |
|----------|--------|----------|-----------|
| Observation | 3 | 19 | Yes |
| FamilyMemberHistory | 4 | 13 | Yes |
| Condition | 2 | 10 | Yes |
| Procedure | 2 | 10 | Yes |
| ActivityDefinition | 3 | 9 | No |
| ServiceRequest | 3 | 8 | Yes |
| NutritionIntake | 3 | 6 | No |
| AllergyIntolerance | 1 | 5 | Yes |
| ChargeItem | 2 | 5 | Yes |
| DeviceRequest | 2 | 5 | No |
| ClinicalImpression | 2 | 4 | Yes |
| Contract | 2 | 4 | No |
| AdverseEvent | 1 | 3 | No |
| ArtifactAssessment | 1 | 3 | No |
| DeviceUsage | 1 | 3 | No |
| GuidanceResponse | 1 | 3 | No |
| MedicationAdministration | 1 | 3 | Yes |
| MedicationStatement | 1 | 3 | Yes |
| PlanDefinition | 1 | 3 | No |
| SupplyDelivery | 1 | 3 | No |
| SupplyRequest | 1 | 3 | No |
| AuditEvent | 1 | 2 | Yes |
| CarePlan | 1 | 2 | Yes |
| Communication | 1 | 2 | Yes |
| CommunicationRequest | 1 | 2 | No |
| Consent | 1 | 2 | Yes |
| CoverageEligibilityRequest | 1 | 2 | Yes |
| CoverageEligibilityResponse | 1 | 2 | No |
| DetectedIssue | 1 | 2 | Yes |
| DiagnosticReport | 1 | 2 | Yes |
| EventDefinition | 1 | 2 | No |
| GenomicStudy | 1 | 2 | No |
| Goal | 1 | 2 | Yes |
| Immunization | 1 | 2 | Yes |
| Invoice | 1 | 2 | Yes |
| Library | 1 | 2 | No |
| Measure | 1 | 2 | No |
| MessageDefinition | 1 | 2 | No |
| MessageHeader | 1 | 2 | No |
| NutritionOrder | 1 | 2 | Yes |
| Patient | 1 | 2 | Yes (covered by **named** deceased scenarios) |
| Person | 1 | 2 | No |
| Practitioner | 1 | 2 | Yes (covered by **named** deceased scenarios) |
| Provenance | 1 | 2 | No |
| RequestOrchestration | 1 | 2 | No |
| RiskAssessment | 1 | 2 | Yes |
| SpecimenDefinition | 1 | 2 | No |
| Task | 1 | 2 | Yes |
| Transport | 1 | 2 | No |

```powershell
# Full Observation variant matrix (19 documents)
fhir-gen --seed 50 generate Observation --scenarios --no-save --output observation-all-scenarios.json

# Full Condition onset/abatement matrix (10 documents)
fhir-gen --seed 51 generate Condition --scenarios --save

# FamilyMemberHistory: born/age/deceased variants (13 documents)
fhir-gen --seed 52 generate FamilyMemberHistory --scenarios --no-save
```

---

### Coverage map: `generate-many` bundles vs scenarios

Bundles in this doc **exercise** the following; use `--scenarios` on individual types when you need **every** choice variant in MongoDB or JSON exports.

| Bundle / scenario | Section | Resources |
|-------------------|---------|-----------|
| Healthcare 1–21 | [Healthcare workflow scenarios](#healthcare-workflow-scenarios) | All **84** types across registration, clinical, RCM, quality, research, audit |
| Industrial A–K | [Industrial scenarios](#industrial--enterprise-scenarios) | Hospital ops, payer, devices, supply chain, trials,interop, population health |
| Legacy bundles | [Multi-resource bundles](#multi-resource-bundles-generate-many) | Outpatient, ED, chronic disease, oncology, etc. |
| Polymorphic QA | [Generation scenarios](#generation-scenarios-full-coverage) | `--scenarios` / `--variants` per choice group |

---

### Not yet covered (gaps)

| Gap | Workaround |
|-----|------------|
| **`multipleBirth[x]`** not in schema `poly_groups` (parser groups `multipleBirthBoolean` incorrectly) | Use named scenarios `multiple_birth_boolean` / `multiple_birth_integer` on Patient |
| **Per-status exhaust** (every `Encounter.status`, every `Claim.status`, …) | Enrichers pick realistic codes; use `--seed` + volume or extend `scenarios.py` |
| **All 158 types** with named scenarios | Only 3 types have named lifecycle; other types use schema poly + enrichers |
| **R6 schema** | `--schema-version R6`; re-run `list-poly-groups` (counts may differ) |
| **Combine poly + named on Patient** | `--scenarios` = 9 named only (poly deceased deduped); use named ids explicitly |

---

### Scenario + MongoDB search

```powershell
fhir-gen --seed 42 generate Patient --scenarios --save
fhir-gen db-stats
fhir-gen search Patient --limit 20
```

Inspect JSON exports for `deceasedDateTime` vs `deceasedBoolean` to confirm coverage.

---

## Healthcare workflow scenarios

End-to-end **data generation** patterns for all 84 enricher-backed types. Use `--seed` for reproducible cohorts; `--save` loads MongoDB for `fhir-mql search` (see [pipeline](#generate--search-pipeline-fhir-mql)).

### 1. Patient registration, MPI & person index

```powershell
fhir-gen --seed 2001 generate-many Patient Person RelatedPerson Organization `
  --count Patient=40 --count Person=10 --count RelatedPerson=35 --count Organization=4 --save

fhir-gen --seed 2002 generate Patient --scenario deceased_datetime -n 5 --save
fhir-gen search Patient --limit 10
```

### 2. Provider directory, roles & network

```powershell
fhir-gen --seed 2101 generate-many Organization Practitioner PractitionerRole OrganizationAffiliation Endpoint HealthcareService Location `
  --count Organization=6 --count Practitioner=20 --count PractitionerRole=25 `
  --count OrganizationAffiliation=12 --count Endpoint=4 --count HealthcareService=8 --save
```

### 3. Facility, location & care episodes

```powershell
fhir-gen --seed 2201 generate-many Organization Location EpisodeOfCare Account Patient `
  --count Organization=4 --count Location=10 --count Patient=30 `
  --count EpisodeOfCare=15 --count Account=20 --save
```

### 4. Scheduling & access management

```powershell
fhir-gen --seed 2301 generate-many Patient Practitioner Organization Location Schedule Slot Appointment Encounter `
  --count Patient=25 --count Schedule=5 --count Slot=100 --count Appointment=90 --count Encounter=85 --save
```

### 5. Encounters, ward board & visit documents

```powershell
fhir-gen --seed 2401 generate-many Patient Practitioner Encounter Composition DocumentReference Observation `
  --count Patient=20 --count Encounter=40 --count Composition=15 --count DocumentReference=20 --count Observation=80 --save

fhir-gen --seed 2402 generate Composition -n 5 --no-save --output discharge-summaries.json
```

### 6. Problem list, allergies & clinical impressions

```powershell
fhir-gen --seed 2501 generate-many Patient Encounter Condition AllergyIntolerance ClinicalImpression FamilyMemberHistory BodyStructure `
  --count Patient=30 --count Condition=50 --count AllergyIntolerance=18 `
  --count ClinicalImpression=12 --count FamilyMemberHistory=10 --count BodyStructure=8 --save
```

### 7. Vitals, labs, imaging & specimens

```powershell
fhir-gen --seed 2601 generate-many Patient Encounter Observation DiagnosticReport ImagingStudy Specimen Procedure `
  --count Patient=25 --count Observation=200 --count DiagnosticReport=35 `
  --count ImagingStudy=15 --count Specimen=25 --count Procedure=20 --save

fhir-gen --seed 2602 generate Observation --scenarios --no-save --output observation-poly-matrix.json
```

### 8. Medication management (inpatient & outpatient)

```powershell
fhir-gen --seed 2701 generate-many Patient Practitioner Medication MedicationRequest MedicationAdministration MedicationDispense MedicationStatement DetectedIssue `
  --count Patient=30 --count MedicationRequest=60 --count MedicationAdministration=50 `
  --count MedicationDispense=35 --count MedicationStatement=40 --save
```

### 9. Orders, procedures & care coordination

```powershell
fhir-gen --seed 2801 generate-many Patient Practitioner Encounter ServiceRequest Procedure DeviceRequest RequestOrchestration CarePlan CareTeam Goal Task `
  --count Patient=25 --count ServiceRequest=30 --count Procedure=22 --count DeviceRequest=12 `
  --count RequestOrchestration=8 --count CarePlan=18 --count Task=35 --save
```

### 10. Devices, supplies & implants

```powershell
fhir-gen --seed 2901 generate-many Patient Organization Device DeviceUsage DeviceDispense SupplyRequest SupplyDelivery BiologicallyDerivedProduct `
  --count Patient=20 --count Device=15 --count DeviceUsage=18 --count DeviceDispense=12 `
  --count SupplyRequest=10 --count SupplyDelivery=10 --count BiologicallyDerivedProduct=6 --save
```

### 11. Immunizations & public health

```powershell
fhir-gen --seed 3001 generate-many Patient Practitioner Location Appointment Immunization ImmunizationRecommendation Observation `
  --count Patient=50 --count Immunization=48 --count ImmunizationRecommendation=40 --save
```

### 12. Nutrition, vision & specialty orders

```powershell
fhir-gen --seed 3101 generate-many Patient Practitioner Encounter NutritionOrder NutritionIntake VisionPrescription `
  --count Patient=20 --count NutritionOrder=12 --count NutritionIntake=25 --count VisionPrescription=10 --save
```

### 13. Eligibility, claims & revenue cycle

```powershell
fhir-gen --seed 3201 generate-many Patient Organization Practitioner Coverage CoverageEligibilityRequest CoverageEligibilityResponse Claim ClaimResponse ExplanationOfBenefit ChargeItem ChargeItemDefinition Invoice PaymentNotice PaymentReconciliation `
  --count Patient=25 --count Coverage=30 --count Claim=22 --count ExplanationOfBenefit=15 `
  --count ChargeItemDefinition=5 --count PaymentReconciliation=8 --save
```

### 14. Payer enrollment & plan catalog

```powershell
fhir-gen --seed 3301 generate-many Organization Patient Coverage EnrollmentRequest EnrollmentResponse InsurancePlan `
  --count Organization=4 --count Patient=20 --count EnrollmentRequest=12 --count InsurancePlan=6 --save
```

### 15. Quality reporting (eCQM / HEDIS-style)

```powershell
fhir-gen --seed 3401 generate-many Measure MeasureReport Patient Practitioner Organization `
  --count Measure=5 --count MeasureReport=30 --count Patient=25 --save
```

### 16. Research, genomics & safety

```powershell
fhir-gen --seed 3501 generate-many Organization ResearchStudy ResearchSubject Patient GenomicStudy AdverseEvent Group Condition `
  --count ResearchStudy=3 --count ResearchSubject=25 --count GenomicStudy=10 --count AdverseEvent=8 --save
```

### 17. Forms, PROs & patient-reported data

```powershell
fhir-gen --seed 3601 generate-many Questionnaire QuestionnaireResponse Patient Practitioner Encounter `
  --count Questionnaire=4 --count QuestionnaireResponse=30 --count Patient=20 --save
```

### 18. Privacy, consent, contracts & communications

```powershell
fhir-gen --seed 3701 generate-many Patient Organization Practitioner Consent Contract Communication Flag RiskAssessment `
  --count Patient=20 --count Consent=15 --count Contract=6 --count Communication=20 --save
```

### 19. Security audit & provenance

```powershell
fhir-gen --seed 3801 generate-many Patient Practitioner Organization Encounter AuditEvent Provenance Basic `
  --count Patient=15 --count AuditEvent=40 --count Provenance=25 --count Basic=10 --save
```

### 20. Full MQL sandbox (all 84 types, one each)

```powershell
fhir-gen --seed 4000 generate-many `
  Account AdverseEvent AllergyIntolerance Appointment AuditEvent Basic BiologicallyDerivedProduct BodyStructure `
  CarePlan CareTeam ChargeItem ChargeItemDefinition Claim ClaimResponse ClinicalImpression Communication Composition `
  Condition Consent Contract Coverage CoverageEligibilityRequest CoverageEligibilityResponse DetectedIssue Device `
  DeviceDispense DeviceRequest DeviceUsage DiagnosticReport DocumentReference Encounter Endpoint EnrollmentRequest `
  EnrollmentResponse EpisodeOfCare ExplanationOfBenefit FamilyMemberHistory Flag GenomicStudy Goal Group `
  HealthcareService Immunization ImmunizationRecommendation ImagingStudy InsurancePlan Invoice Location Measure `
  MeasureReport Medication MedicationAdministration MedicationDispense MedicationRequest MedicationStatement `
  NutritionIntake NutritionOrder Observation Organization OrganizationAffiliation Patient PaymentNotice `
  PaymentReconciliation Person Practitioner PractitionerRole Procedure Provenance Questionnaire QuestionnaireResponse `
  RelatedPerson RequestOrchestration ResearchStudy ResearchSubject RiskAssessment Schedule ServiceRequest Slot `
  Specimen Substance SupplyDelivery SupplyRequest Task VisionPrescription `
  --save
```

Verify counts after load:

```powershell
fhir-gen --seed 4000 db-stats
python -c "from fhir_gen.resolvers.dependency import MQL_SHIPPED_RESOURCES as R; print(len(R))"
```

### 21. Regression dataset + QA export

```powershell
fhir-gen --seed 5000 --db fhir_qa generate-many Patient Encounter Observation Condition --count Patient=10 --count Encounter=15 --count Observation=40 --count Condition=8 --save
fhir-gen --seed 5000 --db fhir_qa db-stats
pytest tests/test_mql_shipped_resources.py tests/test_mql_integration.py -q --no-cov
```

---

## Industrial & enterprise scenarios

Cross-industry patterns on the same **84-resource** stack (hospitals, payers, pharma, device manufacturers, public health, clinical research).

### A. Hospital operations & capacity

```powershell
fhir-gen --seed 6001 generate-many Organization Location Encounter Appointment Task Patient Practitioner `
  --count Encounter=80 --count Appointment=100 --count Task=50 --count Patient=60 --save
```

### B. Revenue cycle & patient accounting

```powershell
fhir-gen --seed 6101 generate-many Patient Organization Encounter ChargeItem Invoice Account Claim `
  --count ChargeItem=80 --count Invoice=15 --count Account=25 --count Claim=30 --save
```

### C. Payer: eligibility → claim → EOB

```powershell
fhir-gen --seed 6201 generate-many Patient Organization Coverage CoverageEligibilityRequest CoverageEligibilityResponse Claim ClaimResponse ExplanationOfBenefit PaymentNotice PaymentReconciliation `
  --count CoverageEligibilityRequest=15 --count CoverageEligibilityResponse=15 `
  --count ExplanationOfBenefit=12 --count PaymentReconciliation=10 --save
```

### D. Pharmacy & medication safety

```powershell
fhir-gen --seed 6301 generate-many Patient Medication MedicationRequest MedicationDispense DetectedIssue AdverseEvent `
  --count MedicationRequest=70 --count DetectedIssue=12 --count AdverseEvent=10 --save
```

### E. Medical device lifecycle (manufacturer / HTM)

```powershell
fhir-gen --seed 6401 generate-many Organization Device DeviceRequest DeviceUsage DeviceDispense Provenance Patient `
  --count Device=30 --count DeviceRequest=15 --count DeviceUsage=20 --count Provenance=15 --save
```

### F. Supply chain & blood bank

```powershell
fhir-gen --seed 6501 generate-many Organization Practitioner SupplyRequest SupplyDelivery BiologicallyDerivedProduct Specimen Patient `
  --count SupplyRequest=12 --count SupplyDelivery=12 --count BiologicallyDerivedProduct=8 --save
```

### G. Clinical trials & RWE

```powershell
fhir-gen --seed 6601 generate-many Organization ResearchStudy ResearchSubject Patient AdverseEvent GenomicStudy Condition `
  --count ResearchStudy=2 --count ResearchSubject=40 --count GenomicStudy=12 --save
```

### H. Quality & regulatory reporting

```powershell
fhir-gen --seed 6701 generate-many Measure MeasureReport AuditEvent Patient Organization `
  --count Measure=8 --count MeasureReport=50 --count AuditEvent=30 --save
```

### I. Interoperability hub (organizations & endpoints)

```powershell
fhir-gen --seed 6801 generate-many Organization OrganizationAffiliation Endpoint Provenance HealthcareService `
  --count OrganizationAffiliation=15 --count Endpoint=6 --count Provenance=20 --save
```

### J. Population health & cohort analytics

```powershell
fhir-gen --seed 6901 generate-many Patient Group Organization Observation Condition RiskAssessment ServiceRequest `
  --count Patient=120 --count Group=6 --count Observation=400 --count Condition=90 --count RiskAssessment=25 --save
```

### K. Load all 84 types for search/index demos

```powershell
fhir-gen --seed 7000 --db fhir_synthetic generate-many `
  Account AdverseEvent AllergyIntolerance Appointment AuditEvent Basic BiologicallyDerivedProduct BodyStructure `
  CarePlan CareTeam ChargeItem ChargeItemDefinition Claim ClaimResponse ClinicalImpression Communication Composition `
  Condition Consent Contract Coverage CoverageEligibilityRequest CoverageEligibilityResponse DetectedIssue Device `
  DeviceDispense DeviceRequest DeviceUsage DiagnosticReport DocumentReference Encounter Endpoint EnrollmentRequest `
  EnrollmentResponse EpisodeOfCare ExplanationOfBenefit FamilyMemberHistory Flag GenomicStudy Goal Group `
  HealthcareService Immunization ImmunizationRecommendation ImagingStudy InsurancePlan Invoice Location Measure `
  MeasureReport Medication MedicationAdministration MedicationDispense MedicationRequest MedicationStatement `
  NutritionIntake NutritionOrder Observation Organization OrganizationAffiliation Patient PaymentNotice `
  PaymentReconciliation Person Practitioner PractitionerRole Procedure Provenance Questionnaire QuestionnaireResponse `
  RelatedPerson RequestOrchestration ResearchStudy ResearchSubject RiskAssessment Schedule ServiceRequest Slot `
  Specimen Substance SupplyDelivery SupplyRequest Task VisionPrescription `
  --save
```

---

## Terminology & CodeSystems

Generated **enriched** resources use codes from `fhir_gen/hl7_codes/healthcare_codes.yaml`. The default schema file is packaged at `fhir_gen/schema/fhir.schema.v5.json` (not the repo root).

### Condition verification status (common question)

| Correct | Incorrect |
|---------|-----------|
| `http://terminology.hl7.org/CodeSystem/condition-ver-status` | `CodeSystem/condition-ver-status` (relative) |
| Codes: `confirmed`, `provisional`, `unconfirmed`, … | `http://hl7.org/fhir/ValueSet/condition-ver-status` as `Coding.system` |

`fhir-gen generate Condition` with enrichers sets `verificationStatus` and `clinicalStatus` from YAML sections `condition_verification_status` and `condition_clinical_status`.

### Verify terminology in generated output

```powershell
# Generate and inspect Condition codings
fhir-gen --seed 42 generate Condition -n 1 --no-save --output condition-sample.json

# Run terminology + MQL resource suites
pytest tests/test_terminology_validation.py tests/test_mql_shipped_resources.py -v --no-cov
```

### Regenerate terminology YAML

After editing `fhir_gen/hl7_codes/_build_healthcare_codes.py` (new sections, deduped codes):

```powershell
python fhir_gen/hl7_codes/_build_healthcare_codes.py
pytest tests/test_terminology_validation.py tests/test_codes_loader.py -v --no-cov
```

### Python API (terminology validation)

```python
from fhir_gen import ResourceGenerator
from fhir_gen.codes.validation import validate_resource_codings

gen = ResourceGenerator(seed=42)
gen.generate("Patient", count=1)
condition = gen.generate("Condition", count=1)[0]
assert not validate_resource_codings(condition, strict_registered=True)
```

---

## Single-resource generation (`generate`)

Dependencies (e.g. `Patient` before `Encounter`) are created automatically unless `--no-deps` is set.

### Patient lifecycle coverage (deceased & administrative variants)

```powershell
# All 9 Patient scenarios (recommended for QA / validator fixtures)
fhir-gen --seed 1100 generate Patient --scenarios --save

# Explicit deceased records
fhir-gen --seed 1101 generate Patient --scenario deceased_boolean -n 3 --save
fhir-gen --seed 1102 generate Patient --scenario deceased_datetime -n 3 --save

# Mixed living + deceased cohort (27 = 3 full scenario cycles)
fhir-gen --seed 1103 generate Patient -n 27 --save
```

### Administrative foundation

```powershell
fhir-gen --seed 1001 generate Patient -n 25 --save
fhir-gen --seed 1002 generate Practitioner -n 15 --save
fhir-gen --seed 1003 generate Organization -n 8 --save
fhir-gen --seed 1004 generate Location -n 12 --save
fhir-gen --seed 1005 generate HealthcareService -n 6 --save
```

### Ambulatory visit (encounter-centric)

```powershell
fhir-gen --seed 2001 generate Encounter -n 30 --save
fhir-gen --seed 2002 generate Appointment -n 40 --save
fhir-gen --seed 2003 generate ServiceRequest -n 18 --save
```

### Clinical documentation

```powershell
fhir-gen --seed 3001 generate Condition -n 45 --save
fhir-gen --seed 3002 generate Procedure -n 22 --save
fhir-gen --seed 3003 generate ClinicalImpression -n 10 --save
fhir-gen --seed 3004 generate FamilyMemberHistory -n 8 --save
```

### Vitals & laboratory (LOINC-enriched observations)

```powershell
fhir-gen --seed 4001 generate Observation -n 120 --save
fhir-gen --seed 4002 generate DiagnosticReport -n 35 --save
fhir-gen --seed 4003 generate Specimen -n 28 --save
fhir-gen --seed 4004 generate BodyStructure -n 5 --no-save --output body-structures.json
```

### Medication therapy

```powershell
fhir-gen --seed 5001 generate Medication -n 20 --save
fhir-gen --seed 5002 generate MedicationRequest -n 55 --save
fhir-gen --seed 5003 generate MedicationAdministration -n 40 --save
fhir-gen --seed 5004 generate MedicationDispense -n 30 --save
fhir-gen --seed 5005 generate MedicationStatement -n 25 --save
```

### Allergy & immunization safety

```powershell
fhir-gen --seed 6001 generate AllergyIntolerance -n 18 --save
fhir-gen --seed 6002 generate Immunization -n 50 --save
fhir-gen --seed 6003 generate DetectedIssue -n 6 --save
```

### Imaging & devices

```powershell
fhir-gen --seed 7001 generate ImagingStudy -n 15 --save
fhir-gen --seed 7002 generate Device -n 10 --save
fhir-gen --seed 7003 generate DeviceUsage -n 12 --save
```

### Care coordination & workflow

```powershell
fhir-gen --seed 8001 generate CarePlan -n 20 --save
fhir-gen --seed 8002 generate CareTeam -n 8 --save
fhir-gen --seed 8003 generate Goal -n 25 --save
fhir-gen --seed 8004 generate Task -n 35 --save
fhir-gen --seed 8005 generate DocumentReference -n 15 --save
```

### Financial / revenue cycle

```powershell
fhir-gen --seed 9001 generate Coverage -n 30 --save
fhir-gen --seed 9002 generate Claim -n 22 --save
fhir-gen --seed 9003 generate ClaimResponse -n 18 --save
fhir-gen --seed 9004 generate Invoice -n 10 --save
fhir-gen --seed 9005 generate ChargeItem -n 40 --save
fhir-gen --seed 9006 generate ExplanationOfBenefit -n 15 --save
fhir-gen --seed 9007 generate CoverageEligibilityRequest -n 12 --save
fhir-gen --seed 9008 generate CoverageEligibilityResponse -n 12 --save
fhir-gen --seed 9009 generate EnrollmentRequest -n 8 --save
fhir-gen --seed 9010 generate InsurancePlan -n 5 --save
fhir-gen --seed 9011 generate ChargeItemDefinition -n 4 --save
fhir-gen --seed 9012 generate PaymentNotice -n 10 --save
fhir-gen --seed 9013 generate PaymentReconciliation -n 8 --save
```

### Gap resources (interop, quality, devices, forms)

```powershell
fhir-gen --seed 9101 generate Composition -n 10 --save
fhir-gen --seed 9102 generate AdverseEvent -n 8 --save
fhir-gen --seed 9103 generate DeviceRequest -n 12 --save
fhir-gen --seed 9104 generate DeviceDispense -n 10 --save
fhir-gen --seed 9105 generate DeviceUsage -n 10 --save
fhir-gen --seed 9106 generate SupplyRequest -n 8 --save
fhir-gen --seed 9107 generate SupplyDelivery -n 8 --save
fhir-gen --seed 9108 generate BiologicallyDerivedProduct -n 6 --save
fhir-gen --seed 9109 generate OrganizationAffiliation -n 10 --save
fhir-gen --seed 9110 generate Endpoint -n 4 --save
fhir-gen --seed 9111 generate Provenance -n 15 --save
fhir-gen --seed 9112 generate Basic -n 10 --save
fhir-gen --seed 9113 generate Questionnaire -n 4 --save
fhir-gen --seed 9114 generate RequestOrchestration -n 6 --save
fhir-gen --seed 9115 generate NutritionIntake -n 15 --save
fhir-gen --seed 9116 generate VisionPrescription -n 8 --save
fhir-gen --seed 9117 generate Measure -n 5 --save
fhir-gen --seed 9118 generate MeasureReport -n 20 --save
fhir-gen --seed 9119 generate GenomicStudy -n 8 --save
fhir-gen --seed 9120 generate ImmunizationRecommendation -n 15 --save
fhir-gen --seed 9121 generate Person -n 10 --save
fhir-gen --seed 9122 generate BodyStructure -n 6 --save
```

### Export to JSON (no MongoDB)

```powershell
fhir-gen --seed 42 generate RiskAssessment -n 5 --no-save --output risk-assessments.json
fhir-gen --seed 42 generate QuestionnaireResponse -n 3 --no-save --output questionnaire-responses.json
fhir-gen --seed 42 generate Observation --variants --no-save --output observation-variants.json
```

### Schema-only (no dependency auto-generation)

```powershell
fhir-gen generate Patient -n 1 --no-deps --no-save
```

---

## Multi-resource bundles (`generate-many`)

Resources are generated in **dependency order** (e.g. `Patient` before `Encounter`, `Coverage` before `Claim`). Types without an explicit count default to **1**.

### Outpatient primary-care panel

Typical ambulatory chart: demographics, visit, problems, vitals, orders.

```powershell
fhir-gen --seed 101 generate-many Patient Practitioner Organization Location Encounter Condition Observation ServiceRequest `
  --count Patient=50 --count Practitioner=12 --count Organization=4 --count Location=6 `
  --count Encounter=80 --count Condition=60 --count Observation=200 --count ServiceRequest=25 `
  --save
```

### Emergency & acute presentation

```powershell
fhir-gen --seed 202 generate-many Patient Practitioner Organization Encounter Observation Procedure DiagnosticReport AllergyIntolerance `
  --count Patient=30 --count Encounter=45 --count Observation=150 --count Procedure=20 `
  --count DiagnosticReport=15 --count AllergyIntolerance=12 `
  --save
```

### Chronic disease management (diabetes / hypertension cohort)

```powershell
fhir-gen --seed 303 generate-many Patient Organization CareTeam CarePlan Goal Condition Observation MedicationRequest Task `
  --count Patient=40 --count CarePlan=35 --count Goal=50 --count Condition=55 `
  --count Observation=180 --count MedicationRequest=45 --count Task=30 `
  --save
```

### Medication reconciliation & administration

RxNorm-enriched `Medication` / `MedicationRequest` with linked patients and practitioners.

```powershell
fhir-gen --seed 404 generate-many Patient Practitioner Medication MedicationRequest MedicationAdministration MedicationDispense DetectedIssue `
  --count Patient=25 --count Practitioner=8 --count Medication=30 `
  --count MedicationRequest=70 --count MedicationAdministration=55 --count MedicationDispense=40 `
  --save
```

### Pre-operative & perioperative

```powershell
fhir-gen --seed 505 generate-many Patient Practitioner Organization Encounter Procedure Observation Specimen DiagnosticReport ServiceRequest `
  --count Patient=20 --count Encounter=25 --count Procedure=18 --count Observation=60 `
  --count Specimen=15 --count DiagnosticReport=12 --count ServiceRequest=10 `
  --save
```

### Oncology / imaging pathway

```powershell
fhir-gen --seed 606 generate-many Patient Organization Practitioner Encounter Condition ImagingStudy DiagnosticReport Observation CarePlan `
  --count Patient=15 --count Condition=20 --count ImagingStudy=12 --count DiagnosticReport=18 `
  --count Observation=90 --count CarePlan=10 `
  --save
```

### Immunization clinic day

```powershell
fhir-gen --seed 707 generate-many Patient Practitioner Organization Location Appointment Immunization Observation `
  --count Patient=60 --count Appointment=65 --count Immunization=58 --count Observation=40 `
  --save
```

### Scheduling & access

```powershell
fhir-gen --seed 808 generate-many Patient Practitioner Organization Location Schedule Slot Appointment Encounter `
  --count Patient=20 --count Schedule=4 --count Slot=80 --count Appointment=75 --count Encounter=70 `
  --save
```

### Population health & screening

```powershell
fhir-gen --seed 909 generate-many Patient Group Organization Observation Condition RiskAssessment ServiceRequest `
  --count Patient=100 --count Group=5 --count Observation=300 --count Condition=80 `
  --count RiskAssessment=25 --count ServiceRequest=40 `
  --save
```

### Revenue cycle & claims

Coverage and insurer must exist before claims; aligns with X12/FHIR claim use cases.

```powershell
fhir-gen --seed 1111 generate-many Patient Organization Practitioner Coverage Claim ClaimResponse Account ChargeItem `
  --count Patient=30 --count Organization=6 --count Coverage=35 `
  --count Claim=28 --count ClaimResponse=22 --count ChargeItem=50 `
  --save
```

### Research study enrollment

```powershell
fhir-gen --seed 1212 generate-many Organization ResearchStudy Patient Group Consent Observation Condition `
  --count Organization=3 --count ResearchStudy=2 --count Patient=45 --count Group=4 `
  --count Observation=100 --count Condition=30 `
  --save
```

### Public health reporting

```powershell
fhir-gen --seed 1313 generate-many Patient Organization Immunization Condition Observation DiagnosticReport DocumentReference `
  --count Patient=80 --count Immunization=120 --count Condition=40 --count Observation=200 `
  --count DocumentReference=25 `
  --save
```

### Device & home monitoring

```powershell
fhir-gen --seed 1414 generate-many Patient Practitioner Device DeviceUsage Observation CarePlan `
  --count Patient=25 --count Device=15 --count DeviceUsage=30 --count Observation=120 `
  --save
```

### Mental health & assessment

```powershell
fhir-gen --seed 1515 generate-many Patient Practitioner Encounter ClinicalImpression Observation CarePlan Goal QuestionnaireResponse `
  --count Patient=20 --count Encounter=30 --count ClinicalImpression=15 `
  --count Observation=40 --count CarePlan=12 --count QuestionnaireResponse=8 `
  --save
```

### Substance & allergy documentation

```powershell
fhir-gen --seed 1616 generate-many Patient AllergyIntolerance MedicationRequest Observation Encounter `
  --count Patient=15 --count AllergyIntolerance=20 --count MedicationRequest=25 --count Observation=50 `
  --save
```

### Large mixed hospital census (stress / search demo)

```powershell
fhir-gen --seed 2024 generate-many Patient Practitioner Organization Location Encounter Condition Observation Procedure MedicationRequest DiagnosticReport Claim `
  --count Patient=200 --count Practitioner=40 --count Organization=10 --count Location=15 `
  --count Encounter=350 --count Condition=280 --count Observation=1200 --count Procedure=90 `
  --count MedicationRequest=180 --count DiagnosticReport=60 --count Claim=45 `
  --save
```

### Export bundle to files (JSON per type)

```powershell
fhir-gen --seed 42 generate-many Patient Encounter Observation Condition `
  --count Patient=5 --count Encounter=8 --count Observation=20 --count Condition=6 `
  --no-save --output-dir ./output/clinical-bundle
```

### Brace-style counts (PowerShell-friendly)

```powershell
fhir-gen --seed 77 generate-many Patient Practitioner Organization Encounter Observation `
  --counts '{Patient:10,Encounter:20,Observation:50}' `
  --save
```

---

## Generate → search pipeline (fhir-mql)

After generating with `--save` into `fhir_synthetic` (or your `--db`), use [fhir-search-to-mql](../fhir-search-to-mql/CLI_COMMANDS.md) to index and query the same resource types.

```powershell
$URI = "mongodb://localhost:27017/"
$DB  = "fhir_synthetic"

# 1) Generate clinical + gap data (example: full 84-type sandbox)
fhir-gen --seed 4000 --mongo-uri $URI --db $DB generate-many Patient Encounter Observation Composition DeviceRequest MeasureReport `
  Account AdverseEvent Endpoint Provenance ExplanationOfBenefit Questionnaire `
  --count Patient=50 --count Encounter=80 --count Observation=200 --save

# Or load one of each shipped type (see Healthcare scenario 20)
fhir-gen --seed 4000 --db $DB generate-many Account Patient ... VisionPrescription --save

# 2) Denormalize for search (from fhir-search-to-mql repo)
fhir-mql indexes --all --uri $URI --db $DB
fhir-mql denormalize --all --uri $URI --db $DB --batch-size 500

# 3) Search examples (same DB)
fhir-mql search Patient "name=Smith&active=true" --uri $URI --db $DB --limit 10
fhir-mql search Composition "status=final&type=18842-5" --uri $URI --db $DB --limit 10
fhir-mql search DeviceRequest "patient=<id>&status=active" --uri $URI --db $DB --limit 10
fhir-mql search MeasureReport "status=complete&measure=<measure-id>" --uri $URI --db $DB --limit 10

# 4) Local stats
fhir-gen db-stats --db $DB
fhir-mql stats --all --uri $URI --db $DB
```

---

## Polymorphic variants (`--variants`)

`--variants` and `--scenarios` (without `--scenarios-named-only`) both emit **one resource per schema choice variant** for types without named lifecycle tables. For `Observation`, they produce the same 19 documents; `--variants` is kept for backward compatibility.

| Flag | Named lifecycle enricher | Schema `poly_*` variants |
|------|--------------------------|-------------------------|
| `--scenario deceased_datetime` | Yes | — |
| `--scenarios-named-only` | Yes | No |
| `--scenarios` | Yes (if type has named catalog) | Yes |
| `--variants` | No (schema engine only) | Yes |

```powershell
# Equivalent for Observation (19 variants)
fhir-gen --seed 1 generate Observation --variants --no-save --output observation-variants.json
fhir-gen --seed 1 generate Observation --scenarios --no-save --output observation-scenarios.json

# Condition: all onset + abatement branches (10)
fhir-gen --seed 3 generate Condition --variants --no-save --output condition-variants.json
```

Do **not** combine `--variants`, `--scenarios`, `--scenarios-named-only`, and `--scenario` on one command.

---

## Custom schema (R6 or project-specific)

Default R5 schema: `fhir_gen/schema/fhir.schema.v5.json` (shipped with the package). Optional R6 preview:

```powershell
fhir-gen --schema-version R6 generate Patient -n 3 --no-save
fhir-gen --schema-version R6 generate-many Patient Observation `
  --count Patient=5 --count Observation=10 --no-save
```

Environment variable (persistent):

```powershell
$env:FHIR_GEN_SCHEMA_PATH = ".\fhir_gen\schema\fhir.schema.v6.json"
fhir-gen generate Patient -n 1 --no-save
```

---

## MongoDB operations

After generation with `--save`, collections are named by resource type (e.g. `Patient`, `Observation`) unless `FHIR_GEN_MONGODB_COLLECTION_PREFIX` is set.

```powershell
# Counts per resource type
fhir-gen db-stats

# Patient search (family name, gender, identifier)
fhir-gen search Patient --limit 10
fhir-gen search Patient --limit 5

# Observation for a patient (LOINC 8867-4 = heart rate)
fhir-gen search Observation --patient-id <patient-id> --code 8867-4 --limit 20

# Conditions for a patient
fhir-gen search Condition --patient-id <patient-id> --status active --limit 10

# Drop one type or entire DB contents managed by fhir-gen
fhir-gen clear Patient --yes
fhir-gen clear --yes
```

Use `db-stats` output to copy real `id` values into `search` commands.

---

## Reproducible regression datasets

Same seed → same synthetic cohort (IDs, codes, references).

```powershell
# Dataset A — ambulatory
fhir-gen --seed 10001 generate-many Patient Encounter Observation --count Patient=10 --count Encounter=15 --count Observation=40 --save

# Dataset B — inpatient meds
fhir-gen --seed 10002 --db fhir_inpatient generate-many Patient MedicationRequest MedicationAdministration `
  --count Patient=8 --count MedicationRequest=20 --count MedicationAdministration=15 --save

# Dataset C — claims (separate database)
fhir-gen --seed 10003 --db fhir_billing generate-many Patient Organization Coverage Claim `
  --count Patient=12 --count Coverage=15 --count Claim=10 --save
```

---

## Industry alignment (what these commands exercise)

| Requirement | How fhir-gen satisfies it |
|-------------|---------------------------|
| Valid FHIR R5 structure | Schema-driven `ResourceGenerator` for all 158 types (`fhir_gen/schema/fhir.schema.v5.json`) |
| Terminology (LOINC, SNOMED, RxNorm, CVX, …) | 146 YAML sections + **84** MQL enrichers; `fhir_gen/codes/validation.py` |
| CodeSystem URL correctness | Absolute URIs; rejects ValueSet URLs and relative `CodeSystem/...` paths |
| Referential integrity | `ReferenceStore`, dependency order, repair pass, `tests/test_schema_reference_integrity.py` |
| Searchable synthetic data | MongoDB indexes on `id`, `subject.reference`, `identifier.value`, codes, status |
| Realistic clinical bundles | `generate-many` bundles below mirror common care paths |
| Lifecycle & choice coverage | `--scenarios` / `--scenario` (Patient deceased, active, multiple birth, …) |
| Polymorphic correctness | `--variants` for `value[x]` and similar schema choice groups |
| Field & datatype quality | Enrichers, `field_fill`, `tests/test_schema_field_validation.py` |
| Reproducibility | `--seed` / `FHIR_GEN_SEED` |
| Isolated environments | `--db`, `--mongo-uri`, optional collection prefix in `.env` |

---

## Enriched vs schema-only resources

**Enriched (84 MQL-aligned types)** — realistic codes, statuses, references, YAML terminology from `healthcare_codes.yaml`; registered in `fhir_gen/generators/resources/` (`ENRICHERS`):

| Module | Count | Examples |
|--------|-------|----------|
| `clinical.py` | 20 | Patient, Composition, AdverseEvent, BodyStructure, Person, ImmunizationRecommendation |
| `medication.py` | 6 | Medication, MedicationRequest, MedicationStatement, … |
| `workflow.py` | 23 | Appointment, Questionnaire, DeviceRequest, SupplyRequest, Provenance, … |
| `financial.py` | 15 | Coverage, Claim, ExplanationOfBenefit, PaymentReconciliation, InsurancePlan, … |
| `specialized.py` | 21 | DeviceUsage, Endpoint, Measure, MeasureReport, GenomicStudy, … |

Canonical list: `MQL_SHIPPED_RESOURCES` in `fhir_gen/resolvers/dependency.py`. **Named lifecycle scenarios** remain on Patient (9), Practitioner (4), Person (4) in `fhir_gen/generators/scenarios.py`.

**Also enriched (not in MQL shipped set):** `MedicationKnowledge` — available via `generate` but not in the 84-type search config set.

**Schema-only** (valid FHIR, no clinical enricher): remaining types among **158** R5 schema resources, e.g. `Subscription`, `SubscriptionStatus`.

```powershell
fhir-gen generate Subscription -n 2 --no-save
fhir-gen list-resources
```

---

## Quick troubleshooting

| Symptom | Fix |
|---------|-----|
| PowerShell `Missing expression after '--'` | Use one line or backtick `` ` ``, not `\` |
| `JSONDecodeError` on `--counts` | Use `--count Patient=10` or `{Patient:10,Encounter:20}` |
| Empty `db-stats` | Run with `--save`; check `FHIR_GEN_MONGODB_URI` / `--db` |
| Unknown resource | `fhir-gen list-resources` for exact PascalCase names |
| Old `fhir_Patient` collections | Set `FHIR_GEN_MONGODB_COLLECTION_PREFIX=fhir_` in `.env` |
| No `deceasedDateTime` on Patient | Use `--scenario deceased_datetime` or `--scenarios`, not `-n 1` alone |
| `Unknown scenario` | Run `fhir-gen list-scenarios <Type>` for valid ids (named + `poly_*`) |
| `--variants` vs `--scenarios` | Same poly output for clinical types; `--scenarios` also runs named enricher scenarios |
| Need every Observation `value[x]` | `fhir-gen generate Observation --scenarios` (19 docs) or `--variants` |
| Full matrix for all types | `fhir-gen list-scenarios --all-types` then `--scenarios` per resource |
| Invalid `Coding.system` on Condition | Expected: `http://terminology.hl7.org/CodeSystem/condition-ver-status`; run `pytest tests/test_terminology_validation.py` |
| Terminology YAML out of date | `python fhir_gen/hl7_codes/_build_healthcare_codes.py` then re-run tests |

---

## Development & test commands

```powershell
pip install -e ".[dev]"

# Full suite (includes coverage gate in pyproject)
pytest tests/

# MQL shipped resources (84 × generate, deps, enrichers, codings)
pytest tests/test_mql_shipped_resources.py -q --no-cov

# Integration: batch generate-many, dependency chains, store integrity
pytest tests/test_mql_integration.py -q --no-cov
pytest -m integration tests/ -q --no-cov

# Module enricher tests
pytest tests/test_clinical.py tests/test_workflow.py tests/test_financial.py `
  tests/test_specialized.py tests/test_medication.py -q --no-cov

# Terminology & schema quality
pytest tests/test_terminology_validation.py -v --no-cov
pytest tests/test_scenarios.py -v --no-cov
pytest tests/test_schema_reference_integrity.py -v --no-cov
pytest tests/test_schema_field_validation.py -v --no-cov
pytest tests/test_codes_loader.py -v --no-cov

ruff check fhir_gen tests
```

### CLI_COMMANDS E2E (healthcare 1–21 + industrial A–K)

See **[E2E_COMMANDS.md](E2E_COMMANDS.md)** (pytest, this repo) and **[E2E_COMBINED.md](E2E_COMBINED.md)** (`scripts/run_cli_e2e.py` with fhir-mql).

Each scenario uses one MongoDB database (`fhir_e2e_gen_<id>`) for both
fhir-gen and fhir-mql. Cleanup: `python scripts/drop_e2e_databases.py` (see
[E2E_COMBINED.md](E2E_COMBINED.md)). Minimal counts unless `E2E_FULL_COUNTS=1`.

```powershell
# In-memory: all 32 generate-many scenarios (no MongoDB)
pytest tests/e2e/test_cli_commands_e2e.py::TestGenerateManyScenariosApi -m e2e --no-cov -q

# CLI + MongoDB: one scenario
pytest tests/e2e/test_cli_commands_e2e.py::TestGenerateManyScenariosCli -m "e2e and mongodb" --no-cov -q

# Full runner (gen + optional pipeline in sibling fhir-search-to-mql)
python scripts/run_cli_e2e.py
python scripts/run_cli_e2e.py --section healthcare --gen-only
python scripts/run_cli_e2e.py --full-counts   # documented volumes (slow)
```

---

## See also

- [README.md](README.md) — install, architecture, environment variables
- [fhir-search-to-mql CLI_COMMANDS.md](../fhir-search-to-mql/CLI_COMMANDS.md) — search/denormalize for the same 84 resources
- `.env.example` — MongoDB, seed, collection prefix
