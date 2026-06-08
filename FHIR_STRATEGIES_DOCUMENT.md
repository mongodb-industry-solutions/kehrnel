# FHIR Strategies — Healthcare & Industry Blueprint

This document defines **multiple FHIR strategies** for Kehrnel: industry-standard use cases, storage models, configuration, operations, and implementation checklists. Use it as the source of truth when adding strategy packs under:

```
src/kehrnel/engine/strategies/fhir/<pack_name>/
```

**Reference implementation (shipped):** `fhir.clinical_cdr` — native FHIR R5 Clinical Data Repository with **fhir-gen** + **fhir-mql**.

**Shared engines (vendored, do not reimplement in strategy code):**

| Product | Python import | Role |
|---------|---------------|------|
| **fhir-gen** | `fhir_gen` | Synthetic FHIR generation, `FHIRMongoStore` |
| **fhir-mql** | `fhir_search_to_mql` | Denormalize `_search` / `_compartments`, FHIR search → MQL |

Install path: `src/kehrnel/engine/domains/fhir/libs/` · Kehrnel extra: `[fhir]`.

---

## 1. Kehrnel FHIR strategy anatomy

Every FHIR strategy is a **strategy pack** (`pack_format: strategy-pack/v1`).

### 1.1 Pack layout

```
src/kehrnel/engine/strategies/fhir/<pack_name>/
├── __init__.py              # optional submodule re-exports
├── _paths.py                # PACK_ROOT, SPEC_DIR, FHIR_LIBS_DIR
├── strategy.py              # entrypoint shim → scripts/strategy.py
├── README.md
├── specification/
│   ├── manifest.json        # id, entrypoint, ops, capabilities
│   ├── schema.json          # activation config JSON Schema
│   ├── defaults.json        # merged with activation config
│   ├── spec.json            # logical model / strategy metadata
│   ├── activate_dev.json    # sample POST /activate body
│   └── job_generate_*.json  # optional synthetic job samples
└── scripts/
    ├── bridge.py            # config merge, Mongo bindings, fhir-gen/mql clients
    ├── generation.py        # synthetic_generate_batch
    ├── denormalize.py       # fhir_denormalize
    ├── indexes.py           # fhir_ensure_indexes
    ├── query.py             # compile_query, execute_query, fhir_search
    ├── stats.py             # fhir_stats
    ├── watermark.py         # synthetic provenance tags
    ├── strategy.py          # FHIR*Strategy(StrategyPlugin)
    └── spike_*.py           # optional local smoke scripts
```

### 1.2 Naming conventions

| Item | Pattern | Example |
|------|---------|---------|
| Strategy ID | `fhir.<snake_case>` | `fhir.clinical_cdr` |
| Pack folder | `snake_case` | `clinical_cdr` |
| Entrypoint class | `FHIR<PascalCase>Strategy` | `FHIRClinicalCDRStrategy` |
| Entrypoint module | `kehrnel.engine.strategies.fhir.<pack>.strategy:Class` | `...clinical_cdr.strategy:FHIRClinicalCDRStrategy` |
| Docusaurus slug | `clinical-cdr` | `/strategies/fhir/clinical-cdr` |
| Domain (activation) | `fhir` (lowercase) | `"domain": "fhir"` |
| Mongo layout | `collections.mode: per_resource_type` | One collection per resource type |

### 1.3 Required activation config (baseline)

All strategies in this document **extend** the clinical CDR baseline unless noted:

```json
{
  "database": "<mongodb_database>",
  "schema_version": "R5",
  "collection_prefix": "",
  "collections": { "mode": "per_resource_type" },
  "search": {
    "enabled": true,
    "denormalize_on_generate": false,
    "auto_index": true,
    "config_dir": null,
    "compartment_definitions_dir": null
  },
  "generation": {
    "seed": 42,
    "use_enrichers": true,
    "watermark": { "enabled": true }
  }
}
```

**Bindings (required for generation/search against real MongoDB):**

```json
{
  "bindings": {
    "db": {
      "provider": "mongodb",
      "uri": "mongodb://localhost:27017",
      "database": "<same as config.database>"
    }
  },
  "allow_plaintext_bindings": true
}
```

### 1.4 Standard operations (inherit from clinical_cdr)

| Op | Kind | Purpose |
|----|------|---------|
| `synthetic_generate_batch` | synthetic | Batch generate via fhir-gen |
| `fhir_denormalize` | maintenance | Build `_search` / `_compartments` in place |
| `fhir_ensure_indexes` | maintenance | Mongo indexes from fhir-mql configs |
| `fhir_search` | query | Compile + execute FHIR search |
| `fhir_list_search_params` | query | List YAML search params for a type |
| `fhir_stats` | diagnostic | Counts, denorm coverage, search gaps |
| `negotiate_fhir_search` | extension | NL → search criteria (planned) |

**StrategyPlugin methods:** `compile_query`, `execute_query`, `validate_config`, `run_op`.

**HTTP surfaces:**

- `POST /v1/environments/{env}/activate` — bind strategy + MongoDB
- `POST /v1/environments/{env}/run` — `{ "domain": "fhir", "operation": "<op>", "payload": {} }`
- `POST /api/domains/fhir/search` — Bundle response (requires active `fhir.clinical_cdr` or env override `KEHRNEL_FHIR_STRATEGY_ID`)

### 1.5 Implementation patterns

| Pattern | When to use |
|---------|-------------|
| **Fork** `clinical_cdr` pack | New storage model, different ops, or major config schema |
| **Extend** via `defaults.json` + recipes | Same code path; different default DB name, resource sets, search focus |
| **Shared base module** | Extract `bridge.py` / `query.py` to `engine/strategies/fhir/_common/` when ≥3 packs duplicate logic |

---

## 2. Strategy catalog (summary)

| # | Strategy ID | Industry domain | HL7 / standard anchor | Status |
|---|-------------|-----------------|----------------------|--------|
| 1 | `fhir.clinical_cdr` | General clinical repository | FHIR R5 RESTful + US Core patterns | **Implemented (beta)** |
| 2 | `fhir.scheduling_exchange` | Access & scheduling | FHIR Scheduling, Da Vinci PDex scheduling | Planned |
| 3 | `fhir.quality_reporting` | Quality & population measures | eCQM, HEDIS, FHIR Measure/$evaluate-measure | Planned |
| 4 | `fhir.payer_administration` | Payer / financial | Da Vinci PAS, CRD, FHIR Financial Module | Planned |
| 5 | `fhir.clinical_research` | Clinical trials & registries | FHIR ResearchStudy, CDISC-aligned workflows | Planned |
| 6 | `fhir.population_health` | ACO / risk stratification | FHIR Group, RiskAssessment, HCC models | Planned |
| 7 | `fhir.pharmacy_formulary` | Medication management | FHIR Medication, RxNorm, NCPDP | Planned |
| 8 | `fhir.imaging_archive` | Radiology / imaging | FHIR ImagingStudy, DICOM mapping IG | Planned |
| 9 | `fhir.public_health` | Surveillance & immunization | PHIN VADS, FHIR Bulk Data, IPS | Planned |
| 10 | `fhir.dev_sandbox` | Developer / CI minimal corpus | Subset of R5 for fast tests | Planned |

### 2.1 fhir-mql shipped search configs (84 resources)

Kehrnel strategies **must only claim FHIR search** for resource types that have a bundled YAML in fhir-mql (`MQL_SHIPPED_RESOURCES`). Source of truth:

```
src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql/src/fhir_search_to_mql/configs/*.yaml
```

All **84** shipped types are generatable by fhir-gen. fhir-gen supports **158** resource schemas; the other **74** are generation-only until a YAML config lands in fhir-mql (see `PROMPTS_FHIR_MQL_GAP_RESOURCES.md` in the fhir-mql repo).

| Domain | Searchable resource types (fhir-mql YAML) |
|--------|---------------------------------------------|
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

**Not searchable today (common IG types — use substitutes in strategies):**

| Desired type | Substitute in recipes / search | Notes |
|--------------|-------------------------------|-------|
| `MedicationKnowledge` | `Medication` + `ChargeItemDefinition` + `InsurancePlan` | Formulary tier / coverage rules |
| `Library` | `Measure` | Measure definitions embed logic; no separate search YAML |
| `ImmunizationEvaluation` | `Immunization` + `ImmunizationRecommendation` | Forecast / due-date workflows |
| `Bundle` | N/A (ingest container) | Store as documents or expand on ingest; not a search target |
| `Task` (payer PA) | `Task` + `Claim` + `ServiceRequest` | `Task` **is** in the 84 — use for PAS workflows |

**Validation rule:** The union of each strategy's **searchable resource set** (below) must equal all **84** shipped types. Run `fhir_stats` after denormalize to confirm index + `_search` coverage per type.

### 2.2 Strategy ↔ resource ownership

Each planned strategy owns a **primary** subset for recipes, golden queries, and README documentation. `fhir.clinical_cdr` remains the **umbrella** for general clinical types not owned by a vertical pack. Overlap is intentional (e.g. `Patient` appears in every strategy).

| Strategy | Count | Searchable resources (fhir-mql) |
|----------|------:|--------------------------------|
| `fhir.clinical_cdr` | 52 | Patient, Person, Practitioner, PractitionerRole, RelatedPerson, Organization, OrganizationAffiliation, Location, Endpoint, HealthcareService, Encounter, EpisodeOfCare, Condition, AllergyIntolerance, Observation, DiagnosticReport, Specimen, ClinicalImpression, FamilyMemberHistory, BodyStructure, Composition, DocumentReference, Procedure, ServiceRequest, DeviceRequest, RequestOrchestration, CarePlan, CareTeam, Goal, Task, Device, DeviceUsage, DeviceDispense, SupplyRequest, SupplyDelivery, BiologicallyDerivedProduct, NutritionOrder, NutritionIntake, VisionPrescription, AdverseEvent, DetectedIssue, Flag, Consent, Contract, Questionnaire, QuestionnaireResponse, Communication, Provenance, AuditEvent, Basic, GenomicStudy, MedicationAdministration |
| `fhir.scheduling_exchange` | 11 | Schedule, Slot, Appointment, HealthcareService, Location, PractitionerRole, Account, Coverage, Encounter, Patient, Organization |
| `fhir.quality_reporting` | 10 | Measure, MeasureReport, Observation, Condition, Encounter, Procedure, MedicationRequest, Organization, Practitioner, Patient |
| `fhir.payer_administration` | 21 | Coverage, CoverageEligibilityRequest, CoverageEligibilityResponse, Claim, ClaimResponse, ExplanationOfBenefit, Invoice, ChargeItem, ChargeItemDefinition, PaymentNotice, PaymentReconciliation, InsurancePlan, EnrollmentRequest, EnrollmentResponse, Account, Organization, Patient, Practitioner, Encounter, ServiceRequest, Task |
| `fhir.clinical_research` | 13 | ResearchStudy, ResearchSubject, Consent, AdverseEvent, Specimen, Condition, Observation, Procedure, Patient, Encounter, GenomicStudy, Organization, Practitioner |
| `fhir.population_health` | 10 | Group, RiskAssessment, Measure, MeasureReport, Patient, Condition, Observation, CarePlan, Goal, EpisodeOfCare |
| `fhir.pharmacy_formulary` | 11 | Medication, MedicationRequest, MedicationDispense, MedicationAdministration, MedicationStatement, Substance, AllergyIntolerance, ChargeItemDefinition, Patient, Practitioner, Organization |
| `fhir.imaging_archive` | 9 | ImagingStudy, DiagnosticReport, ServiceRequest, Observation, Specimen, Patient, Encounter, Practitioner, Organization |
| `fhir.public_health` | 9 | Immunization, ImmunizationRecommendation, Condition, Observation, Patient, Organization, Location, Encounter, Practitioner |
| `fhir.dev_sandbox` | 3 | Patient, Observation, Encounter (inherits full 84 search path via same fhir-mql configs) |

**Full-84 corpus recipe** (clinical CDR / CI soak): use fhir-gen scenario `full84` or explicit counts for all 84 types, then `fhir_ensure_indexes` + `fhir_denormalize` without `resource_types` filter. Reference: fhir-mql `CLI_COMMANDS.md` scenario 20.

---

## 3. Strategy definitions (implementation-ready)

---

### 3.1 `fhir.clinical_cdr` — Clinical Data Repository (reference)

**Purpose:** General-purpose **native FHIR R5** persistence: synthetic corpora, FHIR search, per-type MongoDB collections. Default strategy for Kehrnel FHIR domain APIs.

| Field | Value |
|-------|-------|
| **Name** | FHIR Clinical CDR |
| **Maturity** | beta |
| **Entrypoint** | `kehrnel.engine.strategies.fhir.clinical_cdr.strategy:FHIRClinicalCDRStrategy` |
| **Capabilities** | ingest, query, search, synthetic, validate |
| **Target users** | Platform engineers, FHIR integrators, demo / PoC CDR |

**Storage model**

- One MongoDB collection per resource type (`Patient`, `Observation`, …).
- In-place denormalization: `_search.*`, `_compartments.*` on canonical documents.
- No shredded relational tables; documents are FHIR JSON.

**Default generation profile** (small dev job)

```json
{
  "resources": {
    "Patient": 50,
    "Practitioner": 20,
    "Organization": 10,
    "Encounter": 80,
    "Observation": 200,
    "Condition": 60,
    "Procedure": 40,
    "MedicationRequest": 30,
    "DiagnosticReport": 25,
    "AllergyIntolerance": 20
  }
}
```

**Search golden paths** (contract-tested)

- `Patient?gender=female`
- `Patient?name=<family>`
- `Schedule?active=true`
- `Slot?schedule=<Schedule/id>`
- `Appointment?status=booked`

**fhir-mql coverage:** owns **52** of **84** shipped types (see §2.2). All baseline ops (`fhir_search`, `fhir_denormalize`, `fhir_ensure_indexes`) accept any shipped type — vertical strategies do not gate fhir-mql configs.

**Full corpus recipe** (`clinical_full84` — soak / regression):

```json
{
  "resources": {
    "Patient": 50, "Person": 10, "Practitioner": 20, "PractitionerRole": 15,
    "Organization": 10, "OrganizationAffiliation": 5, "Location": 8, "Endpoint": 5,
    "HealthcareService": 5, "RelatedPerson": 15, "Encounter": 80, "EpisodeOfCare": 20,
    "Condition": 60, "AllergyIntolerance": 20, "Observation": 200, "DiagnosticReport": 25,
    "Specimen": 30, "ClinicalImpression": 15, "FamilyMemberHistory": 10, "BodyStructure": 10,
    "Composition": 20, "DocumentReference": 25, "Procedure": 40, "ServiceRequest": 30,
    "DeviceRequest": 15, "RequestOrchestration": 5, "CarePlan": 20, "CareTeam": 10, "Goal": 15,
    "Task": 20, "Device": 10, "DeviceUsage": 10, "DeviceDispense": 10, "SupplyRequest": 5,
    "SupplyDelivery": 5, "BiologicallyDerivedProduct": 5, "NutritionOrder": 10, "NutritionIntake": 10,
    "VisionPrescription": 5, "AdverseEvent": 10, "DetectedIssue": 10, "Flag": 5,
    "Consent": 10, "Contract": 5, "Questionnaire": 5, "QuestionnaireResponse": 15,
    "Communication": 10, "Provenance": 10, "AuditEvent": 5, "Basic": 5, "GenomicStudy": 5,
    "MedicationAdministration": 20
  }
}
```

Scheduling (`Schedule`, `Slot`, `Appointment`), payer, quality, research, pharmacy, imaging, and public-health types are owned by vertical strategies (§2.2) but remain searchable when present in the same database.

**Config extensions:** none beyond baseline schema.

**Files:** `src/kehrnel/engine/strategies/fhir/clinical_cdr/` (existing).

---

### 3.2 `fhir.scheduling_exchange` — Scheduling & Access

**Purpose:** **Appointment access** workflows: find schedules, free slots, book/reschedule appointments. Aligns with patient scheduling portals and payer scheduling (Da Vinci).

| Field | Value |
|-------|-------|
| **Name** | FHIR Scheduling Exchange |
| **Maturity** | planned |
| **Entrypoint** | `kehrnel.engine.strategies.fhir.scheduling_exchange.strategy:FHIRSchedulingExchangeStrategy` |
| **Capabilities** | query, search, synthetic, validate |
| **IG / standard** | [FHIR Scheduling](http://hl7.org/fhir/R5/scheduling.html), US Core Schedule/Slot/Appointment |

**Use cases**

- Patient self-scheduling (find `Slot` by `Schedule`, book `Appointment`).
- Provider directory + location-based search (`PractitionerRole`, `Location`, `HealthcareService`).
- Wait-list and overbooking simulation (synthetic scenarios).

**Primary resource types** (all searchable — fhir-mql YAML present)

| Tier | Resource types |
|------|----------------|
| Core | `Schedule`, `Slot`, `Appointment`, `Patient` |
| Directory | `Practitioner`, `PractitionerRole`, `Organization`, `Location`, `HealthcareService`, `Endpoint` |
| Context | `Encounter`, `Account`, `Coverage` |

**fhir-mql coverage:** **11** types (§2.2). Golden queries must use `Schedule`, `Slot`, `Appointment` search params only — no `MedicationKnowledge` or `AppointmentResponse` (not in shipped set).

**Default `defaults.json`**

```json
{
  "database": "fhir_scheduling",
  "schema_version": "R5",
  "collection_prefix": "",
  "collections": { "mode": "per_resource_type" },
  "search": { "enabled": true, "denormalize_on_generate": true, "auto_index": true },
  "generation": {
    "seed": 1001,
    "use_enrichers": true,
    "recipes": {
      "scheduling_corridor": {
        "resources": {
          "Organization": 5,
          "Location": 8,
          "Practitioner": 15,
          "HealthcareService": 5,
          "Schedule": 12,
          "Slot": 200,
          "Patient": 40,
          "Appointment": 60
        },
        "scenarios": [
          "Schedule:weekly_recurring",
          "Slot:booked_and_free_mix",
          "Appointment:booked_with_participants"
        ]
      }
    }
  },
  "scheduling": {
    "default_slot_duration_minutes": 30,
    "booking_horizon_days": 90,
    "timezone": "America/New_York"
  }
}
```

**Config schema extensions** (`schema.json` → `scheduling` object)

| Property | Type | Description |
|----------|------|-------------|
| `scheduling.default_slot_duration_minutes` | integer | Default slot length for synthetic slots |
| `scheduling.booking_horizon_days` | integer | How far ahead slots are generated |
| `scheduling.timezone` | string | IANA timezone for `Slot.start` / `Schedule` planning |

**Ops (baseline + extensions)**

| Op | Notes |
|----|-------|
| All baseline ops | Same as clinical_cdr |
| `scheduling_find_slots` | **New** — input: `schedule_id`, `start`, `end`, `status`; wraps `fhir_search` on `Slot` |
| `scheduling_book_appointment` | **New** — input: `slot_id`, `patient_id`, `participant[]`; creates/updates `Appointment`, marks `Slot` busy |
| `run_recipe` | **New** — run named `generation.recipes.*` entry |

**Search priorities** (all types in §2.2 — YAML shipped)

1. `Slot?schedule=&start=&status=`
2. `Appointment?patient=&date=&status=`
3. `Schedule?actor=&service-type=`

**Compartments:** `Patient` compartment for `Appointment?patient=`.

**Domain API extension (optional)**

- `POST /api/domains/fhir/scheduling/slots` — thin wrapper over `scheduling_find_slots`.

**Implementation checklist**

1. Copy `clinical_cdr/` → `scheduling_exchange/`.
2. Add `scheduling` to `schema.json` / `defaults.json`.
3. Implement `scheduling_find_slots`, `scheduling_book_appointment` in `scripts/scheduling_ops.py`.
4. Register ops in `manifest.json` and `_KNOWN_OPS`.
5. Add `specification/job_generate_scheduling.json` with `scheduling_corridor` recipe.
6. Contract tests: slot search, book flow, golden queries under `tests/contract/scheduling_exchange/fixtures/` (when pack exists).
7. Docusaurus: `docs/website/docs/strategies/fhir/scheduling-exchange/`.

**Reuse:** 90% of `bridge.py`, `generation.py`, `query.py` unchanged.

---

### 3.3 `fhir.quality_reporting` — eCQM & Measure Reporting

**Purpose:** **Quality measurement** corpora: `Measure`, `MeasureReport`, clinical evidence (`Observation`, `Condition`, `Encounter`) for numerator/denominator testing.

| Field | Value |
|-------|-------|
| **Name** | FHIR Quality Reporting |
| **Maturity** | planned |
| **Entrypoint** | `kehrnel.engine.strategies.fhir.quality_reporting.strategy:FHIRQualityReportingStrategy` |
| **Capabilities** | query, search, synthetic, validate |
| **IG / standard** | FHIR Measure / MeasureReport, CMS eCQM, HEDIS MY measures |

**Use cases**

- Synthetic patients with measure-relevant clinical facts (HbA1c, BP, screenings).
- Store and search `MeasureReport` by `measure`, `period`, `status`.
- Pipeline hook for future `$evaluate-measure` (external engine; store results only).

**Primary resource types** (all searchable — fhir-mql YAML present)

| Tier | Resource types |
|------|----------------|
| Measure layer | `Measure`, `MeasureReport` |
| Clinical evidence | `Patient`, `Encounter`, `Condition`, `Observation`, `Procedure`, `MedicationRequest` |
| Attribution | `Organization`, `Practitioner` |

**fhir-mql coverage:** **10** types (§2.2). `Library` is **not** in the shipped 84 — embed measure logic in `Measure` resources or store Libraries as documents without FHIR search until a YAML config is added.

**Default generation recipe `hedis_diabetes_bundle`**

```json
{
  "resources": {
    "Patient": 100,
    "Encounter": 150,
    "Condition": 80,
    "Observation": 400,
    "Measure": 5,
    "MeasureReport": 20
  },
  "scenarios": [
    "Condition:type2_diabetes",
    "Observation:hbA1c_panel",
    "MeasureReport:proportion_completed"
  ]
}
```

**Config extensions**

```json
{
  "quality": {
    "default_measure_period": "2025",
    "reporting_organization_ref": "Organization/quality-org-1",
    "evidence_profile": "us-core"
  }
}
```

| Property | Type | Description |
|----------|------|-------------|
| `quality.default_measure_period` | string | Reporting period label |
| `quality.reporting_organization_ref` | string | Default `MeasureReport.reporter` |
| `quality.evidence_profile` | string | `us-core` \| `international` — guides fhir-gen scenario selection |

**Ops extensions**

| Op | Description |
|----|-------------|
| `quality_generate_measure_reports` | Generate `MeasureReport` for existing `Measure` + patient cohort |
| `quality_list_gaps` | Compare `fhir_stats` with required types for active measures |

**Search priorities**

- `MeasureReport?measure=&period=&status=`
- `Observation?patient=&code=` (LOINC vitals/labs)
- `Condition?patient=&category=`

**Implementation checklist**

1. New pack `quality_reporting/`.
2. `generation.recipes` for measure-aligned scenarios (coordinate with fhir-gen scenario IDs).
3. `quality_generate_measure_reports` op — may use fhir-gen `MeasureReport` generator only initially.
4. Golden tests: MeasureReport search, Observation code filters.
5. Document mapping table: measure → required resource types (in pack README).

---

### 3.4 `fhir.payer_administration` — Payer & Claims

**Purpose:** **Payer administrative** data: coverage, claims, prior auth, EOB. Supports Da Vinci PAS/CRD-style demos without building a full claims adjudication engine.

| Field | Value |
|-------|-------|
| **Name** | FHIR Payer Administration |
| **Maturity** | planned |
| **Entrypoint** | `kehrnel.engine.strategies.fhir.payer_administration.strategy:FHIRPayerAdministrationStrategy` |
| **Capabilities** | query, search, synthetic, validate |
| **IG / standard** | FHIR Financial Module, Da Vinci PAS STU |

**Use cases**

- Member coverage verification (`Coverage`, `Patient`, `Organization` payer).
- Claim submission storage (`Claim`, `ClaimResponse`).
- Prior authorization artifacts (`Claim`, `Task`, `ServiceRequest`, `CoverageEligibilityRequest` / `CoverageEligibilityResponse`).

**Primary resource types** (all searchable — fhir-mql YAML present)

| Tier | Resource types |
|------|----------------|
| Member & coverage | `Patient`, `Organization`, `Coverage`, `InsurancePlan`, `Account` |
| Eligibility (Da Vinci CRD) | `CoverageEligibilityRequest`, `CoverageEligibilityResponse` |
| Claims & payment | `Claim`, `ClaimResponse`, `ExplanationOfBenefit`, `Invoice`, `ChargeItem`, `ChargeItemDefinition`, `PaymentNotice`, `PaymentReconciliation` |
| Enrollment | `EnrollmentRequest`, `EnrollmentResponse` |
| Clinical context | `Encounter`, `Practitioner`, `ServiceRequest`, `Task` |

**fhir-mql coverage:** **21** types (§2.2) — full FHIR Financial Module subset in the shipped set.

**Default database:** `fhir_payer`

**Config extensions**

```json
{
  "payer": {
    "default_payer_org_id": "Organization/payer-001",
    "line_of_business": "commercial",
    "adjudication_mode": "store_only"
  }
}
```

| Property | Values | Description |
|----------|--------|-------------|
| `payer.line_of_business` | commercial, medicare, medicaid | Influences synthetic coverage templates |
| `payer.adjudication_mode` | store_only, stub_rules | `store_only` = no claim math; persist FHIR only |

**Ops extensions**

| Op | Description |
|----|-------------|
| `payer_verify_coverage` | Search `Coverage` by `patient` + `status=active` |
| `payer_submit_claim` | Validate + insert `Claim` (stub adjudication returns `ClaimResponse`) |

**Search priorities**

- `Coverage?patient=&status=`
- `Claim?patient=&created=`
- `ExplanationOfBenefit?patient=&status=`

**Storage note:** Same per-type layout; consider `collection_prefix: "payer_"` for multi-tenant demos.

---

### 3.5 `fhir.clinical_research` — Trials & Registries

**Purpose:** **Clinical research** datasets: studies, subjects, consents, protocol-driven encounters and observations.

| Field | Value |
|-------|-------|
| **Name** | FHIR Clinical Research |
| **Maturity** | planned |
| **Entrypoint** | `kehrnel.engine.strategies.fhir.clinical_research.strategy:FHIRClinicalResearchStrategy` |
| **Capabilities** | query, search, synthetic, validate |
| **IG / standard** | FHIR ResearchStudy, ResearchSubject, mCODE for oncology registries |

**Primary resource types** (all searchable — fhir-mql YAML present)

| Tier | Resource types |
|------|----------------|
| Study & subject | `ResearchStudy`, `ResearchSubject`, `Consent` |
| Clinical facts | `Patient`, `Encounter`, `Condition`, `Observation`, `Procedure`, `AdverseEvent` |
| Biospecimen / omics | `Specimen`, `GenomicStudy` |
| Attribution | `Organization`, `Practitioner` |

**fhir-mql coverage:** **13** types (§2.2). `MedicationAdministration` for trial dosing is searchable via `fhir.clinical_cdr` overlap or add to research recipes explicitly.

**Default recipe `oncology_registry_slice`**

```json
{
  "resources": {
    "ResearchStudy": 3,
    "ResearchSubject": 30,
    "Patient": 30,
    "Condition": 30,
    "Observation": 120,
    "Procedure": 25
  },
  "scenarios": ["ResearchSubject:enrolled", "Condition:primary_cancer", "Observation:tumor_marker"]
}
```

**Config extensions**

```json
{
  "research": {
    "default_study_status": "active",
    "require_consent": true,
    "registry_profile": "mcode"
  }
}
```

**Ops extensions**

| Op | Description |
|----|-------------|
| `research_enroll_subject` | Create `ResearchSubject` linked to `Patient` + `ResearchStudy` |
| `research_study_cohort_search` | Compartment / chained search on subjects by study |

**Compartments:** `ResearchSubject` linked to `Patient` compartment for subject queries.

---

### 3.6 `fhir.population_health` — Population & Risk

**Purpose:** **Population health** and risk stratification: cohorts (`Group`), risk scores, care gaps at population level.

| Field | Value |
|-------|-------|
| **Name** | FHIR Population Health |
| **Maturity** | planned |
| **Entrypoint** | `kehrnel.engine.strategies.fhir.population_health.strategy:FHIRPopulationHealthStrategy` |
| **Capabilities** | query, search, synthetic, validate |
| **IG / standard** | FHIR Group, RiskAssessment, HCC / RAF models (logical, not actuarial engine) |

**Primary resource types** (all searchable — fhir-mql YAML present)

| Tier | Resource types |
|------|----------------|
| Cohort & risk | `Group`, `RiskAssessment` |
| Clinical panel | `Patient`, `Condition`, `Observation`, `CarePlan`, `Goal`, `EpisodeOfCare` |
| Quality overlay | `Measure`, `MeasureReport` |

**fhir-mql coverage:** **10** types (§2.2). Cohort materialization uses `Group` + chained `_has` searches on `Condition` / `Observation`.

**Config extensions**

```json
{
  "population": {
    "default_cohort_id": "Group/chronic-panel-1",
    "risk_model": "hcc_v28_stub",
    "panel_size_target": 5000
  }
}
```

**Ops extensions**

| Op | Description |
|----|-------------|
| `population_build_cohort` | Materialize `Group.member` from search criteria (batch) |
| `population_risk_summary` | Aggregate counts by risk category from `RiskAssessment` |

**Search priorities**

- `Group?identifier=`
- `RiskAssessment?patient=&method=`
- `Patient?active=true` + `_has` chains to `Condition`

---

### 3.7 `fhir.pharmacy_formulary` — Medication & Formulary

**Purpose:** **Medication knowledge**, formulary tiers, prescriptions, and dispensations for pharmacy/clinical decision support demos.

| Field | Value |
|-------|-------|
| **Name** | FHIR Pharmacy Formulary |
| **Maturity** | planned |
| **Entrypoint** | `kehrnel.engine.strategies.fhir.pharmacy_formulary.strategy:FHIRPharmacyFormularyStrategy` |
| **Capabilities** | query, search, synthetic, validate |
| **IG / standard** | FHIR Medication, RxNorm, NCPDP SCRIPT patterns |

**Primary resource types** (all searchable — fhir-mql YAML present)

| Tier | Resource types |
|------|----------------|
| Drug identity | `Medication`, `Substance` |
| Orders & fulfillment | `MedicationRequest`, `MedicationDispense`, `MedicationAdministration`, `MedicationStatement` |
| Safety | `AllergyIntolerance` |
| Formulary / billing | `ChargeItemDefinition`, `InsurancePlan` |
| Actors | `Patient`, `Practitioner`, `Organization` |

**fhir-mql coverage:** **11** types (§2.2). `MedicationKnowledge` is **not** in the shipped 84 — model formulary tiers with `ChargeItemDefinition` + `Medication` code bindings and `InsurancePlan` network rules.

**Config extensions**

```json
{
  "pharmacy": {
    "formulary_charge_item_definition": "ChargeItemDefinition/formulary-2025",
    "rxnorm_enrichment": true,
    "default_route": "oral"
  }
}
```

**Ops extensions**

| Op | Description |
|----|-------------|
| `pharmacy_check_interactions` | Stub: search `AllergyIntolerance` + `MedicationRequest` for patient |
| `pharmacy_formulary_search` | Search `Medication?code=` and `ChargeItemDefinition?identifier=` |

**Search priorities**

- `MedicationRequest?patient=&status=`
- `Medication?code=`
- `MedicationDispense?patient=`
- `ChargeItemDefinition?identifier=`

---

### 3.8 `fhir.imaging_archive` — Imaging & Diagnostics

**Purpose:** **Radiology / imaging** workflow storage: imaging studies, diagnostic reports, service requests, performers.

| Field | Value |
|-------|-------|
| **Name** | FHIR Imaging Archive |
| **Maturity** | planned |
| **Entrypoint** | `kehrnel.engine.strategies.fhir.imaging_archive.strategy:FHIRImagingArchiveStrategy` |
| **Capabilities** | query, search, synthetic, validate |
| **IG / standard** | FHIR ImagingStudy, DICOM FHIR mapping IG |

**Primary resource types** (all searchable — fhir-mql YAML present)

| Tier | Resource types |
|------|----------------|
| Imaging | `ImagingStudy`, `DiagnosticReport`, `ServiceRequest` |
| Context | `Patient`, `Encounter`, `Practitioner`, `Organization` |
| Supporting | `Observation` (measurements), `Specimen` |

**fhir-mql coverage:** **9** types (§2.2). DICOM binaries stay external; search `ImagingStudy?identifier=` (accession) and `DiagnosticReport?imaging-study=`.

**Config extensions**

```json
{
  "imaging": {
    "default_modality": "CT",
    "pacs_ae_title": "KEHRNEL_PACS_STUB",
    "store_dicom_uids": true
  }
}
```

**Ops extensions**

| Op | Description |
|----|-------------|
| `imaging_study_by_accession` | Search `ImagingStudy?identifier=` |
| `imaging_link_report` | Associate `DiagnosticReport` with `ImagingStudy` refs |

**Note:** Binary DICOM objects stay external; store FHIR metadata + `ImagingStudy.series.instance.uid` only unless Blob adapter added later.

---

### 3.9 `fhir.public_health` — Surveillance & Immunization

**Purpose:** **Public health** reporting: immunizations, notifiable conditions, outbreak clusters. Supports bulk export patterns conceptually (implementation via ops, not FHIR $export server required for v1).

| Field | Value |
|-------|-------|
| **Name** | FHIR Public Health |
| **Maturity** | planned |
| **Entrypoint** | `kehrnel.engine.strategies.fhir.public_health.strategy:FHIRPublicHealthStrategy` |
| **Capabilities** | query, search, synthetic, validate, ingest |
| **IG / standard** | PHIN VADS, FHIR Bulk Data, IPS |

**Primary resource types** (all searchable — fhir-mql YAML present)

| Tier | Resource types |
|------|----------------|
| Immunization | `Immunization`, `ImmunizationRecommendation` |
| Surveillance | `Condition`, `Observation` |
| Reporting context | `Patient`, `Organization`, `Location`, `Encounter`, `Practitioner` |

**fhir-mql coverage:** **9** types (§2.2). `ImmunizationEvaluation` is **not** shipped — use `Immunization` status + `ImmunizationRecommendation` for forecast/due-date queries. `Bundle` is an **ingest envelope** only (expand into typed resources on `ph_submit_bundle`; no FHIR search YAML).

**Config extensions**

```json
{
  "public_health": {
    "jurisdiction": "US-CA",
    "notifiable_condition_codes": ["840539006"],
    "immunization_schedule": "cdc_child"
  }
}
```

**Ops extensions**

| Op | Description |
|----|-------------|
| `ph_submit_bundle` | Ingest transaction `Bundle` of reporting resources |
| `ph_immunization_coverage` | Aggregate `Immunization` counts by vaccine code |

**Search priorities**

- `Immunization?patient=&status=`
- `ImmunizationRecommendation?patient=&status=`
- `Condition?code=` (notifiable conditions)
- `Observation?patient=&code=` (surveillance labs)

---

### 3.10 `fhir.dev_sandbox` — Minimal Developer Sandbox

**Purpose:** **Fast CI / local dev** with tiny corpora and aggressive `denormalize_on_generate`. Subset of clinical CDR—not a separate domain semantics.

| Field | Value |
|-------|-------|
| **Name** | FHIR Dev Sandbox |
| **Maturity** | planned |
| **Entrypoint** | `kehrnel.engine.strategies.fhir.dev_sandbox.strategy:FHIRDevSandboxStrategy` |
| **Capabilities** | query, search, synthetic |
| **Implementation** | Thin wrapper: inherit `FHIRClinicalCDRStrategy`, override `defaults.json` only **or** symlink pack spec |

**Default `defaults.json`**

```json
{
  "database": "fhir_dev_sandbox",
  "schema_version": "R5",
  "collections": { "mode": "per_resource_type" },
  "search": { "enabled": true, "denormalize_on_generate": true, "auto_index": true },
  "generation": { "seed": 1, "use_enrichers": false, "watermark": { "enabled": false } }
}
```

**Default job (3 searchable types only)**

```json
{ "resources": { "Patient": 5, "Observation": 10, "Encounter": 5 } }
```

**fhir-mql coverage:** generates **3** types; search/index/denorm still use the full **84** YAML configs when other types are loaded into the same DB. Keeps CI fast while reusing `clinical_cdr` bridge code.

**Use:** Contract test environments, `spike_generate_and_search` tutorials, CI under 30s.

---

## 4. Cross-strategy design rules

### 4.1 MongoDB database isolation

| Strategy | Recommended default DB | Notes |
|----------|---------------------|-------|
| clinical_cdr | `fhir_synthetic` | General demos |
| scheduling_exchange | `fhir_scheduling` | Slot volume can be large |
| quality_reporting | `fhir_quality` | Many Observations |
| payer_administration | `fhir_payer` | Financial resources |
| clinical_research | `fhir_research` | |
| population_health | `fhir_pophealth` | |
| pharmacy_formulary | `fhir_pharmacy` | |
| imaging_archive | `fhir_imaging` | |
| public_health | `fhir_public_health` | |
| dev_sandbox | `fhir_dev_sandbox` | Ephemeral / drop after CI |

**Never mix** per-type collections from two strategies in one database without `collection_prefix`.

### 4.2 fhir-mql coverage gaps

fhir-gen supports **158** resource schemas; fhir-mql ships **84** search YAML configs (`MQL_SHIPPED_RESOURCES`). The two lists are kept in sync for the shared set via `fhir_gen/resolvers/dependency.py` and `fhir_search_to_mql/resolvers/dependency.py`.

Each strategy README must list:

1. **Owned searchable types** — from §2.2 for that pack
2. **Generation recipe types** — subset used in `defaults.json` / job JSON
3. **Cross-strategy types** — clinical CDR types searchable when co-located in one DB
4. **Known non-searchable IG types** — substitutes from §2.1 table (`MedicationKnowledge` → `Medication` + `ChargeItemDefinition`, etc.)

**CLI checks:**

```powershell
fhir-mql resources
python -c "from fhir_search_to_mql.resolvers.dependency import MQL_SHIPPED_RESOURCES; print(len(MQL_SHIPPED_RESOURCES))"
```

Run `fhir_stats` after denormalize to surface per-collection gaps (`search_resource_types` vs documents present).

### 4.3 Synthetic watermark

When `generation.watermark.enabled` is true, resources get Kehrnel synthetic provenance (`meta.tag`, extension). **Disable** for strategies that simulate production feeds (`payer_administration` prod-like mode).

### 4.4 Domain search API routing

`POST /api/domains/fhir/search` uses `KEHRNEL_FHIR_STRATEGY_ID` (default `fhir.clinical_cdr`). Options for multi-strategy:

1. **Per-environment activation** — only one active FHIR strategy per env; API uses that strategy's compile path.
2. **Future:** `?strategy_id=` query param (not implemented; document as roadmap).

### 4.5 Compartments

Use FHIR R5 compartment definitions bundled in fhir-mql for:

- `Patient` — clinical, scheduling, payer member context
- `Encounter` — episodic queries
- `Practitioner` — scheduling directory

Strategy-specific ops should pass `compartment: { "type": "Patient", "id": "..." }` into `fhir_search`.

---

## 5. Manifest template (copy for new packs)

```json
{
  "id": "fhir.<pack_name>",
  "name": "FHIR <Display Name>",
  "version": "0.1.0",
  "maturity": "alpha",
  "pack_format": "strategy-pack/v1",
  "spec": { "path": "spec.json", "version": "1.0" },
  "domain": "FHIR",
  "summary": "<one line>",
  "description": "<paragraph>",
  "config": {
    "strategy": {
      "schema": "schema.json",
      "defaults": "defaults.json",
      "description": "<pack-specific config>"
    }
  },
  "capabilities": ["query", "search", "synthetic", "validate"],
  "entrypoint": "kehrnel.engine.strategies.fhir.<pack_name>.strategy:FHIR<Class>Strategy",
  "ui": {
    "tags": ["FHIR", "<tag>"],
    "domain_badge": "FHIR",
    "docs": "/guide/docs/strategies/fhir/<kebab-name>"
  },
  "ops": [],
  "adapters": { "storage": ["mongo"] }
}
```

Copy `ops` array from `clinical_cdr/specification/manifest.json` and append pack-specific ops.

---

## 6. Activation template

```json
{
  "strategy_id": "fhir.<pack_name>",
  "version": "0.1.0",
  "domain": "fhir",
  "config": {},
  "bindings": {
    "db": {
      "provider": "mongodb",
      "uri": "mongodb://localhost:27017",
      "database": "<from defaults.database>"
    }
  },
  "allow_plaintext_bindings": true
}
```

Merge `specification/defaults.json` at activation time; override only what differs per environment.

---

## 7. Implementation roadmap

| Phase | Strategies | Effort | Dependency |
|-------|------------|--------|------------|
| **P0 (done)** | `fhir.clinical_cdr` | — | fhir-gen, fhir-mql vendored |
| **P1** | `fhir.dev_sandbox` | 1–2 days | defaults-only fork |
| **P2** | `fhir.scheduling_exchange` | 1–2 weeks | fhir-gen scheduling scenarios, golden tests |
| **P3** | `fhir.quality_reporting` | 2 weeks | MeasureReport scenarios |
| **P4** | `fhir.payer_administration` | 2–3 weeks | financial resource generators |
| **P5** | `fhir.pharmacy_formulary` | 1–2 weeks | Medication + ChargeItemDefinition recipes |
| **P6** | `fhir.clinical_research` | 2 weeks | ResearchStudy / ResearchSubject generators |
| **P7** | `fhir.population_health` | 2 weeks | Group materialization op |
| **P8** | `fhir.imaging_archive` | 1–2 weeks | ImagingStudy golden queries (YAML shipped) |
| **P9** | `fhir.public_health` | 2 weeks | Bundle ingest op |

**Shared refactor (before P3):** extract `engine/strategies/fhir/_common/` from `clinical_cdr/scripts/{bridge,query,denormalize,indexes}.py` to avoid copy-paste across packs.

---

## 8. Testing requirements (per new strategy)

| Layer | Location | Scope |
|-------|----------|-------|
| Bridge / config | `tests/contract/<pack>/test_bridge.py` | Config merge, bindings, validation |
| Generation | `tests/contract/<pack>/test_generation.py` | `synthetic_generate_batch` against MongoDB |
| Denorm + search | `tests/contract/<pack>/test_search.py` | Golden FHIR queries → MQL |
| Ops | `tests/contract/<pack>/test_ops.py` | Pack-specific ops |
| API | `tests/contract/<pack>/test_domain_search.py` | Extend if default strategy changes |
| Fixtures | `tests/contract/<pack>/fixtures/` | Golden queries, scheduling samples, etc. |
| Smoke | `scripts/spike_generate_and_search.py` | Pack-local copy or parameterised |

Use `MONGODB_URI=mongodb://localhost:27017` and `FHIR_CONTRACT_MONGO=1` for integration contract tests.

**Do not** run vendored `libs/**/tests/e2e` as part of Kehrnel CI unless explicitly testing library releases.

---

## 9. Related documents

| Document | Purpose |
|----------|---------|
| [FHIR_TESTING.md](./FHIR_TESTING.md) | Install, sync libs, run Kehrnel FHIR tests |
| [src/kehrnel/engine/strategies/fhir/clinical_cdr/README.md](./src/kehrnel/engine/strategies/fhir/clinical_cdr/README.md) | Reference pack |
| [src/kehrnel/engine/domains/fhir/libs/README.md](./src/kehrnel/engine/domains/fhir/libs/README.md) | fhir-gen / fhir-mql vendoring |
| `fhir-kehrnel-integration/PROMPT_FHIR_KEHRNEL_INTEGRATION.md` | Step-by-step build prompts (update strategy id to `fhir.clinical_cdr`) |

---

## 10. Glossary

| Term | Meaning |
|------|---------|
| **CDR** | Clinical Data Repository — authoritative store for clinical FHIR resources |
| **CDS** | Clinical Decision Support |
| **eCQM** | electronic Clinical Quality Measure |
| **PAS** | Prior Authorization Support (Da Vinci) |
| **mCODE** | Minimal Common Oncology Data Elements |
| **IPS** | International Patient Summary |
| **fhir-gen** | Synthetic data generator (Python `fhir_gen`) |
| **fhir-mql** | FHIR search → MongoDB MQL (Python `fhir_search_to_mql`) |

---

*Last updated: aligns with Kehrnel `fhir.clinical_cdr` (beta), fhir-mql **84** shipped search configs, and FHIR R5 vendored libraries. Update §2.1–§2.2 when adding YAML configs or new strategy packs.*
