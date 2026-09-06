---
sidebar_position: 5
---

# Synthetic cohorts and the Healthcare Data Lab journey

FHIR Clinical CDR supports two deliberately different generation modes:

- **Flat generation** asks fhir-gen for counts of individual resource types. It is useful for schema, search, and load testing.
- **Cohort generation** starts from a versioned patient-centred blueprint, derives a deterministic resource distribution, generates isolated patient graphs, applies curated longitudinal rules, and returns quality evidence.

The cohort layer does not duplicate the FHIR release schemas, resource generators,
terminology tables, or search configuration. Those remain owned by the bundled
fhir-gen and fhir-mql libraries. It is an orchestration and evidence layer above
them.

## Honest maturity levels

| Level | Claim |
|---|---|
| `structural` | Generated from the selected FHIR release schema |
| `enriched` | Resource enrichers and terminology-backed values were applied |
| `curated-demo` | A deterministic patient cohort, longitudinal rules, and measured quality checks were applied |

`curated-demo` does **not** mean epidemiological, actuarial, or clinical-protocol
validity. Every result repeats that boundary. Customer profile constraints may
require additional generation rules.

## Bundled starter assets

| Blueprint | Demonstrates |
|---|---|
| `cardiometabolic-monitoring` | Longitudinal encounters, diagnoses, observations, reports, medications, and correlated systolic/diastolic blood-pressure panels |
| `oncology-care-pathway` | A connected diagnosis, diagnostic work-up, pathology, procedure, care-plan, and treatment-oriented graph |
| `payer-claims-journey` | Eligibility, coverage, encounter, claim, adjudication response, and explanation-of-benefit relationships |

The source of truth is
`specification/cohort_blueprints.json`; its public contract is
`fhir-cohort-blueprint/v1`. An inline customer blueprint can use the same
contract without copying the strategy pack.

## Backend workflow

### 1. Discover

```http
GET /api/domains/fhir/synthetic/cohorts
x-active-env: <environment>
```

This returns the active release's assets, defaults, distributions, clinical
rules, learning objectives, limits, and maturity definitions. Clients must not
hard-code this catalog.

### 2. Plan

```http
POST /api/domains/fhir/synthetic/cohorts/plan
Content-Type: application/json
x-active-env: <environment>

{
  "blueprint_id": "cardiometabolic-monitoring",
  "patients": 100,
  "history_years": 4,
  "reference_date": "2026-01-01",
  "seed": 7812
}
```

The response contains the exact resource totals, observed per-patient
distribution, shared resources, clinical rules, limitations, and a stable
`plan_digest`. `execution.persistable` says whether every planned resource has
the active release's mandatory search/projection contract; any incompatible
types are listed in `execution.preview_only_resource_types`. Planning never
writes data.

A bundled asset can be personalized without copying the strategy. The request
may replace its `population` and `clinical_rules`, and may override individual
`shared_resources` or `per_patient_resources`. A probability below `1` models
optional/missing resource groups. For a completely new journey, send a full
inline `blueprint` conforming to `fhir-cohort-blueprint/v1`.

```json
{
  "blueprint_id": "cardiometabolic-monitoring",
  "patients": 100,
  "population": {
    "age_bands": [{"min": 60, "max": 70, "weight": 1.0}],
    "gender_distribution": {"female": 0.55, "male": 0.45}
  },
  "per_patient_resources": {
    "Observation": {"min": 12, "max": 24, "probability": 0.9}
  },
  "clinical_rules": [
    {"id": "longitudinal-dates-v1"},
    {"id": "blood-pressure-panel-v1", "fraction": 0.5}
  ]
}
```

### 3. Preview

```http
POST /api/domains/fhir/synthetic/cohorts/preview
Content-Type: application/json
x-active-env: <environment>

{
  "cohort": {
    "blueprint_id": "cardiometabolic-monitoring",
    "patients": 2,
    "seed": 7812
  },
  "sample_limit": 50
}
```

Preview is synchronous, limited to ten patients, and cannot persist. It returns
sample canonical resources and the same quality-evidence shape as a full job.

### 4. Generate into the active strategy database

Use the existing asynchronous job API. Activation does not generate data.

```http
POST /environments/<environment>/synthetic/jobs
Content-Type: application/json

{
  "domain": "fhir",
  "op": "synthetic_generate_batch",
  "payload": {
    "cohort": {
      "blueprint_id": "cardiometabolic-monitoring",
      "patients": 100,
      "history_years": 4,
      "reference_date": "2026-01-01",
      "seed": 7812
    },
    "store_canonical": true
  }
}
```

Poll `GET /environments/<environment>/synthetic/jobs/<job-id>` or cancel with
`POST /environments/<environment>/synthetic/jobs/<job-id>/cancel`. Persistence
always uses the activated strategy database and the mandatory validation,
search-projection, compartment, version, and index contract.

Generation always validates against the selected FHIR Core release. If profile
enforcement is explicitly required, the normal persistence boundary also invokes
the configured external validator. A starter cohort that does not declare or
satisfy a selected customer profile will therefore fail rather than silently
claiming profile conformance.

## Evidence returned with every cohort

The generation result includes:

- the complete reviewed plan and its digest;
- planned and actual counts by resource type;
- relative-reference integrity, complete direct patient-linkage measurements,
  and dates checked against the requested history window;
- clinical-rule measurements, including blood-pressure consistency;
- final base-schema conformance plus generator-level evidence showing any
  invalid optional values removed before cohort curation;
- observed population distribution and auto-generated dependencies;
- an explicit non-epidemiological claim.

The generator guard and final schema check are intentionally conservative. The
guard removes invalid optional generator output and reports every affected path
in `generation_conformance`; it never invents missing required content. The
post-curation check appears in
`quality_report.checks.base_schema_conformance`. Any unresolved base-schema
error still fails at the normal import boundary.

## Healthcare Data Lab experience contract

Healthcare Data Lab should present this as one continuous workflow, not as a
large configuration form hidden inside activation.

1. **Choose an outcome.** Show the three backend-provided asset cards with their purpose, learning objectives, resource families, maturity, and disclaimer. Add “Start from a custom blueprint” as an advanced path.
2. **Configure the cohort.** Keep patient count, history, reference date, and seed visible. Put demographics, resource distributions, missingness, and rule overrides in an expandable advanced section. Every control changes only a draft.
3. **Review the plan.** Call the plan endpoint and show total documents, a resource-distribution chart, a small patient/resource graph, clinical rules, database destination, release/profile context, limitations, and plan digest. Make “no data has been written” explicit.
4. **Preview patients.** Generate one to five patients through the preview endpoint. Provide a patient timeline, connected-resource graph, canonical JSON, and quality panel. A resource click should open the same resource inspector used elsewhere in the FHIR laboratory.
5. **Generate.** Require an explicit final confirmation showing tenant, strategy database, document count, and whether existing logical ids can be updated. Submit the existing asynchronous synthetic job; show progress, cancel, and final counts.
6. **Explore the result.** Deep-link to a selected patient, the FHIR API sandbox, FHIR Search-to-MQL explain, native MongoDB query workbench, collections/index view, and saved-query workspace. Pre-fill useful searches from the blueprint's learning objectives.
7. **Reopen operational history.** Keep generated datasets and jobs discoverable by blueprint id, version, plan digest, seed, creator, time, destination, and quality status. This makes HDL an operational enablement portal, not a disposable tutorial.

### Interaction rules for the HDL implementation

- Fetch catalog, capabilities, resource definitions, search parameters, and run state from Kehrnel. Do not duplicate them in frontend constants.
- Activation selects release, optional IG/profile overlays, persistence policy, and optional semantic pipelines. Data generation remains an explicit user action after activation.
- Show selected profiles separately from enforcement. Never imply conformance
  unless capabilities report `profile_conformance: true`.
- Label generated values as synthetic everywhere and keep the blueprint disclaimer visible in preview and completion states.
- An AI assistant may propose a draft blueprint from a phrase such as “breast cancer pathway,” but the user must see the resolved distributions and diff before planning or generation. The deterministic Kehrnel engine—not the language model—produces the resources.
- Do not expose flat resource-count generation as the primary journey. Retain it under an “Advanced / load test” mode.

## Acceptance criteria for the HDL journey

- A first-time FHIR user can select an asset, preview two patients, generate a cohort, and run a pre-filled FHIR search without leaving the journey.
- An experienced user can modify distributions, inspect exact JSON, compare FHIR Search with MQL, run native MongoDB queries, and save queries.
- Refreshing the page preserves job visibility and links back to generated data.
- Every screen uses the active environment and strategy database; no hidden local MongoDB fallback exists.
- Loading, empty, degraded, validation-error, canceled, partial, and completed states are designed explicitly.
- The UI displays the quality report without converting warnings into green success claims.
