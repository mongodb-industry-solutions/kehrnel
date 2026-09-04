---
sidebar_position: 1
---

# FHIR Clinical CDR Strategy

The **FHIR Clinical CDR** strategy (`fhir.clinical_cdr`) is \{kehrnel\}'s reference persistence and search strategy for native **FHIR R4, R5, and R6** resources in MongoDB. R4 is currently an explicitly minimal Patient/Observation tier; R5 and R6 use the bundled release schemas.

It composes two vendored libraries under `src/kehrnel/engine/domains/fhir/libs/` (installed via the kehrnel `[fhir]` extra):

| Product | Python import | Role in this strategy |
|---------|---------------|---------------------|
| **fhir-gen** | `fhir_gen` | Synthetic canonical FHIR generation (`synthetic_generate_batch`) |
| **fhir-mql** | `fhir_search_to_mql` | Denormalized `_search` fields, indexes, FHIR search → MQL |

This page is strategy-specific. It describes behavior for `fhir.clinical_cdr`, not cross-strategy platform guarantees.

## Overview

FHIR Clinical CDR stores **canonical FHIR JSON** in **per-resource-type MongoDB collections** (`Patient`, `Observation`, `Schedule`, …). Search-ready documents add **`_search.*`** and **`_compartments.*`** in place via fhir-mql denormalization—there is no separate monolithic search collection.

Typical workflow:

1. Activate the FHIR Core baseline on an environment (MongoDB bindings); no IG is required.
2. Optionally compile one or more customer, jurisdictional, or sector FHIR packages.
3. Optionally select profiles and configure semantic projection pipelines.
4. Import real data through a checkpointed migration run, or plan and generate a patient-centred synthetic cohort on R5/R6. Projection and indexes are mandatory before persistence.
5. Inspect the saved migration report and informational reference-integrity evidence.
6. Query via **`compile_query` / `execute_query`**, the **`fhir_search`** op, or **`POST /api/domains/fhir/search`**.

Healthcare Data Lab does not persist copies of the standard resource definitions
to render its FHIR catalog. It reads the active package model from
`GET /api/domains/fhir/resource-catalog` and loads detailed structure, choice
elements, search projections, and MongoDB indexes from
`GET /api/domains/fhir/resource-catalog/{resourceType}`. The list response also
provides the generation recipes valid for the selected release, so clients do
not need to hard-code recipe scope or document counts.

## API placement

| Layer | Responsibility |
|-------|----------------|
| **Core** | Strategy registry, environment activation (`/environments/{env}/activate`) |
| **Common** | Auth, error format, HTTP conventions |
| **Domain** | `POST /api/domains/fhir/search` (FHIR Bundle response) |
| **Strategy** | Generation, denormalize, indexes, `fhir_search`, `fhir_stats` via `/environments/{env}/run` or synthetic jobs |

Practical implication: use the **FHIR domain search route** for Bundle-shaped HTTP clients; use **strategy ops** for batch generation, denormalization, and diagnostics.

## Benefits

| Capability | What you get |
|------------|--------------|
| **Canonical FHIR** | Documents match FHIR JSON shape; no proprietary shredding |
| **Per-type collections** | Aligns with fhir-gen and fhir-mql defaults; simpler ops and indexing |
| **Search denormalization** | `_search.<param>` fields compiled from fhir-mql YAML per resource type |
| **Synthetic jobs** | Asynchronous generation through `domain: fhir` and `op: synthetic_generate_batch` |
| **Patient-centred cohort assets** | Versioned cardiometabolic, oncology, and payer blueprints with deterministic plans and quality evidence |
| **Migration workbench** | Client-streamed chunks, tenant-scoped checkpoints, retry/cancel, and bounded audit reports without retaining source payloads |
| **Runtime support evidence** | Downloadable JSON/Markdown matrix generated from the same capability contract used by HDL |
| **Contract-tested** | Golden queries for Patient, Schedule, Slot, Appointment in kehrnel contract tests |

## Capabilities and profiles are separate

The runtime capability matrix answers which resource types, REST interactions,
search parameters, and controls Kehrnel currently implements. A profile is a
constraint on a base FHIR resource. An IG package may contain multiple profiles,
SearchParameters, terminology, examples, and CapabilityStatements; multiple
packages can be compiled together.

Loading an IG does not make its requested APIs implemented. Kehrnel reports the
intersection of the active release and real runtime behavior. Compiled profiles
are catalogued as evidence. Customers may then explicitly enable fail-closed
enforcement through a configured external profile-validation adapter; capability
responses distinguish selected profiles, adapter availability, and enforcement.

## Maturity

The manifest advertises only implemented resource-store operations. The active CapabilityStatement is authoritative for supported FHIR interactions and resource types.

The provisional R4 tier supports structural validation, canonical storage and a
reviewed search subset for Patient and Observation. It does not claim bundled
R4 base-schema validation or synthetic generation. This boundary lets clients
use the same activation and capability contracts while generated R4 assets are
completed independently.

## Next steps

- [Configuration](./configuration.md) — activation config, bindings, schema
- [CLI workflows](./cli-workflows.md) — curl and job examples
- [Data model](./data-model.md) — collections, `_search`, compartments
- [Query translation](./query-translation.md) — FHIR search → MQL pipeline
- [Semantic projections](./semantic-projections.md) — opt-in clinical-text selection and preview
- [Synthetic cohorts](./synthetic-cohorts.md) — patient-centred assets, API contract, evidence, and HDL journey
- [Supported pilot runbook](./pilot-runbook.md) — deployment, migration, recovery, security, and delivery boundaries
- [FHIR domain API](/docs/api/domains/fhir/) — HTTP search contract
