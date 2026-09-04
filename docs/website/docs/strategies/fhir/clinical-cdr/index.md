---
sidebar_position: 1
---

# FHIR Clinical CDR Strategy

The **FHIR Clinical CDR** strategy (`fhir.clinical_cdr`) is \{kehrnel\}'s reference persistence and search strategy for native **FHIR R5 and R6** resources in MongoDB.

It composes two vendored libraries under `src/kehrnel/engine/domains/fhir/libs/` (installed via the kehrnel `[fhir]` extra):

| Product | Python import | Role in this strategy |
|---------|---------------|---------------------|
| **fhir-gen** | `fhir_gen` | Synthetic canonical FHIR generation (`synthetic_generate_batch`) |
| **fhir-mql** | `fhir_search_to_mql` | Denormalized `_search` fields, indexes, FHIR search → MQL |

This page is strategy-specific. It describes behavior for `fhir.clinical_cdr`, not cross-strategy platform guarantees.

## Overview

FHIR Clinical CDR stores **canonical FHIR JSON** in **per-resource-type MongoDB collections** (`Patient`, `Observation`, `Schedule`, …). Search-ready documents add **`_search.*`** and **`_compartments.*`** in place via fhir-mql denormalization—there is no separate monolithic search collection.

Typical workflow:

1. Activate the strategy on an environment (MongoDB bindings).
2. Run **`synthetic_generate_batch`** (directly or via synthetic jobs API).
3. Run **`fhir_denormalize`** and **`fhir_ensure_indexes`** (inline after generation or as maintenance ops).
4. Query via **`compile_query` / `execute_query`**, the **`fhir_search`** op, or **`POST /api/domains/fhir/search`**.

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
| **Contract-tested** | Golden queries for Patient, Schedule, Slot, Appointment in kehrnel contract tests |

## Maturity

The manifest advertises only implemented resource-store operations. The active CapabilityStatement is authoritative for supported FHIR interactions and resource types.

## Next steps

- [Configuration](./configuration.md) — activation config, bindings, schema
- [CLI workflows](./cli-workflows.md) — curl and job examples
- [Data model](./data-model.md) — collections, `_search`, compartments
- [Query translation](./query-translation.md) — FHIR search → MQL pipeline
- [FHIR domain API](/docs/api/domains/fhir/) — HTTP search contract
