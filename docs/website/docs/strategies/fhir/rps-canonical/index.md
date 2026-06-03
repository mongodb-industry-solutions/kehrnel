---
sidebar_position: 1
---

# FHIR RPS Canonical Strategy

The **FHIR RPS Canonical** strategy (`fhir.rps_canonical`) is \{kehrnel\}'s reference persistence and search strategy for native **FHIR R5** resources in MongoDB.

It composes two vendored libraries under `src/kehrnel/engine/domains/fhir/libs/` (installed via the kehrnel `[fhir]` extra):

| Product | Python import | Role in this strategy |
|---------|---------------|---------------------|
| **fhir-gen** | `fhir_gen` | Synthetic canonical FHIR generation (`synthetic_generate_batch`) |
| **fhir-mql** | `fhir_search_to_mql` | Denormalized `_search` fields, indexes, FHIR search → MQL |

This page is strategy-specific. It describes behavior for `fhir.rps_canonical`, not cross-strategy platform guarantees.

## Overview

FHIR RPS Canonical stores **canonical FHIR JSON** in **per-resource-type MongoDB collections** (`Patient`, `Observation`, `Schedule`, …). Search-ready documents add **`_search.*`** and **`_compartments.*`** in place via fhir-mql denormalization—there is no separate monolithic search collection.

Typical workflow:

1. Activate the strategy on an environment (MongoDB bindings).
2. Run **`synthetic_generate_batch`** (directly or via synthetic jobs API).
3. Run **`fhir_denormalize`** and **`fhir_ensure_indexes`** (inline after generation or as maintenance ops).
4. Query via **`compile_query` / `execute_query`**, the **`fhir_search`** op, or **`POST /api/domains/fhir/search`**.

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
| **Synthetic jobs** | Same async job model as openEHR (`domain: fhir`, `op: synthetic_generate_batch`) |
| **Contract-tested** | Golden queries for Patient, Schedule, Slot, Appointment in kehrnel contract tests |

## Compared to `fhir.contextobjects.vitals_window`

| Aspect | `fhir.rps_canonical` | `fhir.contextobjects.vitals_window` |
|--------|----------------------|-------------------------------------|
| **Storage** | Canonical FHIR resources per type | ContextObjects + vitals windows from CSV |
| **Generation** | fhir-gen (`generate_many`, scenarios) | CSV ingest / window builders |
| **Search** | FHIR search parameters → MQL on `_search` | Con2L negotiate/compile; different query model |
| **Use case** | Synthetic CDR, FHIR search demos, scheduling corpora | IoT / operational vitals pipelines |

Do **not** mix ContextObject collections with fhir-gen canonical collections in the same search op. Align cross-strategy references via shared `Patient/{id}` (or other) logical ids when needed.

## Compared to `fhir.resource_first` (legacy preview)

`fhir.resource_first` used a single `fhir_resources` bag and stub query paths. **New work should use `fhir.rps_canonical`**: per-type collections, real compile/execute, and maintenance ops wired to fhir-mql.

## Maturity

Manifest maturity: **beta** (`0.1.0`). Domain search and synthetic generation are implemented; NL search (`negotiate_fhir_search`) and agentic surfaces are planned in later integration prompts.

## Next steps

- [Configuration](./configuration.md) — activation config, bindings, schema
- [CLI workflows](./cli-workflows.md) — curl and job examples
- [Data model](./data-model.md) — collections, `_search`, compartments
- [Query translation](./query-translation.md) — FHIR search → MQL pipeline
- [FHIR domain API](/docs/api/domains/fhir/) — HTTP search contract
