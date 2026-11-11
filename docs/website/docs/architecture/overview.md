---
sidebar_position: 1
---

# Architecture Overview

\{kehrnel\} is a runtime platform for healthcare data execution, not a single fixed pipeline. Domain standards define the data and the interactions; strategy packs decide how that data is stored, transformed and queried on MongoDB.

## Core Principle

**Control plane vs execution plane**:

- **Healthcare Data Lab**: workspace, governance, UX, orchestration
- **\{kehrnel\}**: strategy runtime, query execution, transformations, operations

## High-Level Architecture

```text
Clients (HDL UI, API consumers, kehrnel CLI, automation)
        |
        v
FastAPI runtime
  domain APIs    /api/domains/openehr | fhir | snomedct
  runtime APIs   /environments/{env_id}/...  (activate, plan, apply, ingest, query, run, ops)
        |
        v
Strategy runtime
  manifest registry  ->  activation per (environment, domain)  ->  strategy plugin
        |
        v
MongoDB (storage adapter; Atlas Search where the strategy uses it)
```

The layers:

- **Domain APIs** expose standard-shaped interactions (openEHR REST and AQL, FHIR REST and search, SNOMED CT terminology).
- **Runtime APIs** expose the same operations for every domain: activation, `plan` / `apply`, `transform`, `ingest`, `query` / `compile_query`, strategy ops and synthetic jobs.
- **The strategy runtime** (`StrategyRuntime` in `kehrnel.engine.core.runtime`) resolves the activation for the requested environment and domain, resolves bindings, and calls the strategy plugin named in the manifest entrypoint.
- **Strategy packs** carry a `manifest.json` (id, domain, capabilities, ops, interaction contracts), configuration defaults and schema, and the plugin code.

## Official Domains and Strategies

| Domain | Strategy | Stores | Query contract |
|--------|----------|--------|----------------|
| openEHR | `openehr.rps_dual` | flattened compositions plus a slim search projection | AQL |
| FHIR | `fhir.resource_store` | FHIR resources with search projections | FHIR REST search |
| CDISC | `cdisc.sdr` | study datasets, snapshots, artifacts and projections | `cdisc-query/v1`, `cdisc-analysis/v1` |
| SNOMED CT | `snomedct.mongodb` | release concepts plus a term sidecar | lookup, search, ECL, subsumption |

Each environment has at most one activation per domain, and domains are activated independently. A request that names a domain is executed only by that domain's activation.

## Execution Paths

- [Query and Execution](/docs/domains/openehr/aql-query-engine) - how a query reaches a domain compiler and runs on MongoDB
- [Ingest and Transformation](/docs/domains/openehr/composition-flattening) - the shared ingest, transform, ops and synthetic-job model

## Related

- [Concepts Overview](/docs/concepts/overview)
- [Domains](/docs/domains/)
- [Strategies Overview](/docs/strategies/overview)
- [Vision & Roadmap](/docs/vision/roadmap)
