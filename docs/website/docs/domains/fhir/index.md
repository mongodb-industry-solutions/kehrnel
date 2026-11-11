---
sidebar_position: 2
---

# FHIR Domain

This section centralizes HL7 FHIR runtime documentation for \{kehrnel\}.

The FHIR domain is served by a single official strategy, `fhir.resource_store`. Domain routes require an active activation with `domain: fhir` and that strategy on the environment resolved from `x-active-env` (or the platform default). The default strategy id can be overridden with `KEHRNEL_FHIR_STRATEGY_ID`.

## Scope

| Topic | What the code provides |
|-------|------------------------|
| Releases | Activation selects a release tier: a deliberately minimal R4 Patient/Observation baseline, or a package-backed R5 or R6 FHIR Core baseline |
| Authoritative contract | The active `CapabilityStatement` (`GET /api/domains/fhir/metadata`) and the detailed capability response; both are generated from runtime capabilities, not uploaded |
| Implementation guides | Optional local FHIR NPM packages (`.tgz`) can be staged and inspected before a voluntary activation; no IG is required to begin |
| Storage | Canonical FHIR JSON per resource type, with `_search` and `_compartments` projections for indexable search |
| Search | FHIR Search compiled to MongoDB filters with visible MQL; unsupported parameters fail closed |
| Terminology | Standard FHIR terminology operations routed through the tenant terminology gateway |
| Semantic retrieval | Optional preview, materialization, and vector search over configured sidecars, advertised only when the required adapters are available |

R4 does not claim synthetic generation.

## Quick Navigation

### Domain API

- [FHIR Domain API](/docs/api/domains/fhir/)
- [FHIR Preview Endpoints](/docs/api/endpoints/fhir-preview)
- [Provider-neutral Terminology API](/docs/api/core/terminology)

The domain router is mounted at `/api/domains/fhir` (and under `/v1`). Its surface groups into:

- **Discovery**: `metadata`, `capabilities`, `support-matrix`, `resource-catalog`, `search-parameters/{resource_type}`, `index-manifest`, `stats`
- **REST interactions**: create (`POST /{resource_type}`), update (`PUT /{resource_type}/{resource_id}`), read (`GET /{resource_type}/{resource_id}`), type search (`GET /{resource_type}`), and compartment search (`GET /{compartment_type}/{compartment_id}/{resource_type}`)
- **Search and diagnostics**: `search`, `explain`, `explain/execute`, `native-query` (bounded, read-only find; not part of the FHIR REST specification)
- **Import and migration**: `import`, `migration/runs` and its chunk, cancel, and reference-integrity sub-routes
- **Implementation guides**: `implementation-guides/stage`
- **Synthetic cohorts**: `synthetic/cohorts`, `synthetic/cohorts/plan`, `synthetic/cohorts/preview`
- **Semantic retrieval**: `semantic/preview`, `semantic/materialize`, `semantic/search`, and `semantic/terminology-query-executions` (resolves a pinned SNOMED CT expansion through the terminology gateway and runs a bounded `Condition` search)
- **Terminology operations** (GET and POST): `CodeSystem/$lookup`, `CodeSystem/$validate-code`, `CodeSystem/$subsumes`, `ValueSet/$validate-code`, `ValueSet/$expand`, `ConceptMap/$translate`

### Strategy

- [FHIR Resource Store Strategy](/docs/strategies/fhir/resource-store/)
- [FHIR Resource Store Configuration](/docs/strategies/fhir/resource-store/configuration)
- [FHIR Resource Store CLI Workflows](/docs/strategies/fhir/resource-store/cli-workflows)
- [FHIR Resource Store Data Model](/docs/strategies/fhir/resource-store/data-model)
- [FHIR Search to MQL Translation](/docs/strategies/fhir/resource-store/query-translation)
- [Semantic Projections](/docs/strategies/fhir/resource-store/semantic-projections)
- [Synthetic Cohorts](/docs/strategies/fhir/resource-store/synthetic-cohorts)

`fhir.resource_store` manifest: version `0.13.1`, maturity **published**.

### Core CLI Entry Points

- [CLI Overview](/docs/cli/overview)
- [Domain CLI Layer](/docs/cli/domains)
- [Strategy CLI Layer](/docs/cli/strategies)

The `kehrnel fhir` command group calls the domain API of the activated environment:

| Command | Purpose |
|---------|---------|
| `kehrnel fhir metadata` | Read the activated `CapabilityStatement` |
| `kehrnel fhir capabilities` | Runtime-derived accelerator capability contract |
| `kehrnel fhir support-matrix` | Release/resource/interaction/search support matrix (for example `--format markdown --out fhir-support-matrix.md`) |
| `kehrnel fhir resource-catalog` | Package-backed resource, polymorphism, search, and index metadata |
| `kehrnel fhir search-parameters` | Search parameters supported for a resource type |
| `kehrnel fhir index-manifest` | Generated index definitions, digests, budgets, and violations |
| `kehrnel fhir stats` | Bounded store, index, and provenance statistics |
| `kehrnel fhir stage-ig` | Stage a local IG package for later activation |
| `kehrnel fhir import-data` | Validate and optionally import a Bundle, NDJSON batch, or resource |
| `kehrnel fhir search` | Run a FHIR Search (for example `'Patient?name=Smith'`) and return a searchset Bundle |
| `kehrnel fhir explain` | Show the compiled MQL (`--execution-stats` for evidence) |
| `kehrnel fhir cohorts` | List patient-centred synthetic cohort blueprints |
| `kehrnel fhir plan-cohort` | Deterministic resource counts for a cohort, without writing |

Terminology for FHIR codings is handled by the shared `kehrnel terminology` group; see the [SNOMED CT Domain](/docs/domains/snomedct).

## Recommended Reading Order

1. FHIR domain API
2. strategy introduction and configuration
3. CLI workflows
4. data model and query translation
5. semantic projections and synthetic cohorts
