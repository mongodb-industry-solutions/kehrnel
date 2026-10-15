---
sidebar_position: 1
---

# Strategies Overview

\{kehrnel\} uses a **strategy-pack architecture**: each strategy defines how data is transformed, persisted, queried, and operated for a specific problem space.

## What Is a Strategy?

A strategy is a self-contained package that can implement:

- **Transformation**: canonicalization, mapping, enrichment
- **Persistence**: collection/table layout, index policy, retention rules
- **Query compilation**: domain query language to execution plan
- **Operations**: maintenance tasks, validation, synthetic generation

## \{kehrnel\} Is Multi-Strategy by Design

\{kehrnel\} is not tied to one strategy. A workspace can evolve through different strategies over time as requirements change.

Strategy families today and on the roadmap:

- **Clinical record and resource strategies** (current): openEHR RPS Dual, FHIR Resource Store
- **Study data strategies** (current): CDISC Study Data Repository
- **Terminology strategies** (current): SNOMED CT on MongoDB
- **Extraction strategies** (roadmap): unstructured clinical report extraction
- **Hybrid mapping strategies** (roadmap): deterministic rules + LLM

## Official Strategies

The official strategies are the ones \{kehrnel\} supports for every organization and documents in this manual:

| Strategy | Id | Domain |
|----------|----|--------|
| openEHR RPS Dual | `openehr.rps_dual` | openehr |
| FHIR Resource Store | `fhir.resource_store` | fhir |
| CDISC Study Data Repository | `cdisc.sdr` | cdisc |
| SNOMED CT on MongoDB | `snomedct.mongodb` | snomedct |

Being official is about who a strategy is for, not how mature it is: each strategy still declares its own maturity in its manifest.

Two other kinds of strategy exist and are not documented here:

- **Organization-specific strategies** are built for one customer and are only visible to and activatable by that organization.
- **Preview strategies** explore future capabilities. They can change or be removed and are labelled *Preview* wherever they appear.

## Official Strategy Details

### openEHR RPS Dual

The current reference implementation for high-scale openEHR persistence.

| Aspect | Description |
|--------|-------------|
| **Domain** | openehr |
| **Storage** | Dual-collection (canonical + search projection) |
| **Query** | AQL compilation to MongoDB aggregation |
| **Scale target** | Patient-centric and population analytics |

[Learn more about RPS Dual →](/docs/strategies/openehr/rps-dual/introduction)

### FHIR Resource Store

Reference strategy for native **FHIR** in MongoDB (fhir-gen + fhir-mql): R5 and R6 with the bundled release schemas, R4 as a minimal Patient/Observation tier.

| Aspect | Description |
|--------|-------------|
| **Domain** | fhir |
| **Storage** | Per-resource-type collections with in-place `_search` denormalization |
| **Query** | FHIR search parameters → MongoDB aggregation (MQL) |
| **Synthetic** | `synthetic_generate_batch` via fhir-gen |

[Learn more about FHIR Resource Store →](/docs/strategies/fhir/resource-store/)

### SNOMED CT on MongoDB

Official strategy for licensed **SNOMED CT** terminology releases in MongoDB (manifest maturity: preview).

| Aspect | Description |
|--------|-------------|
| **Domain** | snomedct |
| **Storage** | Canonical concept collection + optional term sidecar |
| **Query** | Lookup, basic ECL/subsumption, lexical terminology search |
| **Grounding** | Mention-to-candidate retrieval for clinical NLP/LLM workflows |

[Learn more about SNOMED CT on MongoDB →](/docs/strategies/snomedct/mongodb/)

### CDISC Study Data Repository

Official strategy for governed **CDISC** study snapshots (SEND, SDTM, ADaM and TIG), document-first (manifest maturity: preview).

| Aspect | Description |
|--------|-------------|
| **Domain** | cdisc |
| **Storage** | Preserved CDISC artifacts and canonical dataset records, with derived typed facets, entity references and lineage |
| **Exchange** | Dataset-JSON v1.1 and SAS XPORT, in and out |
| **Query** | Governed cross-domain study queries compiled to bounded MongoDB plans (`cdisc-query/v1`) |

## Strategy Pack Structure

```text
strategy_pack/
├── manifest.json       # identity, capabilities, ops, metadata
├── spec.json           # transformation/persistence/query specification
├── schema.json         # configuration schema
├── defaults.json       # default config
├── ingest/             # ingestion/transformation logic
├── query/              # query compilation logic
└── assets/             # docs and supporting artifacts
```

## Activation Model

Strategies are activated per environment:

```bash
RUNTIME_URL="${RUNTIME_URL:-http://localhost:8080}"

curl -X POST "${RUNTIME_URL}/environments/dev/activate" \
  -H "Content-Type: application/json" \
  -d '{
    "strategy_id": "openehr.rps_dual",
    "version": "latest",
    "domain": "openehr",
    "config": { ... },
    "bindings_ref": "<resolver-specific-ref>"
  }'
```

Many strategy maintenance workflows now go through the universal runtime runner. For example, RPS Dual can generate its Atlas Search definition from the active mappings with:

```bash
kehrnel strategy build-search-index --env dev --domain openehr --strategy openehr.rps_dual --out .kehrnel/search-index.json
```

## Related

- [Vision & Roadmap](/docs/vision/roadmap)
- [Architecture Overview](/docs/architecture/overview)
- [API Reference](/docs/api/overview)

