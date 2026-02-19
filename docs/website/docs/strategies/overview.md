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

## Strategy Contract Checklist

For consistency across current and future strategy packs, each strategy should define:

- **Model scope**: logical data model/metamodel.
- **Storage model**: collections/stores and record representations.
- **Transformation model**: how source data is mapped/materialized.
- **Index and search model**: required indexes, search definitions, and rationale.
- **Interface surface**: supported runtime operations and expected inputs/outputs.
- **Standards conformance scope**: which interfaces of a standard are supported.
- **Configuration surface**: required/optional config, defaults, and profile variants.
- **Operational invariants**: identity, lineage, and consistency guarantees.
- **Examples/references**: sample inputs/outputs and implementation references.

This is the baseline needed to keep strategies machine-readable, comparable, and extensible.

## \{kehrnel\} Is Multi-Strategy by Design

\{kehrnel\} is not tied to one strategy. A workspace can evolve through different strategies over time as requirements change.

Current and target strategy families include:

- **Structured CDR strategies** (current): openEHR reference implementations
- **Extraction strategies** (roadmap): unstructured clinical report extraction
- **Hybrid mapping strategies** (roadmap): deterministic rules + LLM
- **Interoperability strategies** (roadmap): multi-standard normalization

## Current Reference Strategy

### openEHR RPS Dual

The current reference implementation for high-scale openEHR persistence.

| Aspect | Description |
|--------|-------------|
| **Domain** | openehr |
| **Storage** | Dual-collection (canonical + search projection) |
| **Query** | AQL compilation to MongoDB aggregation |
| **Scale target** | Patient-centric and population analytics |

[Learn more about RPS Dual →](/docs/strategies/openehr/rps-dual/introduction)

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

## Shared Components

When a capability is reusable across strategies, it should move to `engine/common` instead of staying pack-local.

Current example:

- `engine/common/ingest`: shared ingest utilities (`PathCodec`, remap helpers, ingest exceptions)

This keeps strategy packs focused on domain-specific logic while preserving one reusable base for future packs.

## Activation Model

Strategies are activated per environment:

```bash
curl -X POST "http://localhost:8000/environments/dev/activate" \
  -H "Content-Type: application/json" \
  -d '{
    "strategy_id": "openehr.rps_dual",
    "version": "latest",
    "domain": "openehr",
    "config": { ... },
    "bindings_ref": "env://DB_BINDINGS"
  }'
```

## Related

- [Vision & Roadmap](/docs/vision/roadmap)
- [Architecture Overview](/docs/architecture/overview)
- [API Reference](/docs/api/overview)
