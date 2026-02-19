---
sidebar_position: 1
sidebar_label: Introduction
slug: /
---

# Introduction to kehrnel

**\{kehrnel\}** is a healthcare data execution platform, not just a code library.

It gives teams one consistent way to ingest, transform, validate, map, query, and operate healthcare data while allowing different domains to use different technical strategies.

## What \{kehrnel\} Is

\{kehrnel\} provides one runtime model to:

- activate and version strategies per environment
- run domain and strategy operations through CLI, API, or embedded Python
- execute ingestion, transformation, mapping, querying, and operational tasks with explicit contracts
- keep operations traceable with an activation-centric control model

Healthcare Data Lab is the control plane/UI. \{kehrnel\} is the execution plane.

## Multi-Domain, Multi-Strategy by Design

A core design principle is **multi-domain, multi-strategy** operation under one runtime.

- Domain APIs represent canonical standards and contracts.
- Strategy packs implement the technical execution approach for a domain/workload.
- The runtime activates `domain + strategy + version + config` explicitly per environment.

Today, this repository is strongest in openEHR strategy execution, with roadmap expansion for broader domain coverage (including FHIR, genomics, and others).

## What a Strategy Defines

In \{kehrnel\}, a strategy is a formal execution contract. At minimum, each strategy should define:

- **Model scope**: the logical data model/metamodel used by the strategy.
- **Storage model**: collections/stores involved and how records are represented in each.
- **Transformation model**: how source data is mapped/materialized between stores when needed.
- **Index and search model**: required indexes, search index definitions, and their purpose.
- **Interface surface**: supported runtime operations (ingest, transform, query, synthetic, maintenance).
- **Standards conformance scope**: for standards-bound strategies, which interfaces/contracts are supported.
- **Configuration surface**: required and optional configuration, defaults, and profile variants.
- **Operational invariants**: explicit rules that must always hold (identity, lineage, referential behavior).
- **Examples and references**: sample inputs/outputs and implementation references for reproducibility.

This is the baseline structure that allows future strategy packs to remain consistent and machine-learnable.

## One Engine, Multiple Interfaces

\{kehrnel\} is intentionally consumable at different integration levels:

- **Python embedding** for backend/service integration.
- **CLI** for repeatable workflows, CI/CD jobs, and operations.
- **HTTP API** (FastAPI) for application-to-application integration.

This enables incremental adoption: start with one interface, scale to others, keep the same execution logic.

## Layered API Model

The API surface is layered so teams can work at the right abstraction:

- **Core**: discovery, activation, environment/runtime controls.
- **Domain**: canonical domain operations.
- **Strategy**: strategy-specific capabilities.
- **Admin/compatibility**: operational and integration support endpoints.

This balances standardization (stable contracts) with flexibility (strategy-level control).

## Persistence Modes

\{kehrnel\} supports database-backed and file-backed operation patterns:

- **MongoDB** for production-style and scalable workloads.
- **Filesystem JSONL** for local development, demos, tests, and lightweight pipelines.

Teams can validate quickly on files and move to database-backed execution without changing the functional model.

## Activation-Centric Runtime

Environments are activated explicitly by:

- domain
- strategy
- version
- configuration/bindings

This makes strategy changes an operational process (auditable and controlled), which is critical as requirements, standards, and performance needs evolve.

## Documentation Entry Points

For daily development, start from the runtime module surfaces:

- [CLI](/docs/cli/overview): command UX and workflows
- [API](/docs/api/overview): runtime HTTP contract by layer
- [Engine](/docs/engine/overview): internal execution architecture by module
- [Documentation Structure](/docs/documentation/structure): where new docs must be added

## Licensing and Maturity

- Repository software is licensed under **Apache 2.0**.
- Strategy assets under `src/kehrnel/engine/strategies/` are licensed under **CC BY 4.0**.

\{kehrnel\} is currently positioned as an **experimental, non-production** environment. Its current value is as a practical foundation for architecture validation, interoperability experimentation, and strategy delivery patterns.

## Next Steps

- [Vision & Roadmap](/docs/vision/roadmap)
- [Installation Guide](/docs/getting-started/installation)
- [Quick Start](/docs/getting-started/quickstart)
- [Strategy Overview](/docs/strategies/overview)
- [Architecture Overview](/docs/architecture/overview)
