---
sidebar_position: 1
sidebar_label: Introduction
slug: /
---

# Introduction to kehrnel

**\{kehrnel\}** is a strategy runtime that turns healthcare data models into operational capabilities.

Defining a model is not enough. Teams also need a repeatable way to validate data, transform it into an operational representation, ingest it, query it, maintain it, and evolve it as requirements change. Without that execution layer, models can remain documentation, storage schemas, or isolated specifications.

It starts with strong support for openEHR on MongoDB because openEHR is a demanding reference point: archetypes, templates, terminology, paths, temporal context, and query semantics all have to remain meaningful after the data is transformed, indexed, queried, and exposed through APIs.

That openEHR foundation is the starting point, not the boundary. The broader idea is to define a repeatable document-first way to operationalize healthcare data models: load a strategy, activate it for an environment, bind it to data, expose the right runtime interfaces, and make the result inspectable for humans, applications, and AI agents.

## What \{kehrnel\} Is

\{kehrnel\} is the execution plane behind model-driven healthcare workflows. It provides a runtime layer to:

- Activate and version strategy packs per environment
- Bind model-specific configuration, dictionaries, mappings, indexes, and operations
- Ingest, transform, compile, query, and run strategy-specific jobs with explicit contracts
- Expose strategy-customized workflows for activation, validation, ingestion, transformation, query, synthetic data, and maintenance
- Expose OpenAPI-documented interfaces for applications and educational portals
- Support semantic and agentic workflows through governed runtime surfaces

Healthcare Data Lab is the control plane/UI. \{kehrnel\} is the execution plane.

## Why This Matters

Healthcare data models often carry meaning that gets lost once data is flattened, indexed, copied, or exposed through ad hoc APIs. \{kehrnel\} tries to keep that meaning operational:

- the model remains visible through strategy manifests and specifications
- activation records connect a domain, environment, config, version, and data binding
- query compilation is deterministic and inspectable
- operations such as activation, validation, ingestion, dictionary setup, search rebuilds, synthetic generation, or maintenance are strategy capabilities rather than one-off scripts
- AI-facing tools can work through model-aware APIs and semantic contracts instead of guessing against raw collections

This makes \{kehrnel\} useful for education as well as engineering. A learner can follow the path from source model to strategy, from strategy to API, and from API to governed execution.

## Platform Direction

\{kehrnel\} is intentionally not limited to one strategy or one standard.

### Multi-Strategy

Different strategy packs can coexist for different data problems and maturity levels:

- Canonical CDR persistence
- Search-optimized projections
- Data quality/validation pipelines
- Extraction pipelines for unstructured clinical reports
- Synthetic data and sampling workflows
- Semantic retrieval and agentic context assembly

### Multi-Domain

Current focus includes openEHR, with roadmap expansion to additional healthcare standards, ContextObjects, synthetic data workflows, semantic catalogs, natural language retrieval, document-centric workflows, and domain-specific strategy families.

### Hybrid Extraction (Rules + LLM)

A key roadmap direction is unstructured clinical report processing using hybrid mapping techniques:

- deterministic rules for compliance-critical mapping
- LLM-assisted extraction where heuristics alone are insufficient
- validation/normalization to produce trustworthy golden records

### Open Ecosystem

\{kehrnel\} is designed as an open data ecosystem runtime where new teams can add strategies and tools without replacing the whole platform:

- open APIs and explicit strategy contracts
- domain-agnostic architecture
- pluggable persistence backends (MongoDB today; extensible model)
- portability across deployment topologies
- room for community-defined model strategies, operational tooling, and semantic products

## AI and Semantic Workflows

\{kehrnel\} does not make AI safer by hiding complexity. It makes the execution boundary more explicit.

Agentic workflows benefit when tools can rely on:

- model-aware APIs
- versioned strategy manifests
- semantic catalogs and context contracts
- deterministic query and operation execution
- auditable activation and runtime metadata

The goal is not to let an agent invent a query over an unknown database. The goal is to give agents and copilots better semantic handles, clearer tool contracts, and inspectable paths from intent to execution.

## Current Reference Implementation

The current production-grade reference strategy is **openEHR RPS Dual** on MongoDB. It demonstrates how \{kehrnel\} handles patient-centric and population-wide access patterns at scale.

This is a reference strategy, not the boundary of the platform.

## How to Contribute New Direction

\{kehrnel\} is deliberately strategy-oriented so new work can start small:

- define a new strategy pack for a model family or workflow
- add runtime operations for validation, indexing, generation, or retrieval
- expose a clearer API surface for an educational or application use case
- connect semantic catalogs and context contracts to agentic workflows
- test new persistence or query targets behind the same activation model

If you are working on healthcare data models, semantic APIs, synthetic data, or agentic tooling, the most useful contribution is a concrete strategy or workflow that can be inspected, activated, and improved by others.

## Next Steps

- [Vision & Roadmap](/docs/vision/roadmap)
- [Installation Guide](/docs/getting-started/installation)
- [Quick Start](/docs/getting-started/quickstart)
- [Strategy Overview](/docs/strategies/overview)
- [Architecture Overview](/docs/architecture/overview)
