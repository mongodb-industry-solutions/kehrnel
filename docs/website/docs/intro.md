---
sidebar_position: 1
sidebar_label: Introduction
slug: /
---

# Introduction to kehrnel

**\{kehrnel\}** is a multi-strategy, multi-domain runtime for healthcare data engineering.

It starts with strong support for openEHR and MongoDB, but the platform direction is broader: interoperable standards, document-first workloads, hybrid extraction pipelines, and pluggable persistence targets.

## What \{kehrnel\} Is

\{kehrnel\} provides a runtime layer to:

- Activate and version strategy packs per environment
- Ingest and transform healthcare data with explicit contracts
- Compile domain query languages into storage-native execution plans
- Operate data pipelines with auditable APIs and operational controls

Healthcare Data Lab is the control plane/UI. \{kehrnel\} is the execution plane.

## Platform Direction

\{kehrnel\} is intentionally not limited to one strategy or one standard.

### Multi-Strategy

Different strategy packs can coexist for different data problems:

- Canonical CDR persistence
- Search-optimized projections
- Data quality/validation pipelines
- Extraction pipelines for unstructured clinical reports

### Multi-Domain

Current focus includes openEHR, with roadmap expansion to additional healthcare standards and document-centric workflows.

### Hybrid Extraction (Rules + LLM)

A key roadmap direction is unstructured clinical report processing using hybrid mapping techniques:

- deterministic rules for compliance-critical mapping
- LLM-assisted extraction where heuristics alone are insufficient
- validation/normalization to produce trustworthy golden records

### Open Ecosystem

\{kehrnel\} is designed as an open data ecosystem runtime:

- open APIs and explicit strategy contracts
- domain-agnostic architecture
- pluggable persistence backends (MongoDB today; extensible model)
- portability across deployment topologies

## Current Reference Implementation

The current production-grade reference strategy is **openEHR RPS Dual** on MongoDB. It demonstrates how \{kehrnel\} handles patient-centric and population-wide access patterns at scale.

This is a reference strategy, not the boundary of the platform.

## Next Steps

- [Vision & Roadmap](/docs/vision/roadmap)
- [Installation Guide](/docs/getting-started/installation)
- [Quick Start](/docs/getting-started/quickstart)
- [Strategy Overview](/docs/strategies/overview)
- [Architecture Overview](/docs/architecture/overview)
