---
sidebar_position: 2
---

# Canonical To Operational Transformation

Flattening (more generally, canonical-to-operational transformation) converts rich hierarchical records into execution-oriented shapes for persistence, search, and runtime operations. In \{kehrnel\}, the openEHR RPS Dual strategy flattens canonical COMPOSITION documents.

## Conceptual Stages

1. parse the canonical record
2. extract semantically relevant nodes
3. encode paths and codes compactly
4. build one or more operational projections
5. persist the projections

## Goals

- keep canonical meaning intact
- improve runtime query performance
- allow reconstruction: RPS Dual exposes `reverse_transform`, a best-effort rebuild of a nested composition from a flattened base document

## Outputs in RPS Dual

- base document: full flattened view (high fidelity)
- search document: compact projection driven by template mapping rules

## Related

- [Composition Flattening Pipeline](/docs/domains/openehr/composition-flattening) - stage-by-stage implementation
- [openEHR RPS Dual CLI Workflows](/docs/strategies/openehr/rps-dual/cli-workflows)
- [openEHR RPS Dual Data Model](/docs/strategies/openehr/rps-dual/data-model)
- [Ingest and Transformation](/docs/architecture/transformation-pipeline) - cross-domain ingest model

