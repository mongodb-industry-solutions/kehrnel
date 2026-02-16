---
sidebar_position: 6
---

# Canonical To Operational Transformation

Flattening (more generally, canonical-to-operational transformation) converts rich hierarchical records into execution-oriented shapes for persistence, search, and runtime operations.

## Conceptual Stages

1. parse canonical record
2. extract semantically relevant nodes
3. normalize values
4. build one or more operational projections
5. persist with lineage metadata

## Goals

- keep canonical meaning intact
- improve runtime query performance
- support deterministic round-trip when required

## Common Outputs

- full operational view (high fidelity)
- search/query projection (compact)
- optional diagnostics/quality metrics

## Where Implementation Lives

This page is concept-only. Concrete field mappings and transformation parameters are strategy-specific.

Current implementation examples:
- [openEHR RPS Dual CLI Workflows](/docs/strategies/openehr/rps-dual/cli-workflows)
- [openEHR RPS Dual Data Model](/docs/strategies/openehr/rps-dual/data-model)
