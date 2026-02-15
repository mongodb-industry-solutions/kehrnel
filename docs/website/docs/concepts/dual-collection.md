---
sidebar_position: 4
---

# Dual-View Persistence

Dual-view persistence stores the same logical record in two synchronized shapes optimized for different workloads.

## Typical Split

- primary view: fidelity-first, full record, operational consistency
- query/search view: access-first, projection optimized for discovery/analytics

## Why Use It

- avoid forcing one storage shape to satisfy incompatible query patterns
- reduce latency for cross-record search workloads
- keep canonical operational behavior intact

## Required Guarantees

- deterministic transform from primary to query/search view
- version and lineage traceability
- clear failure/retry semantics for sync
- observability for drift detection

## Where Implementation Lives

This page is concept-only. Collection names, schemas, and index definitions are strategy-specific.

Current implementation example:
- [openEHR RPS Dual Introduction](/docs/strategies/openehr-rps-dual/introduction)
