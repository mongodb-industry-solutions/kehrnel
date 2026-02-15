---
sidebar_position: 2
---

# Data Sampling

Data sampling is a cross-domain operational concept used to:

- quick strategy validation
- safe dry-runs before full-scale ingestion
- developer testing against realistic subsets
- quality checks during transformation evolution

## Typical Sampling Modes

- count-limited samples (`N` records)
- ratio-based samples (for example 1% of source)
- predicate-driven samples (for example specific cohort/site/date range)
- stratified samples (balanced by class/source)

## Design Principles

- deterministic option for reproducibility (seeded)
- clear traceability from sample back to source
- optional de-identification and governance controls
- fast feedback loop for strategy iteration

## Where Implementation Lives

This page defines the concept only. Exact parameters and runtime behavior are strategy-specific.

Current implementation example:
- [openEHR RPS Dual CLI Workflows](/docs/strategies/openehr-rps-dual/cli-workflows)
