---
sidebar_position: 1
sidebar_label: Introduction
slug: /
---

# Introduction to kehrnel

**\{kehrnel\}** is a multi-strategy, multi-domain runtime for healthcare data engineering.

It runs on MongoDB with official strategies for four standards: openEHR, FHIR, CDISC and SNOMED CT. The direction is broader still: document-first workloads, hybrid extraction pipelines, and pluggable persistence targets.

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

The official strategies cover openEHR, FHIR, CDISC and SNOMED CT (see [Strategies](/docs/strategies/overview)). Document-centric workflows are on the roadmap.

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

## Official Strategies

| Domain | Strategy | What it does |
|--------|----------|--------------|
| openEHR | [RPS Dual](/docs/strategies/openehr/rps-dual/introduction) (`openehr.rps_dual`) | Canonical compositions plus a search projection; AQL compiled to MongoDB. The most mature, production-grade path. |
| FHIR | [Resource Store](/docs/strategies/fhir/resource-store/) (`fhir.resource_store`) | Native FHIR resources with FHIR search compiled to MongoDB. |
| CDISC | [Study Data Repository](/docs/strategies/cdisc/sdr/) (`cdisc.sdr`) | Governed study snapshots with Dataset-JSON and XPT exchange and governed study queries. |
| SNOMED CT | [SNOMED CT on MongoDB](/docs/strategies/snomedct/mongodb/) (`snomedct.mongodb`) | Licensed SNOMED CT releases served as a terminology provider. |

Each strategy declares its own maturity; see [Strategy Status & Roadmap](/docs/strategies/status-and-roadmap).

## Next Steps

- [Vision & Roadmap](/docs/vision/roadmap)
- [Installation Guide](/docs/getting-started/installation)
- [Quick Start](/docs/getting-started/quickstart)
- [Strategy Overview](/docs/strategies/overview)
- [Architecture Overview](/docs/architecture/overview)

