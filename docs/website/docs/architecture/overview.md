---
sidebar_position: 1
---

# Architecture Overview

\{kehrnel\} is a runtime platform for healthcare data execution, not a single fixed pipeline.

## Core Principle

**Control plane vs execution plane**:

- **Healthcare Data Lab**: workspace, governance, UX, orchestration
- **\{kehrnel\}**: strategy runtime, query execution, transformations, operations

## High-Level Architecture

```text
Clients (HDL UI, API consumers, automation)
        |
        v
FastAPI Runtime (domain APIs + admin APIs + strategy APIs)
        |
        v
Strategy Runtime (activation registry + strategy packs + bindings)
        |
        v
Persistence Backends (MongoDB today, extensible model)
```

## Multi-Domain / Multi-Strategy Runtime

\{kehrnel\} supports:

- multiple domain APIs (current: openEHR, extensible)
- multiple strategy packs per domain
- independent activation per environment
- operational probes and lifecycle controls per activation

## Data Workloads Covered

- canonical structured CDR workflows
- search and analytics projections
- synthetic data generation
- unstructured clinical report extraction (roadmap)
- hybrid rule + LLM mapping to golden records (roadmap)

## Deployment Topologies

- shared instance for multiple workspaces
- dedicated tenant runtime and data stack
- cloud and self-managed variants

## Related

- [Vision & Roadmap](/docs/vision/roadmap)
- [Strategies Overview](/docs/strategies/overview)
