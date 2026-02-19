---
sidebar_position: 2
---

# Strategy Status & Roadmap

## Current state

`openehr.rps_dual` is the current production-grade reference strategy.

## About other strategy directories

You may see additional strategy folders in the repository intended to communicate direction, experimentation, or future packaging patterns.

Not all strategy directories are activation-ready.

Examples include exploratory scaffolds or partial packs that illustrate architecture patterns but are not yet shipped as supported runtime strategies.

Readiness criteria for "functional" strategies include:

- complete manifest/spec/schema/defaults
- runtime activation support
- stable ingest/query operations
- documented operational behavior

## Design intent

\{kehrnel\} strategy packs are designed as a learning and execution model for formal persistence strategy definition (`manifest.json`, `spec.json`, schema contracts).

Roadmap exploration includes stronger formal modeling, including potential LinkML-aligned specifications.

## Where Maintenance Tools Live

`build_indexes` and `migrate_schema` are implemented as strategy operations (ops), not as a separate top-level folder.

This is intentional in `{kehrnel}`:

- operation contract: declared in `manifest.json` under `ops`
- operation implementation: executed by `run_op(...)` in the strategy plugin
- API access: generic runtime operations endpoint (`/environments/{env_id}/run`) and activation op endpoint (`/environments/{env_id}/activations/{domain}/ops/{op}`)
- CLI access: `kehrnel run <op>`
- docs: strategy CLI workflows
- tests: strategy contract tests

This keeps one execution model for all strategies. If a strategy grows significantly, ops can later be split into internal modules (for example, `maintenance/indexes.py`, `maintenance/migrations.py`) without changing the external CLI/API contract.
