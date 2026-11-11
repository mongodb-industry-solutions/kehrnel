---
sidebar_position: 1
---

# Dual-View Persistence

Dual-view persistence stores the same logical record in two synchronized shapes optimized for different workloads. In \{kehrnel\} this is the core idea of the openEHR `openehr.rps_dual` strategy.

## The Split in RPS Dual

| View | Default collection | Purpose |
|------|--------------------|---------|
| primary | `compositions_rps` | fidelity-first: every flattened node of the composition (`cn`) |
| search | `compositions_search` | access-first: slim projection of mapped nodes (`sn`), queried through Atlas Search |

The search view is optional per activation (`collections.search.enabled`).

## Why Use It

- avoid forcing one storage shape to satisfy incompatible query patterns
- keep patient-scoped reads on B-tree indexes and cross-patient search on a search index
- keep canonical operational behavior intact

## How RPS Dual Keeps the Views Aligned

- the search document is derived from the base document by the same flattener and mapping rules ([Composition Flattening Pipeline](/docs/domains/openehr/composition-flattening))
- ingest fences semantic-projection readiness before writing, because base and search writes are not one transaction
- maintenance ops rebuild or recover the search view: `rebuild_slim_search_collection`, `recover_search_projection_rebuild`, `semantic_projection_preflight`

## Related

- [AQL Query Engine](/docs/domains/openehr/aql-query-engine) - how queries choose a view
- [openEHR RPS Dual Introduction](/docs/strategies/openehr/rps-dual/introduction)

