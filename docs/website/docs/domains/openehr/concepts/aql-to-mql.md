---
sidebar_position: 4
---

# Query Translation

Query translation compiles a domain query language into an execution-ready backend plan. For openEHR, the RPS Dual strategy compiles AQL into MongoDB aggregation pipelines.

## Pipeline

1. parse AQL into an AST
2. bind query parameters and validate the AST
3. resolve aliases, archetype paths and codes
4. choose the target view (composition collection or search collection)
5. produce the pipeline, execute it and shape the result rows

## Why It Matters

- clients keep using AQL while storage stays optimized for MongoDB
- domain semantics stay separate from persistence details
- the compiler can pick `$match` or `$search` based on query shape

## Related

- [AQL Query Engine](/docs/domains/openehr/aql-query-engine) - components and routing rules
- [openEHR RPS Dual Query Translation](/docs/strategies/openehr/rps-dual/query-translation)
- [openEHR Domain API](/docs/api/domains/openehr)
- [Query and Execution](/docs/architecture/query-engine) - how other domains compile queries

