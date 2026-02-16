---
sidebar_position: 5
---

# Query Translation

Query translation is the concept of compiling a domain query language into an execution-ready backend query plan.

Generic pipeline:
1. parse source query
2. validate semantic constraints
3. resolve paths/aliases/bindings
4. produce executable backend plan
5. execute and project response shape

## Why It Matters

- allows domain-native querying while keeping storage flexibility
- isolates domain semantics from persistence details
- enables optimization by runtime (indexes, search engine, planner)

## Cross-Domain Pattern

- input language: domain-specific (for example AQL, FHIR search, SQL-like DSL)
- target plan: persistence-specific (for example MongoDB aggregation pipeline)
- contract: stable result schema + predictable errors

## Where Implementation Lives

This page is concept-only. Concrete compilers belong to strategy/domain docs.

Current implementation examples:
- [openEHR RPS Dual Query Translation](/docs/strategies/openehr/rps-dual/query-translation)
- [openEHR Domain API](/docs/api/domains/openehr)
