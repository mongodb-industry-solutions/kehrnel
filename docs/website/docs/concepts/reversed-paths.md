---
sidebar_position: 3
---

# Hierarchical Path Encoding

Hierarchical path encoding is a concept for turning nested document paths into compact, queryable keys.

## Why Encode Paths

- nested paths are expensive to query at scale
- wildcard queries need predictable key shapes
- compact keys improve index locality

## Generic Pattern

1. traverse canonical hierarchical path
2. normalize segments
3. optionally reverse segment order for prefix matching
4. encode semantic identifiers into compact tokens
5. persist encoded key with value payload

## Trade-offs

- pros: faster filtering/search, compact storage, stable query surface
- cons: lower human readability, requires dictionary/mapping layer

## Where Implementation Lives

This page is concept-only. Encoding dictionaries and exact key formats are strategy-specific.

Current implementation example:
- [openEHR RPS Dual Data Model](/docs/strategies/openehr/rps-dual/data-model)
