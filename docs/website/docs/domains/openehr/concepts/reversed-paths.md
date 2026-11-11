---
sidebar_position: 3
---

# Hierarchical Path Encoding

Hierarchical path encoding turns nested document paths into compact, queryable keys. The openEHR RPS Dual strategy encodes each flattened node's path this way.

## Why Encode Paths

- nested paths are expensive to query at scale
- wildcard and prefix queries need predictable key shapes
- compact keys keep documents and indexes smaller

## Pattern in RPS Dual

1. traverse the composition, tracking the codes of ancestor nodes
2. replace at-codes and archetype ids with compact codes from the `_codes` dictionary
3. write the path in leaf-to-root order (`<leaf>.<parent>...<root>`), using the configured separator (default `.`)
4. store the path in the node's `p` field, with optional repetition indexes in `pi`

Because the leaf comes first, a query for a given element can match the start of the path regardless of where the element sits deeper in the hierarchy.

## Trade-offs

- pros: compact storage, index-friendly filtering, stable query surface
- cons: lower human readability; requires the codes dictionary to encode queries and decode results

## Related

- [Composition Flattening Pipeline](/docs/domains/openehr/composition-flattening) - code allocation strategies
- [openEHR RPS Dual Data Model](/docs/strategies/openehr/rps-dual/data-model)

