---
sidebar_position: 3
---

# Composition Flattening Pipeline

This page describes how the `openehr.rps_dual` (RPS Dual) strategy converts a canonical openEHR COMPOSITION into its two stored shapes: the flattened base document and the slim search document.

It is specific to openEHR. For the cross-domain ingest model, see [Ingest and Transformation](/docs/architecture/transformation-pipeline).

## Where It Runs

The flattener is `CompositionFlattener` in `kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener`. The strategy uses it in:

| Runtime operation | Behavior |
|-------------------|----------|
| `transform` | Flattens one composition and returns `base` and `search` documents without persisting them. |
| `ingest` | Flattens one or more compositions and inserts base documents into the composition collection and search documents into the search collection (when search is enabled). |
| `synthetic_generate_batch` op | Flattens generated compositions before writing them. |
| `rebuild_slim_search_collection` op | Rebuilds search documents from stored base documents (`project_search_document`). |

Ingest fences semantic-projection readiness before the first base write and completes the fence afterwards, because base and search writes are not one transaction.

## Pipeline Overview

```text
raw document  { _id, ehr_id, composition_version, time_committed, canonicalJSON }
      |
      v
1. Tree walk          every LOCATABLE node with an archetype_node_id -> cn[] node
      |               p  = leaf-to-root numeric path, pi = instance indexes
      v
2. Code allocation    at-codes and archetype ids -> compact codes (_codes dictionary)
      |
      v
3. Search projection  mapping rules for the template select nodes -> sn[]
      |               (no rules for the template -> no search document)
      v
4. Document build     base document + optional search document
      |
      v
5. Shortcuts          key/value shortcuts (_shortcuts dictionary), if enabled
      |
      v
6. Field map          rename output fields per configuration
      |
      +--> base document    -> compositions_rps
      +--> search document  -> compositions_search
```

The step numbering follows the comments in `CompositionFlattener.transform_composition`.

## Stage Details

### 1. Tree Walk

`_walk` traverses the composition. A node is emitted into `cn` when it is the root, a list item, or a LOCATABLE that the strategy splits into its own node. Each emitted node carries:

- `data`: the node's scalar attributes, with dates coerced to BSON dates and `archetype_node_id` replaced by its code
- `p`: the path from the node up to the root, built from codes in leaf-to-root order (see [Hierarchical Path Encoding](/docs/domains/openehr/concepts/reversed-paths))
- `pi`: the list index of each path segment, stored only when some segment repeats
- `kp`: the attribute key chain, when present

### 2. Code Allocation

At-codes and archetype ids become compact codes. The strategy is configurable in `transform.coding`:

| Code type | Strategies | Default |
|-----------|------------|---------|
| at-codes | `negative_int` (for example `at0004` -> `-4`), `compact_prefix`, `sequential`, `literal` | `negative_int` |
| archetype ids | `sequential`, `short_prefix` (for example `openEHR-EHR-OBSERVATION.height.v2` -> `O-height.v2`), `literal` | `sequential` |

New codes are recorded in the code book and flushed to the `_codes` dictionary after ingest. A flattener in a non-primary role raises an error for unknown codes instead of allocating them.

### 3. Search Projection

`_project_search_nodes` applies the compiled mapping rules for the composition's template to the flattened nodes. Only matching nodes are projected into `sn`. If the template has no rules, no search document is produced.

### 4. Document Build

Base document fields (default names from the strategy configuration):

| Field | Content |
|-------|---------|
| `ehr_id` | EHR id |
| `comp_id` | composition id |
| `v` | composition version |
| `time_c` | commit time |
| `tid` | template id |
| `cn` | flattened nodes |

The search document carries `_id`, `ehr_id`, `comp_id`, `tid`, `sort_time` and `sn`.

### 5 and 6. Shortcuts and Field Map

When `transform.apply_shortcuts` is true (the default), keys and values are shortened using the shortcuts dictionary. Finally, the configured field map renames root and node fields for each output document.

## Configuration

Flattening is controlled by the RPS Dual configuration (`collections`, `fields`, `paths.separator`, `transform.coding`, `transform.apply_shortcuts`, `transform.mappings`) and by the mapping files under `ingest/config/` in the strategy pack. See [openEHR RPS Dual Configuration](/docs/strategies/openehr/rps-dual/configuration).

## Related

- [Canonical To Operational Transformation (concept)](/docs/domains/openehr/concepts/flattening)
- [Hierarchical Path Encoding](/docs/domains/openehr/concepts/reversed-paths)
- [AQL Query Engine](/docs/domains/openehr/aql-query-engine) - how flattened data is queried
- [openEHR RPS Dual Data Model](/docs/strategies/openehr/rps-dual/data-model) - storage format

