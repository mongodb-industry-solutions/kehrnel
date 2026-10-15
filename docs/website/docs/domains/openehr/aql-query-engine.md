---
sidebar_position: 2
---

# AQL Query Engine

This page describes the openEHR query path: how an AQL query is compiled by the `openehr.rps_dual` (RPS Dual) strategy into a MongoDB aggregation pipeline and routed to one of the two RPS Dual collections.

It is specific to openEHR. For how query requests reach any domain and strategy, see [Query and Execution](/docs/architecture/query-engine).

## Entry Points

AQL reaches the RPS Dual compiler through two surfaces:

| Surface | Route | Notes |
|---------|-------|-------|
| openEHR domain API | `POST /api/domains/openehr/query/aql` (plain-text AQL body) | Optional `ehr_id` and `force_search` query parameters; optional `X-AQL-Feature-Mode: extended` header. Also `/query/aql/validate`, `/query/aql/parse`, `/query/aql/mql`, `/query/ast` and stored-query routes. |
| Runtime query surface | `POST /environments/{env_id}/query` and `/environments/{env_id}/compile_query` with `domain: "openehr"` | The runtime accepts `aql` (or `query.aql`) and rejects raw AQL for any other domain (`RAW_AQL_UNSUPPORTED`). |

Both surfaces use the same RPS Dual modules: the AQL parser in `kehrnel.engine.domains.openehr.aql`, and the compiler, transformers and executor in `kehrnel.engine.strategies.openehr.rps_dual.query`.

## Processing Flow

```text
AQL text
   |
   v
AQLToASTParser                      (engine/domains/openehr/aql/aql_to_ast.py)
   |  bind $parameters; missing values -> MISSING_QUERY_PARAMETERS
   v
AST validation + alias detection    (query/transformers/ast_validator.py)
   |
   v
Context map, format and archetype resolution
   |  uses the _codes and _shortcuts dictionaries
   v
Collection selection                (query/strategy_selector.py, AQLtoMQLTransformer)
   |
   +--> $match pipeline  on the composition collection   (engine: mongo_pipeline)
   |
   +--> $search pipeline on the search collection        (engine: text_search_dual)
   |
   v
Executor                            (query/executor.py)
   |  storage.aggregate(collection, pipeline)
   v
Rows + explain
```

`build_query_pipeline_from_ast` in `query/compiler.py` drives the compile steps. If the codes or shortcuts dictionaries are missing, compilation continues and the plan carries `dict_missing_codes` / `dict_missing_shortcuts` warnings.

## Collection Selection

RPS Dual stores each composition in two shapes (see [Dual-View Persistence](/docs/domains/openehr/concepts/dual-collection)):

| Collection (default name) | Shape | Pipeline |
|---------------------------|-------|----------|
| `compositions_rps` | full flattened composition (`cn` nodes) | `$match` |
| `compositions_search` | slim search projection (`sn` nodes) | `$search` on the Atlas Search index (default `search_nodes_index`) |

The routing rule in `AQLtoMQLTransformer.should_use_search_strategy`:

- `force_search=true` selects the search collection.
- An `ehr_id` scope selects the `$match` path on the composition collection.
- Without an `ehr_id`, `should_prefer_match_for_cross_patient_ast` keeps the query on the `$match` path when the query only uses predicates expected to be backed by B-tree indexes on the composition store (for example `e/ehr_id/value`, `c/uid/value`, `c/archetype_details/template_id/value`) and no `LET` clause. Other cross-patient queries use the search collection.

On the domain API, if Atlas Search is unavailable on the MongoDB target, the service retries with the standard `$match` pipeline.

## Execution

The executor in `query/executor.py` accepts plans whose engine is `mongo_pipeline`, `text_search_dual` or an `atlas_search*` engine, and runs them with `storage.aggregate(collection, pipeline)`. Any other engine fails with `OPENEHR_QUERY_ENGINE_UNSUPPORTED`; a plan without a target collection fails with `OPENEHR_QUERY_PLAN_INVALID`.

The explain block reports the activation, strategy id and version, config hash, manifest digest, engine, scope (`patient` or `cross_patient`), target collection and the generated pipeline.

## Query Examples

The examples below show the shape of the generated pipelines. Exact encoded paths depend on the active coding configuration and the contents of the `_codes` dictionary.

### Patient-Scoped Query

```aql
SELECT c/uid/value, c/name/value
FROM EHR e
CONTAINS COMPOSITION c
WHERE e/ehr_id/value = 'patient-001'
```

Routed to `compositions_rps` with a `$match` on the EHR id, followed by projection stages.

### Cross-Patient Query

```aql
SELECT e/ehr_id/value, o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude
FROM EHR e
CONTAINS OBSERVATION o[openEHR-EHR-OBSERVATION.blood_pressure.v2]
WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude > 140
```

Routed to `compositions_search`. The pipeline starts with a `$search` stage that uses `embeddedDocument` over the `sn` nodes, matching the encoded path on `sn.p` and the value range on the node data.

## Related

- [Query Translation (concept)](/docs/domains/openehr/concepts/aql-to-mql)
- [Dual-View Persistence](/docs/domains/openehr/concepts/dual-collection)
- [openEHR RPS Dual Query Translation](/docs/strategies/openehr/rps-dual/query-translation) - strategy specifics
- [Query and Execution](/docs/architecture/query-engine) - cross-domain query model

