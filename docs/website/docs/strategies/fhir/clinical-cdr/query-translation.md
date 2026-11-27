---
sidebar_position: 5
---

# FHIR Search → MQL Translation

`fhir.clinical_cdr` compiles **FHIR REST search parameters** into **MongoDB aggregation pipelines** using **fhir-mql** (`fhir_search_to_mql.FHIRSearchConverter`).

Execution runs the compiled pipeline against the per-resource-type collection for the requested `resource_type`.

## Translation pipeline

```
┌─────────────────────────────────────────────────────────────┐
│  Client input                                               │
│  • criteria: { "family": "Smith", "given": "John" }       │
│  • fhir_search: "family=Smith&given=John"                   │
│  • or FHIR search URL                                       │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Normalize (query.py)                                       │
│  criteria → query string; parse URL query if needed         │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  fhir-mql FHIRSearchConverter                               │
│  Resource YAML + compartments → MQL filter / pipeline       │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  QueryPlan (engine: fhir_mql)                               │
│  filter, collection, resource_type, database, explain       │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  MongoDB aggregation on {ResourceType} collection           │
│  _count / _offset from query_input                          │
└─────────────────────────────────────────────────────────────┘
```

## Entry points

| Surface | Method |
|---------|--------|
| Strategy `compile_query` / `execute_query` | Universal `/environments/{env}/run` with `operation: compile_query` or `query` |
| `fhir_search` op | Single op: compile + execute; `explain_only: true` for plan only |
| Domain API | `POST /api/domains/fhir/search` → Bundle |
| `fhir_list_search_params` | Lists parameters from fhir-mql config for a resource type |

All paths share `query.compile_fhir_query` and `query.execute_fhir_query` in the strategy pack.

## Input shapes

### Structured criteria

```json
{
  "resource_type": "Patient",
  "criteria": {
    "family": "Smith",
    "given": "John"
  },
  "_count": 20,
  "_offset": 0
}
```

Criteria keys map to FHIR search parameter names. List values emit repeated parameters.

### FHIR search string

```json
{
  "resource_type": "Slot",
  "fhir_search": "status=free&schedule=Schedule/abc"
}
```

### Universal runner wrapper

When calling `/run` with `operation: query`, nest search fields under `query`:

```json
{
  "domain": "fhir",
  "operation": "query",
  "payload": {
    "query": {
      "resource_type": "Patient",
      "criteria": { "family": "Smith" },
      "_count": 10
    }
  }
}
```

Top-level `resource_type` without a `query` wrapper is not forwarded by the runtime dispatch path.

## QueryPlan output

`compile_query` returns a plan similar to:

```json
{
  "engine": "fhir_mql",
  "filter": { "...": "..." },
  "collection": "Patient",
  "resource_type": "Patient",
  "database": "fhir_synthetic",
  "query_input": { "resource_type": "Patient", "criteria": { ... } },
  "explain": { }
}
```

`explain` is enriched with kehrnel strategy metadata for debugging. Use `fhir_search` with `explain_only: true` to return the plan without executing.

## Execution semantics

- **Collection routing**: `collection` = `{prefix}{resource_type}` from activation config.
- **Pagination**: `_count` and `_offset` applied from normalized `query_input`.
- **Multi-step queries**: Some chained searches resolve in multiple MQL steps (fhir-mql spike semantics); the strategy merges step results before returning rows.
- **Prerequisite**: Documents must be denormalized (`_search` present) for parameters defined in YAML; otherwise compile may succeed but matches can be empty.

## Configuration dependencies

`FHIRSearchConverter` is constructed with:

- `config_dir` — optional override for resource YAML
- `compartment_definitions_dir` — optional override for compartments

When null, fhir-mql bundled defaults apply.

Install requirement:

```text
pip install -e ".[fhir]"
```

Missing install raises `FHIR_LIBS_NOT_INSTALLED`.

## Golden tests

Contract tests in `tests/contract/clinical_cdr/fixtures/fhir_golden_queries.json` cover compile (and execute when `FHIR_CONTRACT_MONGO=1`) for Patient, Schedule, Slot, and Appointment searches.

## Related

- [Data model](./data-model.md) — `_search` and indexes
- [CLI workflows](./cli-workflows.md) — curl examples
- [FHIR domain API](/docs/api/domains/fhir/) — Bundle HTTP response
