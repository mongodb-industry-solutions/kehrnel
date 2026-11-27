# FHIR schema & search assets (outside `src/`)

This directory is **not** part of the installable Python package. It holds
shared FHIR reference data used by `fhir-search-to-mql` and resource-config tooling.

```
schema/
  fhir.schema.v5.json
  fhir.schema.v6.json
  hl7.fhir.r5.search/package/    # SearchParameter-*.json
  hl7.fhir.r6.search/package/
  indexes/                      # generated — see indexes/README.md
```

Python code lives in `src/fhir_search_to_mql/schema/`. Point elsewhere with
`FHIR_SCHEMA_ROOT` if this folder is relocated.
