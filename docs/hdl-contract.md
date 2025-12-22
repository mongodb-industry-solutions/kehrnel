# Healthcare Data Lab ↔ Kehrnel Contract

Kehrnel is the source of truth for executable strategies. HDL (Healthcare Data Lab) is a UI/blueprint that references Kehrnel manifests and calls Kehrnel APIs for runtime operations.

## What lives where
- **Kehrnel**: strategy manifests, config schemas, defaults, ops, runtime execution (plan/apply/transform/ingest/query/compile_query, extensions/ops).
- **HDL**: UX/narrative (“blueprints”), per-environment selections and config, bindings secrets managed client-side.

## API calls HDL should use
- List strategies (catalog): `GET /v1/strategies`
- Strategy details: `GET /v1/strategies/{id}`
- Activate environment: `POST /v1/environments/{env}/activate` with `{strategy_id, version, config, bindings}`
- Plan/apply/transform/ingest/query: `POST /v1/environments/{env}/{op}`
- Compile-only (preview for explain): `POST /v1/environments/{env}/compile_query` (add `debug=true` to see builder/scope/reason and legacy AST)
- Maintenance ops: `POST /v1/environments/{env}/extensions/{strategy}/{op}` (ops are declared in `manifest.json -> ops[]`)

## UI expectations
- Use `manifest.ui`, `config_schema`, `default_config`, and `ops` to render catalog, config forms, and maintenance buttons.
- Verb is **query** (not “search”); cross-patient queries may use Atlas `$search` internally, but the API verb is query. Explain payload includes `builder` (chosen pipeline/search), `scope`, and `reason`.
- Bindings: Kehrnel stores only `bindings_meta` by default; plaintext bindings are stored only if explicitly allowed.

## Security/behavior
- If bindings are not persisted, Kehrnel will require them per request and return a clear error.
- Atlas Search must remain stage 0 for cross-patient pipelines; patient pipelines start with `$match`. Unsupported predicates move to post-`$search` `$match` with warnings in explain.
