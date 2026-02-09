# Healthcare Data Lab ↔ Kehrnel Contract

Kehrnel is the source of truth for executable strategies. HDL (Healthcare Data Lab) is a UI/blueprint that references Kehrnel manifests and calls Kehrnel APIs for runtime operations.

## What lives where
- **Kehrnel**: strategy manifests, config schemas, defaults, ops, runtime execution (plan/apply/transform/ingest/query/compile_query, extensions/ops).
- **HDL**: UX/narrative (“blueprints”), per-environment selections and config, plus secure secret management per workspace environment.

## API calls HDL should use
- List strategies (catalog): `GET /strategies`
- Strategy details: `GET /strategies/{id}`
- Activate environment: `POST /environments/{env}/activate` with `{strategy_id, version, config, bindings}` or `{..., bindings_ref}`
- Plan/apply/transform/ingest/query: `POST /environments/{env}/{op}`
- Compile-only (preview for explain): `POST /environments/{env}/compile_query` (add `debug=true` to see builder/scope/reason and legacy AST)
- Maintenance ops: `POST /environments/{env}/extensions/{strategy}/{op}` (ops are declared in `manifest.json -> ops[]`)

## UI expectations
- Use `manifest.ui`, `config_schema`, `default_config`, and `ops` to render catalog, config forms, and maintenance buttons.
- Verb is **query** (not “search”); cross-patient queries may use Atlas `$search` internally, but the API verb is query. Explain payload includes `builder` (chosen pipeline/search), `scope`, and `reason`.
- Bindings: Kehrnel stores only `bindings_meta` by default; plaintext bindings are stored only if explicitly allowed.
- Recommended for HDL: send `bindings_ref` (reference to encrypted secret owned by HDL). Kehrnel resolves it via `KEHRNEL_BINDINGS_RESOLVER=module:function`.

## Security/behavior
- If bindings are not persisted, Kehrnel will require them per request and return a clear error.
- Atlas Search must remain stage 0 for cross-patient pipelines; patient pipelines start with `$match`. Unsupported predicates move to post-`$search` `$match` with warnings in explain.
