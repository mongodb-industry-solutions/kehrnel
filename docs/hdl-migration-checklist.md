# HDL Migration Checklist (Kehrnel Runtime)

Use Kehrnel as the source of truth for executable strategies; HDL should remain a pure UI over these endpoints.

1) Catalog
- Fetch `GET /v1/strategies` (and optional `/v1/strategies/{id}`) to render catalog cards from manifest.ui, config_schema, default_config, and ops.

2) Activation
- POST `/v1/environments/{env}/activate` with {strategy_id, version, config, bindings} OR {strategy_id, version, config, bindings_ref}.
- Preferred for HDL: use `bindings_ref` to point to encrypted workspace/env secrets; keep raw bindings out of Kehrnel.
- Store env_id + strategy_id per HDL environment; do not persist secrets in HDL client.

3) Compile preview (explain)
- POST `/v1/environments/{env}/compile_query` (optionally `?debug=true`).
- Render `engine`, `pipeline`, `explain.builder/scope/reason`, `dicts` sources, and warnings.

4) Execute
- POST `/v1/environments/{env}/query` with payload matching the strategy protocol (e.g., {protocol, query}).

5) Maintenance ops
- Render ops from manifest.ops[*].input_schema and call `POST /v1/environments/{env}/extensions/{strategy}/{op}`.
- Treat `kind == "maintenance"` as cache-invalidation events; results are structured per output_schema.

6) Security / bindings
- Kehrnel stores only bindings_meta unless explicitly configured with `allow_plaintext_bindings=true`.
- Configure Kehrnel resolver with `KEHRNEL_BINDINGS_RESOLVER=module:function` to resolve `bindings_ref` at runtime.

7) Naming
- User-facing verb is `query`. Atlas Search is an engine/stage; do not present “full-text search” as the feature.
