# AQL→MQL Parity Status (RPS-Dual)

Current state
- Legacy AQL transformer stack is vendored and wired through the new runtime/strategy plugin.
- Strict goldens for basic patient and cross-patient flows now pass (PipelineBuilder → `$match`, SearchPipelineBuilder → `$search` with compound/must).
- Debug payload from `/v1/environments/{env}/compile_query?debug=true` includes builder selection, scope, and reason.

Known gaps
- Full vaccination AQL parity is not guaranteed yet (projection breadth, full CONTAINS nesting, and complete dictionary coverage still pending).
- Path resolution and regex/wildcard patterns depend on fixture dictionaries; production dictionaries must be verified before claiming full parity.

How to debug
- Call `POST /v1/environments/{env}/compile_query?debug=true` with your AQL payload.
- Inspect `explain.builder` (chosen, scope, reason), `legacy_ast`, `schema`, and `dicts` in the response.
- If patient scope routes to `$search`, check the scope/predicate shaping in the adapted AST.
- If projections lose fields, verify SELECT columns in the adapted AST and dictionary/shortcut content.

Next steps (future tickets)
- Enrich AST adapter with full CONTAINS/alias tree for vaccination examples.
- Expand `_codes` / `_shortcuts` with real-world coverage.
- Lock golden tests to full vaccination AQLs once fixtures and adapter are complete.
