# HDL ↔ kehrnel contract

Healthcare Data Lab is the design studio.
kehrnel is the execution kernel.

The split is:

- HDL authors ContextObject definitions, Blocks, Context Maps, Instances, and Con2L artifacts.
- HDL previews and inspects runtime behavior.
- kehrnel resolves ContextObject contracts, negotiates Con2L, compiles deterministic query plans, and executes strategy-specific runtime work.

For the ContextObjects stack, the concrete seam is:

- HDL publishes tenant definitions into `kehrnel_context_catalog`
- HDL calls strategy ops on a context-capable kehrnel strategy
- kehrnel resolves and compiles the request against those published definitions

Current shared ops:

- `resolve_context_contract`
- `compile_con2l`
- `summarize_object_map`
- `negotiate_con2l`
