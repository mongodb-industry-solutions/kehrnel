# HDL ↔ kehrnel contract

Healthcare Data Lab is the design studio.
kehrnel is the execution kernel.

The split is:

- HDL selects source semantic authorities and consumption contracts. It authors
  ContextObject definitions, Blocks, Context Maps, Instances, and Con2L
  artifacts only when a derived or composed semantic mode requires them.
- HDL previews and inspects runtime behavior.
- kehrnel preserves native FHIR, openEHR, or CDISC semantics where sufficient;
  resolves ContextObject contracts where required; and compiles deterministic,
  strategy-specific runtime work.

For the ContextObjects stack, the concrete seam is:

- HDL publishes tenant definitions into `kehrnel_context_catalog`
- HDL calls strategy ops on a context-capable kehrnel strategy
- kehrnel resolves and compiles the request against those published definitions

Current shared ops:

- `resolve_context_contract`
- `compile_con2l`
- `summarize_object_map`
- `negotiate_con2l`

