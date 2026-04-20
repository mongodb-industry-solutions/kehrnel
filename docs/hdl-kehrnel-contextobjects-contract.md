# HDL ↔ kehrnel ContextObjects contract

The public vocabulary is:

- ContextObject definitions
- Blocks
- Context Maps
- Instances
- Con2L

HDL owns the authoring side.
kehrnel owns the execution side.

The contract works in two modes:

1. Inline mode
- HDL passes definitions directly in the op payload.

2. Published tenant mode
- HDL publishes normalized definitions into `kehrnel_context_catalog`.
- kehrnel loads that catalog at runtime using the strategy storage adapter.

The current payload families are:

- Con2L draft payloads
- executable Con2L payloads
- Context Map payloads
- catalog descriptors

The goal is to keep HDL flexible while keeping the final runtime compilation deterministic.
