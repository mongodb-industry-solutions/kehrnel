# ContextObject strategy ops

The following shared ops are now available on context-capable strategies:

- `resolve_context_contract`
- `compile_con2l`
- `summarize_object_map`
- `negotiate_con2l`

They are currently wired into:

- [FHIR ContextObjects vitals window](/Users/francesc.mateu/Documents/GitHub/kehrnel/src/kehrnel/engine/strategies/fhir/fhir_contextobjects_vitals_window/strategy.py)
- [X12 CO single](/Users/francesc.mateu/Documents/GitHub/kehrnel/src/kehrnel/engine/strategies/x12/co_single/strategy.py)

This lets HDL talk to the kernel through stable runtime verbs while each strategy keeps its own storage/query specifics.
