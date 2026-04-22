# Kehrnel ContextObjects docs

This folder contains the execution-kernel documentation that HDL links to when it explains the ContextObjects, Con2L, Context Maps, and copilot seam.

Recommended order:

1. [HDL contract](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/hdl-contract.md)
2. [ContextObjects runtime overview](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/contextobjects-runtime-overview.md)
3. [HDL ↔ kehrnel ContextObjects contract](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/hdl-kehrnel-contextobjects-contract.md)
4. [Con2L negotiation and runtime](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/con2l-negotiation-and-runtime.md)
5. [Context Maps runtime](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/objectmaps-runtime.md)
6. [ContextObject strategy ops](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/contextobjects-strategy-ops.md)
7. [Tenant context catalog publication](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/tenant-context-catalog-publication.md)
8. [Synthetic contract](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/hdl-kehrnel-synthetic-contract-v2.md)

The current kernel implementation lives in:

- [contextobjects package](/Users/francesc.mateu/Documents/GitHub/kehrnel/src/kehrnel/contextobjects)
- [FHIR ContextObjects vitals strategy](/Users/francesc.mateu/Documents/GitHub/kehrnel/src/kehrnel/engine/strategies/fhir/fhir_contextobjects_vitals_window/strategy.py)
- [X12 CO single strategy](/Users/francesc.mateu/Documents/GitHub/kehrnel/src/kehrnel/engine/strategies/x12/co_single/strategy.py)
