# Kehrnel ContextObjects docs

This folder contains the execution-kernel documentation that HDL links to when it explains the ContextObjects, Con2L, Context Maps, and copilot seam.

Recommended order:

1. [HDL contract](hdl-contract.md)
2. [ContextObjects runtime overview](contextobjects-runtime-overview.md)
3. [HDL ↔ kehrnel ContextObjects contract](hdl-kehrnel-contextobjects-contract.md)
4. [Con2L negotiation and runtime](con2l-negotiation-and-runtime.md)
5. [Context Maps runtime](objectmaps-runtime.md)
6. [ContextObject strategy ops](contextobjects-strategy-ops.md)
7. [Tenant context catalog publication](tenant-context-catalog-publication.md)
8. [Synthetic contract](hdl-kehrnel-synthetic-contract-v2.md)
9. [CDISC Study Data Repository strategy](cdisc-sdr-strategy.md)

The current kernel implementation lives in:

- [contextobjects package](../src/kehrnel/contextobjects)
- [FHIR Resource Store strategy](../src/kehrnel/engine/strategies/fhir/resource_store/)
- [CDISC Study Data Repository strategy](../src/kehrnel/engine/strategies/cdisc/sdr)
- [X12 CO single strategy](../src/kehrnel/engine/strategies/x12/co_single/strategy.py)
