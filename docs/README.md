# Kehrnel ContextObjects docs

This folder contains the execution-kernel documentation that HDL links to when it explains the ContextObjects, Con2L, Context Maps, and copilot seam.

Recommended order:

1. [Data Lab Core evolution and composite strategy compiler](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/data-lab-core-evolution.md)
2. [Outcome catalog and reference architecture layer](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/outcome-catalog-and-reference-architectures.md)
3. [Data Lab Core execution plan](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/data-lab-core-execution-plan.md)
4. [Semantic execution qualification](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/semantic-execution-qualification.md)
5. [Clinical intent resolution architecture](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/clinical-intent-resolution-architecture.md)
6. [Source-aware strategy compilation](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/source-aware-strategy-compilation.md)
7. [Semantic authority and execution plans](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/semantic-authority-and-execution-plans.md)
8. [Builder narrative and UI hand-off](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/builder-narrative-ui-handoff.md)
9. [Modeler Agent architecture](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/modeler-agent-architecture.md)
10. [M0 readiness baseline](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/baselines/data-lab-core-m0-readiness.md)
11. [Data Lab Core foundation contracts](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/data-lab-core-contracts.md)
12. [HDL contract](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/hdl-contract.md)
13. [ContextObjects runtime overview](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/contextobjects-runtime-overview.md)
14. [HDL ↔ kehrnel ContextObjects contract](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/hdl-kehrnel-contextobjects-contract.md)
15. [Con2L negotiation and runtime](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/con2l-negotiation-and-runtime.md)
16. [Context Maps runtime](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/objectmaps-runtime.md)
17. [ContextObject strategy ops](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/contextobjects-strategy-ops.md)
18. [Tenant context catalog publication](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/tenant-context-catalog-publication.md)
19. [Synthetic contract](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/hdl-kehrnel-synthetic-contract-v2.md)
20. [CDISC Study Data Repository strategy](/Users/francesc.mateu/Documents/GitHub/kehrnel/docs/cdisc-sdr-strategy.md)

The current kernel implementation lives in:

- [industry outcome catalog](/Users/francesc.mateu/Documents/GitHub/kehrnel/src/kehrnel/engine/outcome_catalog/catalog.json)
- [Solution Blueprint template catalog](/Users/francesc.mateu/Documents/GitHub/kehrnel/src/kehrnel/engine/blueprint_templates/catalog.json)
- [strategy delivery portfolio](/Users/francesc.mateu/Documents/GitHub/kehrnel/src/kehrnel/engine/strategies/portfolio.json)
- [source category catalog](/Users/francesc.mateu/Documents/GitHub/kehrnel/src/kehrnel/engine/source_categories/catalog.json)
- [known source profile catalog](/Users/francesc.mateu/Documents/GitHub/kehrnel/src/kehrnel/engine/source_profiles/catalog.json)
- [openEHR RPS Dual strategy](/Users/francesc.mateu/Documents/GitHub/kehrnel/src/kehrnel/engine/strategies/openehr/rps_dual)
- [openEHR IBM exact-model profile](/Users/francesc.mateu/Documents/GitHub/kehrnel/src/kehrnel/engine/strategies/openehr/rps_dual_ibm)
- [FHIR Resource Store strategy](../src/kehrnel/engine/strategies/fhir/resource_store/)
- [CDISC Study Data Repository strategy](/Users/francesc.mateu/Documents/GitHub/kehrnel/src/kehrnel/engine/strategies/cdisc/sdr)

ContextObjects, genomics, standalone SNOMED and X12 packs remain preview-only.

