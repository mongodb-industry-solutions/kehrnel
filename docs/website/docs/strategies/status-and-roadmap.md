---
sidebar_position: 2
---

# Strategy Status & Roadmap

## Current state

`openehr.rps_dual` is the current production-grade reference strategy.

`fhir.clinical_cdr` is an implemented reference strategy for native FHIR R5 persistence, synthetic generation, denormalization, and FHIR Search.

`snomedct.mongodb` is an implemented preview strategy for SNOMED CT terminology persistence. It includes pack discovery, manifest/spec/schema/defaults, canonical transform, local release discovery, inspect/diff/ingest ops, sidecar rebuild, index creation, lookup, basic ECL, lexical search, grounding, domain API routes, and contract tests.

Remaining work before declaring it production-grade:

- Validate the customer-staged licensed JSON release workflow on the full official file.
- Run a large-release soak test on the full official JSON file.
- Validate ingest, sidecar rebuild, and index creation against a real MongoDB tenant binding.
- Extend ECL support beyond the basic subset.
- Decide whether Atlas Search/vector retrieval should be a separate optional search strategy or an optional mode inside this pack.

## About other strategy directories

You may see additional strategy folders in the repository intended to communicate direction, experimentation, or future packaging patterns.

Not all strategy directories are activation-ready.

Examples include exploratory scaffolds or partial packs that illustrate architecture patterns but are not yet shipped as supported runtime strategies.

Readiness criteria for "functional" strategies include:

- complete manifest/spec/schema/defaults
- runtime activation support
- stable ingest/query operations
- documented operational behavior

## Design intent

\{kehrnel\} strategy packs are designed as a learning and execution model for formal persistence strategy definition (`manifest.json`, `spec.json`, schema contracts).

Roadmap exploration includes stronger formal modeling, including potential LinkML-aligned specifications.
