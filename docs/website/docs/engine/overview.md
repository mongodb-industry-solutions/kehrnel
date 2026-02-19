---
sidebar_position: 1
---

# Engine Overview

The engine is the execution core behind `{kehrnel}` runtime behavior.

Its documentation mirrors `src/kehrnel/engine/` exactly:

- `engine/core`: runtime contracts, activation, pack loading, registry, runtime dispatch.
- `engine/common`: shared cross-domain execution utilities.
- `engine/domains`: domain execution logic (for example openEHR AQL parsing).
- `engine/strategies`: concrete strategy-pack execution behavior.

## Why This Structure Matters

When you add a new engine module, the docs path is deterministic.

Examples:

- `src/kehrnel/engine/domains/fhir/...` -> `docs/website/docs/engine/domains/fhir/...`
- `src/kehrnel/engine/strategies/x12/claim_first/...` -> `docs/website/docs/engine/strategies/x12/claim-first/...`

## Navigation

- [Engine Core](/docs/engine/core)
- [Engine Common](/docs/engine/common)
- [Engine Domains](/docs/engine/domains)
- [Engine Strategies](/docs/engine/strategies)
