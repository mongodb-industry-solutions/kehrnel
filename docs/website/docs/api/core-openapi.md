---
sidebar_position: 2
---

# Core OpenAPI

Canonical OpenAPI entry points:

- `/openapi.json` (full runtime spec)
- `/openapi/core.json` (core/admin subset)
- `/openapi/domains/{domain}.json` (domain slice)
- `/openapi/strategies/{domain}/{strategy}.json` (strategy slice)

Use these endpoints as the source of truth for code generation and contract validation.

## Why this matters

\{kehrnel\} evolves as a multi-strategy runtime. OpenAPI slices let teams consume only the part they need while preserving a single canonical contract model.
