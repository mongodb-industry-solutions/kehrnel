---
sidebar_position: 3
---

# Core Layer API

The core layer provides runtime-level platform APIs that are independent of a specific clinical domain or strategy implementation.

## OpenAPI Contracts

Canonical contract entry points:

- `/openapi.json` (full runtime spec)
- `/openapi/core.json` (core/admin subset)
- `/openapi/domains/{domain}.json` (domain slice)
- `/openapi/strategies/{domain}/{strategy}.json` (strategy slice)

Use these endpoints as the source of truth for code generation and contract validation.

## Runtime Control Surfaces

Primary core/admin endpoints:

- `GET /strategies`
- `GET /strategies/{strategy_id}`
- `GET /strategies/{strategy_id}/spec`
- `POST /environments/{env_id}/activate`
- `GET /environments/{env_id}/endpoints`
- `POST /environments/{env_id}/activations/{domain}/ops/{op}`

## Why This Layer Matters

\{kehrnel\} evolves as a multi-strategy runtime. OpenAPI slices let teams consume only the part they need while preserving a single canonical contract model.

## Related

- [API Layers](/docs/api/layers)
- [Common Layer API](/docs/api/common-api)
- [Admin & Environment API (Detailed)](/docs/api/admin-environments)
- [Strategy Registry Endpoints (Detailed)](/docs/api/endpoints/strategy-registry)
