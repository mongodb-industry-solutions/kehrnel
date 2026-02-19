---
sidebar_position: 3
---

# Core Layer API

The core layer provides runtime-level platform APIs that are independent of a specific clinical domain or strategy implementation.

## Interactive Docs

Use Swagger or ReDoc to explore the runtime contract:

- Swagger UI: `/docs`
- ReDoc: `/redoc`

Layer slices are also available:

- Core (Swagger): `/docs/core`
- Core (ReDoc): `/redoc/core`
- Domain (Swagger): `/docs/domains/{domain}`
- Domain (ReDoc): `/redoc/domains/{domain}`
- Strategy (Swagger): `/docs/strategies/{domain}/{strategy}`
- Strategy (ReDoc): `/redoc/strategies/{domain}/{strategy}`

## Runtime Control Surfaces

Primary core/admin endpoints:

- `GET /strategies`
- `GET /strategies/{strategy_id}`
- `GET /strategies/{strategy_id}/spec`
- `POST /environments/{env_id}/activate`
- `GET /environments/{env_id}/capabilities`
- `POST /environments/{env_id}/run`
- `GET /environments/{env_id}/endpoints`
- `POST /environments/{env_id}/activations/{domain}/ops/{op}`

## Why This Layer Matters

\{kehrnel\} evolves as a multi-strategy runtime. The core layer is where strategy discovery, activation, and diagnostics live across domains.

## Related

- [API Layers](/docs/api/layers)
- [Common Layer API](/docs/api/common)
- [Admin & Environment API (Detailed)](/docs/api/admin-environments)
- [Strategy Registry Endpoints (Detailed)](/docs/api/endpoints/strategy-registry)
