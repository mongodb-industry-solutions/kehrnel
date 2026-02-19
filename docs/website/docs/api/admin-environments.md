---
sidebar_position: 6
---

# Admin & Environment API

Administrative APIs manage runtime strategy lifecycle and environment behavior.

## OpenAPI Docs

- Swagger (core): `/docs/core`
- ReDoc (core): `/redoc/core`

Examples:

- `GET /strategies`
- `GET /strategies/{strategy_id}`
- `GET /strategies/{strategy_id}/spec`
- `POST /environments/{env_id}/activate`
- `GET /environments/{env_id}/capabilities`
- `POST /environments/{env_id}/run`
- `GET /environments/{env_id}/endpoints`

These APIs are central to controlled rollout, activation upgrades, and operational governance.

## Environment Capabilities

`GET /environments/{env_id}/capabilities` returns:

- active domains in the environment
- standard runtime operations (`plan`, `apply`, `transform`, `ingest`, `query`, `compile_query`)
- strategy-specific operations exposed by active strategy manifests

This is the preferred discovery contract for CLI and UI workflow builders.

## Universal Run Endpoint

`POST /environments/{env_id}/run` executes one operation with a single contract.
