---
sidebar_position: 6
---

# Admin & Environment API

Administrative APIs manage runtime strategy lifecycle and environment behavior.

Examples:

- `GET /strategies`
- `GET /strategies/{strategy_id}`
- `GET /strategies/{strategy_id}/spec`
- `GET /environments`
- `POST /environments`
- `GET /environments/{env_id}`
- `PATCH /environments/{env_id}`
- `DELETE /environments/{env_id}`
- `POST /environments/{env_id}/activate`
- `GET /environments/{env_id}/activations`
- `GET /environments/{env_id}/capabilities`
- `POST /environments/{env_id}/run`
- `GET /environments/{env_id}/endpoints`
- `POST /environments/{env_id}/activations/{domain}/upgrade`
- `POST /environments/{env_id}/activations/{domain}/rollback`
- `DELETE /environments/{env_id}/activations/{domain}`

These APIs are central to controlled rollout, activation upgrades, and operational governance.

## Environment Capabilities

`GET /environments/{env_id}/capabilities` returns:

- active domains in the environment
- standard runtime operations (`plan`, `apply`, `transform`, `ingest`, `query`, `compile_query`)
- strategy-specific operations exposed by active strategy manifests

This is the preferred discovery contract for CLI and UI workflow builders.

## Environment Lifecycle

The environment shell is managed before activation:

- `GET /environments` lists available environments
- `POST /environments` creates one
- `GET /environments/{env_id}` returns current metadata and activation summary
- `PATCH /environments/{env_id}` updates name, description, bindings reference, or metadata
- `DELETE /environments/{env_id}` removes the environment shell

Activation history is managed separately through the activation endpoints (`list`, `upgrade`, `rollback`, `delete`).

## Universal Run Endpoint

`POST /environments/{env_id}/run` executes one operation with a single contract.

Request body (typical):

```json
{
  "operation": "synthetic_generate_batch",
  "domain": "openehr",
  "strategy_id": "openehr.rps_dual",
  "data_mode": "profile.search_shortcuts",
  "source": {"type": "resource", "name": "src"},
  "sink": {"type": "resource", "name": "dst"},
  "payload": {"patient_count": 100, "dry_run": true}
}
```

The runtime routes to either:

- standard environment operation (`plan/apply/transform/ingest/query/compile_query`), or
- strategy operation (`run_op`) when `operation` is strategy-specific.

In practice, newer automation should prefer `POST /environments/{env_id}/run` for one-off workflows such as `ensure_dictionaries`, `synthetic_generate_batch`, or `build_search_index_definition`, while retaining the explicit query endpoints when you want a dedicated `compile_query` or `query` surface.
