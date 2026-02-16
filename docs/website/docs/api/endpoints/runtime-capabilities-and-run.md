# Runtime Capabilities And Run Endpoints

- `GET /environments/{env_id}/capabilities`
- `POST /environments/{env_id}/run`

These endpoints define the universal workflow contract for multi-strategy automation.

## `GET /environments/{env_id}/capabilities`

Returns:

- active domains and activation metadata in the environment
- standard runtime operations (`plan`, `apply`, `transform`, `ingest`, `query`, `compile_query`)
- strategy operations for active strategies
- optional input/output schemas (controlled by `include_schemas`)

Query parameter:

- `include_schemas` (default: `true`)

## `POST /environments/{env_id}/run`

Executes one operation with a single request shape.

Typical request body:

```json
{
  "operation": "ensure_dictionaries",
  "domain": "openehr",
  "strategy_id": "openehr.rps_dual",
  "data_mode": "profile.search_shortcuts",
  "source": {"type": "resource", "name": "src"},
  "sink": {"type": "resource", "name": "dst"},
  "payload": {}
}
```

Routing behavior:

- if `operation` is one of `plan`, `apply`, `transform`, `ingest`, `query`, `compile_query`, runtime dispatches that core operation.
- otherwise runtime dispatches strategy operation (`run_op`) with the active domain activation.

## Recommended CLI Pair

```bash
kehrnel op capabilities --env dev
kehrnel run ensure_dictionaries --env dev --domain openehr
```
