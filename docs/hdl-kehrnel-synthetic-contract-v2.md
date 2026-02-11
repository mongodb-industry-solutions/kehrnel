# HDL <-> Kehrnel Synthetic Contract (V2)

This document defines the integration contract for synthetic batch generation with multi-strategy support.

## 1) Goal

HDL owns user interaction, auth/session, and environment selection.  
Kehrnel owns strategy execution and data generation.

## 2) Runtime Contract

1. HDL activates strategy for environment with `bindings_ref` (no plaintext URI).
2. HDL submits synthetic batch jobs to Kehrnel.
3. HDL polls job status and renders progress/history.

## 3) Kehrnel API Endpoints

- `POST /environments/{env_id}/synthetic/jobs`
- `GET /environments/{env_id}/synthetic/jobs`
- `GET /environments/{env_id}/synthetic/jobs/{job_id}`
- `POST /environments/{env_id}/synthetic/jobs/{job_id}/cancel`

Request body:

```json
{
  "domain": "openehr",
  "op": "synthetic_generate_batch",
  "payload": {}
}
```

## 4) Payload (Preferred)

```json
{
  "patient_count": 10000,
  "generation_mode": "auto",
  "model_source": {
    "database_name": "hdl-team",
    "catalog_collection": "user-data-models",
    "links_collection": "semantic_links"
  },
  "models": [
    {
      "model_id": "opt.tumour_summary.v1",
      "min_per_patient": 1,
      "max_per_patient": 2,
      "weight": 1.0,
      "sample_pool_size": 25
    }
  ],
  "links": [
    {
      "from": "opt.tumour_summary.v1",
      "to": "opt.followup.v1",
      "probability": 0.6,
      "min_to_per_patient": 1,
      "type": "temporal_after"
    }
  ],
  "dry_run": false,
  "plan_only": false
}
```

Notes:
- `models` is preferred.
- Legacy `templates` array is still accepted for compatibility.
- `plan_only=true` validates and estimates without generating.
- `generation_mode=auto` tries source samples first (if configured) and falls back to model-definition generation from `user-data-models`.
- If your workspace only stores model definitions, use `generation_mode=from_models`.

## 5) Semantic Catalog Collections

### `user-data-models` (HDL team DB, polymorphic)

Suggested document:

```json
{
  "_id": "694c028d8bdec6c4db1d1190",
  "name": "ASSIST_V1",
  "domain": "openehr",
  "metadata": { "templateId": "ASSIST_V1" },
  "domainData": { "webTemplate": { "...": "..." } }
}
```

### `semantic_links`

Suggested document:

```json
{
  "from": "opt.tumour_summary.v1",
  "to": "opt.followup.v1",
  "probability": 0.6,
  "min_to_per_patient": 1,
  "type": "temporal_after"
}
```

## 6) HDL GUI Rules (Implement In Parallel)

Build a 4-step wizard:

1. Environment + Strategy
- Select team environment (DEV/PROD)
- Select strategy binding
- Validate activation exists

2. Models
- Query `hdl-team.user-data-models` for selected `domain`
- Multi-select models
- Set `min/max per patient`, optional `weight`

3. Links
- Optional graph editor for dependencies
- Persist into `payload.links`

4. Run
- Set `patient_count`
- Optional `dry_run` / `plan_only`
- Submit job and start polling

Capacity planning:
- Use `plan_only=true` before real execution.
- Kehrnel returns:
  - `estimated_docs`
  - `estimated_base_bytes`
  - `estimated_search_bytes`
  - `estimated_total_bytes`

Polling behavior:
- Poll every 2-5s while `queued|running|canceling`
- Display `phase`, `progress`, and `stats`
- Allow cancel action

## 7) Security Rules

- HDL never sends decrypted Mongo URI to browser.
- HDL backend calls Kehrnel with API key.
- Kehrnel resolves DB creds from `bindings_ref`.
- Keep auth/session in HDL; Kehrnel remains stateless per request/job.

## 8) Backward Compatibility

- Existing HDL flow using legacy synthetic endpoints can coexist temporarily.
- Migrate UI to env-scoped job endpoints first.
- Then retire direct single-composition generation path.
