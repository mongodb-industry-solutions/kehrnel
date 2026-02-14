# CLI And API Reference

Canonical runtime reference for `kehrnel` CLI and HTTP API.

## CLI Commands

Declared under `[project.scripts]` in `pyproject.toml`.

- `kehrnel-api`
- `kehrnel-export-bundle`
- `kehrnel-generate`
- `kehrnel-identify`
- `kehrnel-import-bundle`
- `kehrnel-ingest`
- `kehrnel-list-bundles`
- `kehrnel-map`
- `kehrnel-map-skeleton`
- `kehrnel-skeleton`
- `kehrnel-transform`
- `kehrnel-validate`
- `kehrnel-validate-bundle`
- `kehrnel-validate-pack`

## HTTP API Endpoints

Source of truth: OpenAPI schema served by `GET /openapi.json`.

| Method | Path | Summary |
|---|---|---|
| `POST` | `/api/domains/fhir/search` | Search Fhir Preview |
| `GET` | `/api/domains/openehr/definition/template/{template_format}` | List templates by format |
| `POST` | `/api/domains/openehr/definition/template/{template_format}` | Upload a new clinical template |
| `GET` | `/api/domains/openehr/definition/template/{template_format}/{template_id}` | Get template by ID and format |
| `GET` | `/api/domains/openehr/ehr` | Get EHR by subject ID or list EHRs |
| `POST` | `/api/domains/openehr/ehr` | Create EHR |
| `GET` | `/api/domains/openehr/ehr/{ehr_id}` | Get EHR by ID |
| `PUT` | `/api/domains/openehr/ehr/{ehr_id}` | Create EHR with specified ID |
| `POST` | `/api/domains/openehr/ehr/{ehr_id}/composition` | Create Composition |
| `GET` | `/api/domains/openehr/ehr/{ehr_id}/composition-unflatten/{uid_based_id}` | Get Composition by un-flattening |
| `DELETE` | `/api/domains/openehr/ehr/{ehr_id}/composition/{preceding_version_uid}` | Delete composition by version ID |
| `PUT` | `/api/domains/openehr/ehr/{ehr_id}/composition/{preceding_version_uid}` | Update Composition by version ID |
| `GET` | `/api/domains/openehr/ehr/{ehr_id}/composition/{uid_based_id}` | Get Composition by version or object ID |
| `GET` | `/api/domains/openehr/ehr/{ehr_id}/contribution/{contribution_uid}` | Get Contribution by ID |
| `DELETE` | `/api/domains/openehr/ehr/{ehr_id}/directory` | Delete directory |
| `GET` | `/api/domains/openehr/ehr/{ehr_id}/directory` | Get directory version |
| `POST` | `/api/domains/openehr/ehr/{ehr_id}/directory` | Create directory |
| `PUT` | `/api/domains/openehr/ehr/{ehr_id}/directory` | Update directory |
| `GET` | `/api/domains/openehr/ehr/{ehr_id}/directory/{version_uid}` | Get directory by version ID |
| `GET` | `/api/domains/openehr/ehr/{ehr_id}/ehr_status` | Get latest EHR Status |
| `PUT` | `/api/domains/openehr/ehr/{ehr_id}/ehr_status` | Update EHR Status |
| `GET` | `/api/domains/openehr/ehr/{ehr_id}/ehr_status/{version_uid}` | GET EHR Status by version ID |
| `GET` | `/api/domains/openehr/ehr/{ehr_id}/versioned_composition/{versioned_object_uid}` | Get Versioned Composition metadata |
| `GET` | `/api/domains/openehr/ehr/{ehr_id}/versioned_composition/{versioned_object_uid}/revision_history` | Get revision history of a Composition |
| `GET` | `/api/domains/openehr/ehr/{ehr_id}/versioned_composition/{versioned_object_uid}/version` | Get Composition version at time |
| `GET` | `/api/domains/openehr/ehr/{ehr_id}/versioned_ehr_status` | Get Versioned EHR_STATUS metadata |
| `GET` | `/api/domains/openehr/ehr/{ehr_id}/versioned_ehr_status/revision_history` | Get revision history of the EHR_STATUS |
| `GET` | `/api/domains/openehr/ehr/{ehr_id}/versioned_ehr_status/version` | Get EHR_STATUS version at time |
| `GET` | `/api/domains/openehr/ehr/{ehr_id}/versioned_ehr_status/version/{version_uid}` | Get EHR_STATUS version by ID |
| `GET` | `/api/domains/openehr/query` | List Stored Queries |
| `POST` | `/api/domains/openehr/query/aql` | Execute AQL Query |
| `POST` | `/api/domains/openehr/query/aql/mql` | Translate AQL to MongoDB Query Language (MQL) |
| `POST` | `/api/domains/openehr/query/aql/parse` | Parse AQL to AST |
| `POST` | `/api/domains/openehr/query/aql/validate` | Validate AQL Query |
| `POST` | `/api/domains/openehr/query/ast` | Execute AQL AST Query (Testing) |
| `POST` | `/api/domains/openehr/query/ast/debug` | Debug AQL AST Query Pipeline |
| `GET` | `/api/domains/openehr/query/strategy/info` | Get Query Strategy Information |
| `DELETE` | `/api/domains/openehr/query/{name}` | Delete Stored Query |
| `GET` | `/api/domains/openehr/query/{name}` | Get Stored Query |
| `PUT` | `/api/domains/openehr/query/{name}` | Create/Update Stored Query |
| `POST` | `/api/strategies/openehr/rps_dual/config` | Set ingestion configuration at runtime |
| `POST` | `/api/strategies/openehr/rps_dual/ingest/body` | Ingest and Flatten a Canonical Composition from Request Body |
| `POST` | `/api/strategies/openehr/rps_dual/ingest/database` | Ingest and Flatten a Canonical Composition from the Source Database |
| `POST` | `/api/strategies/openehr/rps_dual/ingest/file` | Ingest and Flatten a Canonical Composition from a Local File |
| `POST` | `/api/strategies/openehr/rps_dual/synthetic/generate` | Generate Synthetic EHR Data |
| `GET` | `/api/strategies/openehr/rps_dual/synthetic/stats` | Get Synthetic Data Statistics |
| `GET` | `/environments/{env_id}/synthetic/jobs` | List Synthetic Jobs |
| `POST` | `/environments/{env_id}/synthetic/jobs` | Create Synthetic Job |
| `GET` | `/environments/{env_id}/synthetic/jobs/{job_id}` | Get Synthetic Job |
| `POST` | `/environments/{env_id}/synthetic/jobs/{job_id}/cancel` | Cancel Synthetic Job |
| `GET` | `/strategies` | List Strategies |
| `GET` | `/strategies/{strategy_id}` | Get Strategy |
| `GET` | `/strategies/{strategy_id}/assets/{asset_path}` | Get Strategy Asset |
| `GET` | `/strategies/{strategy_id}/spec` | Get Strategy Spec |

## Focused OpenAPI Views

- Core: `GET /openapi/core.json`
- Per-domain: `GET /openapi/domains/{domain}.json`
- Per-strategy: `GET /openapi/strategies/{domain}/{strategy}.json`

## Notes

- Routes may require authentication (`X-API-Key`) and an active environment header (`x-active-env`) depending on endpoint class.
- Some operation-specific routes intentionally return `4xx` when payload/path identifiers are invalid or missing.
