# openEHR Versioning Endpoints

## OpenAPI Docs

- Swagger: `/docs/domains/openehr`
- ReDoc: `/redoc/domains/openehr`

## Versioned Composition

- `GET /api/domains/openehr/ehr/{ehr_id}/versioned_composition/{versioned_object_uid}`
- `GET /api/domains/openehr/ehr/{ehr_id}/versioned_composition/{versioned_object_uid}/revision_history`
- `GET /api/domains/openehr/ehr/{ehr_id}/versioned_composition/{versioned_object_uid}/version`

## Versioned EHR Status

- `GET /api/domains/openehr/ehr/{ehr_id}/versioned_ehr_status`
- `GET /api/domains/openehr/ehr/{ehr_id}/versioned_ehr_status/revision_history`
- `GET /api/domains/openehr/ehr/{ehr_id}/versioned_ehr_status/version`
- `GET /api/domains/openehr/ehr/{ehr_id}/versioned_ehr_status/version/{version_uid}`
