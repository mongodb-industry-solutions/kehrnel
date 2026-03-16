# openEHR Composition, Contribution And Directory

## Composition

- `POST /api/domains/openehr/ehr/{ehr_id}/composition`
- `GET /api/domains/openehr/ehr/{ehr_id}/composition/{uid_based_id}`
- `PUT /api/domains/openehr/ehr/{ehr_id}/composition/{preceding_version_uid}`
- `DELETE /api/domains/openehr/ehr/{ehr_id}/composition/{preceding_version_uid}`
- `GET /api/domains/openehr/ehr/{ehr_id}/composition-unflatten/{uid_based_id}`

## Contribution

- `GET /api/domains/openehr/ehr/{ehr_id}/contribution/{contribution_uid}`

## Directory

- `GET /api/domains/openehr/ehr/{ehr_id}/directory`
- `POST /api/domains/openehr/ehr/{ehr_id}/directory`
- `PUT /api/domains/openehr/ehr/{ehr_id}/directory`
- `DELETE /api/domains/openehr/ehr/{ehr_id}/directory`
- `GET /api/domains/openehr/ehr/{ehr_id}/directory/{version_uid}`
