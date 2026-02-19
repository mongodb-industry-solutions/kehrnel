---
sidebar_position: 1
---

# Domain Layer API

The domain layer exposes canonical domain operations under:

- `/api/domains/<domain>/...`

## OpenAPI Docs

- openEHR Swagger: `/docs/domains/openehr`
- openEHR ReDoc: `/redoc/domains/openehr`
- FHIR Swagger: `/docs/domains/fhir`
- FHIR ReDoc: `/redoc/domains/fhir`

## Current Domains

- [openEHR](/docs/api/domains/openehr)
- [FHIR (Preview)](/docs/api/domains/fhir)

## Rules For New Domains

1. Add `docs/website/docs/api/domains/<domain>/index.md`.
2. Add grouped endpoint pages under `docs/website/docs/api/endpoints/`.
3. Link it from this page.
