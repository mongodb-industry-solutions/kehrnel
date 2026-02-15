---
sidebar_position: 5
---

# Domain Layer API

The domain layer exposes canonical healthcare-domain operations.

## openEHR Domain (Current Primary Implementation)

Primary prefix:

- `/api/domains/openehr/...`

Capability groups:

- EHR lifecycle
- Composition create/read/update/delete
- AQL execution and translation helpers
- Template upload/list/get
- Versioning and contribution audit APIs

This is the current most mature domain implementation in \{kehrnel\}.

Detailed endpoint groups:

- [openEHR Templates](/docs/api/endpoints/openehr-templates)
- [openEHR EHR](/docs/api/endpoints/openehr-ehr)
- [openEHR Composition & Directory](/docs/api/endpoints/openehr-composition-directory)
- [openEHR Versioning](/docs/api/endpoints/openehr-versioning)
- [openEHR Query](/docs/api/endpoints/openehr-query)

## FHIR Domain (Preview)

Known preview route example:

- `POST /api/domains/fhir/search`

See: [FHIR Preview Endpoints](/docs/api/endpoints/fhir-preview)

## Related

- [API Layers](/docs/api/layers)
- [Strategy Layer API](/docs/api/strategy-runtime)
