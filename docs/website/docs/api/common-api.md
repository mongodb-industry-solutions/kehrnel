---
sidebar_position: 4
---

# Common Layer API

The common layer defines conventions shared across core, domain, and strategy endpoints.

## Authentication

When `KEHRNEL_AUTH_ENABLED=true`, pass an API key:

```http
X-API-Key: <your-key>
```

Public routes typically include:

- `/health`
- `/docs`, `/redoc`

## Request/Response Conventions

- JSON is the default payload format (`application/json`)
- Some domain endpoints accept specialized formats (for example AQL text)
- Standard HTTP semantics are used for create/read/update/delete and async operations

## Common Status Codes

| Code | Meaning |
|------|---------|
| `200 OK` | Request succeeded |
| `201 Created` | Resource created |
| `202 Accepted` | Async job started |
| `204 No Content` | Successful deletion |
| `400 Bad Request` | Invalid request |
| `401 Unauthorized` | Authentication required |
| `403 Forbidden` | Access denied |
| `404 Not Found` | Resource not found |
| `409 Conflict` | Resource conflict |
| `422 Unprocessable Entity` | Validation error |
| `503 Service Unavailable` | Runtime not initialized |

## Related

- [API Overview](/docs/api/overview)
- [Core Layer API](/docs/api/core-openapi)
- [Domain Layer API](/docs/api/domain-openehr)
- [Strategy Layer API](/docs/api/strategy-runtime)
