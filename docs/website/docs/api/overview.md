---
sidebar_position: 1
---

# API Overview

\{kehrnel\} provides a comprehensive REST API for multi-strategy, multi-domain healthcare data operations. The API documentation is auto-generated from OpenAPI specifications.

## Interactive Documentation

Access the interactive API documentation when the server is running:

| Documentation | URL | Description |
|--------------|-----|-------------|
| **Swagger UI** | [http://localhost:8000/docs](http://localhost:8000/docs) | Interactive API explorer |
| **ReDoc** | [http://localhost:8000/redoc](http://localhost:8000/redoc) | Clean API reference |
| **OpenAPI JSON** | [http://localhost:8000/openapi.json](http://localhost:8000/openapi.json) | Raw specification |

### Strategy-Specific Documentation

| Strategy | Swagger | ReDoc |
|----------|---------|-------|
| openEHR RPS Dual | `/docs/strategies/openehr/rps_dual` | `/redoc/strategies/openehr/rps_dual` |
| openEHR Domain | `/docs/domains/openehr` | `/redoc/domains/openehr` |

## API Categories

For grouped, menu-driven endpoint navigation, use [Endpoint Catalog](/docs/api/endpoint-catalog).

### openEHR Domain API

Standard openEHR REST API endpoints following the openEHR specification:

- **EHR** (`/api/domains/openehr/ehr`) - Electronic Health Record management
- **Composition** (`/api/domains/openehr/ehr/{ehr_id}/composition`) - Clinical document storage
- **Query (AQL)** (`/api/domains/openehr/query`) - Archetype Query Language
- **Template** (`/api/domains/openehr/definition/template`) - Operational templates
- **EHR Status** (`/api/domains/openehr/ehr/{ehr_id}/ehr_status`) - EHR status management
- **Contribution** (`/api/domains/openehr/ehr/{ehr_id}/contribution`) - Audit trail
- **Directory** (`/api/domains/openehr/ehr/{ehr_id}/directory`) - Folder structure

### Strategy API

Strategy-specific endpoints for the RPS Dual persistence layer:

- **Ingest** (`/api/strategies/openehr/rps_dual/ingest`) - Composition transformation and preview
- **Config** (`/api/strategies/openehr/rps_dual/config`) - Strategy configuration
- **Synthetic** (`/api/strategies/openehr/rps_dual/synthetic`) - Synthetic data generation

### Admin API

Administrative endpoints for environment and strategy management:

- **Strategies** (`/strategies`) - List and inspect strategy packs
- **Environments** (`/environments/{env_id}`) - Environment activation and management
- **Bundles** (`/bundles`) - Template and mapping bundles

## Canonical Inventory

For exhaustive CLI and endpoint inventory generated from `pyproject.toml` and OpenAPI:

- [`docs/cli-api-reference.md`](https://github.com/mongodb-industry-solutions/kehrnel/blob/main/docs/cli-api-reference.md)

## Authentication

When `KEHRNEL_AUTH_ENABLED=true`, include the API key in headers:

```bash
curl -H "X-API-Key: your-key" http://localhost:8000/api/domains/openehr/ehr
```

### Public Endpoints

These endpoints don't require authentication:
- `/health` - Health check
- `/docs`, `/redoc` - API documentation
- `/openapi.json` - OpenAPI specification
- Strategy assets (documentation, specs)

## Response Format

### Success Response

```json
{
  "ehr_id": "patient-001",
  "system_id": "kehrnel",
  "time_created": "2025-01-15T10:30:00Z"
}
```

### Error Response

```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "EHR not found",
    "details": {
      "ehr_id": "unknown-001"
    }
  }
}
```

## Common HTTP Status Codes

| Code | Meaning |
|------|---------|
| `200 OK` | Request succeeded |
| `201 Created` | Resource created |
| `202 Accepted` | Async job started |
| `204 No Content` | Successful deletion |
| `400 Bad Request` | Invalid request syntax |
| `401 Unauthorized` | Authentication required |
| `403 Forbidden` | Access denied |
| `404 Not Found` | Resource not found |
| `409 Conflict` | Resource already exists |
| `422 Unprocessable Entity` | Validation error |
| `503 Service Unavailable` | Runtime not initialized |

## Quick Examples

### Create an EHR

```bash
curl -X POST "http://localhost:8000/api/domains/openehr/ehr" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-key"
```

### Execute AQL Query

```bash
curl -X POST "http://localhost:8000/api/domains/openehr/query/aql" \
  -H "Content-Type: text/plain" \
  -H "X-API-Key: your-key" \
  -d "SELECT c/uid/value FROM EHR e CONTAINS COMPOSITION c"
```

### Activate Strategy

```bash
curl -X POST "http://localhost:8000/environments/dev/activate" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: admin-key" \
  -d '{
    "strategy_id": "openehr.rps_dual",
    "version": "0.2.0",
    "domain": "openehr",
    "bindings_ref": "env://DB_BINDINGS"
  }'
```

## Related

- [Quick Start](/docs/getting-started/quickstart) - Getting started with the API
- [Configuration](/docs/getting-started/configuration) - API server configuration
- [AQL Query Guide](/docs/concepts/aql-to-mql) - Understanding AQL to MQL translation
