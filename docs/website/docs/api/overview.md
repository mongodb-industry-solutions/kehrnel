---
sidebar_position: 1
---

import Link from '@docusaurus/Link';

# API Overview

\{kehrnel\} exposes a layered REST API for multi-domain and multi-strategy healthcare workflows.

Instead of navigating endpoint-by-endpoint in the sidebar, use the four API layers:

- **Core**: runtime contracts, strategy registry, environment activation
- **Common**: authentication, conventions, and shared behaviors
- **Domain**: canonical domain operations (openEHR, FHIR preview)
- **Strategy**: strategy-specific runtime operations

Start at [API Layers](./layers), then go deeper into the layer page you need.

## Interactive Documentation

These buttons open endpoints on the same server hosting this documentation (local default: `http://localhost:8000`).

<div className="apiCtaGrid apiCtaGrid--2">
  <div className="apiCtaCard">
    <div className="apiCtaKicker">Interactive</div>
    <div className="apiCtaTitle">Swagger UI</div>
    <div className="apiCtaBody">Explore endpoints, try requests, and see schemas live.</div>
    <div className="apiCtaActions">
      <a className="button button--primary button--lg" href="/docs" target="_blank" rel="noopener noreferrer">Open Swagger UI</a>
      <Link className="button button--outline button--primary" to="/docs/api/endpoint-catalog">Browse Catalog</Link>
    </div>
  </div>

  <div className="apiCtaCard">
    <div className="apiCtaKicker">Reference</div>
    <div className="apiCtaTitle">ReDoc</div>
    <div className="apiCtaBody">A clean, read-only OpenAPI reference for quick scanning.</div>
    <div className="apiCtaActions">
      <a className="button button--secondary button--lg" href="/redoc" target="_blank" rel="noopener noreferrer">Open ReDoc</a>
      <Link className="button button--outline button--primary" to="/docs/api/layers">API Layers</Link>
    </div>
  </div>
</div>

### Strategy-Specific Documentation

<div className="apiCtaGrid apiCtaGrid--2">
  <div className="apiCtaCard">
    <div className="apiCtaKicker">Strategy</div>
    <div className="apiCtaTitle">openEHR RPS Dual</div>
    <div className="apiCtaBody">Strategy-specific operations (ingest, synthetic, diagnostics) for the dual-engine runtime.</div>
    <div className="apiCtaActions">
      <a className="button button--primary" href="/docs/strategies/openehr/rps_dual" target="_blank" rel="noopener noreferrer">Swagger</a>
      <a className="button button--secondary" href="/redoc/strategies/openehr/rps_dual" target="_blank" rel="noopener noreferrer">ReDoc</a>
      <Link className="button button--outline button--primary" to="/docs/strategies/openehr/rps-dual/introduction">Docs</Link>
    </div>
  </div>

  <div className="apiCtaCard">
    <div className="apiCtaKicker">Domain</div>
    <div className="apiCtaTitle">openEHR</div>
    <div className="apiCtaBody">Canonical openEHR domain endpoints (EHR, compositions, templates, AQL).</div>
    <div className="apiCtaActions">
      <a className="button button--primary" href="/docs/domains/openehr" target="_blank" rel="noopener noreferrer">Swagger</a>
      <a className="button button--secondary" href="/redoc/domains/openehr" target="_blank" rel="noopener noreferrer">ReDoc</a>
      <Link className="button button--outline button--primary" to="/docs/api/domains/openehr">Docs</Link>
    </div>
  </div>
</div>

### Mapping (Where Is The API?)

Mapping in \{kehrnel\} exists in two places:

<div className="apiCtaGrid apiCtaGrid--2">
  <div className="apiCtaCard">
    <div className="apiCtaKicker">CLI</div>
    <div className="apiCtaTitle">Mapping Workflows</div>
    <div className="apiCtaBody">Use the CLI for mapping skeleton generation and applying mapping rules.</div>
    <div className="apiCtaActions">
      <Link className="button button--primary" to="/docs/cli/common">Open CLI Mapping Docs</Link>
      <Link className="button button--outline button--primary" to="/docs/strategies/openehr/rps-dual/cli-workflows">RPS Dual Workflows</Link>
    </div>
  </div>

  <div className="apiCtaCard">
    <div className="apiCtaKicker">API (Admin)</div>
    <div className="apiCtaTitle">Mapping Studio Compatibility</div>
    <div className="apiCtaBody">Privileged endpoints used by HDL Mapping Studio. They require an admin API key and are intentionally not shown in Swagger.</div>
    <div className="apiCtaActions">
      <a className="button button--primary" href="/redoc/core" target="_blank" rel="noopener noreferrer">Open Core ReDoc</a>
    </div>
  </div>
</div>

Admin mapping endpoints (HDL compatibility):
- `POST /api/transform` (multipart upload + mapping + OPT → canonical composition)
- `POST /api/validate-composition` (canonical composition + OPT → validation issues)

<details>
  <summary>Show raw URLs</summary>

  | Documentation | URL |
  |--------------|-----|
  | Swagger UI | `/docs` |
  | ReDoc | `/redoc` |
  | openEHR RPS Dual (Swagger) | `/docs/strategies/openehr/rps_dual` |
  | openEHR RPS Dual (ReDoc) | `/redoc/strategies/openehr/rps_dual` |
  | openEHR Domain (Swagger) | `/docs/domains/openehr` |
  | openEHR Domain (ReDoc) | `/redoc/domains/openehr` |
  | Mapping transform (Admin) | `/api/transform` |
  | Mapping validate (Admin) | `/api/validate-composition` |
</details>

## Layer Navigation

- [Core Layer API](./core)
- [Common Layer API](./common/mappings)
- [Domain Layer API](./domains/openehr)
- [Strategy Layer API](./strategies/openehr/rps-dual)

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

- [Quick Start](../getting-started/quickstart) - Getting started with the API
- [Configuration](../getting-started/configuration) - API server configuration
- [AQL Query Guide](../concepts/aql-to-mql) - Understanding AQL to MQL translation
- [API Layers](./layers) - Layer model and navigation
