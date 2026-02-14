---
sidebar_position: 2
---

# kehrnel-api

Start the \{kehrnel\} REST API server.

## Synopsis

```bash
kehrnel-api [OPTIONS]
```

## Description

Launches a FastAPI-based REST API server that provides:

- openEHR domain endpoints (EHR, Composition, Query, Template)
- Strategy-specific endpoints (RPS Dual ingest, preview, synthetic data)
- Admin endpoints (environments, activation, strategy management)
- Health checks and OpenAPI documentation

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `--host HOST` | `0.0.0.0` | Network interface to bind to |
| `--port PORT` | `8000` | Port number to listen on |
| `--reload` | `false` | Enable auto-reload for development |

## Environment Variables

The following environment variables override command-line defaults:

| Variable | Description |
|----------|-------------|
| `KEHRNEL_API_HOST` | Default bind host |
| `KEHRNEL_API_PORT` | Default bind port |
| `KEHRNEL_API_RELOAD` | Enable auto-reload (`true`/`false`) |
| `KEHRNEL_DEBUG` | Enable debug mode |

## Examples

### Basic Usage

```bash
# Start with defaults (0.0.0.0:8000)
kehrnel-api

# Custom host and port
kehrnel-api --host 127.0.0.1 --port 9000

# Development mode with auto-reload
kehrnel-api --reload
```

### Using Environment Variables

```bash
# Set defaults via environment
export KEHRNEL_API_HOST=0.0.0.0
export KEHRNEL_API_PORT=8080
export KEHRNEL_API_RELOAD=true

kehrnel-api
```

### Production Deployment

```bash
# Production with authentication
export KEHRNEL_AUTH_ENABLED=true
export KEHRNEL_API_KEYS=key1,key2,key3
export CORE_MONGODB_URL="mongodb+srv://..."

kehrnel-api --host 0.0.0.0 --port 8000
```

## API Documentation

Once running, access the interactive documentation at:

- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`
- **OpenAPI JSON**: `http://localhost:8000/openapi.json`

## Health Check

The `/health` endpoint returns server status:

```bash
curl http://localhost:8000/health
# {"status": "healthy"}
```

## Authentication

When `KEHRNEL_AUTH_ENABLED=true`, all endpoints (except `/health`, `/docs`, `/redoc`) require the `X-API-Key` header:

```bash
curl -H "X-API-Key: your-key" http://localhost:8000/api/domains/openehr/ehr
```

See [Configuration](/docs/getting-started/configuration) for security setup details.
