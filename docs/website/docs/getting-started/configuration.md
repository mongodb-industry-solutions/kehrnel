---
sidebar_position: 3
---

# Configuration

\{kehrnel\} uses environment variables for configuration. This guide covers all available options.

## Environment Variables

### MongoDB Connection

| Variable | Description | Default |
|----------|-------------|---------|
| `CORE_MONGODB_URL` | MongoDB connection string | `mongodb://localhost:27017` |
| `CORE_DATABASE_NAME` | Database name | `kehrnel_db` |
| `KEHRNEL_MONGO_TLS_ALLOW_INVALID_CERTS` | Allow invalid TLS certificates | `false` |
| `KEHRNEL_MONGO_TLS_CA_FILE` | Path to CA certificate file | - |

### API Server

| Variable | Description | Default |
|----------|-------------|---------|
| `KEHRNEL_API_HOST` | API server host | `0.0.0.0` |
| `KEHRNEL_API_PORT` | API server port | `8000` |
| `KEHRNEL_API_RELOAD` | Enable auto-reload for development | `false` |
| `KEHRNEL_DEBUG` | Enable debug mode | `false` |

### Security

| Variable | Description | Default |
|----------|-------------|---------|
| `KEHRNEL_AUTH_ENABLED` | Enable API key authentication | `true` |
| `KEHRNEL_API_KEYS` | Comma-separated list of valid API keys | - |
| `KEHRNEL_ADMIN_API_KEYS` | Comma-separated list of admin API keys | - |
| `KEHRNEL_API_KEY_ENV_SCOPES` | JSON mapping of API keys to environment scopes | - |
| `KEHRNEL_CORS_ORIGINS` | Comma-separated list of allowed CORS origins | `*` |
| `ENV_SECRETS_KEY` | Encryption key for environment secrets | - |

### Rate Limiting

| Variable | Description | Default |
|----------|-------------|---------|
| `KEHRNEL_RATE_LIMIT` | Requests per minute per client | `60` |
| `KEHRNEL_RATE_LIMIT_MAX_CLIENTS` | Maximum tracked clients | `5000` |

### Query Limits

| Variable | Description | Default |
|----------|-------------|---------|
| `KEHRNEL_MAX_QUERY_RESULTS` | Maximum rows returned per query | `1000` |
| `KEHRNEL_MAX_QUERY_TIME_MS` | Query timeout in milliseconds | `15000` |
| `KEHRNEL_MAX_STORED_QUERY_LIST` | Maximum stored queries | `500` |

### File Handling

| Variable | Description | Default |
|----------|-------------|---------|
| `KEHRNEL_ALLOW_LOCAL_FILE_INPUTS` | Allow local file paths in API | `false` |
| `KEHRNEL_ALLOW_ABSOLUTE_CONFIG_PATHS` | Allow absolute paths in config | `false` |
| `KEHRNEL_MAX_UPLOAD_BYTES` | Maximum upload size | `10485760` (10MB) |
| `KEHRNEL_MAX_OPT_BYTES` | Maximum OPT template size | `5242880` (5MB) |

### Strategy Runtime

| Variable | Description | Default |
|----------|-------------|---------|
| `KEHRNEL_REGISTRY_PATH` | Path to activation registry file | `.kehrnel_registry.json` |
| `KEHRNEL_STRATEGY_PATHS` | Additional strategy pack locations (`:` or `,` separated) | - |
| `KEHRNEL_ENABLE_STRATEGY_LOAD` | Enable dynamic strategy loading | `true` |

## Configuration File

Create a `.env.local` file in your project root:

```bash
# .env.local

# MongoDB
CORE_MONGODB_URL=mongodb+srv://user:pass@cluster.mongodb.net
CORE_DATABASE_NAME=my_cdr

# API
KEHRNEL_API_HOST=0.0.0.0
KEHRNEL_API_PORT=8000

# Security
KEHRNEL_AUTH_ENABLED=true
KEHRNEL_API_KEYS=key1,key2,key3
KEHRNEL_CORS_ORIGINS=http://localhost:3000,https://myapp.com

# Limits
KEHRNEL_MAX_QUERY_RESULTS=5000
KEHRNEL_MAX_QUERY_TIME_MS=30000
KEHRNEL_RATE_LIMIT=120
```

## Strategy Configuration

Each strategy has its own configuration schema. For the openEHR RPS Dual strategy:

### Schema (`schema.json`)

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "database": { "type": "string" },
    "collections": {
      "type": "object",
      "properties": {
        "compositions": {
          "type": "object",
          "properties": {
            "name": { "type": "string", "default": "compositions_rps" },
            "encodingProfile": { "type": "string", "default": "profile.codedpath" }
          }
        },
        "search": {
          "type": "object",
          "properties": {
            "name": { "type": "string", "default": "compositions_search" },
            "enabled": { "type": "boolean", "default": true },
            "atlasIndex": {
              "type": "object",
              "properties": {
                "name": { "type": "string", "default": "search_nodes_index" }
              }
            }
          }
        }
      }
    }
  }
}
```

### Defaults (`defaults.json`)

```json
{
  "collections": {
    "compositions": {
      "name": "compositions_rps",
      "encodingProfile": "profile.codedpath"
    },
    "search": {
      "name": "compositions_search",
      "enabled": true,
      "atlasIndex": { "name": "search_nodes_index" }
    },
    "codes": { "name": "_codes", "mode": "extend" },
    "ehr": { "name": "ehr" },
    "contributions": { "name": "contributions" }
  },
  "transform": {
    "coding": {
      "arcodes": { "strategy": "sequential" },
      "atcodes": { "strategy": "negative_int" }
    }
  }
}
```

## API Key Scopes

You can restrict API keys to specific environments:

```bash
KEHRNEL_API_KEY_ENV_SCOPES='{"key1": ["dev", "staging"], "key2": ["production"]}'
```

## Next Steps

- [CLI Reference](/docs/cli/overview) - Command-line tools
- [Strategy Configuration](/docs/strategies/openehr-rps-dual/configuration) - Deep dive into strategy settings
