---
sidebar_position: 1
---

# Installation

This guide covers installing \{kehrnel\} on your local machine for development and testing.

## Prerequisites

- **Python 3.10+**
- **MongoDB Atlas** cluster (M10+ recommended for production) or local MongoDB 6.0+
- **Git**

## Installation Methods

### From Source (Recommended for Development)

1. **Clone the repository**

```bash
git clone https://github.com/mongodb-industry-solutions/kehrnel.git
cd kehrnel
```

2. **Create a virtual environment**

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. **Install dependencies**

```bash
# Install all dependencies including development tools
pip install -e .[all]

# Or install minimal dependencies only
pip install -e .
```

4. **Verify installation**

```bash
kehrnel-api --help
```

### Using pip (For Users)

```bash
pip install kehrnel
```

## Environment Setup

Create a `.env.local` file in the project root:

```bash
# MongoDB Connection
CORE_MONGODB_URL=mongodb+srv://<username>:<password>@cluster.mongodb.net
CORE_DATABASE_NAME=kehrnel_db

# API Configuration
KEHRNEL_API_HOST=0.0.0.0
KEHRNEL_API_PORT=8000
KEHRNEL_API_RELOAD=true

# Security (optional for development)
KEHRNEL_AUTH_ENABLED=false
```

## MongoDB Atlas Setup

For production use, we recommend MongoDB Atlas:

1. Create an Atlas cluster
2. Enable Atlas Search on the cluster
3. Create a database user with readWrite permissions
4. Whitelist your IP address or use VPC peering
5. Copy the connection string to your `.env.local`

### Required Atlas Search Definition

Generate the Atlas Search definition from the active strategy mappings instead of pasting a static JSON snippet:

```bash
export RUNTIME_URL="${RUNTIME_URL:-http://localhost:8080}"

kehrnel setup --runtime-url "$RUNTIME_URL" --env dev --domain openehr --strategy openehr.rps_dual
kehrnel strategy build-search-index --env dev --domain openehr --strategy openehr.rps_dual --out .kehrnel/search-index.json
```

Apply the generated `.kehrnel/search-index.json` to the `compositions_search` collection in Atlas Search.

## Verifying the Installation

Recommended local startup:

```bash
./startKehrnel
```

That serves:

- Swagger UI: http://localhost:8080/docs
- ReDoc: http://localhost:8080/redoc
- Docusaurus site: http://localhost:8080/guide

Direct API startup:

```bash
kehrnel-api
```

Check the health endpoint:

```bash
curl http://localhost:8000/health
```

Expected response:

```json
{"ok": true}
```

Access the API documentation:
- `./startKehrnel`: `http://localhost:8080/docs`, `http://localhost:8080/redoc`, `http://localhost:8080/guide`
- `kehrnel-api`: `http://localhost:8000/docs`, `http://localhost:8000/redoc`

## Next Steps

- [Quick Start Guide](/docs/getting-started/quickstart) - Create your first EHR
- [Configuration Reference](/docs/getting-started/configuration) - Full configuration options
