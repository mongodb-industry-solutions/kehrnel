---
sidebar_position: 1
---

# Installation

This guide covers installing \{kehrnel\} on your local machine for development and testing.

## Prerequisites

- **Python 3.10+**
- **MongoDB Atlas** cluster (M50+ recommended for production) or local MongoDB 6.0+
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
CORE_MONGODB_URL=mongodb+srv://user:password@cluster.mongodb.net
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

1. Create an Atlas cluster (M50+ for production workloads)
2. Enable Atlas Search on the cluster
3. Create a database user with readWrite permissions
4. Whitelist your IP address or use VPC peering
5. Copy the connection string to your `.env.local`

### Required Atlas Search Index

Create an Atlas Search index on the `compositions_search` collection:

```json
{
  "mappings": {
    "dynamic": false,
    "fields": {
      "ehr_id": { "type": "string" },
      "tid": { "type": "number" },
      "sn": {
        "type": "embeddedDocuments",
        "fields": {
          "p": { "type": "string", "analyzer": "keyword" },
          "data": { "type": "document", "dynamic": true }
        }
      }
    }
  }
}
```

## Verifying the Installation

Start the API server:

```bash
kehrnel-api
```

Check the health endpoint:

```bash
curl http://localhost:8000/health
```

Expected response:

```json
{"status": "healthy"}
```

Access the API documentation:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Next Steps

- [Quick Start Guide](/docs/getting-started/quickstart) - Create your first EHR
- [Configuration Reference](/docs/getting-started/configuration) - Full configuration options
