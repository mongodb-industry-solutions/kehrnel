---
sidebar_position: 2
---

# Quick Start

Get up and running with \{kehrnel\} in 5 minutes. This guide walks you through creating an EHR, storing a composition, and executing queries.

## Prerequisites

- \{kehrnel\} installed ([Installation Guide](/docs/getting-started/installation))
- MongoDB connection configured
- API server running (`kehrnel-api`)

## Step 1: Activate a Strategy

First, activate the openEHR RPS Dual strategy for your environment:

```bash
curl -X POST "http://localhost:8000/api/environments/dev/activate" \
  -H "Content-Type: application/json" \
  -d '{
    "strategy_id": "openehr.rps_dual",
    "version": "0.2.0",
    "config": {
      "database": "kehrnel_db",
      "collections": {
        "compositions": { "name": "compositions_rps" },
        "search": { "name": "compositions_search", "enabled": true }
      }
    }
  }'
```

## Step 2: Create an EHR

Create a new Electronic Health Record:

```bash
curl -X POST "http://localhost:8000/api/domains/openehr/ehr" \
  -H "Content-Type: application/json" \
  -d '{
    "ehr_id": "patient-001"
  }'
```

Response:

```json
{
  "ehr_id": "patient-001",
  "system_id": "kehrnel",
  "time_created": "2025-01-15T10:30:00Z"
}
```

## Step 3: Upload a Template (Optional)

Upload an openEHR template (OPT):

```bash
curl -X POST "http://localhost:8000/api/domains/openehr/definition/template/adl1.4" \
  -H "Content-Type: application/xml" \
  --data-binary @my_template.opt
```

## Step 4: Create a Composition

Store a clinical composition:

```bash
curl -X POST "http://localhost:8000/api/domains/openehr/ehr/patient-001/composition" \
  -H "Content-Type: application/json" \
  -d '{
    "_type": "COMPOSITION",
    "name": { "value": "Vital Signs" },
    "archetype_details": {
      "archetype_id": { "value": "openEHR-EHR-COMPOSITION.encounter.v1" }
    },
    "content": [
      {
        "_type": "OBSERVATION",
        "name": { "value": "Blood Pressure" },
        "archetype_node_id": "openEHR-EHR-OBSERVATION.blood_pressure.v2",
        "data": {
          "events": [
            {
              "time": { "value": "2025-01-15T10:30:00Z" },
              "data": {
                "items": [
                  {
                    "archetype_node_id": "at0004",
                    "value": { "_type": "DV_QUANTITY", "magnitude": 120, "units": "mm[Hg]" }
                  },
                  {
                    "archetype_node_id": "at0005",
                    "value": { "_type": "DV_QUANTITY", "magnitude": 80, "units": "mm[Hg]" }
                  }
                ]
              }
            }
          ]
        }
      }
    ]
  }'
```

Response:

```json
{
  "uid": "composition-uuid-12345::kehrnel::1",
  "ehr_id": "patient-001"
}
```

## Step 5: Query with AQL

### Patient-Scoped Query

Query compositions for a specific patient:

```bash
curl -X POST "http://localhost:8000/api/domains/openehr/query/aql" \
  -H "Content-Type: text/plain" \
  -d "SELECT c/uid/value AS uid, c/name/value AS name
      FROM EHR e
      CONTAINS COMPOSITION c
      WHERE e/ehr_id/value = 'patient-001'"
```

### Cross-Patient Query

Search across all patients (uses Atlas Search):

```bash
curl -X POST "http://localhost:8000/api/domains/openehr/query/aql" \
  -H "Content-Type: text/plain" \
  -d "SELECT c/uid/value AS uid, e/ehr_id/value AS patient
      FROM EHR e
      CONTAINS COMPOSITION c
        CONTAINS OBSERVATION o[openEHR-EHR-OBSERVATION.blood_pressure.v2]
      WHERE o/data/events/data/items[at0004]/value/magnitude > 140"
```

## Step 6: Retrieve a Composition

Get a composition by its UID:

```bash
curl "http://localhost:8000/api/domains/openehr/ehr/patient-001/composition/composition-uuid-12345::kehrnel::1"
```

## Understanding the Output

When you store a composition, \{kehrnel\}:

1. **Flattens** the hierarchical document into nodes
2. **Encodes** archetype paths using reversed numeric paths
3. **Stores** the full document in `compositions_rps`
4. **Creates** a slim projection in `compositions_search`
5. **Updates** the code dictionary with any new archetype mappings

You can inspect the transformed output:

```bash
curl -X POST "http://localhost:8000/api/strategies/openehr/rps_dual/ingest/preview" \
  -H "Content-Type: application/json" \
  -d '{ "composition": { ... } }'
```

## Next Steps

- [Configuration Reference](/docs/getting-started/configuration) - Customize your setup
- [CLI Commands](/docs/cli/overview) - Use command-line tools
- [AQL to MQL Translation](/docs/concepts/aql-to-mql) - Understand query compilation
