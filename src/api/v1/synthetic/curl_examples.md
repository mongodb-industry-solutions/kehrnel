# Synthetic Data API Examples

This file contains examples of how to use the synthetic data generation API.

## Generate Synthetic Data with Default Template

### Generate 5 synthetic records using the default vaccination template

```bash
curl -X POST "http://localhost:9000/v1/synthetic/generate" \
  -H "Content-Type: application/json" \
  -d '{
    "count": 5
  }'
```

### Generate 10 synthetic records with detailed response

```bash
curl -X POST "http://localhost:9000/v1/synthetic/generate" \
  -H "Content-Type: application/json" \
  -d '{
    "count": 10
  }' | jq '.'
```

## Generate Synthetic Data with Custom Template

### Use a custom composition template

```bash
curl -X POST "http://localhost:9000/v1/synthetic/generate" \
  -H "Content-Type: application/json" \
  -d '{
    "count": 3,
    "base_composition": {
      "_type": "COMPOSITION",
      "name": {
        "_type": "DV_TEXT",
        "value": "Custom Test Composition"
      },
      "archetype_details": {
        "archetype_id": {
          "value": "openEHR-EHR-COMPOSITION.test.v1"
        },
        "template_id": {
          "value": "Test Template v1.0"
        },
        "rm_version": "1.0.4"
      },
      "language": {
        "_type": "CODE_PHRASE",
        "terminology_id": {
          "_type": "TERMINOLOGY_ID", 
          "value": "ISO_639-1"
        },
        "code_string": "en"
      },
      "territory": {
        "_type": "CODE_PHRASE",
        "terminology_id": {
          "_type": "TERMINOLOGY_ID",
          "value": "ISO_3166-1"
        },
        "code_string": "US"
      },
      "category": {
        "_type": "DV_CODED_TEXT",
        "value": "event",
        "defining_code": {
          "_type": "CODE_PHRASE",
          "terminology_id": {
            "_type": "TERMINOLOGY_ID",
            "value": "openehr"
          },
          "code_string": "433"
        }
      },
      "composer": {
        "_type": "PARTY_IDENTIFIED",
        "name": "Test System"
      },
      "content": []
    }
  }'
```

## Get Statistics

### Get synthetic data generation statistics

```bash
curl -X GET "http://localhost:9000/v1/synthetic/stats" \
  -H "Accept: application/json"
```

## Response Examples

### Successful Generation Response

```json
{
  "total_requested": 5,
  "total_created": 5,
  "total_errors": 0,
  "generation_time_seconds": 2.456,
  "records": [
    {
      "record_number": 1,
      "ehr_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "subject_id": "synthetic-patient-f1e2d3c4-b5a6-9876-5432-abcdef123456",
      "composition_uid": "comp123::my-openehr-server::1",
      "time_created": "2024-01-01T10:30:45.123Z",
      "error": null
    },
    {
      "record_number": 2,
      "ehr_id": "b2c3d4e5-f6a7-8901-bcde-f23456789012",
      "subject_id": "synthetic-patient-g2f3e4d5-c6b7-0987-6543-bcdef234567a",
      "composition_uid": "comp456::my-openehr-server::1",
      "time_created": "2024-01-01T10:30:46.234Z",
      "error": null
    }
  ]
}
```

### Partial Failure Response

```json
{
  "total_requested": 3,
  "total_created": 2,
  "total_errors": 1,
  "generation_time_seconds": 1.823,
  "records": [
    {
      "record_number": 1,
      "ehr_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "subject_id": "synthetic-patient-f1e2d3c4-b5a6-9876-5432-abcdef123456",
      "composition_uid": "comp123::my-openehr-server::1",
      "time_created": "2024-01-01T10:30:45.123Z",
      "error": null
    },
    {
      "record_number": 2,
      "ehr_id": null,
      "subject_id": null,
      "composition_uid": null,
      "time_created": null,
      "error": "Failed to create record 2: Database connection timeout"
    },
    {
      "record_number": 3,
      "ehr_id": "c3d4e5f6-a7b8-9012-cdef-345678901234",
      "subject_id": "synthetic-patient-h3g4f5e6-d7c8-1098-7654-cdef345678bc",
      "composition_uid": "comp789::my-openehr-server::1",
      "time_created": "2024-01-01T10:30:47.456Z",
      "error": null
    }
  ]
}
```

## Testing with the Provided Vaccination Composition

The default template is based on the `vacc_composition.json` file you provided. 
It will generate variations of vaccination records with:

- Different vaccine types (Meningococcal C, Hepatitis B, Tetanus-Diphtheria, Flu, Pneumococcal)
- Randomized patient identifiers
- Randomized vaccination dates (within the last 1-5 years)
- Randomized healthcare provider information
- Randomized document metadata dates

## Verification

After generating synthetic data, you can verify the created records by:

1. **List EHRs**: `GET /v1/ehr` to see all created EHRs
2. **Get specific EHR**: `GET /v1/ehr/{ehr_id}` to view a specific EHR
3. **Get composition**: `GET /v1/ehr/{ehr_id}/composition/{composition_uid}` to view the composition
4. **Query with AQL**: Use the AQL endpoint to query the generated data

Example AQL query to find all synthetic vaccination compositions:
```sql
SELECT c/uid/value, c/context/start_time/value, c/content[openEHR-EHR-SECTION.immunisation_list.v0]/items[openEHR-EHR-ACTION.medication.v1]/description/items[at0020]/value/value
FROM COMPOSITION c
WHERE c/name/value = 'HC3 Immunization List'
```