# Adding New Resources Guide

This guide explains how to add support for new FHIR resources to the library.

## Overview

The library is designed to support any FHIR resource through configuration files. Adding a new resource requires:

1. Creating a YAML configuration file
2. Defining denormalization rules (optional)
3. Defining search parameters
4. Testing the configuration
5. Creating indexes in MongoDB

---

## Step-by-Step Guide

### Step 1: Create Configuration File

Create a new YAML file in the `configs/` directory named `{ResourceType}.yaml`:

```yaml
# configs/Appointment.yaml
resource: Appointment
fhir_version: R5

denormalization:
  # Define denormalization rules for searchable fields
  
search_parameters:
  # Define search parameters
```

### Step 2: Define Denormalization Rules

Add denormalization rules for fields that will be searched:

```yaml
denormalization:
  # Denormalize participant actors (references)
  participant:
    source: participant
    extractor: ReferenceExtractor
    target: _search
    field_mappings:
      - source_path: "participant[*].actor.reference"
        target_field: participantIds
        datatype: array[string]
        extract_id: true
  
  # Denormalize appointment type (CodeableConcept)
  appointmentType:
    source: appointmentType
    extractor: CodeableConceptExtractor
    target: _search
    field_mappings:
      - source_path: "appointmentType.coding[*]"
        target_field: appointmentTypeSystem_code
        datatype: array[token]
        format: \"{system}|{code}\"
  
  # Denormalize service type
  serviceType:
    source: serviceType
    extractor: CodeableConceptExtractor
    target: _search
    field_mappings:
      - source_path: "serviceType[*].coding[*]"
        target_field: serviceTypeSystem_code
        datatype: array[token]
        format: \"{system}|{code}\"
```

### Step 3: Define Search Parameters

Add search parameters based on FHIR specification:

```yaml
search_parameters:
  # String parameters
  description:
    type: string
    fields:
      default:
        - field: description
          indexed: true
  
  # Token parameters
  status:
    type: token
    fields:
      - field: status
        indexed: true
  
  appointment-type:
    type: token
    fields:
      - field: _search.appointmentTypeSystem_code
        indexed: true
  
  service-type:
    type: token
    fields:
      - field: _search.serviceTypeSystem_code
        indexed: true
  
  # Reference parameters
  patient:
    type: reference
    fields:
      - field: _search.participantIds
        indexed: true
  
  practitioner:
    type: reference
    fields:
      - field: _search.participantIds
        indexed: true
  
  actor:
    type: reference
    fields:
      - field: _search.participantIds
        indexed: true
  
  # Date parameters
  date:
    type: date
    fields:
      - field: start
        indexed: true
  
  # Number parameters
  priority:
    type: number
    fields:
      - field: priority
        indexed: true
```

### Step 4: Choose Appropriate Extractors

Use the correct extractor for each FHIR data type:

| FHIR Data Type | Extractor | Use For |
|----------------|-----------|---------|
| Identifier | `IdentifierExtractor` | identifier fields |
| Reference | `ReferenceExtractor` | subject, patient, practitioner, etc. |
| CodeableConcept | `CodeableConceptExtractor` | code, category, type, etc. |
| Coding | `CodingExtractor` | Single coding elements |
| HumanName | `HumanNameExtractor` | name fields |
| Address | `AddressExtractor` | address fields |
| ContactPoint | `ContactPointExtractor` | telecom fields |
| Quantity | `QuantityExtractor` | value with unit |
| Period | `PeriodExtractor` | start/end dates |
| Timing | `TimingExtractor` | repeat/timing patterns |
| Range | `RangeExtractor` | low/high values |
| Ratio | `RatioExtractor` | numerator/denominator |
| Money | `MoneyExtractor` | currency and value |
| Age/Duration | `AgeExtractor` | age quantities |
| Extension | `ExtensionExtractor` | extensions |

### Step 5: Test Configuration

Create a test to verify the configuration:

```python
# tests/test_new_resource.py
import pytest
from fhir_search_to_mql import ResourceDenormalizer, FHIRSearchConverter

def test_appointment_denormalization():
    \"\"\"Test Appointment denormalization.\"\"\"
    denormalizer = ResourceDenormalizer(config_dir=\"configs\")\n    \n    appointment = {\n        \"resourceType\": \"Appointment\",\n        \"id\": \"example\",\n        \"status\": \"booked\",\n        \"appointmentType\": {\n            \"coding\": [{\n                \"system\": \"http://terminology.hl7.org/CodeSystem/v2-0276\",\n                \"code\": \"ROUTINE\"\n            }]\n        },\n        \"description\": \"Annual checkup\",\n        \"start\": \"2024-06-20T09:00:00Z\",\n        \"end\": \"2024-06-20T10:00:00Z\",\n        \"participant\": [\n            {\"actor\": {\"reference\": \"Patient/patient-123\"}},\n            {\"actor\": {\"reference\": \"Practitioner/pract-456\"}}\n        ]\n    }\n    \n    result = denormalizer.denormalize(appointment)\n    \n    # Verify _search fields\n    assert \"_search\" in result\n    assert \"appointmentTypeSystem_code\" in result[\"_search\"]\n    assert \"participantIds\" in result[\"_search\"]\n    assert \"patient-123\" in result[\"_search\"][\"participantIds\"]\n    assert \"pract-456\" in result[\"_search\"][\"participantIds\"]\n\ndef test_appointment_query_conversion():\n    \"\"\"Test Appointment query conversion.\"\"\"n    converter = FHIRSearchConverter(config_dir=\"configs\")\n    \n    result = converter.convert(\n        'Appointment',\n        'status=booked&patient=patient-123&date=ge2024-06-01'\n    )\n    \n    assert \"mql_query\" in result\n    query_str = str(result[\"mql_query\"])\n    assert \"booked\" in query_str\n    assert \"patient-123\" in query_str\n```

Run the test:

```bash\npytest tests/test_new_resource.py -v\n```\n\n### Step 6: Create MongoDB Indexes\n\nCreate indexes for all search parameters:\n\n```javascript\n// Create indexes for Appointment\ndb.Appointment.createIndex({ \"status\": 1 });\ndb.Appointment.createIndex({ \"_search.appointmentTypeSystem_code\": 1 });\ndb.Appointment.createIndex({ \"_search.serviceTypeSystem_code\": 1 });\ndb.Appointment.createIndex({ \"_search.participantIds\": 1 });\ndb.Appointment.createIndex({ \"start\": 1 });\ndb.Appointment.createIndex({ \"priority\": 1 });\n\n// Compound indexes for common queries\ndb.Appointment.createIndex({\n    \"_search.participantIds\": 1,\n    \"status\": 1,\n    \"start\": 1\n});\n\ndb.Appointment.createIndex({\n    \"status\": 1,\n    \"start\": 1\n});\n```\n\n---\n\n## Complete Example: Medication Resource\n\nLet's add support for the Medication resource:\n\n### Configuration File\n\n```yaml\n# configs/Medication.yaml\nresource: Medication\nfhir_version: R5\n\ndenormalization:\n  # Medication code\n  code:\n    source: code\n    extractor: CodeableConceptExtractor\n    target: _search\n    field_mappings:\n      - source_path: \"code.coding[*]\"\n        target_field: codeSystem_code\n        datatype: array[token]\n        format: \"{system}|{code}\"\n  \n  # Ingredient codes\n  ingredient:\n    source: ingredient\n    extractor: CodeableConceptExtractor\n    target: _search\n    field_mappings:\n      - source_path: \"ingredient[*].itemCodeableConcept.coding[*]\"\n        target_field: ingredientSystem_code\n        datatype: array[token]\n        format: \"{system}|{code}\"\n  \n  # Form\n  form:\n    source: form\n    extractor: CodeableConceptExtractor\n    target: _search\n    field_mappings:\n      - source_path: \"form.coding[*]\"\n        target_field: formSystem_code\n        datatype: array[token]\n        format: \"{system}|{code}\"\n  \n  # Manufacturer\n  manufacturer:\n    source: manufacturer\n    extractor: ReferenceExtractor\n    target: _search\n    field_mappings:\n      - source_path: \"manufacturer.reference\"\n        target_field: manufacturerId\n        datatype: string\n        extract_id: true\n\nsearch_parameters:\n  code:\n    type: token\n    fields:\n      - field: _search.codeSystem_code\n        indexed: true\n  \n  ingredient:\n    type: token\n    fields:\n      - field: _search.ingredientSystem_code\n        indexed: true\n  \n  ingredient-code:\n    type: token\n    fields:\n      - field: _search.ingredientSystem_code\n        indexed: true\n  \n  form:\n    type: token\n    fields:\n      - field: _search.formSystem_code\n        indexed: true\n  \n  manufacturer:\n    type: reference\n    fields:\n      - field: _search.manufacturerId\n        indexed: true\n  \n  status:\n    type: token\n    fields:\n      - field: status\n        indexed: true\n  \n  lot-number:\n    type: token\n    fields:\n      - field: batch.lotNumber\n        indexed: true\n```\n\n### Test File\n\n```python\n# tests/test_medication.py\nimport pytest\nfrom fhir_search_to_mql import ResourceDenormalizer, FHIRSearchConverter\n\n@pytest.fixture\ndef sample_medication():\n    return {\n        \"resourceType\": \"Medication\",\n        \"id\": \"med-example\",\n        \"code\": {\n            \"coding\": [{\n                \"system\": \"http://www.nlm.nih.gov/research/umls/rxnorm\",\n                \"code\": \"582620\",\n                \"display\": \"Nizatidine 15 MG/ML Oral Solution\"\n            }]\n        },\n        \"status\": \"active\",\n        \"manufacturer\": {\n            \"reference\": \"Organization/org-123\"\n        },\n        \"form\": {\n            \"coding\": [{\n                \"system\": \"http://snomed.info/sct\",\n                \"code\": \"385219001\",\n                \"display\": \"Oral Solution\"\n            }]\n        },\n        \"ingredient\": [\n            {\n                \"itemCodeableConcept\": {\n                    \"coding\": [{\n                        \"system\": \"http://www.nlm.nih.gov/research/umls/rxnorm\",\n                        \"code\": \"36378\",\n                        \"display\": \"Nizatidine\"\n                    }]\n                },\n                \"strength\": {\n                    \"numerator\": {\n                        \"value\": 15,\n                        \"unit\": \"mg\",\n                        \"system\": \"http://unitsofmeasure.org\",\n                        \"code\": \"mg\"\n                    },\n                    \"denominator\": {\n                        \"value\": 1,\n                        \"unit\": \"mL\",\n                        \"system\": \"http://unitsofmeasure.org\",\n                        \"code\": \"mL\"\n                    }\n                }\n            }\n        ]\n    }\n\ndef test_medication_denormalization(sample_medication):\n    \"\"\"Test Medication denormalization.\"\"\"\n    denormalizer = ResourceDenormalizer(config_dir=\"configs\")\n    result = denormalizer.denormalize(sample_medication)\n    \n    # Verify _search fields\n    assert \"_search\" in result\n    assert \"codeSystem_code\" in result[\"_search\"]\n    assert \"http://www.nlm.nih.gov/research/umls/rxnorm|582620\" in result[\"_search\"][\"codeSystem_code\"]\n    assert \"ingredientSystem_code\" in result[\"_search\"]\n    assert \"http://www.nlm.nih.gov/research/umls/rxnorm|36378\" in result[\"_search\"][\"ingredientSystem_code\"]\n    assert \"formSystem_code\" in result[\"_search\"]\n    assert \"manufacturerId\" in result[\"_search\"]\n    assert result[\"_search\"][\"manufacturerId\"] == \"org-123\"\n\ndef test_medication_query_conversion():\n    \"\"\"Test Medication query conversion.\"\"\"\n    converter = FHIRSearchConverter(config_dir=\"configs\")\n    \n    # Test code search\n    result = converter.convert(\n        'Medication',\n        'code=http://www.nlm.nih.gov/research/umls/rxnorm|582620'\n    )\n    assert \"mql_query\" in result\n    \n    # Test ingredient search\n    result = converter.convert(\n        'Medication',\n        'ingredient-code=36378&status=active'\n    )\n    assert \"mql_query\" in result\n    query_str = str(result[\"mql_query\"])\n    assert \"36378\" in query_str\n    assert \"active\" in query_str\n\ndef test_medication_integration(sample_medication):\n    \"\"\"Test end-to-end medication workflow.\"\"\"\n    from pymongo import MongoClient\n    \n    # Setup\n    client = MongoClient('mongodb://localhost:27017/')\n    db = client['fhir_test']\n    db.Medication.delete_many({})\n    \n    # Denormalize and insert\n    denormalizer = ResourceDenormalizer(config_dir=\"configs\")\n    denormalized = denormalizer.denormalize(sample_medication)\n    db.Medication.insert_one(denormalized)\n    \n    # Query\n    converter = FHIRSearchConverter(config_dir=\"configs\")\n    result = converter.convert('Medication', 'code=582620&status=active')\n    \n    medications = list(db.Medication.find(result[\"mql_query\"]))\n    \n    assert len(medications) == 1\n    assert medications[0][\"id\"] == \"med-example\"\n    \n    # Cleanup\n    db.Medication.delete_many({})\n```\n\n---\n\n## Common Patterns\n\n### Pattern 1: Simple Resource (No Complex Types)\n\n```yaml\nresource: Flag\nfhir_version: R5\n\nsearch_parameters:\n  status:\n    type: token\n    fields:\n      - field: status\n        indexed: true\n  \n  subject:\n    type: reference\n    fields:\n      - field: subject.reference\n        indexed: true\n  \n  date:\n    type: date\n    fields:\n      - field: period.start\n        indexed: true\n```\n\n### Pattern 2: Resource with CodeableConcepts\n\n```yaml\nresource: Condition\nfhir_version: R5\n\ndenormalization:\n  code:\n    source: code\n    extractor: CodeableConceptExtractor\n    target: _search\n    field_mappings:\n      - source_path: \"code.coding[*]\"\n        target_field: codeSystem_code\n        datatype: array[token]\n        format: \"{system}|{code}\"\n  \n  category:\n    source: category\n    extractor: CodeableConceptExtractor\n    target: _search\n    field_mappings:\n      - source_path: \"category[*].coding[*]\"\n        target_field: categorySystem_code\n        datatype: array[token]\n        format: \"{system}|{code}\"\n\nsearch_parameters:\n  code:\n    type: token\n    fields:\n      - field: _search.codeSystem_code\n        indexed: true\n  \n  category:\n    type: token\n    fields:\n      - field: _search.categorySystem_code\n        indexed: true\n```\n\n### Pattern 3: Resource with References\n\n```yaml\nresource: MedicationRequest\nfhir_version: R5\n\ndenormalization:\n  subject:\n    source: subject\n    extractor: ReferenceExtractor\n    target: _search\n    field_mappings:\n      - source_path: \"subject.reference\"\n        target_field: subjectId\n        datatype: string\n        extract_id: true\n  \n  requester:\n    source: requester\n    extractor: ReferenceExtractor\n    target: _search\n    field_mappings:\n      - source_path: \"requester.reference\"\n        target_field: requesterId\n        datatype: string\n        extract_id: true\n\nsearch_parameters:\n  subject:\n    type: reference\n    fields:\n      - field: _search.subjectId\n        indexed: true\n  \n  patient:\n    type: reference\n    fields:\n      - field: _search.subjectId\n        indexed: true\n  \n  requester:\n    type: reference\n    fields:\n      - field: _search.requesterId\n        indexed: true\n```\n\n---\n\n## Validation Checklist\n\n- [ ] Configuration file created in `configs/` directory\n- [ ] Resource type and FHIR version specified\n- [ ] Denormalization rules defined for searchable fields\n- [ ] Correct extractors chosen for each data type\n- [ ] All FHIR search parameters included\n- [ ] Field paths verified against FHIR specification\n- [ ] Indexed fields marked in configuration\n- [ ] Test file created with unit tests\n- [ ] Integration test with MongoDB included\n- [ ] MongoDB indexes created\n- [ ] Documentation updated\n- [ ] Performance tested with realistic data volume\n\n---\n\n## Troubleshooting\n\n### Issue: Configuration Not Loaded\n\n**Problem:** `ConfigurationError: Configuration file not found`\n\n**Solution:**\n- Verify file exists in `configs/` directory\n- Check filename matches resource type exactly (case-sensitive)\n- Ensure file has `.yaml` extension\n\n### Issue: Extractor Not Found\n\n**Problem:** `KeyError: 'InvalidExtractor'`\n\n**Solution:**\n- Check extractor name spelling\n- Available extractors: IdentifierExtractor, ReferenceExtractor, CodeableConceptExtractor, etc.\n- See [Denormalizer API](../api/denormalizer.md) for full list\n\n### Issue: No _search Fields Generated\n\n**Problem:** Denormalized resource missing `_search` fields\n\n**Solution:**\n- Verify `denormalization` section in config\n- Check `source_path` matches actual resource structure\n- Enable debug logging to see extraction process\n\n### Issue: Query Returns No Results\n\n**Problem:** Query syntax correct but no results\n\n**Solution:**\n- Verify data was denormalized before insertion\n- Check field names in search parameters match _search structure\n- Test query directly in MongoDB\n- Verify indexes exist\n\n---\n\n## Best Practices\n\n1. **Follow FHIR Spec**: Base search parameters on official FHIR specification\n2. **Selective Denormalization**: Only denormalize fields that will be searched\n3. **Use Appropriate Extractors**: Choose extractor that matches FHIR data type\n4. **Index All Search Parameters**: Mark `indexed: true` for all search parameters\n5. **Test Thoroughly**: Write unit and integration tests\n6. **Document**: Add comments in config file explaining complex mappings\n7. **Version Support**: Consider differences between FHIR versions\n8. **Performance**: Test with realistic data volumes\n9. **Compound Indexes**: Create compound indexes for common query combinations\n10. **Maintenance**: Update config when FHIR specification changes\n\n---\n\n## Related Documentation\n\n- [Configuration Guide](configuration.md)\n- [Denormalizer API](../api/denormalizer.md)\n- [Converter API](../api/converter.md)\n- [Performance Tuning](performance_tuning.md)\n- [Getting Started](getting_started.md)\n", "oldString": ""}