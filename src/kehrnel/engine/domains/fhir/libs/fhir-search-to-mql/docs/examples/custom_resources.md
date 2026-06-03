# Custom Resource Examples

This file demonstrates how to add support for custom or less common FHIR resources.

## Overview

The library supports any FHIR resource through YAML configuration files. This guide shows complete examples of adding new resources.

---

## Example 1: Medication Resource

### Step 1: Create Configuration

```yaml
# configs/Medication.yaml
resource: Medication
fhir_version: R5

denormalization:
  code:
    source: code
    extractor: CodeableConceptExtractor
    target: _search
    field_mappings:
      - source_path: "code.coding[*]"
        target_field: codeSystem_code
        datatype: array[token]
        format: "{system}|{code}"
      - source_path: "code.coding[*].code"
        target_field: codeValues
        datatype: array[string]
  
  ingredient:
    source: ingredient
    extractor: CodeableConceptExtractor
    target: _search
    field_mappings:
      - source_path: "ingredient[*].itemCodeableConcept.coding[*]"
        target_field: ingredientSystem_code
        datatype: array[token]
        format: "{system}|{code}"
  
  form:
    source: form
    extractor: CodeableConceptExtractor
    target: _search
    field_mappings:
      - source_path: "form.coding[*]"
        target_field: formSystem_code
        datatype: array[token]
        format: "{system}|{code}"
  
  manufacturer:
    source: manufacturer
    extractor: ReferenceExtractor
    target: _search
    field_mappings:
      - source_path: "manufacturer.reference"
        target_field: manufacturerId
        datatype: string
        extract_id: true

search_parameters:
  code:
    type: token
    fields:
      - field: _search.codeSystem_code
        indexed: true
      - field: _search.codeValues
        indexed: true
  
  ingredient:
    type: token
    fields:
      - field: _search.ingredientSystem_code
        indexed: true
  
  form:
    type: token
    fields:
      - field: _search.formSystem_code
        indexed: true
  
  manufacturer:
    type: reference
    fields:
      - field: _search.manufacturerId
        indexed: true
  
  status:
    type: token
    fields:
      - field: status
        indexed: true
```

### Step 2: Test the Configuration

```python
from fhir_search_to_mql import ResourceDenormalizer, FHIRSearchConverter
from pymongo import MongoClient

# Initialize
denormalizer = ResourceDenormalizer(config_dir="configs")
converter = FHIRSearchConverter(config_dir="configs")
client = MongoClient('mongodb://localhost:27017/')
db = client['fhir_synthetic']

# Sample medication
medication = {
    "resourceType": "Medication",
    "id": "med-nizatidine",
    "code": {
        "coding": [{
            "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
            "code": "582620",
            "display": "Nizatidine 15 MG/ML Oral Solution"
        }]
    },
    "status": "active",
    "manufacturer": {
        "reference": "Organization/org-pharma"
    },
    "form": {
        "coding": [{
            "system": "http://snomed.info/sct",
            "code": "385219001",
            "display": "Oral Solution"
        }]
    },
    "ingredient": [{
        "itemCodeableConcept": {
            "coding": [{
                "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                "code": "36378",
                "display": "Nizatidine"
            }]
        }
    }]
}

# Denormalize and insert
denormalized = denormalizer.denormalize(medication)
db.Medication.insert_one(denormalized)

print("Denormalized _search fields:")
for key, value in denormalized['_search'].items():
    print(f"  {key}: {value}")

# Search by code
result = converter.convert('Medication', 'code=582620')
medications = list(db.Medication.find(result['mql_query']))
print(f"\nFound {len(medications)} medications with code 582620")

# Search by ingredient
result = converter.convert('Medication', 'ingredient=36378')
medications = list(db.Medication.find(result['mql_query']))
print(f"Found {len(medications)} medications with ingredient 36378")

# Cleanup
db.Medication.delete_many({})
```

### Step 3: Create Indexes

```python
# Create recommended indexes
db.Medication.create_index('_search.codeSystem_code')
db.Medication.create_index('_search.codeValues')
db.Medication.create_index('_search.ingredientSystem_code')
db.Medication.create_index('_search.formSystem_code')
db.Medication.create_index('_search.manufacturerId')
db.Medication.create_index('status')

print("Indexes created for Medication")
```

---

## Example 2: CarePlan Resource

### Step 1: Configuration

```yaml
# configs/CarePlan.yaml
resource: CarePlan
fhir_version: R5

denormalization:
  category:
    source: category
    extractor: CodeableConceptExtractor
    target: _search
    field_mappings:
      - source_path: "category[*].coding[*]"
        target_field: categorySystem_code
        datatype: array[token]
        format: "{system}|{code}"
  
  subject:
    source: subject
    extractor: ReferenceExtractor
    target: _search
    field_mappings:
      - source_path: "subject.reference"
        target_field: subjectId
        datatype: string
        extract_id: true
      - source_path: "subject.reference"
        target_field: patientId
        datatype: string
        extract_id: true
        options:
          filter_type: Patient
  
  careTeam:
    source: careTeam
    extractor: ReferenceExtractor
    target: _search
    field_mappings:
      - source_path: "careTeam[*].reference"
        target_field: careTeamIds
        datatype: array[string]
        extract_id: true
  
  activityCode:
    source: activity
    extractor: CodeableConceptExtractor
    target: _search
    field_mappings:
      - source_path: "activity[*].detail.code.coding[*]"
        target_field: activityCodeSystem_code
        datatype: array[token]
        format: "{system}|{code}"

search_parameters:
  category:
    type: token
    fields:
      - field: _search.categorySystem_code
        indexed: true
  
  subject:
    type: reference
    fields:
      - field: _search.subjectId
        indexed: true
  
  patient:
    type: reference
    fields:
      - field: _search.patientId
        indexed: true
  
  care-team:
    type: reference
    fields:
      - field: _search.careTeamIds
        indexed: true
  
  activity-code:
    type: token
    fields:
      - field: _search.activityCodeSystem_code
        indexed: true
  
  status:
    type: token
    fields:
      - field: status
        indexed: true
  
  intent:
    type: token
    fields:
      - field: intent
        indexed: true
  
  date:
    type: date
    fields:
      - field: period.start
        indexed: true

compartments:
  - name: Patient
    param: patient
```

### Step 2: Usage Example

```python
# Sample care plan
care_plan = {
    "resourceType": "CarePlan",
    "id": "cp-diabetes",
    "status": "active",
    "intent": "plan",
    "category": [{
        "coding": [{
            "system": "http://hl7.org/fhir/us/core/CodeSystem/careplan-category",
            "code": "assess-plan"
        }]
    }],
    "subject": {
        "reference": "Patient/patient-123"
    },
    "period": {
        "start": "2024-01-01",
        "end": "2024-12-31"
    },
    "careTeam": [
        {"reference": "CareTeam/team-diabetes"}
    ],
    "activity": [{
        "detail": {
            "code": {
                "coding": [{
                    "system": "http://snomed.info/sct",
                    "code": "229065009",
                    "display": "Exercise therapy"
                }]
            },
            "status": "in-progress"
        }
    }]
}

# Denormalize and insert
denormalized = denormalizer.denormalize(care_plan)
db.CarePlan.insert_one(denormalized)

# Search by patient
result = converter.convert('CarePlan', 'patient=patient-123&status=active')
care_plans = list(db.CarePlan.find(result['mql_query']))
print(f"Found {len(care_plans)} active care plans")

# Use compartment query
result = converter.convert_with_compartment(
    'Patient', 'patient-123', 'CarePlan', 'status=active')
care_plans = list(db.CarePlan.find(result['mql_query']))
```

---

## Example 3: DiagnosticReport Resource

### Configuration

```yaml
# configs/DiagnosticReport.yaml
resource: DiagnosticReport
fhir_version: R5

denormalization:
  code:
    source: code
    extractor: CodeableConceptExtractor
    target: _search
    field_mappings:
      - source_path: "code.coding[*]"
        target_field: codeSystem_code
        datatype: array[token]
        format: "{system}|{code}"
  
  subject:
    source: subject
    extractor: ReferenceExtractor
    target: _search
    field_mappings:
      - source_path: "subject.reference"
        target_field: subjectId
        datatype: string
        extract_id: true
      - source_path: "subject.reference"
        target_field: patientId
        datatype: string
        extract_id: true
        options:
          filter_type: Patient
  
  encounter:
    source: encounter
    extractor: ReferenceExtractor
    target: _search
    field_mappings:
      - source_path: "encounter.reference"
        target_field: encounterId
        datatype: string
        extract_id: true
  
  result:
    source: result
    extractor: ReferenceExtractor
    target: _search
    field_mappings:
      - source_path: "result[*].reference"
        target_field: resultIds
        datatype: array[string]
        extract_id: true

search_parameters:
  code:
    type: token
    fields:
      - field: _search.codeSystem_code
        indexed: true
  
  subject:
    type: reference
    fields:
      - field: _search.subjectId
        indexed: true
  
  patient:
    type: reference
    fields:
      - field: _search.patientId
        indexed: true
  
  encounter:
    type: reference
    fields:
      - field: _search.encounterId
        indexed: true
  
  result:
    type: reference
    fields:
      - field: _search.resultIds
        indexed: true
  
  status:
    type: token
    fields:
      - field: status
        indexed: true
  
  date:
    type: date
    fields:
      - field: effectiveDateTime
        indexed: true
      - field: effectivePeriod.start
        indexed: true

compartments:
  - name: Patient
    param: patient
  - name: Encounter
    param: encounter
```

---

## Example 4: Custom Extension Handling

### Configuration with Extensions

```yaml
# configs/Patient.yaml (with extensions)
resource: Patient
fhir_version: R5

denormalization:
  # Standard fields...
  
  # Custom extension for ethnicity
  ethnicity:
    source: extension
    extractor: ExtensionExtractor
    target: _search
    field_mappings:
      - source_path: "extension[?(@.url=='http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity')].valueCoding.code"
        target_field: ethnicityCode
        datatype: string
  
  # Custom extension for race
  race:
    source: extension
    extractor: ExtensionExtractor
    target: _search
    field_mappings:
      - source_path: "extension[?(@.url=='http://hl7.org/fhir/us/core/StructureDefinition/us-core-race')].valueCoding.code"
        target_field: raceCode
        datatype: string

search_parameters:
  # Standard parameters...
  
  ethnicity:
    type: token
    fields:
      - field: _search.ethnicityCode
        indexed: true
  
  race:
    type: token
    fields:
      - field: _search.raceCode
        indexed: true
```

### Usage with Extensions

```python
# Patient with US Core extensions
patient = {
    "resourceType": "Patient",
    "id": "patient-with-extensions",
    "name": [{"family": "Smith", "given": ["John"]}],
    "extension": [
        {
            "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
            "valueCoding": {
                "system": "urn:oid:2.16.840.1.113883.6.238",
                "code": "2106-3",
                "display": "White"
            }
        },
        {
            "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
            "valueCoding": {
                "system": "urn:oid:2.16.840.1.113883.6.238",
                "code": "2186-5",
                "display": "Not Hispanic or Latino"
            }
        }
    ]
}

# Denormalize
denormalized = denormalizer.denormalize(patient)
db.Patient.insert_one(denormalized)

# Search by race
result = converter.convert('Patient', 'race=2106-3')
patients = list(db.Patient.find(result['mql_query']))
```

---

## Example 5: Batch Resource Processing

```python
def process_resource_batch(resources, resource_type):
    """Process a batch of resources."""
    from fhir_search_to_mql import ResourceDenormalizer
    from pymongo import MongoClient
    
    denormalizer = ResourceDenormalizer(config_dir="configs")
    client = MongoClient('mongodb://localhost:27017/')
    db = client['fhir_synthetic']
    
    # Denormalize batch
    print(f"Denormalizing {len(resources)} {resource_type} resources...")
    denormalized = denormalizer.denormalize_batch(resources)
    
    # Insert batch
    print(f"Inserting into MongoDB...")
    result = db[resource_type].insert_many(denormalized)
    
    print(f"Inserted {len(result.inserted_ids)} documents")
    
    return result.inserted_ids

# Example: Process 100 medications
medications = [create_medication(i) for i in range(100)]
inserted = process_resource_batch(medications, 'Medication')
print(f"Successfully processed {len(inserted)} medications")
```

---

## Testing Custom Resources

```python
import pytest
from fhir_search_to_mql import ResourceDenormalizer, FHIRSearchConverter

class TestCustomResource:
    @pytest.fixture
    def sample_medication(self):
        return {
            "resourceType": "Medication",
            "id": "test-med",
            "code": {
                "coding": [{
                    "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                    "code": "582620"
                }]
            },
            "status": "active"
        }
    
    def test_denormalization(self, sample_medication):
        denormalizer = ResourceDenormalizer(config_dir="configs")
        result = denormalizer.denormalize(sample_medication)
        
        assert "_search" in result
        assert "codeSystem_code" in result["_search"]
        assert "http://www.nlm.nih.gov/research/umls/rxnorm|582620" in result["_search"]["codeSystem_code"]
    
    def test_query_conversion(self):
        converter = FHIRSearchConverter(config_dir="configs")
        result = converter.convert('Medication', 'code=582620&status=active')
        
        assert "mql_query" in result
        query_str = str(result["mql_query"])
        assert "582620" in query_str
        assert "active" in query_str
    
    def test_end_to_end(self, sample_medication):
        from pymongo import MongoClient
        
        client = MongoClient('mongodb://localhost:27017/')
        db = client['fhir_test']
        db.Medication.delete_many({})
        
        # Denormalize and insert
        denormalizer = ResourceDenormalizer(config_dir="configs")
        denormalized = denormalizer.denormalize(sample_medication)
        db.Medication.insert_one(denormalized)
        
        # Query
        converter = FHIRSearchConverter(config_dir="configs")
        result = converter.convert('Medication', 'code=582620')
        
        medications = list(db.Medication.find(result["mql_query"]))
        
        assert len(medications) == 1
        assert medications[0]["id"] == "test-med"
        
        # Cleanup
        db.Medication.delete_many({})
```

---

## Tips for Custom Resources

1. **Follow FHIR Spec**: Base configuration on official FHIR specification
2. **Use Appropriate Extractors**: Match extractor to FHIR data type
3. **Test Thoroughly**: Write unit and integration tests
4. **Document**: Add comments explaining complex configurations
5. **Create Indexes**: Index all searchable fields
6. **Performance Test**: Test with realistic data volumes
7. **Validate**: Use validation script to check configuration

---

## Related Documentation

- [Adding Resources Guide](../guides/adding_resources.md)
- [Configuration Reference](../api/configuration.md)
- [Integration Examples](integration.md)
