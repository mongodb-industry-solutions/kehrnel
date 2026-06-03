# Resource Denormalizer API Reference

## Overview

The `ResourceDenormalizer` class handles the denormalization of FHIR resources by extracting searchable fields and storing them in optimized `_search` structures.

## Class: ResourceDenormalizer

### Constructor

```python
from fhir_search_to_mql import ResourceDenormalizer

denormalizer = ResourceDenormalizer(
    config_dir: Optional[str] = None,
    extractors: Optional[Dict[str, FieldExtractor]] = None
)
```

**Parameters:**
- `config_dir` (Optional[str]): Path to directory containing resource configuration YAML files. Defaults to `configs/` in project root.
- `extractors` (Optional[Dict[str, FieldExtractor]]): Custom extractors to use instead of default ones.

**Example:**
```python
# Use default config directory
denormalizer = ResourceDenormalizer()

# Use custom config directory
denormalizer = ResourceDenormalizer(config_dir="/path/to/configs")
```

### Methods

#### denormalize()

Denormalize a FHIR resource using its configured extractors.

```python
def denormalize(
    resource: Dict[str, Any],
    fhir_version: Optional[str] = None
) -> Dict[str, Any]:
```

**Parameters:**
- `resource` (Dict[str, Any]): FHIR resource to denormalize
- `fhir_version` (Optional[str]): FHIR version ('R4', 'R5', 'R6'). Auto-detected from resource if not provided.

**Returns:**
- `Dict[str, Any]`: Resource with `_search` fields added

**Raises:**
- `ConfigurationError`: If resource configuration not found
- `ValidationError`: If resource is invalid

**Example:**
```python
patient = {
    "resourceType": "Patient",
    "id": "example",
    "name": [{"family": "Smith", "given": ["John"]}],
    "gender": "male",
    "birthDate": "1980-05-15"
}

denormalized = denormalizer.denormalize(patient)
print(denormalized["_search"])
# {
#     "familyName_lower": ["smith"],
#     "givenNames_lower": ["john"],
#     ...
# }
```

#### denormalize_with_config()

Denormalize a resource using a specific configuration (useful for testing).

```python
def denormalize_with_config(
    resource: Dict[str, Any],
    config: Dict[str, Any]
) -> Dict[str, Any]:
```

**Parameters:**
- `resource` (Dict[str, Any]): FHIR resource to denormalize
- `config` (Dict[str, Any]): Resource configuration dict

**Returns:**
- `Dict[str, Any]`: Resource with `_search` fields added

**Example:**
```python
custom_config = {
    "resource": "Patient",
    "denormalization": {
        "name": {
            "source": "name",
            "extractor": "HumanNameExtractor",
            ...
        }
    }
}

result = denormalizer.denormalize_with_config(patient, custom_config)
```

#### denormalize_batch()

Denormalize multiple resources efficiently.

```python
def denormalize_batch(
    resources: List[Dict[str, Any]],
    fhir_version: Optional[str] = None
) -> List[Dict[str, Any]]:
```

**Parameters:**
- `resources` (List[Dict[str, Any]]): List of FHIR resources
- `fhir_version` (Optional[str]): FHIR version for all resources

**Returns:**
- `List[Dict[str, Any]]`: List of denormalized resources

**Example:**
```python
patients = [patient1, patient2, patient3]
denormalized = denormalizer.denormalize_batch(patients)
```

---

## Field Extractors

Field extractors handle specific FHIR data types and extract searchable values.

### Available Extractors

| Extractor | FHIR Data Type | Description |
|-----------|----------------|-------------|
| `IdentifierExtractor` | Identifier | Extracts system\|value tokens |
| `ReferenceExtractor` | Reference | Extracts resource IDs |
| `CodeableConceptExtractor` | CodeableConcept | Extracts code system\|code tokens |
| `HumanNameExtractor` | HumanName | Extracts family and given names |
| `AddressExtractor` | Address | Extracts address components |
| `ContactPointExtractor` | ContactPoint | Extracts phone/email values |
| `QuantityExtractor` | Quantity | Extracts numeric values and units |
| `PeriodExtractor` | Period | Extracts start and end dates |
| `TimingExtractor` | Timing | Extracts timing patterns |
| `RangeExtractor` | Range | Extracts range boundaries |
| `RatioExtractor` | Ratio | Extracts ratio values |
| `RatioRangeExtractor` | RatioRange | Extracts ratio range values |
| `CodingExtractor` | Coding | Extracts system\|code tokens |
| `MoneyExtractor` | Money | Extracts currency and value |
| `AgeExtractor` | Age | Extracts age quantities |
| `ExtensionExtractor` | Extension | Extracts extension values |
| `DosageExtractor` | Dosage | Extracts dosage information |
| `AvailabilityExtractor` | Availability | Extracts availability times |

### Custom Extractors

Create custom extractors by inheriting from `FieldExtractor`:

```python
from fhir_search_to_mql.extractors.base import FieldExtractor
from typing import Dict, Any, List

class CustomExtractor(FieldExtractor):
    def extract(
        self,
        resource: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Extract custom fields."""
        result = {}
        
        # Your extraction logic here
        source = config.get("source")
        value = resource.get(source)
        
        result["customField"] = self.process_value(value)
        
        return result
    
    def process_value(self, value: Any) -> Any:
        """Process extracted value."""
        # Your processing logic
        return value
```

**Register custom extractor:**
```python
extractors = {
    "CustomExtractor": CustomExtractor()
}

denormalizer = ResourceDenormalizer(extractors=extractors)
```

---

## Configuration Format

Resource configurations are defined in YAML files:

```yaml
resource: Patient
fhir_version: R5

denormalization:
  # Identifier denormalization
  identifier:
    source: identifier
    extractor: IdentifierExtractor
    target: _search
    field_mappings:
      - source_path: "identifier[*].value"
        target_field: identifierValues
        datatype: array[string]
      - source_path: "identifier[*]"
        target_field: identifierSystem_value
        datatype: array[token]
        format: "{system}|{value}"
  
  # Name denormalization
  name:
    source: name
    extractor: HumanNameExtractor
    target: _search
    field_mappings:
      - source_path: "name[*].family"
        target_field: familyName
        datatype: array[string]
      - source_path: "name[*].family"
        target_field: familyName_lower
        datatype: array[string]
        normalize: lowercase
      - source_path: "name[*].given"
        target_field: givenNames
        datatype: array[string]
      - source_path: "name[*].given"
        target_field: givenNames_lower
        datatype: array[string]
        normalize: lowercase
```

### Configuration Fields

| Field | Description | Required |
|-------|-------------|----------|
| `resource` | FHIR resource type | Yes |
| `fhir_version` | FHIR version (R4, R5, R6) | Yes |
| `denormalization` | Denormalization rules | No |
| `denormalization.{field}.source` | Source field path in resource | Yes |
| `denormalization.{field}.extractor` | Extractor class name | Yes |
| `denormalization.{field}.target` | Target object (_search) | Yes |
| `denormalization.{field}.field_mappings` | List of field mappings | Yes |
| `field_mappings[].source_path` | JSONPath to source data | Yes |
| `field_mappings[].target_field` | Target field name | Yes |
| `field_mappings[].datatype` | Data type (string, array[string], etc.) | Yes |
| `field_mappings[].normalize` | Normalization (lowercase, etc.) | No |
| `field_mappings[].format` | Format template | No |

---

## Examples

### Example 1: Basic Patient Denormalization

```python
from fhir_search_to_mql import ResourceDenormalizer

denormalizer = ResourceDenormalizer(config_dir="configs")

patient = {
    "resourceType": "Patient",
    "id": "patient-123",
    "name": [
        {"family": "Smith", "given": ["John", "Michael"]}
    ],
    "gender": "male",
    "birthDate": "1980-05-15"
}

result = denormalizer.denormalize(patient)

print(result["_search"])
# Output:
# {
#     "familyName": ["Smith"],
#     "familyName_lower": ["smith"],
#     "givenNames": ["John", "Michael"],
#     "givenNames_lower": ["john", "michael"]
# }
```

### Example 2: Observation with CodeableConcept

```python
observation = {
    "resourceType": "Observation",
    "id": "obs-123",
    "status": "final",
    "code": {
        "coding": [
            {
                "system": "http://loinc.org",
                "code": "8480-6",
                "display": "Systolic blood pressure"
            }
        ]
    },
    "subject": {
        "reference": "Patient/patient-123"
    },
    "valueQuantity": {
        "value": 120,
        "unit": "mmHg"
    }
}

result = denormalizer.denormalize(observation)

print(result["_search"])
# Output:
# {
#     "codeSystem_code": ["http://loinc.org|8480-6"],
#     "subjectId": "patient-123",
#     "value": 120,
#     "valueUnit": "mmHg"
# }
```

### Example 3: Batch Processing

```python
patients = []
for i in range(100):
    patients.append({
        "resourceType": "Patient",
        "id": f"patient-{i}",
        "name": [{"family": f"Patient{i}"}],
        "gender": "male"
    })

denormalized = denormalizer.denormalize_batch(patients)

# Insert into MongoDB
from pymongo import MongoClient
client = MongoClient()
db = client['fhir_synthetic']
db.Patient.insert_many(denormalized)
```

### Example 4: Multi-Version Support

```python
# R4 Patient
patient_r4 = {
    "resourceType": "Patient",
    "meta": {"profile": ["http://hl7.org/fhir/R4/Patient"]},
    "name": [{"family": "Smith"}]
}

# R5 Patient
patient_r5 = {
    "resourceType": "Patient",
    "meta": {"profile": ["http://hl7.org/fhir/R5/Patient"]},
    "name": [{"family": "Jones"}]
}

# Auto-detects version from meta.profile
result_r4 = denormalizer.denormalize(patient_r4)
result_r5 = denormalizer.denormalize(patient_r5)
```

---

## Error Handling

### ConfigurationError

Raised when resource configuration is not found or invalid.

```python
from fhir_search_to_mql.core.exceptions import ConfigurationError

try:
    result = denormalizer.denormalize(unknown_resource)
except ConfigurationError as e:
    print(f"Configuration error: {e}")
```

### ValidationError

Raised when resource data is invalid.

```python
from fhir_search_to_mql.core.exceptions import ValidationError

try:
    invalid_resource = {"resourceType": "Patient"}  # Missing required fields
    result = denormalizer.denormalize(invalid_resource)
except ValidationError as e:
    print(f"Validation error: {e}")
```

---

## Best Practices

1. **Use Configuration Files**: Define denormalization rules in YAML configs, not code
2. **Batch Processing**: Use `denormalize_batch()` for better performance with multiple resources
3. **Lowercase Fields**: Always create lowercase versions of string fields for case-insensitive search
4. **Token Format**: Use `system|value` format for token fields (identifiers, codes)
5. **Array Fields**: Store multi-valued fields as arrays for efficient querying
6. **Index Optimization**: Mark frequently queried fields in configuration for index recommendations

---

## Performance Tips

- **Config Caching**: Configuration files are cached after first load
- **Extractor Reuse**: Extractors are instantiated once and reused
- **Batch Processing**: Process 100+ resources at once for 2-3x speedup
- **Selective Denormalization**: Only configure fields that will be searched
- **Memory Efficiency**: Large batches are processed sequentially to avoid memory issues

---

## Related Documentation

- [FHIR Search Converter API](converter.md)
- [Configuration Guide](../guides/configuration.md)
- [Adding New Resources](../guides/adding_resources.md)
- [Performance Tuning](../guides/performance_tuning.md)
