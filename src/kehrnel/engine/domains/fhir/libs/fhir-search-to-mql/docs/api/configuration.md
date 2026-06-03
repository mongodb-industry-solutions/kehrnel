# Configuration File Format

This document describes the YAML configuration file format used to define resource denormalization and search parameters.

## Overview

Each FHIR resource requires a configuration file that specifies:
- **Denormalization rules**: How to extract and transform fields for searching
- **Search parameters**: How FHIR search parameters map to MongoDB fields
- **Index recommendations**: Which fields should be indexed

Configuration files are located in the `configs/` directory and named `{ResourceType}.yaml`.

---

## File Structure

```yaml
# Resource metadata
resource: ResourceType
fhir_version: R5

# Denormalization rules (optional)
denormalization:
  field_name:
    source: source_field
    extractor: ExtractorName
    target: _search
    field_mappings:
      - source_path: "path.to.field"
        target_field: search_field_name
        datatype: type
        options: {}

# Search parameters (required)
search_parameters:
  parameter_name:
    type: string|token|reference|date|number|quantity
    fields:
      default:
        - field: field_name
          indexed: true
      modifier:
        - field: field_name_modified
          indexed: true

# Compartment definitions (optional)
compartments:
  - name: Patient
    param: patient
  - name: Encounter
    param: encounter
```

---

## Resource Metadata

### `resource` (required)

The FHIR resource type name (e.g., `Patient`, `Observation`).

```yaml
resource: Patient
```

### `fhir_version` (required)

The FHIR version supported by this configuration (e.g., `R4`, `R5`).

```yaml
fhir_version: R5
```

---

## Denormalization Section

The `denormalization` section defines how to extract and transform fields for efficient searching.

### Structure

```yaml
denormalization:
  field_configuration_name:
    source: source_field_in_resource
    extractor: ExtractorClassName
    target: _search
    field_mappings:
      - source_path: "path.to.source.field"
        target_field: target_field_name
        datatype: field_data_type
        options:
          option_key: option_value
```

### Parameters

#### `source` (required)

The top-level field in the FHIR resource to extract from.

```yaml
source: name  # Extract from resource.name
```

#### `extractor` (required)

The extractor class to use for processing the field. Available extractors:

| Extractor | FHIR Data Type | Description |
|-----------|----------------|-------------|
| `IdentifierExtractor` | Identifier | Extract identifier values and system\|value pairs |
| `ReferenceExtractor` | Reference | Extract reference IDs and types |
| `CodeableConceptExtractor` | CodeableConcept | Extract coding system\|code pairs |
| `CodingExtractor` | Coding | Extract single coding system\|code |
| `HumanNameExtractor` | HumanName | Extract family/given names with lowercase variants |
| `AddressExtractor` | Address | Extract address components |
| `ContactPointExtractor` | ContactPoint | Extract telecom values |
| `QuantityExtractor` | Quantity | Extract quantity values with units |
| `PeriodExtractor` | Period | Extract start/end dates |
| `TimingExtractor` | Timing | Extract timing patterns |
| `RangeExtractor` | Range | Extract low/high values |
| `RatioExtractor` | Ratio | Extract numerator/denominator |
| `MoneyExtractor` | Money | Extract currency and value |
| `AgeExtractor` | Age/Duration | Extract age quantities |
| `ExtensionExtractor` | Extension | Extract extension values |
| `StringExtractor` | string | Extract string values with lowercase |
| `BooleanExtractor` | boolean | Extract boolean values |

```yaml
extractor: HumanNameExtractor
```

#### `target` (required)

The target location for extracted fields. Always use `_search` to store search-optimized fields.

```yaml
target: _search
```

#### `field_mappings` (required)

Array of field mapping configurations defining how to extract and transform data.

```yaml
field_mappings:
  - source_path: "name[*].family"
    target_field: familyName
    datatype: array[string]
  
  - source_path: "name[*].given[*]"
    target_field: givenNames
    datatype: array[string]
```

### Field Mapping Parameters

#### `source_path` (required)

JSONPath-like expression to the source field. Supports:
- Dot notation: `field.nested.value`
- Array wildcards: `array[*].field`
- Multiple levels: `field[*].nested[*].value`

```yaml
source_path: "identifier[*].value"
```

#### `target_field` (required)

Name of the field in `_search` where extracted values will be stored.

```yaml
target_field: identifierValues
```

#### `datatype` (required)

Data type of the target field:
- `string`: Single string value
- `array[string]`: Array of strings
- `array[token]`: Array of token values (system|code)
- `number`: Numeric value
- `boolean`: Boolean value
- `date`: ISO date string

```yaml
datatype: array[string]
```

#### `format` (optional)

Template for formatting extracted values. Use `{field}` placeholders.

```yaml
format: "{system}|{code}"  # For tokens
```

#### `extract_id` (optional)

For Reference types, extract only the ID portion (after last `/`).

```yaml
extract_id: true
# "Patient/123" -> "123"
```

#### `options` (optional)

Extractor-specific options.

```yaml
options:
  lowercase: true
  trim: true
  normalize: true
```

---

## Search Parameters Section

The `search_parameters` section defines how FHIR search parameters map to MongoDB fields.

### Structure

```yaml
search_parameters:
  parameter_name:
    type: parameter_type
    fields:
      default:
        - field: mongodb_field_name
          indexed: true
      modifier_name:
        - field: mongodb_field_for_modifier
          indexed: true
```

### Parameters

#### `parameter_name` (key)

The FHIR search parameter name (e.g., `name`, `gender`, `birthdate`).

```yaml
search_parameters:
  name:  # Search parameter name
    type: string
    fields:
      default:
        - field: _search.familyName_lower
          indexed: true
```

#### `type` (required)

The FHIR search parameter type:

| Type | Description | Examples |
|------|-------------|----------|
| `string` | Text search with modifiers | name, address, description |
| `token` | Exact match on codes/identifiers | gender, status, code |
| `reference` | Reference to another resource | patient, subject, encounter |
| `date` | Date/DateTime with prefixes | birthdate, date, period |
| `number` | Numeric values | length, duration, value |
| `quantity` | Quantity with units | value-quantity |

```yaml
type: string
```

#### `fields` (required)

Mapping of search modifiers to MongoDB fields.

##### Simple Mapping (No Modifiers)

```yaml
fields:
  - field: gender
    indexed: true
```

##### With Modifiers

```yaml
fields:
  default:
    - field: _search.familyName_lower
      indexed: true
  exact:
    - field: _search.familyName
      indexed: true
  contains:
    - field: _search.familyName_lower
      indexed: true
```

#### `field` (required)

MongoDB field path to query.

```yaml
field: _search.familyName_lower
```

#### `indexed` (required)

Boolean indicating whether this field should have a MongoDB index.

```yaml
indexed: true
```

### Common Search Modifiers

#### String Modifiers

```yaml
search_parameters:
  name:
    type: string
    fields:
      default:  # Prefix search (default)
        - field: _search.familyName_lower
          indexed: true
      exact:  # Exact match
        - field: _search.familyName
          indexed: true
      contains:  # Substring search
        - field: _search.familyName_lower
          indexed: true
```

#### Token Modifiers

```yaml
search_parameters:
  identifier:
    type: token
    fields:
      default:  # Value only
        - field: _search.identifierValues
          indexed: true
      text:  # Display text
        - field: _search.identifierDisplay_lower
          indexed: true
```

---

## Compartments Section

Define compartment memberships for this resource:

```yaml
compartments:
  - name: Patient
    param: patient
  
  - name: Encounter
    param: encounter
  
  - name: Practitioner
    param: performer
```

### Parameters

#### `name` (required)

Compartment name (Patient, Encounter, Practitioner, Device, RelatedPerson).

#### `param` (required)

Search parameter name used for compartment filtering.

---

## Complete Examples

### Example 1: Patient Configuration

```yaml
resource: Patient
fhir_version: R5

denormalization:
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
        options:
          lowercase: true
      - source_path: "name[*].given[*]"
        target_field: givenNames
        datatype: array[string]
      - source_path: "name[*].given[*]"
        target_field: givenNames_lower
        datatype: array[string]
        options:
          lowercase: true
  
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
  
  address:
    source: address
    extractor: AddressExtractor
    target: _search
    field_mappings:
      - source_path: "address[*].city"
        target_field: addressCity_lower
        datatype: array[string]
        options:
          lowercase: true
      - source_path: "address[*].state"
        target_field: addressState_lower
        datatype: array[string]
        options:
          lowercase: true
      - source_path: "address[*].postalCode"
        target_field: addressPostalCode
        datatype: array[string]

search_parameters:
  name:
    type: string
    fields:
      default:
        - field: _search.familyName_lower
          indexed: true
        - field: _search.givenNames_lower
          indexed: true
      exact:
        - field: _search.familyName
          indexed: true
        - field: _search.givenNames
          indexed: true
  
  family:
    type: string
    fields:
      default:
        - field: _search.familyName_lower
          indexed: true
  
  given:
    type: string
    fields:
      default:
        - field: _search.givenNames_lower
          indexed: true
  
  identifier:
    type: token
    fields:
      - field: _search.identifierValues
        indexed: true
      - field: _search.identifierSystem_value
        indexed: true
  
  gender:
    type: token
    fields:
      - field: gender
        indexed: true
  
  birthdate:
    type: date
    fields:
      - field: birthDate
        indexed: true
  
  address-city:
    type: string
    fields:
      default:
        - field: _search.addressCity_lower
          indexed: true
  
  address-state:
    type: string
    fields:
      default:
        - field: _search.addressState_lower
          indexed: true
  
  address-postalcode:
    type: string
    fields:
      default:
        - field: _search.addressPostalCode
          indexed: true
  
  active:
    type: token
    fields:
      - field: active
        indexed: true

compartments:
  - name: Patient
    param: _id
```

### Example 2: Observation Configuration

```yaml
resource: Observation
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

search_parameters:
  code:
    type: token
    fields:
      - field: _search.codeSystem_code
        indexed: true
      - field: _search.codeValues
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
  
  value-quantity:
    type: quantity
    fields:
      - field: valueQuantity.value
        indexed: true
      - field: valueQuantity.code
        indexed: true

compartments:
  - name: Patient
    param: patient
  - name: Encounter
    param: encounter
```

---

## Index Recommendations

### Essential Indexes

Create indexes for all fields marked with `indexed: true`:

```python
import yaml
from pathlib import Path
from pymongo import MongoClient, ASCENDING

client = MongoClient('mongodb://localhost:27017/')
db = client['fhir_synthetic']

# Load configuration
config_file = Path('configs/Patient.yaml')
with open(config_file) as f:
    config = yaml.safe_load(f)

resource = config['resource']

# Create indexes for all indexed fields
for param_name, param_config in config['search_parameters'].items():
    fields = param_config.get('fields', [])
    
    # Handle both dict and list formats
    if isinstance(fields, dict):
        field_list = []
        for modifier_fields in fields.values():
            field_list.extend(modifier_fields)
    else:
        field_list = fields
    
    # Create index for each field
    for field_config in field_list:
        if field_config.get('indexed', False):
            field_name = field_config['field']
            print(f"Creating index on {resource}.{field_name}")
            db[resource].create_index([(field_name, ASCENDING)])
```

### Compound Indexes

Create compound indexes for common query combinations:

```javascript
// Patient compound indexes
db.Patient.createIndex({
    "_search.familyName_lower": 1,
    "gender": 1,
    "birthDate": 1
});

db.Patient.createIndex({
    "active": 1,
    "_search.familyName_lower": 1
});

// Observation compound indexes
db.Observation.createIndex({
    "_search.patientId": 1,
    "_search.codeSystem_code": 1,
    "effectiveDateTime": 1
});

db.Observation.createIndex({
    "_search.patientId": 1,
    "status": 1,
    "effectiveDateTime": -1
});
```

### Index Maintenance

```python
# List all indexes
for index in db.Patient.list_indexes():
    print(f"Index: {index['name']}")
    print(f"  Keys: {index['key']}")
    print(f"  Size: {db.command('collStats', 'Patient')['indexSizes'][index['name']]} bytes")

# Rebuild indexes
db.Patient.reindex()
```

---

## Validation Rules

### Configuration Validation

```python
import yaml
from pathlib import Path

def validate_config(config_file):
    \"\"\"Validate configuration file.\"\"\"
    with open(config_file) as f:
        config = yaml.safe_load(f)
    
    errors = []
    
    # Check required fields
    if 'resource' not in config:
        errors.append("Missing 'resource' field")
    
    if 'fhir_version' not in config:
        errors.append("Missing 'fhir_version' field")
    
    if 'search_parameters' not in config:
        errors.append("Missing 'search_parameters' field")
    
    # Validate denormalization
    if 'denormalization' in config:
        for field_name, field_config in config['denormalization'].items():
            if 'source' not in field_config:
                errors.append(f"Denormalization '{field_name}': missing 'source'")
            
            if 'extractor' not in field_config:
                errors.append(f"Denormalization '{field_name}': missing 'extractor'")
            
            if 'field_mappings' not in field_config:
                errors.append(f"Denormalization '{field_name}': missing 'field_mappings'")
    
    # Validate search parameters
    for param_name, param_config in config['search_parameters'].items():
        if 'type' not in param_config:
            errors.append(f"Search parameter '{param_name}': missing 'type'")
        
        if 'fields' not in param_config:
            errors.append(f"Search parameter '{param_name}': missing 'fields'")
        
        valid_types = ['string', 'token', 'reference', 'date', 'number', 'quantity']
        if param_config.get('type') not in valid_types:
            errors.append(f"Search parameter '{param_name}': invalid type '{param_config['type']}'")
    
    if errors:
        print(f"Validation errors in {config_file}:")
        for error in errors:
            print(f"  - {error}")
        return False
    
    print(f"Configuration {config_file} is valid")
    return True

# Validate all configurations
config_dir = Path('configs')
for config_file in config_dir.glob('*.yaml'):
    validate_config(config_file)
```

---

## Best Practices

1. **Lowercase Fields**: Always create lowercase versions of string fields for case-insensitive search
2. **Indexed Fields**: Mark all searchable fields as `indexed: true`
3. **Token Format**: Use `{system}|{code}` format for token fields
4. **Extract IDs**: Use `extract_id: true` for reference fields
5. **Array Support**: Use `[*]` wildcards for array fields
6. **Modifiers**: Define all relevant search modifiers
7. **Compartments**: Include compartment definitions for compartment-based queries
8. **Documentation**: Add comments explaining complex configurations
9. **Validation**: Validate configurations before deployment
10. **Testing**: Test all search parameters with sample data

---

## Related Documentation

- [Getting Started](../guides/getting_started.md)
- [Adding Resources](../guides/adding_resources.md)
- [Denormalizer API](denormalizer.md)
- [Converter API](converter.md)
