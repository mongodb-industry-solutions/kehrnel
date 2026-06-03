# Phase 3: Query Parser - Quick Reference

## 📦 New Components

### 1. ModifierValidator
```python
from fhir_search_to_mql.parser import ModifierValidator

validator = ModifierValidator()

# Validate a modifier
validator.validate_modifier("exact", "name", "string")  # ✓ Valid

# Check if modifier is valid
is_valid = validator.is_valid_modifier("exact", "string")  # True

# Get valid modifiers for a type
modifiers = validator.get_valid_modifiers("string")  # ["exact", "contains", "missing"]
```

### 2. CompartmentParser
```python
from fhir_search_to_mql.parser import CompartmentParser

parser = CompartmentParser()

# Parse compartment URL
result = parser.parse("/Patient/123/Observation?code=8480-6")
# Returns:
# {
#   'compartment_type': 'Patient',
#   'compartment_id': '123',
#   'resource_type': 'Observation',
#   'parameters': [...],
#   'parameter_count': 1
# }

# Check if URL is compartment URL
is_compartment = parser.is_compartment_url("/Patient/123/Observation")  # True
```

### 3. Enhanced QueryParser
```python
from fhir_search_to_mql.parser import QueryParser

parser = QueryParser()

# Parse query string
result = parser.parse(query_string="name=Smith&gender=male&birthdate=ge1980-01-01")

# Parse full URL (extracts resource type automatically)
result = parser.parse(url="http://example.org/fhir/Patient?name=Smith")

# Returns:
# {
#   'resource_type': 'Patient',
#   'parameters': [
#     {
#       'name': 'name',
#       'value': 'Smith',
#       'modifier': None,
#       'prefix': None,
#       'type': 'string',
#       'values': ['Smith']
#     }
#   ],
#   'parameter_count': 1,
#   'unique_parameters': 1
# }
```

### 4. Enhanced ParameterParser
```python
from fhir_search_to_mql.parser import ParameterParser

parser = ParameterParser()

# Parse parameter with type inference
result = parser.parse_parameter("name:exact", "Smith")
# Returns:
# {
#   'name': 'name',
#   'modifier': 'exact',
#   'prefix': None,
#   'value': 'Smith',
#   'type': 'string',  # Automatically inferred
#   'values': ['Smith']
# }

# Validate syntax
parser.validate_syntax("name", "Smith")  # ✓ Valid

# Extract chaining
chaining = parser.extract_chaining("subject:Patient.name")
# Returns: {'base': 'subject', 'type': 'Patient', 'chain': 'name'}
```

## 🎯 Common Use Cases

### Parse FHIR Search Query
```python
from fhir_search_to_mql.parser import QueryParser

parser = QueryParser()

# Complex query with modifiers and prefixes
url = "http://fhir.example.com/Patient?name:contains=Smith&gender=male&birthdate=ge1980-01-01"
result = parser.parse(url=url)

print(f"Resource Type: {result['resource_type']}")  # Patient
print(f"Parameters: {result['parameter_count']}")   # 3

for param in result['parameters']:
    print(f"  {param['name']}: {param['value']} (type: {param['type']})")
```

### Parse Compartment Search
```python
from fhir_search_to_mql.parser import CompartmentParser

parser = CompartmentParser()

# Get all Observations for Patient 123 with specific code
url = "/Patient/123/Observation?code=8480-6&status=final"
result = parser.parse(url)

print(f"Compartment: {result['compartment_type']}/{result['compartment_id']}")
print(f"Searching: {result['resource_type']}")
print(f"Filters: {result['parameter_count']} parameters")
```

### Validate Modifiers
```python
from fhir_search_to_mql.parser import ModifierValidator

validator = ModifierValidator()

# Check if modifier is valid for parameter type
if validator.is_valid_modifier("exact", "string"):
    print("Valid!")

# Get description
desc = validator.get_modifier_description("contains", "string")
print(desc)  # "Substring match, case-insensitive"

# Validate and raise error if invalid
try:
    validator.validate_modifier("exact", "status", "token")
except InvalidModifierError as e:
    print(f"Error: {e}")
```

### Handle Multiple Values
```python
from fhir_search_to_mql.parser import QueryParser

parser = QueryParser()

# Comma-separated values (OR logic)
result = parser.parse(query_string="name=Smith,Johnson,Williams")

param = result['parameters'][0]
print(param['values'])  # ['Smith', 'Johnson', 'Williams']

# Repeated parameters (OR logic)
result = parser.parse(query_string="name=Smith&name=Johnson")
print(result['parameter_count'])  # 2 (both name parameters)
```

## 📋 Supported Features

### Modifiers by Parameter Type:
- **String:** exact, contains, missing
- **Token:** not, text, missing, in, not-in, of-type
- **Reference:** identifier, missing, :Patient, :Practitioner, etc.
- **Date:** missing
- **Number:** missing
- **Quantity:** missing
- **URI:** below, above, missing

### Prefixes (all types):
- **eq** - Equal (default)
- **ne** - Not equal
- **gt** - Greater than
- **lt** - Less than
- **ge** - Greater or equal
- **le** - Less or equal
- **sa** - Starts after
- **eb** - Ends before
- **ap** - Approximately

### Compartment Types:
- Patient
- Encounter
- Practitioner
- Device
- RelatedPerson

## 🧪 Testing

Run tests with:
```bash
pytest tests/test_query_parser.py -v
```

## 📖 Full Documentation

See [PHASE_3_COMPLETE.md](PHASE_3_COMPLETE.md) for complete documentation.
