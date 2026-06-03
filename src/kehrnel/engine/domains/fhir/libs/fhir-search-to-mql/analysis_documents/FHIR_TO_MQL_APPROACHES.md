# FHIR to MQL Conversion: Architectural Approaches

**Document Purpose:** Compare different architectural approaches for building a FHIR search query to MongoDB MQL conversion library, considering performance, maintainability, and flexibility.

**Last Updated:** May 13, 2026  
**Version:** 1.0

---

## Table of Contents

1. [Problem Statement](#problem-statement)
2. [Key Design Challenges](#key-design-challenges)
3. [Approach 1: Direct Field Mapping (Simple)](#approach-1-direct-field-mapping-simple)
4. [Approach 2: FHIRPath Expression Mapping](#approach-2-fhirpath-expression-mapping)
5. [Approach 3: SearchParameter Definition-Based](#approach-3-searchparameter-definition-based)
6. [Approach 4: Convention-Based with Override](#approach-4-convention-based-with-override)
7. [Approach 5: Hybrid Mapping Configuration (RECOMMENDED)](#approach-5-hybrid-mapping-configuration-recommended)
8. [Comparison Matrix](#comparison-matrix)
9. [Recommendation](#recommendation)

---

## Problem Statement

### Requirements

1. **Convert FHIR search queries to MongoDB MQL** efficiently
2. **Use `_search` denormalized fields** for performance
3. **Support mapping configuration** - define which MongoDB fields to search for each FHIR parameter
4. **Handle nested fields** - FHIR canonical resources have complex nested structures
5. **Determine search scope** - when a parameter like `name` can search multiple fields (family, given, text), how do we know which fields to include?
6. **Performance-first** - generate simple, fast MQL queries
7. **Maintainable** - easy to add new resources and parameters
8. **Flexible** - support both canonical and `_search` fields

### Example Scenario: The `name` Parameter Problem

**FHIR Query:**
```
GET /Patient?name=Smith
```

**Question:** Which MongoDB fields should we search?

**Canonical Structure:**
```javascript
{
  "name": [
    {
      "family": "Smith",
      "given": ["John", "Michael"],
      "prefix": ["Dr."],
      "text": "Dr. John Michael Smith"
    }
  ]
}
```

**Possible Search Targets:**
- `name[].family` - Last name
- `name[].given[]` - First/middle names
- `name[].text` - Full name string
- `_search.familyName` - Denormalized
- `_search.givenNames[]` - Denormalized array
- `_search.fullName` - Denormalized concatenated

**The Challenge:** How does the library know which fields to search without hardcoding logic for each parameter?

---

## Key Design Challenges

### Challenge 1: Field Resolution

**Problem:** Given a FHIR search parameter name (e.g., `name`, `identifier`, `code`), determine the MongoDB field path(s) to search.

**Complexity:**
- One parameter → Multiple fields (e.g., `name` → family, given, text)
- Canonical vs `_search` fields
- Different resources have different structures
- Nested arrays vs simple fields

### Challenge 2: Type-Specific Conversion

**Problem:** Different FHIR parameter types require different MQL operators:
- String → `$regex`
- Token → `$eq` or array membership
- Date → `$gte`, `$lte` with ranges
- Reference → ID extraction

**Complexity:**
- Need to know parameter type
- Apply correct conversion logic
- Handle modifiers (`:exact`, `:contains`, `:not`)

### Challenge 3: Performance Optimization

**Problem:** Must generate simple, indexed queries.

**Requirements:**
- Prefer `_search` fields over canonical nested structures
- Avoid `$elemMatch` when possible
- Use compound indexes effectively
- Minimize query complexity

### Challenge 4: Maintainability

**Problem:** Adding new resources or parameters should be easy.

**Requirements:**
- Minimal code changes
- Configuration-driven
- Clear documentation
- Validation of mappings

### Challenge 5: Flexibility vs Convention

**Problem:** Balance between explicit configuration and convention-based defaults.

**Trade-offs:**
- Explicit: Verbose but clear
- Convention: Concise but may need overrides

---

## Approach 1: Direct Field Mapping (Simple)

### Overview

Each FHIR search parameter directly maps to one or more MongoDB field paths via simple configuration.

### Configuration Example

```yaml
# config/mappings/Patient.yaml
resource: Patient
mappings:
  name:
    type: string
    fields:
      - _search.familyName
      - _search.givenNames
      - _search.fullName
    operator: OR
    
  birthdate:
    type: date
    fields:
      - birthDate
    
  gender:
    type: token
    fields:
      - gender
    
  identifier:
    type: token
    fields:
      - _search.identifier.values      # Value-only search
      - _search.identifier.systemValues # System|value search
    
  active:
    type: token
    fields:
      - active
```

### Implementation

```python
class DirectFieldMapper:
    def __init__(self, mapping_config: Dict):
        self.mappings = mapping_config['mappings']
    
    def get_field_paths(self, param_name: str) -> List[str]:
        """Get MongoDB field paths for a FHIR parameter."""
        if param_name not in self.mappings:
            raise ValueError(f"Unknown parameter: {param_name}")
        
        return self.mappings[param_name]['fields']
    
    def get_param_type(self, param_name: str) -> str:
        """Get parameter type (string, token, date, etc.)."""
        return self.mappings[param_name]['type']
    
    def get_operator(self, param_name: str) -> str:
        """Get operator for combining multiple fields (AND/OR)."""
        return self.mappings[param_name].get('operator', 'OR')

# Usage
mapper = DirectFieldMapper(load_config('Patient.yaml'))

# Convert name search
field_paths = mapper.get_field_paths('name')  # ['_search.familyName', ...]
param_type = mapper.get_param_type('name')    # 'string'
operator = mapper.get_operator('name')        # 'OR'

# Generate MQL
mql = {
    "$or": [
        {"_search.familyName": {"$regex": "^Smith", "$options": "i"}},
        {"_search.givenNames": {"$regex": "^Smith", "$options": "i"}},
        {"_search.fullName": {"$regex": "^Smith", "$options": "i"}}
    ]
}
```

### Pros

✅ **Simple to understand** - direct mapping, no magic  
✅ **Easy to configure** - YAML/JSON config files  
✅ **Fast lookup** - O(1) dictionary access  
✅ **Explicit** - clear which fields are searched  
✅ **Easy to validate** - check if fields exist in DB  
✅ **Performance-first** - directly targets `_search` fields

### Cons

❌ **Verbose** - need to list all fields explicitly  
❌ **Duplication** - similar patterns across resources  
❌ **No dynamic field resolution** - can't handle computed fields  
❌ **Limited flexibility** - can't express complex logic  
❌ **Maintenance overhead** - update config for every field change

### Use Cases

Best for:
- Simple, well-defined resources
- Performance-critical applications
- Teams that prefer explicit over implicit
- Projects with stable schema

---

## Approach 2: FHIRPath Expression Mapping

### Overview

Use FHIRPath expressions to define how to extract search values from FHIR resources. The mapping configuration stores FHIRPath expressions that are evaluated at query time.

### Configuration Example

```yaml
# config/mappings/Patient.yaml
resource: Patient
mappings:
  name:
    type: string
    fhirPath: "Patient.name.family | Patient.name.given | Patient.name.text"
    mongoFields:
      - path: _search.familyName
        fhirPath: Patient.name.family
      - path: _search.givenNames
        fhirPath: Patient.name.given
      - path: _search.fullName
        fhirPath: Patient.name.text
    
  birthdate:
    type: date
    fhirPath: "Patient.birthDate"
    mongoFields:
      - path: birthDate
        fhirPath: Patient.birthDate
    
  identifier:
    type: token
    fhirPath: "Patient.identifier.value"
    mongoFields:
      - path: _search.identifier.values
        fhirPath: Patient.identifier.value
      - path: _search.identifier.systemValues
        fhirPath: Patient.identifier.system + '|' + Patient.identifier.value
```

### Implementation

```python
from fhirpath import FHIRPathEvaluator

class FHIRPathMapper:
    def __init__(self, mapping_config: Dict):
        self.mappings = mapping_config['mappings']
        self.evaluator = FHIRPathEvaluator()
    
    def get_field_mappings(self, param_name: str) -> List[Dict]:
        """Get field mappings with FHIRPath expressions."""
        if param_name not in self.mappings:
            raise ValueError(f"Unknown parameter: {param_name}")
        
        return self.mappings[param_name]['mongoFields']
    
    def extract_values_for_indexing(self, resource: Dict, param_name: str) -> Dict:
        """
        Extract values from FHIR resource to populate _search fields.
        Used during data ingestion.
        """
        field_mappings = self.get_field_mappings(param_name)
        result = {}
        
        for mapping in field_mappings:
            fhir_path = mapping['fhirPath']
            mongo_path = mapping['path']
            
            # Evaluate FHIRPath expression
            values = self.evaluator.evaluate(resource, fhir_path)
            
            # Store in _search structure
            self._set_nested_value(result, mongo_path, values)
        
        return result

# Usage
mapper = FHIRPathMapper(load_config('Patient.yaml'))

# At ingestion time: extract values from FHIR resource
patient_resource = {
    "resourceType": "Patient",
    "name": [
        {"family": "Smith", "given": ["John", "Michael"]}
    ]
}

search_fields = mapper.extract_values_for_indexing(patient_resource, 'name')
# Result: {
#   "_search": {
#     "familyName": "Smith",
#     "givenNames": ["John", "Michael"],
#     "fullName": "John Michael Smith"
#   }
# }

# At query time: use pre-populated _search fields
field_mappings = mapper.get_field_mappings('name')
mql = {
    "$or": [
        {"_search.familyName": {"$regex": "^Smith", "$options": "i"}},
        {"_search.givenNames": {"$regex": "^Smith", "$options": "i"}},
        {"_search.fullName": {"$regex": "^Smith", "$options": "i"}}
    ]
}
```

### Pros

✅ **Standards-compliant** - uses FHIR's official path language  
✅ **Flexible** - can express complex extractions  
✅ **Self-documenting** - FHIRPath shows what's extracted  
✅ **Dual use** - same config for indexing and querying  
✅ **Dynamic** - can compute derived values  
✅ **Type-safe** - FHIRPath has type system

### Cons

❌ **Complex** - need FHIRPath evaluator library  
❌ **Performance overhead** - expression evaluation at query time (if used dynamically)  
❌ **Learning curve** - team needs to know FHIRPath  
❌ **Debugging difficulty** - FHIRPath errors can be cryptic  
❌ **Overkill for simple cases** - just need direct field mapping

### Use Cases

Best for:
- Complex field extractions
- Standards-focused teams
- Need to support custom SearchParameters
- Shared logic between ingestion and querying
- Large teams with FHIR expertise

---

## Approach 3: SearchParameter Definition-Based

### Overview

Use FHIR SearchParameter resources as the source of truth. Load SearchParameter definitions from FHIR specification or custom definitions, then map to MongoDB fields.

### Configuration Example

```json
// config/search_parameters/Patient.json
{
  "resourceType": "Bundle",
  "type": "collection",
  "entry": [
    {
      "resource": {
        "resourceType": "SearchParameter",
        "id": "Patient-name",
        "name": "name",
        "status": "active",
        "code": "name",
        "base": ["Patient"],
        "type": "string",
        "expression": "Patient.name.family | Patient.name.given | Patient.name.text",
        "xpathUsage": "normal",
        "extension": [
          {
            "url": "http://example.org/fhir/StructureDefinition/mongo-mapping",
            "extension": [
              {
                "url": "fieldPath",
                "valueString": "_search.familyName"
              },
              {
                "url": "fieldPath",
                "valueString": "_search.givenNames"
              },
              {
                "url": "fieldPath",
                "valueString": "_search.fullName"
              }
            ]
          }
        ]
      }
    },
    {
      "resource": {
        "resourceType": "SearchParameter",
        "id": "Patient-birthdate",
        "name": "birthdate",
        "code": "birthdate",
        "base": ["Patient"],
        "type": "date",
        "expression": "Patient.birthDate",
        "extension": [
          {
            "url": "http://example.org/fhir/StructureDefinition/mongo-mapping",
            "extension": [
              {
                "url": "fieldPath",
                "valueString": "birthDate"
              }
            ]
          }
        ]
      }
    }
  ]
}
```

### Implementation

```python
class SearchParameterMapper:
    def __init__(self, search_params_bundle: Dict):
        self.search_params = self._parse_search_parameters(search_params_bundle)
    
    def _parse_search_parameters(self, bundle: Dict) -> Dict:
        """Parse SearchParameter resources from bundle."""
        params = {}
        
        for entry in bundle.get('entry', []):
            sp = entry['resource']
            if sp['resourceType'] != 'SearchParameter':
                continue
            
            code = sp['code']
            params[code] = {
                'name': sp['name'],
                'type': sp['type'],
                'expression': sp.get('expression', ''),
                'mongoFields': self._extract_mongo_fields(sp)
            }
        
        return params
    
    def _extract_mongo_fields(self, search_param: Dict) -> List[str]:
        """Extract MongoDB field paths from SearchParameter extensions."""
        mongo_fields = []
        
        for ext in search_param.get('extension', []):
            if ext['url'].endswith('mongo-mapping'):
                for sub_ext in ext.get('extension', []):
                    if sub_ext['url'] == 'fieldPath':
                        mongo_fields.append(sub_ext['valueString'])
        
        return mongo_fields
    
    def get_search_parameter(self, param_name: str) -> Dict:
        """Get SearchParameter definition."""
        if param_name not in self.search_params:
            raise ValueError(f"Unknown search parameter: {param_name}")
        
        return self.search_params[param_name]
    
    def get_field_paths(self, param_name: str) -> List[str]:
        """Get MongoDB field paths for parameter."""
        sp = self.get_search_parameter(param_name)
        return sp['mongoFields']
    
    def get_param_type(self, param_name: str) -> str:
        """Get parameter type."""
        sp = self.get_search_parameter(param_name)
        return sp['type']

# Usage
mapper = SearchParameterMapper(load_search_parameters('Patient.json'))

# Query conversion
param_name = 'name'
param_type = mapper.get_param_type(param_name)  # 'string'
field_paths = mapper.get_field_paths(param_name)  # ['_search.familyName', ...]

# Generate MQL
converter = StringConverter()
mql = {
    "$or": [
        converter.convert(field, "Smith")
        for field in field_paths
    ]
}
```

### Pros

✅ **FHIR-compliant** - uses official SearchParameter resources  
✅ **Portable** - can share with other FHIR implementations  
✅ **Comprehensive** - includes all metadata (type, expression, description)  
✅ **Extensible** - use FHIR extensions for custom mappings  
✅ **Versioned** - can track changes like any FHIR resource  
✅ **Reusable** - same definitions for validation, documentation, UI generation

### Cons

❌ **Verbose** - FHIR JSON is heavy  
❌ **Complex structure** - nested extensions, bundles  
❌ **Parsing overhead** - need to parse FHIR structures  
❌ **Requires FHIR knowledge** - team must understand SearchParameters  
❌ **Custom extensions** - need to define extension for MongoDB mappings  
❌ **Overkill for simple projects** - too much infrastructure

### Use Cases

Best for:
- Large enterprise projects
- Multi-system integration
- Need FHIR conformance validation
- Sharing definitions across teams
- Advanced FHIR implementations

---

## Approach 4: Convention-Based with Override

### Overview

Follow naming conventions to automatically determine MongoDB fields, with explicit overrides for exceptions.

### Convention Rules

```python
# Convention Rules
# 1. Simple parameters map to _search.{paramName}
#    - name → _search.name
#    - status → _search.status
#
# 2. Token parameters map to _search.{paramName}Codes
#    - code → _search.codeCodes
#    - specialty → _search.specialtyCodes
#
# 3. Reference parameters map to _search.{paramName}Id
#    - subject → _search.subjectId
#    - patient → _search.patientId
#
# 4. Identifier maps to _search.identifier.values
#
# 5. Date/DateTime map to _search.{paramName} or canonical field
```

### Configuration Example

```yaml
# config/conventions.yaml
conventions:
  string:
    pattern: "_search.{paramName}"
    multiField:
      - "_search.{paramName}"
      - "{paramName}"  # Also check canonical field
  
  token:
    simple:
      pattern: "{paramName}"  # For simple tokens like gender, status
    codeable:
      pattern: "_search.{paramName}Codes"
  
  reference:
    pattern: "_search.{paramName}Id"
  
  date:
    pattern: "_search.{paramName}"
    fallback: "{paramName}"

# config/overrides/Patient.yaml
resource: Patient
overrides:
  name:
    # Override: name searches multiple fields, not just _search.name
    fields:
      - _search.familyName
      - _search.givenNames
      - _search.fullName
    operator: OR
  
  identifier:
    # Override: identifier has special structure
    fields:
      - _search.identifier.values
      - _search.identifier.systemValues
```

### Implementation

```python
class ConventionBasedMapper:
    def __init__(self, conventions: Dict, overrides: Dict):
        self.conventions = conventions
        self.overrides = overrides.get('overrides', {})
    
    def get_field_paths(self, param_name: str, param_type: str) -> List[str]:
        """Get MongoDB field paths using convention or override."""
        
        # Check for explicit override first
        if param_name in self.overrides:
            return self.overrides[param_name]['fields']
        
        # Apply convention based on type
        if param_type == 'string':
            return self._apply_string_convention(param_name)
        elif param_type == 'token':
            return self._apply_token_convention(param_name)
        elif param_type == 'reference':
            return self._apply_reference_convention(param_name)
        elif param_type == 'date':
            return self._apply_date_convention(param_name)
        else:
            raise ValueError(f"Unknown parameter type: {param_type}")
    
    def _apply_string_convention(self, param_name: str) -> List[str]:
        """Apply string parameter convention."""
        pattern = self.conventions['string']['pattern']
        fields = [pattern.replace('{paramName}', param_name)]
        
        # Add multiField if configured
        if 'multiField' in self.conventions['string']:
            for field_pattern in self.conventions['string']['multiField']:
                fields.append(field_pattern.replace('{paramName}', param_name))
        
        return fields
    
    def _apply_token_convention(self, param_name: str) -> List[str]:
        """Apply token parameter convention."""
        # Check if it's a simple token or codeable concept
        # This could be enhanced with heuristics or configuration
        if self._is_simple_token(param_name):
            pattern = self.conventions['token']['simple']['pattern']
        else:
            pattern = self.conventions['token']['codeable']['pattern']
        
        return [pattern.replace('{paramName}', param_name)]
    
    def _apply_reference_convention(self, param_name: str) -> List[str]:
        """Apply reference parameter convention."""
        pattern = self.conventions['reference']['pattern']
        return [pattern.replace('{paramName}', param_name)]
    
    def _apply_date_convention(self, param_name: str) -> List[str]:
        """Apply date parameter convention with fallback."""
        pattern = self.conventions['date']['pattern']
        fallback = self.conventions['date'].get('fallback', '')
        
        fields = [pattern.replace('{paramName}', param_name)]
        if fallback:
            fields.append(fallback.replace('{paramName}', param_name))
        
        return fields
    
    def _is_simple_token(self, param_name: str) -> bool:
        """Determine if token is simple (gender, status) or codeable (code, type)."""
        simple_tokens = {'gender', 'status', 'active'}
        return param_name in simple_tokens

# Usage
conventions = load_yaml('config/conventions.yaml')
overrides = load_yaml('config/overrides/Patient.yaml')
mapper = ConventionBasedMapper(conventions, overrides)

# Most parameters use convention
field_paths = mapper.get_field_paths('birthdate', 'date')
# Returns: ['_search.birthdate', 'birthdate']

# Complex parameters use override
field_paths = mapper.get_field_paths('name', 'string')
# Returns: ['_search.familyName', '_search.givenNames', '_search.fullName']
```

### Pros

✅ **Low maintenance** - most parameters "just work" by convention  
✅ **Concise config** - only specify exceptions  
✅ **Easy to extend** - new parameters follow convention automatically  
✅ **Balance** - convention for common cases, override for special cases  
✅ **Self-documenting** - conventions clearly stated  
✅ **Quick setup** - minimal configuration needed

### Cons

❌ **Hidden complexity** - logic is in code, not config  
❌ **Convention must be learned** - team needs to know the rules  
❌ **Heuristics may fail** - distinguishing simple vs codeable tokens  
❌ **Less explicit** - not immediately clear which fields are searched  
❌ **Testing overhead** - need to verify conventions work correctly

### Use Cases

Best for:
- Rapid development
- Consistent naming standards
- Large number of similar parameters
- Teams that value convention over configuration
- Mature projects with established patterns

---

## Approach 5: Hybrid Mapping Configuration (RECOMMENDED)

### Overview

**Combines the best of all approaches:**
1. **Explicit field mapping** (like Approach 1) for clarity
2. **Type-based converters** with pluggable architecture
3. **Resource-specific configuration files** for maintainability
4. **Optional conventions** for common patterns
5. **Validation and defaults** for safety

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   FHIR Search Query                         │
│              GET /Patient?name=Smith&gender=male            │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  Query Parser Module                        │
│  - Parse URL parameters                                     │
│  - Extract: resource_type, parameters, modifiers, prefixes  │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              Mapping Configuration Loader                   │
│  - Load config/mappings/{ResourceType}.yaml                 │
│  - Validate configuration                                   │
│  - Cache for performance                                    │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│               Field Path Resolver                           │
│  - For each search parameter:                               │
│    * Look up in mapping config                              │
│    * Get MongoDB field paths                                │
│    * Get parameter type                                     │
│    * Get operator (AND/OR for multi-field)                  │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│            Type-Specific Converter Selection                │
│  - StringConverter for string parameters                    │
│  - TokenConverter for token parameters                      │
│  - ReferenceConverter for reference parameters              │
│  - DateConverter for date parameters                        │
│  - etc.                                                     │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              MQL Fragment Generation                        │
│  - Each converter generates MQL for its field(s)            │
│  - Apply modifiers (:exact, :contains, :not, etc.)          │
│  - Apply prefixes (gt, lt, ge, le, etc.)                    │
│  - Use _search fields for performance                       │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              Query Combiner & Optimizer                     │
│  - Combine multi-field searches with $or/$and               │
│  - Combine multiple parameters with $and                    │
│  - Optimize query structure                                 │
│  - Add index hints if configured                            │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   Final MQL Query                           │
│  {                                                          │
│    "$and": [                                                │
│      {"$or": [                                              │
│        {"_search.familyName": {"$regex": "^Smith", ...}},   │
│        {"_search.givenNames": {"$regex": "^Smith", ...}}    │
│      ]},                                                    │
│      {"gender": "male"}                                     │
│    ]                                                        │
│  }                                                          │
└─────────────────────────────────────────────────────────────┘
```

### Configuration Format

```yaml
# config/mappings/Patient.yaml
resource: Patient
version: 1.0

# Define search parameter mappings
parameters:
  
  # String parameter with multiple fields
  name:
    type: string
    description: "Search by patient name (family, given, or full name)"
    fields:
      - field: _search.familyName
        weight: 1.0  # Optional: for ranking/relevance
      - field: _search.givenNames
        weight: 0.8
      - field: _search.fullName
        weight: 0.6
    operator: OR  # Combine fields with OR logic
    modifiers:
      - exact
      - contains
    examples:
      - "name=Smith"
      - "name:exact=Smith"
      - "name:contains=mit"
  
  # Simple string field
  family:
    type: string
    description: "Search by family name only"
    fields:
      - field: _search.familyName
    modifiers:
      - exact
      - contains
  
  # Date parameter
  birthdate:
    type: date
    description: "Search by birth date"
    fields:
      - field: birthDate  # Use canonical field
    prefixes:
      - eq
      - ne
      - gt
      - lt
      - ge
      - le
    examples:
      - "birthdate=1985-03-15"
      - "birthdate=gt1980-01-01"
  
  # Simple token
  gender:
    type: token
    description: "Search by gender"
    fields:
      - field: gender  # Use canonical field
    tokenType: simple
    examples:
      - "gender=male"
      - "gender=female"
  
  # Boolean token
  active:
    type: token
    description: "Search by active status"
    fields:
      - field: active
    tokenType: boolean
  
  # Complex token (CodeableConcept)
  marital-status:
    type: token
    description: "Search by marital status"
    fields:
      - field: _search.maritalStatusCodes  # Code array
        tokenType: code
      - field: _search.maritalStatusSystemValues  # System|code array
        tokenType: systemCode
    operator: OR
    examples:
      - "marital-status=M"
      - "marital-status=http://terminology.hl7.org/CodeSystem/v3-MaritalStatus|M"
  
  # Identifier with multiple search formats
  identifier:
    type: token
    description: "Search by identifier"
    fields:
      - field: _search.identifier.values
        tokenType: value
      - field: _search.identifier.systems
        tokenType: system
      - field: _search.identifier.systemValues
        tokenType: systemValue
    modifiers:
      - not
    examples:
      - "identifier=MRN-12345"
      - "identifier=http://hospital.org/mrn|MRN-12345"
  
  # Reference parameter
  general-practitioner:
    type: reference
    description: "Search by general practitioner"
    fields:
      - field: _search.generalPractitionerId
    referenceTypes:
      - Practitioner
      - Organization
      - PractitionerRole
    modifiers:
      - identifier
    examples:
      - "general-practitioner=prac-123"
      - "general-practitioner=Practitioner/prac-123"

# Global settings for this resource
settings:
  # Default operator for combining multiple parameters
  defaultParameterOperator: AND
  
  # Enable query optimization
  optimize: true
  
  # Index hints for common queries
  indexHints:
    - parameters: [name, birthdate]
      index: name_birthdate_idx
    - parameters: [identifier]
      index: identifier_idx

# Validation rules
validation:
  requiredFields:
    - id
    - resourceType
  indexedFields:
    - _search.familyName
    - _search.identifier.values
    - birthDate
    - gender
    - active
```

```yaml
# config/mappings/Observation.yaml
resource: Observation
version: 1.0

parameters:
  
  # Patient reference
  subject:
    type: reference
    description: "The subject of the observation (usually a patient)"
    fields:
      - field: _search.patientId  # Primary field (most common)
      - field: _search.subjectId  # Generic field (for non-patient subjects)
    operator: OR
    referenceTypes:
      - Patient
      - Group
      - Device
      - Location
    modifiers:
      - Patient  # subject:Patient=123
      - identifier
  
  # Code search (CodeableConcept)
  code:
    type: token
    description: "The code of the observation"
    fields:
      - field: _search.codeCodes  # Simple code array
        tokenType: code
        primary: true  # Use this for simple code searches
      - field: _search.codeSystemValues  # System|code array
        tokenType: systemCode
        primary: false  # Use this for system|code searches
    operator: OR
    modifiers:
      - text
      - not
    examples:
      - "code=8480-6"
      - "code=http://loinc.org|8480-6"
      - "code:not=8480-6"
  
  # Date search
  date:
    type: date
    description: "Search by observation date"
    fields:
      - field: _search.start  # Extracted from effectiveDateTime/effectivePeriod
    prefixes:
      - eq
      - ne
      - gt
      - lt
      - ge
      - le
      - sa
      - eb
  
  # Status token
  status:
    type: token
    description: "The status of the observation"
    fields:
      - field: status  # Canonical field
    tokenType: simple
    allowedValues:
      - registered
      - preliminary
      - final
      - amended
      - corrected
      - cancelled
      - entered-in-error
      - unknown
  
  # Category (CodeableConcept)
  category:
    type: token
    description: "The category of the observation"
    fields:
      - field: _search.categoryCodes
        tokenType: code
      - field: _search.categorySystemValues
        tokenType: systemCode
    operator: OR

settings:
  defaultParameterOperator: AND
  optimize: true
  indexHints:
    - parameters: [subject, code, date]
      index: patient_code_date_idx
    - parameters: [subject, date]
      index: patient_date_idx
```

```yaml
# config/mappings/Appointment.yaml
resource: Appointment
version: 1.0

parameters:
  
  # Patient reference
  patient:
    type: reference
    description: "Search by patient participant"
    fields:
      - field: _search.patientId  # Extracted primary patient ID
    referenceTypes:
      - Patient
    modifiers:
      - identifier
    examples:
      - "patient=pat-123"
  
  # Practitioner reference
  practitioner:
    type: reference
    description: "Search by practitioner participant"
    fields:
      - field: _search.practitionerId  # Extracted primary practitioner ID
    referenceTypes:
      - Practitioner
      - PractitionerRole
    modifiers:
      - identifier
  
  # Location reference
  location:
    type: reference
    description: "Search by location"
    fields:
      - field: _search.locationId
    referenceTypes:
      - Location
  
  # Generic actor search (any participant)
  actor:
    type: reference
    description: "Search by any participant actor"
    fields:
      - field: _search.actor.ids  # Array of all participant IDs
    referenceTypes:
      - Patient
      - Practitioner
      - PractitionerRole
      - RelatedPerson
      - Device
      - HealthcareService
      - Location
  
  # Appointment type (CodeableConcept)
  appointment-type:
    type: token
    description: "Type of appointment"
    fields:
      - field: _search.appointmentTypeCodes
        tokenType: code
      - field: _search.appointmentTypeSystemValues
        tokenType: systemCode
    operator: OR
  
  # Date search
  date:
    type: date
    description: "Appointment date"
    fields:
      - field: _search.start
    prefixes:
      - eq
      - ne
      - gt
      - lt
      - ge
      - le
  
  # Status
  status:
    type: token
    description: "Appointment status"
    fields:
      - field: _search.status
    tokenType: simple
    allowedValues:
      - proposed
      - pending
      - booked
      - arrived
      - fulfilled
      - cancelled
      - noshow
      - entered-in-error
      - checked-in
      - waitlist
  
  # Service category
  service-category:
    type: token
    description: "Service category"
    fields:
      - field: _search.serviceCategoryCodes
        tokenType: code
  
  # Service type
  service-type:
    type: token
    description: "Service type"
    fields:
      - field: _search.serviceTypeCodes
        tokenType: code
  
  # Specialty
  specialty:
    type: token
    description: "Medical specialty"
    fields:
      - field: _search.specialtyCodes
        tokenType: code

settings:
  defaultParameterOperator: AND
  optimize: true
  indexHints:
    - parameters: [patient, date, status]
      index: patient_date_status_idx
    - parameters: [practitioner, date]
      index: practitioner_date_idx
    - parameters: [location, date]
      index: location_date_idx
```

### Implementation Structure

```
fhir_query_mql/
├── __init__.py
├── config/
│   ├── __init__.py
│   ├── mappings/
│   │   ├── Patient.yaml
│   │   ├── Observation.yaml
│   │   ├── Appointment.yaml
│   │   ├── Schedule.yaml
│   │   ├── Slot.yaml
│   │   └── ...
│   └── settings.yaml
│
├── core/
│   ├── __init__.py
│   ├── converter.py              # Main FHIRToMQLConverter class
│   ├── mapping_loader.py         # Load and cache mapping configs
│   ├── field_resolver.py         # Resolve MongoDB fields from config
│   ├── query_builder.py          # Build final MQL query
│   └── optimizer.py              # Query optimization
│
├── parsers/
│   ├── __init__.py
│   ├── url_parser.py             # Parse FHIR search URLs
│   └── parameter_parser.py       # Parse individual parameters
│
├── converters/
│   ├── __init__.py
│   ├── base.py                   # BaseConverter abstract class
│   ├── string_converter.py       # String parameter conversion
│   ├── token_converter.py        # Token parameter conversion
│   ├── reference_converter.py    # Reference parameter conversion
│   ├── date_converter.py         # Date parameter conversion
│   ├── number_converter.py       # Number parameter conversion
│   ├── quantity_converter.py     # Quantity parameter conversion
│   └── composite_converter.py    # Composite parameter conversion
│
├── modifiers/
│   ├── __init__.py
│   ├── string_modifiers.py       # :exact, :contains
│   ├── token_modifiers.py        # :not, :text, :in, :not-in
│   ├── reference_modifiers.py    # :identifier, :[type]
│   └── common_modifiers.py       # :missing, etc.
│
├── utils/
│   ├── __init__.py
│   ├── validation.py             # Validate configurations
│   ├── cache.py                  # Caching utilities
│   └── helpers.py                # Helper functions
│
├── exceptions.py                 # Custom exceptions
└── types.py                      # Type definitions

tests/
├── __init__.py
├── test_converters/
├── test_mappings/
├── test_integration/
└── fixtures/
    ├── sample_configs/
    └── test_cases/

examples/
├── basic_usage.py
├── custom_mapping.py
└── performance_test.py

docs/
├── configuration_guide.md
├── adding_new_resource.md
└── troubleshooting.md
```

### Core Implementation

```python
# core/converter.py
from typing import Dict, List, Optional
from .mapping_loader import MappingLoader
from .field_resolver import FieldResolver
from .query_builder import QueryBuilder
from ..parsers.url_parser import URLParser
from ..converters import get_converter

class FHIRToMQLConverter:
    """
    Main converter class for FHIR search queries to MongoDB MQL.
    
    Usage:
        converter = FHIRToMQLConverter()
        mql = converter.convert('Patient', 'name=Smith&gender=male')
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize converter with configuration.
        
        Args:
            config_path: Path to configuration directory (optional)
        """
        self.mapping_loader = MappingLoader(config_path)
        self.field_resolver = FieldResolver(self.mapping_loader)
        self.query_builder = QueryBuilder()
        self.url_parser = URLParser()
    
    def convert(self, resource_type: str, search_url: str) -> Dict:
        """
        Convert FHIR search query to MongoDB MQL.
        
        Args:
            resource_type: FHIR resource type (e.g., 'Patient', 'Observation')
            search_url: FHIR search query string or full URL
            
        Returns:
            MongoDB query dictionary
            
        Example:
            >>> converter.convert('Patient', 'name=Smith&birthdate=gt1980-01-01')
            {
                "$and": [
                    {"$or": [
                        {"_search.familyName": {"$regex": "^Smith", "$options": "i"}},
                        {"_search.givenNames": {"$regex": "^Smith", "$options": "i"}}
                    ]},
                    {"birthDate": {"$gt": "1980-01-01"}}
                ]
            }
        """
        # Parse search URL
        parsed_query = self.url_parser.parse(search_url)
        
        # Load mapping configuration for resource type
        mapping_config = self.mapping_loader.load(resource_type)
        
        # Convert each parameter
        mql_fragments = []
        
        for param_name, param_values in parsed_query.parameters.items():
            # Resolve field paths and metadata from mapping config
            field_info = self.field_resolver.resolve(
                resource_type, 
                param_name, 
                mapping_config
            )
            
            # Get appropriate converter for parameter type
            converter = get_converter(field_info['type'])
            
            # Convert parameter values to MQL
            for param_value in param_values:
                mql_fragment = converter.convert(
                    field_info=field_info,
                    value=param_value,
                    modifier=parsed_query.get_modifier(param_name),
                    prefix=parsed_query.get_prefix(param_value)
                )
                mql_fragments.append(mql_fragment)
        
        # Build final query
        final_query = self.query_builder.build(
            mql_fragments,
            operator=mapping_config.get('settings', {}).get('defaultParameterOperator', 'AND')
        )
        
        return final_query
```

```python
# core/mapping_loader.py
import os
import yaml
from typing import Dict, Optional
from ..utils.cache import Cache
from ..utils.validation import validate_mapping_config

class MappingLoader:
    """Load and cache mapping configurations."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize mapping loader.
        
        Args:
            config_path: Path to config directory. If None, uses default.
        """
        if config_path is None:
            # Default to config/ directory relative to this file
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                'config'
            )
        
        self.config_path = config_path
        self.mappings_path = os.path.join(config_path, 'mappings')
        self.cache = Cache()
    
    def load(self, resource_type: str) -> Dict:
        """
        Load mapping configuration for a resource type.
        
        Args:
            resource_type: FHIR resource type
            
        Returns:
            Mapping configuration dictionary
            
        Raises:
            FileNotFoundError: If mapping file doesn't exist
            ValidationError: If configuration is invalid
        """
        # Check cache first
        cache_key = f"mapping:{resource_type}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached
        
        # Load from file
        mapping_file = os.path.join(self.mappings_path, f"{resource_type}.yaml")
        
        if not os.path.exists(mapping_file):
            raise FileNotFoundError(
                f"No mapping configuration found for resource type: {resource_type}. "
                f"Expected file: {mapping_file}"
            )
        
        with open(mapping_file, 'r') as f:
            config = yaml.safe_load(f)
        
        # Validate configuration
        validate_mapping_config(config, resource_type)
        
        # Cache and return
        self.cache.set(cache_key, config, ttl=3600)
        return config
    
    def reload(self, resource_type: str) -> Dict:
        """Force reload of mapping configuration (bypass cache)."""
        cache_key = f"mapping:{resource_type}"
        self.cache.delete(cache_key)
        return self.load(resource_type)
```

```python
# core/field_resolver.py
from typing import Dict, List, Optional

class FieldResolver:
    """Resolve MongoDB field paths from mapping configuration."""
    
    def __init__(self, mapping_loader):
        self.mapping_loader = mapping_loader
    
    def resolve(self, resource_type: str, param_name: str, 
                mapping_config: Dict) -> Dict:
        """
        Resolve field information for a search parameter.
        
        Args:
            resource_type: FHIR resource type
            param_name: Search parameter name
            mapping_config: Loaded mapping configuration
            
        Returns:
            Dictionary with field information:
            {
                'type': 'string',
                'fields': [
                    {'field': '_search.familyName', 'weight': 1.0},
                    {'field': '_search.givenNames', 'weight': 0.8}
                ],
                'operator': 'OR',
                'tokenType': 'code',  # For token parameters
                'referenceTypes': ['Patient'],  # For reference parameters
                'prefixes': ['gt', 'lt', 'ge', 'le'],  # For date/number parameters
                'modifiers': ['exact', 'contains']  # Allowed modifiers
            }
            
        Raises:
            ValueError: If parameter not found in configuration
        """
        parameters = mapping_config.get('parameters', {})
        
        if param_name not in parameters:
            raise ValueError(
                f"Unknown search parameter '{param_name}' for resource type '{resource_type}'. "
                f"Available parameters: {list(parameters.keys())}"
            )
        
        param_config = parameters[param_name]
        
        # Build field information
        field_info = {
            'type': param_config['type'],
            'fields': self._parse_fields(param_config['fields']),
            'operator': param_config.get('operator', 'OR'),
            'modifiers': param_config.get('modifiers', []),
        }
        
        # Add type-specific metadata
        if param_config['type'] == 'token':
            field_info['tokenType'] = param_config.get('tokenType', 'code')
        elif param_config['type'] == 'reference':
            field_info['referenceTypes'] = param_config.get('referenceTypes', [])
        elif param_config['type'] in ['date', 'number']:
            field_info['prefixes'] = param_config.get('prefixes', [])
        
        return field_info
    
    def _parse_fields(self, fields_config: List) -> List[Dict]:
        """
        Parse fields configuration.
        
        Handles both simple format:
            fields: [field1, field2]
        
        And complex format:
            fields:
              - field: field1
                weight: 1.0
              - field: field2
                weight: 0.8
        """
        parsed_fields = []
        
        for field in fields_config:
            if isinstance(field, str):
                # Simple format
                parsed_fields.append({
                    'field': field,
                    'weight': 1.0
                })
            elif isinstance(field, dict):
                # Complex format
                parsed_fields.append({
                    'field': field['field'],
                    'weight': field.get('weight', 1.0),
                    'tokenType': field.get('tokenType'),
                    'primary': field.get('primary', True)
                })
        
        return parsed_fields
```

### Usage Examples

```python
# Example 1: Basic usage
from fhir_query_mql import FHIRToMQLConverter

converter = FHIRToMQLConverter()

# Simple query
mql = converter.convert('Patient', 'name=Smith&gender=male')
print(mql)
# Output:
# {
#   "$and": [
#     {"$or": [
#       {"_search.familyName": {"$regex": "^Smith", "$options": "i"}},
#       {"_search.givenNames": {"$regex": "^Smith", "$options": "i"}},
#       {"_search.fullName": {"$regex": "^Smith", "$options": "i"}}
#     ]},
#     {"gender": "male"}
#   ]
# }

# Example 2: Complex query with modifiers
mql = converter.convert(
    'Observation',
    'subject=Patient/pat-123&code=http://loinc.org|8480-6&date=ge2024-01-01&status=final'
)

# Example 3: Custom configuration path
converter = FHIRToMQLConverter(config_path='/path/to/custom/config')
mql = converter.convert('Patient', 'identifier=MRN-12345')

# Example 4: Execute query
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['fhir_db']

converter = FHIRToMQLConverter()
mql = converter.convert('Patient', 'name=Smith&active=true')

results = list(db.Patient.find(mql).limit(10))
print(f"Found {len(results)} patients")

# Example 5: Validation and error handling
try:
    mql = converter.convert('Patient', 'invalid_param=value')
except ValueError as e:
    print(f"Error: {e}")
    # Error: Unknown search parameter 'invalid_param' for resource type 'Patient'
```

### Pros

✅ **Explicit and clear** - mapping config shows exactly which fields are searched  
✅ **Flexible** - supports complex multi-field scenarios  
✅ **Type-safe** - parameter types defined in config  
✅ **Maintainable** - add new resources by creating config file  
✅ **Validated** - config validation prevents errors  
✅ **Performance-focused** - directly targets `_search` fields  
✅ **Well-documented** - config includes descriptions and examples  
✅ **Extensible** - easy to add new parameter types or modifiers  
✅ **Testable** - config can be tested independently  
✅ **Cacheable** - config loaded once and cached

### Cons

❌ **Configuration overhead** - need to create config for each resource  
❌ **Verbosity** - comprehensive configs are longer  
❌ **Learning curve** - team must understand config format

### Use Cases

**Ideal for:**
- Production systems requiring reliability and performance
- Teams needing clear documentation
- Projects with multiple resources
- Systems requiring configuration validation
- Long-term maintainability

---

## Comparison Matrix

| Criteria | Approach 1: Direct Mapping | Approach 2: FHIRPath | Approach 3: SearchParameter | Approach 4: Convention | Approach 5: Hybrid (RECOMMENDED) |
|----------|---------------------------|----------------------|----------------------------|------------------------|----------------------------------|
| **Simplicity** | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Flexibility** | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Performance** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Maintainability** | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Standards Compliance** | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Learning Curve** | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Config Verbosity** | ⭐⭐⭐ | ⭐⭐ | ⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| **Validation** | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Debugging** | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Time to Implement** | Fast | Medium | Slow | Fast | Medium |
| **Best For** | Simple projects | Complex extractions | Enterprise FHIR | Rapid development | Production systems |

---

## Recommendation

### **Approach 5: Hybrid Mapping Configuration** is RECOMMENDED

**Rationale:**

1. **Balances all concerns**: Explicit enough to be clear, flexible enough to handle complexity
2. **Performance-first**: Directly targets `_search` fields, no runtime overhead
3. **Production-ready**: Validation, error handling, caching built-in
4. **Maintainable**: Clear configuration format, easy to extend
5. **Team-friendly**: Low learning curve, good documentation in config
6. **Proven pattern**: Similar to successful projects (Elasticsearch field mappings, API routing configs)

### Implementation Roadmap

**Phase 1: Core Infrastructure (Week 1-2)**
- [ ] Set up project structure
- [ ] Implement mapping loader with caching
- [ ] Create base converter classes
- [ ] Add URL parser
- [ ] Write comprehensive tests

**Phase 2: Basic Converters (Week 2-3)**
- [ ] Implement string converter
- [ ] Implement token converter (simple and CodeableConcept)
- [ ] Implement reference converter
- [ ] Implement date converter
- [ ] Create first resource configs (Patient, Observation)

**Phase 3: Advanced Features (Week 3-4)**
- [ ] Add modifier support
- [ ] Add prefix support
- [ ] Implement query optimization
- [ ] Add validation
- [ ] Create more resource configs (Appointment, Schedule, Slot)

**Phase 4: Polish & Documentation (Week 4-5)**
- [ ] Comprehensive testing
- [ ] Performance benchmarks
- [ ] Documentation
- [ ] Examples and tutorials
- [ ] Error handling improvements

**Phase 5: Production Readiness (Week 5-6)**
- [ ] Load testing
- [ ] Security review
- [ ] Monitoring/logging
- [ ] Deployment guide

### Success Criteria

- ✅ Convert 95%+ of common FHIR search queries correctly
- ✅ Query performance <20ms for typical searches (1M records)
- ✅ Support at least 10 major FHIR resources
- ✅ Configuration validation catches errors before runtime
- ✅ Comprehensive test coverage (>90%)
- ✅ Clear documentation for adding new resources

---

**Document Version:** 1.0  
**Last Updated:** May 13, 2026  
**Next Review:** June 2026
