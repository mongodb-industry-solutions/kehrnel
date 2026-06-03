# Phase 6 Quick Reference: Query Builder

Quick reference for developers using Phase 6 builder components.

---

## 🚀 Quick Start

### Basic Query Building

```python
from fhir_search_to_mql.builder import MQLBuilder

# Initialize builder
builder = MQLBuilder(resource_type="Patient", config=patient_config)

# Build query
queries = [{"name": "John"}, {"age": 30}]
final_query = builder.build(queries, logic="AND")
```

---

## 📦 Components

### MQLBuilder

Main orchestrator for query building.

```python
from fhir_search_to_mql.builder import MQLBuilder

builder = MQLBuilder(
    resource_type="Patient",
    config=patient_config,
    enable_optimization=True,   # Auto-optimize queries
    enable_validation=True       # Auto-validate parameters
)
```

**Key Methods**:

| Method | Purpose | Returns |
|--------|---------|---------|
| `build(queries, logic, parameter_names, dry_run)` | Build final query | Dict (MongoDB query) |
| `build_with_metadata(queries, logic, parameter_names)` | Build with metadata | Tuple[Dict, QueryMetadata] |
| `explain(queries, logic, parameter_names)` | Get explanation | Dict (explanation) |
| `optimize(query)` | Optimize existing query | Dict |
| `validate(query)` | Validate query | ValidationResult |
| `get_index_recommendations(query, format)` | Get index recommendations | str |
| `add_compartment_filter(query, compartment)` | Add compartment filter | Dict |

**Examples**:

```python
# Build simple query
query = builder.build([{"name": "John"}])

# Build with metadata
query, metadata = builder.build_with_metadata(
    [{"name": "John"}, {"age": 30}],
    parameter_names=["name", "age"]
)

# Explain query
explanation = builder.explain([{"name": "John"}], parameter_names=["name"])

# Get recommendations
recommendations = builder.get_index_recommendations(query)
print(recommendations)
```

---

### LogicCombiner

Combine query fragments with intelligent logic.

```python
from fhir_search_to_mql.builder import LogicCombiner

combiner = LogicCombiner()
```

**Methods**:

```python
# AND combination (smart merging)
result = combiner.combine_and([
    {"name": "John"},
    {"age": 30}
])
# → {"name": "John", "age": 30}

# OR combination
result = combiner.combine_or([
    {"name": "John"},
    {"name": "Jane"}
])
# → {"$or": [{"name": "John"}, {"name": "Jane"}]}

# NOR combination
result = combiner.combine_nor([
    {"status": "inactive"},
    {"deleted": True}
])
# → {"$nor": [{"status": "inactive"}, {"deleted": True}]}

# Combine same parameter (uses OR)
result = combiner.combine_same_parameter([
    {"name": "John"},
    {"name": "Jane"}
], "name")
# → {"$or": [{"name": "John"}, {"name": "Jane"}]}

# Combine different parameters (uses AND)
result = combiner.combine_different_parameters([
    {"name": "John"},
    {"age": 30}
])
# → {"name": "John", "age": 30}
```

---

### QueryOptimizer

Optimize query structure for performance.

```python
from fhir_search_to_mql.builder import QueryOptimizer

optimizer = QueryOptimizer()
```

**Methods**:

```python
# Optimize query
query = {"$and": [{"$and": [{"name": "John"}]}, {"age": 30}]}
optimized = optimizer.optimize(query)
# → {"name": "John", "age": 30}

# Estimate complexity
metrics = optimizer.estimate_complexity(optimized)
# →
{
    'num_conditions': 2,
    'max_depth': 0,
    'num_logical_ops': 0,
    'num_fields': 2,
    'has_or': False,
    'has_regex': False,
    'performance': 'fast'  # fast, medium, slow
}
```

**Optimizations Applied**:
1. Flatten nested operators
2. Simplify single-element operators
3. Remove redundant conditions
4. Merge adjacent conditions

**Performance Categories**:
- **fast**: Simple queries, low complexity
- **medium**: Moderate complexity, some OR/ranges
- **slow**: High complexity, regex, many conditions

---

### QueryValidator

Validate parameters and queries.

```python
from fhir_search_to_mql.builder import QueryValidator, ValidationResult

validator = QueryValidator(config=patient_config)
```

**Methods**:

```python
# Validate parameter
result = validator.validate_parameter(
    parameter_name="name",
    value="John",
    modifier=None,          # Optional: :exact, :contains, etc.
    prefix=None,            # Optional: gt, lt, ge, le, etc.
    resource_type="Patient"
)

# Check result
if result.is_valid:
    print("Valid!")
else:
    for error in result.errors:
        print(f"Error: {error}")

for warning in result.warnings:
    print(f"Warning: {warning}")

# Validate query
result = validator.validate_query(query, resource_type="Patient")
```

**ValidationResult**:

```python
@dataclass
class ValidationResult:
    is_valid: bool           # True if no errors
    errors: List[str]        # Blocking errors
    warnings: List[str]      # Non-blocking warnings
```

**Common Errors**:
- Parameter not defined
- Invalid modifier for type
- Invalid prefix for type
- Invalid value format
- Invalid reference type

**Common Warnings**:
- No index found (slow query)
- Complex query (many conditions)
- Deep nesting
- Regex usage

---

### IndexRecommender

Analyze queries and recommend indexes.

```python
from fhir_search_to_mql.builder import IndexRecommender, IndexPriority

recommender = IndexRecommender(
    resource_type="Patient",
    config=patient_config  # Optional
)
```

**Methods**:

```python
# Analyze query
query = {"name": {"$regex": "^John"}, "age": {"$gte": 18}}
recommendations = recommender.analyze(query)

# Format recommendations
formatted = recommender.format_recommendations(
    recommendations,
    format="text"  # or "markdown"
)
print(formatted)
```

**IndexRecommendation**:

```python
@dataclass
class IndexRecommendation:
    fields: List[str]         # Fields to index
    priority: IndexPriority   # CRITICAL, HIGH, MEDIUM, LOW
    reason: str               # Why this index is recommended
    command: str              # MongoDB command
    index_type: str           # single, compound, text
```

**Priority Levels**:

| Priority | When | Example |
|----------|------|---------|
| **CRITICAL** | Full collection scan | Regex, text search without index |
| **HIGH** | Significant improvement | Range queries, ID fields |
| **MEDIUM** | Moderate improvement | OR queries, equality |
| **LOW** | Minor improvement | Secondary optimizations |

**Index Types**:

```python
# Single-field index
"db.Patient.createIndex({\"name\": 1})"

# Compound index
"db.Patient.createIndex({\"name\": 1, \"age\": 1})"

# Text index (for regex/full-text)
"db.Patient.createIndex({\"name\": \"text\"})"
"db.Patient.createIndex({\"$**\": \"text\"})"  # All fields
```

---

## 🎯 Common Use Cases

### 1. Simple Query Building

```python
builder = MQLBuilder("Patient")

# Single parameter
query = builder.build([{"name": "John"}])
# → {"name": "John"}

# Multiple parameters (AND)
query = builder.build([
    {"name": "John"},
    {"age": 30}
], logic="AND")
# → {"name": "John", "age": 30}

# Multiple values (OR)
query = builder.build([
    {"name": "John"},
    {"name": "Jane"}
], logic="OR")
# → {"$or": [{"name": "John"}, {"name": "Jane"}]}
```

### 2. Query with Metadata

```python
query, metadata = builder.build_with_metadata(
    [{"name": "John"}, {"age": {"$gte": 18}}],
    parameter_names=["name", "age"]
)

# Access metadata
print(f"Parameters: {metadata.parsed_parameters}")
print(f"Conditions: {metadata.num_conditions}")
print(f"Performance: {metadata.performance_estimate}")
print(f"Build time: {metadata.build_time_ms}ms")

# Check warnings
for warning in metadata.warnings:
    print(f"⚠️ {warning}")

# Get index hints
for hint in metadata.index_hints:
    print(f"💡 {hint}")
```

### 3. Query Explanation (Dry-Run)

```python
explanation = builder.explain(
    [{"name": {"$regex": "^John"}}, {"age": {"$gte": 18}}],
    parameter_names=["name", "age"]
)

# Show explanation
print(f"Resource: {explanation['resource_type']}")
print(f"Final Query: {explanation['final_query']}")
print(f"Performance: {explanation['performance_estimate']}")

# Show recommendations
for rec in explanation['index_recommendations']:
    print(f"\n[{rec['priority'].upper()}] {rec['type']} index")
    print(f"  Fields: {', '.join(rec['fields'])}")
    print(f"  Reason: {rec['reason']}")
    print(f"  Command: {rec['command']}")
```

### 4. Compartment Filter

```python
# Patient compartment for Observation
query = {"code": "8480-6"}  # Blood pressure
compartment = {"_search.patientId": "patient-123"}

# Combine
final_query = builder.add_compartment_filter(query, compartment)
# → {"code": "8480-6", "_search.patientId": "patient-123"}
```

### 5. Manual Optimization

```python
# Complex unoptimized query
query = {
    "$and": [
        {"$and": [{"name": "John"}]},
        {"$and": [{"age": 30}]}
    ]
}

# Optimize
optimized = builder.optimize(query)
# → {"name": "John", "age": 30}
```

### 6. Parameter Validation

```python
validator = QueryValidator(config)

# Valid parameter
result = validator.validate_parameter("name", "John")
assert result.is_valid

# Invalid modifier
result = validator.validate_parameter("birthDate", "2024-01-01", modifier="exact")
assert not result.is_valid
print(result.errors)  # ["Modifier ':exact' not allowed for parameter type 'date'"]

# Invalid date format
result = validator.validate_parameter("birthDate", "invalid")
assert not result.is_valid
print(result.errors)  # ["Invalid date format: ..."]
```

### 7. Index Analysis

```python
recommender = IndexRecommender("Patient")

# Analyze query
query = {
    "$and": [
        {"name": {"$regex": "^John"}},
        {"age": {"$gte": 18}},
        {"status": "active"}
    ]
}

recommendations = recommender.analyze(query)

# Show recommendations
for rec in recommendations:
    print(f"[{rec.priority.value.upper()}] {rec.index_type} index")
    print(f"  Fields: {rec.fields}")
    print(f"  Reason: {rec.reason}")
    print(f"  Command: {rec.command}\n")

# Format as text
print(recommender.format_recommendations(recommendations, "text"))
```

---

## 🎓 Best Practices

### Query Building

1. **Enable optimization** for production
   ```python
   builder = MQLBuilder(resource_type, config, enable_optimization=True)
   ```

2. **Always validate** user input
   ```python
   result = validator.validate_parameter(param, value)
   if not result.is_valid:
       raise ValueError(result.errors[0])
   ```

3. **Use explain()** to understand queries
   ```python
   explanation = builder.explain(queries, parameter_names=params)
   ```

4. **Check metadata warnings**
   ```python
   query, metadata = builder.build_with_metadata(queries)
   if metadata.warnings:
       log_warnings(metadata.warnings)
   ```

### Performance

1. **Monitor complexity**
   ```python
   if metadata.complexity['num_conditions'] > 10:
       logger.warning("Complex query may be slow")
   ```

2. **Implement recommended indexes**
   ```python
   recommendations = recommender.analyze(query)
   critical = [r for r in recommendations if r.priority == IndexPriority.CRITICAL]
   for rec in critical:
       execute_mongo_command(rec.command)
   ```

3. **Optimize before execution**
   ```python
   query = builder.optimize(raw_query)
   results = collection.find(query)
   ```

### Error Handling

```python
try:
    # Build query
    query = builder.build(queries)
    
    # Validate if not auto-enabled
    if not builder.enable_validation:
        result = builder.validate(query)
        if not result.is_valid:
            raise ConversionError(f"Invalid query: {result.errors}")
    
    # Execute
    results = collection.find(query)
    
except ConversionError as e:
    logger.error(f"Query building failed: {e}")
    # Handle error appropriately
```

---

## 📊 Performance Tips

### Query Optimization

| Before | After | Improvement |
|--------|-------|-------------|
| `{"$and": [{"a": 1}]}` | `{"a": 1}` | Simplified |
| `{"$and": [{"a": 1}, {"b": 2}]}` | `{"a": 1, "b": 2}` | Merged |
| `{"$and": [{"$and": [{"a": 1}]}]}` | `{"a": 1}` | Flattened |
| `{"$and": [{"a": 1}, {"a": 1}]}` | `{"a": 1}` | Deduplicated |

### Index Priorities

1. **CRITICAL** (implement immediately)
   - Regex without index
   - Text search without index
   - Full collection scans

2. **HIGH** (implement soon)
   - Range queries
   - ID fields
   - Frequently queried fields

3. **MEDIUM** (consider implementing)
   - OR queries
   - Equality queries
   - Secondary fields

4. **LOW** (optional)
   - Minor optimizations
   - Rarely used fields

### Complexity Guidelines

| Metric | Fast | Medium | Slow |
|--------|------|--------|------|
| Conditions | < 5 | 5-10 | > 10 |
| Depth | 0-1 | 2-3 | > 3 |
| Logical ops | 0-2 | 3-5 | > 5 |
| Has OR | No | Yes | Many |
| Has regex | No | No | Yes |

---

## 🔧 Configuration

### Resource Configuration

```yaml
parameters:
  name:
    type: string
    fields:
      - field: name
        indexed: true  # Tells recommender index exists
  
  birthDate:
    type: date
    fields:
      - field: birthDate
        indexed: false  # Will trigger warning
  
  identifier:
    type: token
    fields:
      - field: identifier.value
        indexed: true
```

### Builder Flags

```python
builder = MQLBuilder(
    resource_type="Patient",
    config=patient_config,
    enable_optimization=True,   # Auto-optimize (recommended)
    enable_validation=True       # Auto-validate (recommended)
)
```

---

## 📖 API Reference

### MQLBuilder

```python
class MQLBuilder:
    def __init__(
        resource_type: str,
        config: Optional[Dict] = None,
        enable_optimization: bool = True,
        enable_validation: bool = True
    )
    
    def build(
        queries: List[Dict],
        logic: str = "AND",
        parameter_names: Optional[List[str]] = None,
        dry_run: bool = False
    ) -> Dict
    
    def build_with_metadata(
        queries: List[Dict],
        logic: str = "AND",
        parameter_names: Optional[List[str]] = None
    ) -> Tuple[Dict, QueryMetadata]
    
    def explain(
        queries: List[Dict],
        logic: str = "AND",
        parameter_names: Optional[List[str]] = None
    ) -> Dict
    
    def add_compartment_filter(
        query: Dict,
        compartment_query: Dict
    ) -> Dict
    
    def optimize(query: Dict) -> Dict
    
    def validate(query: Dict) -> ValidationResult
    
    def get_index_recommendations(
        query: Dict,
        format: str = "text"  # or "markdown"
    ) -> str
```

### LogicCombiner

```python
class LogicCombiner:
    def combine_and(queries: List[Dict]) -> Dict
    def combine_or(queries: List[Dict]) -> Dict
    def combine_nor(queries: List[Dict]) -> Dict
    def combine_same_parameter(queries: List[Dict], param: str) -> Dict
    def combine_different_parameters(queries: List[Dict]) -> Dict
```

### QueryOptimizer

```python
class QueryOptimizer:
    def optimize(query: Dict) -> Dict
    def estimate_complexity(query: Dict) -> Dict
```

### QueryValidator

```python
class QueryValidator:
    def __init__(config: Dict)
    
    def validate_parameter(
        parameter_name: str,
        value: str,
        modifier: Optional[str] = None,
        prefix: Optional[str] = None,
        resource_type: Optional[str] = None
    ) -> ValidationResult
    
    def validate_query(
        query: Dict,
        resource_type: Optional[str] = None
    ) -> ValidationResult
```

### IndexRecommender

```python
class IndexRecommender:
    def __init__(
        resource_type: str,
        config: Optional[Dict] = None
    )
    
    def analyze(query: Dict) -> List[IndexRecommendation]
    
    def format_recommendations(
        recommendations: List[IndexRecommendation],
        format: str = "text"  # or "markdown"
    ) -> str
```

---

## 🎯 Quick Commands

```python
# Basic building
builder = MQLBuilder("Patient")
query = builder.build([{"name": "John"}])

# With metadata
query, meta = builder.build_with_metadata([{"name": "John"}], ["name"])

# Explain
explanation = builder.explain([{"name": "John"}], ["name"])

# Optimize
optimized = builder.optimize({"$and": [{"name": "John"}]})

# Validate
result = validator.validate_parameter("name", "John")

# Recommend indexes
recs = recommender.analyze(query)
print(recommender.format_recommendations(recs))
```

---

**Phase 6 Complete** ✅  
All builder components ready for use!
