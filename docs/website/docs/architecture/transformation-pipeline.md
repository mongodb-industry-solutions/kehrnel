---
sidebar_position: 2
---

# Transformation Pipeline

The transformation pipeline converts canonical openEHR compositions into the optimized storage format.

## Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                   Canonical COMPOSITION                         │
│  { "_type": "COMPOSITION", "content": [...], ... }              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    1. Input Validation                          │
│  • Check _type = "COMPOSITION"                                  │
│  • Validate required fields                                     │
│  • Extract metadata (template_id, archetype_id)                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    2. Tree Walker                               │
│  • Traverse composition tree                                    │
│  • Track path context                                           │
│  • Identify data value nodes                                    │
│  • Record archetype constraints                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    3. Path Encoder                              │
│  • Reverse path segments                                        │
│  • Encode AT codes (at0004 → -4)                                │
│  • Map archetype IDs to integers                                │
│  • Build encoded path string                                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    4. Value Extractor                           │
│  • Extract data value properties                                │
│  • Compact field names (magnitude → m)                          │
│  • Handle nested structures                                     │
│  • Preserve type information                                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    5. Document Builder                          │
│  • Create base document (full data)                             │
│  • Create search document (slim projection)                     │
│  • Add metadata fields                                          │
│  • Assign version info                                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
    ┌─────────────────────┐   ┌─────────────────────┐
    │   Base Document     │   │   Search Document   │
    │  (compositions_rps) │   │(compositions_search)│
    └─────────────────────┘   └─────────────────────┘
```

## Stage Details

### 1. Input Validation

```python
def validate_input(composition: dict) -> None:
    if composition.get("_type") != "COMPOSITION":
        raise ValidationError("Expected COMPOSITION type")

    # Extract template context
    archetype_details = composition.get("archetype_details", {})
    template_id = archetype_details.get("template_id", {}).get("value")
    archetype_id = archetype_details.get("archetype_id", {}).get("value")

    # Validate required fields
    if not composition.get("name"):
        raise ValidationError("Missing name")
```

### 2. Tree Walker

The walker traverses the composition tree, maintaining path context:

```python
def walk_tree(node, path_context):
    if is_data_value(node):
        yield (path_context, node)
        return

    for key, value in node.items():
        if key.startswith("_"):
            continue

        if isinstance(value, list):
            for i, item in enumerate(value):
                child_path = extend_path(path_context, key, i)
                yield from walk_tree(item, child_path)
        elif isinstance(value, dict):
            child_path = extend_path(path_context, key)
            yield from walk_tree(value, child_path)
```

### 3. Path Encoder

Transforms hierarchical paths to encoded strings:

```python
def encode_path(path_segments):
    # Reverse order
    reversed_segments = path_segments[::-1]

    # Encode each segment
    encoded = []
    for segment in reversed_segments:
        if segment.type == "at_code":
            # at0004 → -4
            encoded.append(-int(segment.code[2:]))
        elif segment.type == "archetype":
            # Lookup or assign integer code
            encoded.append(get_archetype_code(segment.id))
        else:
            # Structural name → fixed code
            encoded.append(SEGMENT_CODES[segment.name])

    return ".".join(str(e) for e in encoded)
```

### 4. Value Extractor

Compacts data values:

```python
DATA_TYPE_MAPPINGS = {
    "DV_QUANTITY": {
        "magnitude": "m",
        "units": "u",
        "precision": "p"
    },
    "DV_CODED_TEXT": {
        "value": "val",
        "defining_code": "dc"
    },
    "DV_DATE_TIME": {
        "value": "val"
    }
}

def extract_value(data_value):
    dtype = data_value.get("_type")
    mapping = DATA_TYPE_MAPPINGS.get(dtype, {})

    result = {}
    for source_key, target_key in mapping.items():
        if source_key in data_value:
            result[target_key] = data_value[source_key]

    return result
```

### 5. Document Builder

Assembles final documents:

```python
def build_documents(ehr_id, composition_id, version, commit_time, template_id, nodes, slim_nodes):
    base_doc = {
        "_id": composition_id,
        "ehr_id": ehr_id,
        "comp_id": composition_id,
        "v": version,
        "time_c": commit_time,
        "tid": template_id,
        "cn": list(nodes)
    }

    search_doc = None
    if slim_nodes:
        search_doc = {
            "_id": composition_id,
            "ehr_id": ehr_id,
            "comp_id": composition_id,
            "v": version,
            "time_c": commit_time,
            "sort_time": commit_time,
            "tid": template_id,
            "sn": list(slim_nodes)
        }

    return base_doc, search_doc
```

## Configuration

### Flattener Mappings

```jsonc
// flattener_mappings_f.jsonc
{
  "structural_codes": {
    "content": 15,
    "data": 13,
    "events": 12,
    "items": 11,
    "value": 10,
    "protocol": 9,
    "context": 8
  },
  "data_type_compaction": {
    "DV_QUANTITY": { "magnitude": "m", "units": "u", "precision": "p" },
    "DV_CODED_TEXT": { "value": "val", "defining_code": "dc" },
    "DV_TEXT": { "value": "val" },
    "DV_DATE_TIME": { "value": "val" },
    "DV_BOOLEAN": { "value": "val" },
    "DV_COUNT": { "magnitude": "m" },
    "DV_PROPORTION": { "numerator": "num", "denominator": "den", "type": "t" },
    "DV_ORDINAL": { "value": "val", "symbol": "sym" }
  },
  "search_extraction": {
    "include_types": ["DV_QUANTITY", "DV_CODED_TEXT", "DV_TEXT", "DV_DATE_TIME"],
    "exclude_paths": ["protocol/*", "context/*"]
  }
}
```

## Performance Optimization

### Batch Processing

```python
async def batch_transform(compositions, batch_size=100):
    for batch in chunks(compositions, batch_size):
        results = await asyncio.gather(*[
            transform_composition(c) for c in batch
        ])
        yield results
```

### Code Dictionary Caching

```python
class CodeDictionary:
    def __init__(self, db):
        self.db = db
        self.cache = {}

    async def get_code(self, archetype_id):
        if archetype_id in self.cache:
            return self.cache[archetype_id]

        code = await self.db._codes.find_one({"_id": f"arcode:{archetype_id}"})
        if not code:
            code = await self.assign_next_code(archetype_id)

        self.cache[archetype_id] = code["code"]
        return code["code"]
```

## Related

- [Query Engine](/docs/architecture/query-engine) - How transformed data is queried
- [Flattening Concepts](/docs/concepts/flattening) - Conceptual overview
- [Data Model](/docs/strategies/openehr/rps-dual/data-model) - Storage format
