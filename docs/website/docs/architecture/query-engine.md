---
sidebar_position: 3
---

# Query Engine

The query engine translates AQL queries into MongoDB aggregation pipelines and routes them to the optimal collection.

## Query Processing Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                        AQL Query                                │
│  "SELECT c/uid/value FROM EHR e CONTAINS COMPOSITION c"         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                       AQL Parser                                │
│  • Lexical analysis                                             │
│  • Syntax validation                                            │
│  • AST construction                                             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    AST (Abstract Syntax Tree)                   │
│  {                                                              │
│    "select": { "columns": [...] },                              │
│    "from": { "expression": "EHR e CONTAINS COMPOSITION c" },    │
│    "contains": { "rmType": "COMPOSITION", "alias": "c" },       │
│    "where": null                                                │
│  }                                                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Path Transformer                             │
│  • Resolve aliases (c → COMPOSITION)                            │
│  • Encode archetype paths                                       │
│  • Map to storage format                                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Strategy Router                              │
│  • Analyze query scope                                          │
│  • Select collection                                            │
│  • Choose pipeline type                                         │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
┌─────────────────────────┐     ┌─────────────────────────┐
│    Match Strategy       │     │    Search Strategy      │
│  (compositions_rps)     │     │  (compositions_search)  │
│  ┌───────────────────┐  │     │  ┌───────────────────┐  │
│  │ $match pipeline   │  │     │  │ $search pipeline  │  │
│  │ B-tree indexed    │  │     │  │ Atlas Search      │  │
│  └───────────────────┘  │     │  └───────────────────┘  │
└─────────────────────────┘     └─────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Query Response                               │
│  { "q": "...", "columns": [...], "rows": [...] }                │
└─────────────────────────────────────────────────────────────────┘
```

## Components

### AQL Parser

The parser uses a grammar-based approach:

```python
class AQLParser:
    def __init__(self, query: str):
        self.query = query
        self.tokens = self.tokenize()

    def parse(self) -> dict:
        return {
            "select": self.parse_select(),
            "from": self.parse_from(),
            "contains": self.parse_contains(),
            "where": self.parse_where(),
            "order_by": self.parse_order_by(),
            "limit": self.parse_limit()
        }
```

### Path Transformer

Converts AQL paths to storage paths:

```python
class PathTransformer:
    def __init__(self, aliases: dict, code_dictionary: CodeDictionary):
        self.aliases = aliases
        self.codes = code_dictionary

    def transform(self, path: str) -> str:
        # Parse path components
        segments = self.parse_aql_path(path)

        # Resolve alias
        alias = segments[0]
        archetype = self.aliases.get(alias)

        # Encode path
        encoded = self.encode_path(segments[1:], archetype)

        return f"n.{encoded}"
```

### Strategy Router

Selects the optimal query strategy:

```python
class StrategyRouter:
    def select_strategy(self, ast: dict, ehr_id: str = None) -> str:
        # Explicit patient scope
        if ehr_id:
            return "match"

        # Check WHERE clause for ehr_id predicate
        if self.has_ehr_id_predicate(ast.get("where")):
            return "match"

        # Cross-patient query
        return "search"

    def has_ehr_id_predicate(self, where_clause) -> bool:
        if not where_clause:
            return False

        return self.find_path_predicate(
            where_clause,
            "e/ehr_id/value"
        )
```

### Pipeline Generator

Builds MongoDB aggregation pipelines:

```python
class PipelineGenerator:
    def generate_match_pipeline(self, ast: dict, ehr_id: str) -> list:
        pipeline = []

        # EHR filter
        if ehr_id:
            pipeline.append({"$match": {"ehr_id": ehr_id}})

        # WHERE predicates
        if ast.get("where"):
            pipeline.append(self.build_match_stage(ast["where"]))

        # ORDER BY
        if ast.get("order_by"):
            pipeline.append(self.build_sort_stage(ast["order_by"]))

        # LIMIT/OFFSET
        if ast.get("limit"):
            if ast.get("offset"):
                pipeline.append({"$skip": ast["offset"]})
            pipeline.append({"$limit": ast["limit"]})

        # SELECT projection
        pipeline.append(self.build_project_stage(ast["select"]))

        return pipeline

    def generate_search_pipeline(self, ast: dict) -> list:
        pipeline = []

        # Build $search stage
        search_query = self.build_search_query(ast.get("where"))
        pipeline.append({
            "$search": {
                "index": "search_nodes_index",
                **search_query
            }
        })

        # Same as match for rest
        # ... ORDER BY, LIMIT, PROJECT

        return pipeline
```

## Query Examples

### Patient-Scoped Query

**AQL:**
```aql
SELECT c/uid/value, c/name/value
FROM EHR e
CONTAINS COMPOSITION c
WHERE e/ehr_id/value = 'patient-001'
```

**Pipeline:**
```javascript
[
  { "$match": { "ehr_id": "patient-001" } },
  {
    "$project": {
      "uid": "$_id",
      "name": "$n.name.v.val"
    }
  }
]
```

### Cross-Patient Query

**AQL:**
```aql
SELECT e/ehr_id/value, o/data/.../magnitude
FROM EHR e
CONTAINS OBSERVATION o[openEHR-EHR-OBSERVATION.blood_pressure.v2]
WHERE o/data/events/data/items[at0004]/value/magnitude > 140
ORDER BY o/data/.../magnitude DESC
LIMIT 100
```

**Pipeline:**
```javascript
[
  {
    "$search": {
      "index": "search_nodes_index",
      "embeddedDocument": {
        "path": "sn",
        "operator": {
          "compound": {
            "must": [
              { "text": { "path": "sn.p", "query": "11.-4*" } },
              { "range": { "path": "sn.data.m", "gt": 140 } }
            ]
          }
        }
      }
    }
  },
  { "$sort": { "n.11.-4.13.-3.12.-6.13.-1.15.42.v.m": -1 } },
  { "$limit": 100 },
  {
    "$project": {
      "ehr_id": 1,
      "magnitude": "$n.11.-4.13.-3.12.-6.13.-1.15.42.v.m"
    }
  }
]
```

## Performance Characteristics

### Match Strategy (Patient-Scoped)

| Metric | Value |
|--------|-------|
| Index | B-tree on ehr_id |
| Lookup | O(log n) |
| Scan | O(compositions per patient) |
| Typical latency | 5ms |

### Search Strategy (Cross-Patient)

| Metric | Value |
|--------|-------|
| Index | Atlas Search |
| Lookup | O(log n) |
| Full-text | Inverted index |
| Typical latency | 13ms |

## Caching

### Query Plan Cache

```python
class QueryPlanCache:
    def __init__(self, max_size=1000):
        self.cache = LRUCache(max_size)

    def get_plan(self, query_hash: str) -> Optional[list]:
        return self.cache.get(query_hash)

    def store_plan(self, query_hash: str, pipeline: list):
        self.cache[query_hash] = pipeline
```

### Code Dictionary Cache

In-memory cache for archetype code lookups reduces database roundtrips.

## Related

- [AQL to MQL](/docs/concepts/aql-to-mql) - Translation details
- [Dual Collection](/docs/concepts/dual-collection) - Collection architecture
- [Query Translation](/docs/strategies/openehr/rps-dual/query-translation) - Strategy specifics
