---
sidebar_position: 6
---

# kehrnel-ingest

Bulk ingest flattened documents into MongoDB.

## Synopsis

```bash
kehrnel-ingest <COMMAND> [OPTIONS]
```

## Commands

### file

Ingest from an NDJSON file containing flattened documents.

```bash
kehrnel-ingest file <JSONL> [OPTIONS]
```

#### Arguments

| Argument | Description |
|----------|-------------|
| `JSONL` | Path to NDJSON file (one flattened doc per line) |

#### Options

| Option | Description |
|--------|-------------|
| `-d PATH` | **Required.** YAML/JSON driver configuration file |
| `-c PATH` | Optional transform configuration |
| `--workers N` | Parallel insert workers (default: 4) |

### mongo-catchup

Migrate canonical compositions from one MongoDB collection to another (flatten and insert).

```bash
kehrnel-ingest mongo-catchup [OPTIONS]
```

#### Options

| Option | Description |
|--------|-------------|
| `--src-cfg PATH` | **Required.** Source MongoDB config JSON |
| `--driver-cfg PATH` | **Required.** Target driver YAML/JSON |
| `--limit N` | Limit number of patients to process |

## Driver Configuration

The driver configuration specifies target MongoDB settings:

```yaml
# driver.yaml
mongodb:
  uri: "mongodb://localhost:27017"
  database: "kehrnel_db"

collections:
  compositions: "compositions_rps"
  search: "compositions_search"

options:
  batch_size: 1000
  ordered: false
```

## Examples

### Ingest from NDJSON File

```bash
# Prepare flattened documents
kehrnel-transform flatten comp1.json >> batch.jsonl
kehrnel-transform flatten comp2.json >> batch.jsonl

# Ingest to MongoDB
kehrnel-ingest file batch.jsonl -d driver.yaml
```

### Parallel Ingestion

```bash
kehrnel-ingest file large_batch.jsonl -d driver.yaml --workers 8
```

### Migrate Between Collections

```bash
# Source config
cat > source.json << 'EOF'
{
  "uri": "mongodb://source-host:27017",
  "database": "old_cdr",
  "collection": "compositions"
}
EOF

# Run migration
kehrnel-ingest mongo-catchup \
  --src-cfg source.json \
  --driver-cfg driver.yaml \
  --limit 1000
```

## NDJSON Format

Each line in the NDJSON file should be a flattened composition document:

```jsonl
{"_id":"comp-001","ehr_id":"patient-001","tid":1,"n":{"13.12.11":{"v":{"m":120,"u":"mm[Hg]"}}}}
{"_id":"comp-002","ehr_id":"patient-001","tid":1,"n":{"13.12.11":{"v":{"m":125,"u":"mm[Hg]"}}}}
{"_id":"comp-003","ehr_id":"patient-002","tid":2,"n":{"13.12.11":{"v":{"m":118,"u":"mm[Hg]"}}}}
```

## Performance Tips

1. **Use multiple workers**: The `--workers` option enables parallel inserts
2. **Batch your data**: Process in reasonable chunks (10k-100k docs)
3. **Pre-flatten**: Transform compositions before ingestion for better throughput
4. **Unordered writes**: The driver uses unordered inserts for better performance

## Related Commands

- [kehrnel-transform](/docs/cli/transform) - Transform compositions to flattened format
- [kehrnel-api](/docs/cli/api-server) - API-based ingestion endpoints
