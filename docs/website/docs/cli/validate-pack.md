---
sidebar_position: 9
---

# kehrnel-validate-pack

Validate strategy pack structure and configuration.

## Synopsis

```bash
kehrnel-validate-pack <PATH> [OPTIONS]
```

## Description

Validates that a strategy pack directory contains all required files and follows the expected structure. Checks include:

- `manifest.json` presence and validity
- Required directories and files
- Schema compliance
- Asset references
- Configuration consistency

## Arguments

| Argument | Description |
|----------|-------------|
| `PATH` | Path to strategy pack directory or `manifest.json` file |

## Options

| Option | Description |
|--------|-------------|
| `--json` | Output results as JSON |

## Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Pack is valid |
| `1` | Validation errors found |
| `2` | manifest.json not found or invalid |

## Examples

### Validate a Strategy Pack

```bash
kehrnel-validate-pack ./strategies/openehr/rps_dual/
```

Output on success:
```
✓ Pack valid (openehr.rps_dual)
```

### Validate with JSON Output

```bash
kehrnel-validate-pack ./my_strategy/ --json
```

```json
{
  "valid": true,
  "errors": []
}
```

### Validate by Manifest Path

```bash
kehrnel-validate-pack ./my_strategy/manifest.json
```

## Pack Structure

A valid strategy pack must have this structure:

```
my_strategy/
├── manifest.json        # Required: Pack metadata
├── schema.json          # Optional: Configuration schema
├── defaults.json        # Optional: Default configuration
├── ingest/
│   ├── config/
│   │   ├── flattener_mappings.jsonc
│   │   └── mappings.yaml
│   └── single.py        # Ingestion logic
├── query/
│   └── ...              # Query handlers
└── assets/
    └── ...              # Static assets (docs, images)
```

## Manifest Schema

The `manifest.json` must include:

```json
{
  "id": "domain.strategy_name",
  "name": "Human Readable Name",
  "version": "1.0.0",
  "domain": "openehr",
  "description": "Strategy description",
  "capabilities": {
    "ingest": true,
    "query": true,
    "synthetic": false
  },
  "links": {
    "docs": "/api/strategies/domain.strategy_name/assets/docs.pdf",
    "spec": "/api/strategies/domain.strategy_name/spec"
  },
  "config": {
    "schema": "schema.json",
    "defaults": "defaults.json"
  }
}
```

## Common Validation Errors

| Error | Cause |
|-------|-------|
| `manifest.json not found` | Missing manifest file |
| `Invalid manifest JSON` | Malformed JSON syntax |
| `Missing required field: id` | Required field not present |
| `Invalid domain: xyz` | Domain not recognized |
| `Referenced file not found: schema.json` | File referenced in manifest doesn't exist |
| `Invalid schema reference` | Schema file has invalid JSON Schema |

## CI/CD Integration

```bash
# Validate all packs in a directory
for pack in ./strategies/*/; do
  echo "Validating $pack..."
  kehrnel-validate-pack "$pack" --json >> validation_results.json
done

# Fail CI if any pack is invalid
kehrnel-validate-pack ./my_strategy/ || exit 1
```

## Related

- [Strategy Overview](/docs/strategies/overview) - Understanding strategy packs
- [kehrnel-validate-bundle](/docs/cli/bundles) - Validate bundles
- [Configuration](/docs/getting-started/configuration) - Environment setup
