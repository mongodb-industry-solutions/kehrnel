---
sidebar_position: 4
---

# kehrnel-validate

Validate openEHR compositions against OPT templates.

## Synopsis

```bash
kehrnel-validate [OPTIONS]
```

## Description

Validates a composition JSON file against an Operational Template (OPT), checking:

- Required fields presence
- Data type conformance
- Cardinality constraints
- Terminology bindings
- Archetype node IDs

## Options

| Option | Description |
|--------|-------------|
| `-c, --composition PATH` | **Required.** Path to composition JSON file |
| `-t PATH` | **Required.** Path to OPT template file |
| `-v, --verbose` | Show detailed validation information |
| `--json` | Output results as JSON |
| `--no-color` | Disable colored output |
| `--fail-on-warning` | Exit with error code if warnings are found |
| `-s, --stats` | Show validation statistics |

## Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Composition is valid |
| `1` | Validation errors found (or warnings with `--fail-on-warning`) |
| `2` | File not found or parse error |

## Examples

### Basic Validation

```bash
kehrnel-validate -c composition.json -t template.opt
```

Output on success:
```
✓ Composition is valid
```

### Verbose Output

```bash
kehrnel-validate -c composition.json -t template.opt -v
```

Shows detailed information including template ID and validation table.

### JSON Output

```bash
kehrnel-validate -c composition.json -t template.opt --json
```

```json
{
  "valid": false,
  "template_id": "vital_signs.v1",
  "issues": [
    {
      "path": "/content[0]/data/events[0]/data/items[at0004]",
      "message": "Required element missing",
      "severity": "ERROR",
      "code": "REQUIRED_MISSING",
      "expected": "DV_QUANTITY",
      "found": null
    }
  ],
  "summary": {
    "total": 1,
    "errors": 1,
    "warnings": 0,
    "info": 0
  }
}
```

### Show Statistics

```bash
kehrnel-validate -c composition.json -t template.opt -s
```

```
[ERROR] /content[0]/data/...: Required element missing

Validation Summary:
Errors    1
Total     1
```

### CI/CD Integration

```bash
# Fail on any warnings
kehrnel-validate -c composition.json -t template.opt --fail-on-warning

# Machine-readable output
kehrnel-validate -c composition.json -t template.opt --json --no-color > result.json
```

## Issue Severities

| Severity | Description |
|----------|-------------|
| **ERROR** | Composition does not conform to template |
| **WARNING** | Potential issues that may affect interoperability |
| **INFO** | Informational messages about the validation |

## Common Validation Errors

### REQUIRED_MISSING
A required element is not present in the composition.

### TYPE_MISMATCH
The data type doesn't match the template definition.

### CARDINALITY_VIOLATION
Too few or too many occurrences of an element.

### INVALID_TERMINOLOGY
Coded value doesn't match allowed terminology.

## Related Commands

- [kehrnel-generate](/docs/cli/generate) - Generate valid compositions from templates
- [kehrnel-validate-pack](/docs/cli/validate-pack) - Validate strategy packs
