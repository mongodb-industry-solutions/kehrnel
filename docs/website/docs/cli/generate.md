---
sidebar_position: 5
---

# kehrnel-generate

Generate composition skeletons from OPT templates.

## Synopsis

```bash
kehrnel-generate [OPTIONS]
```

## Description

Creates openEHR composition JSON structures from Operational Templates (OPT). Useful for:

- Testing and development
- Creating sample data
- Understanding template structure
- Generating base compositions for mapping workflows

## Options

| Option | Description |
|--------|-------------|
| `-t PATH` | **Required.** Path to OPT template file |
| `-o, --output PATH` | Output file path (default: stdout) |
| `-r, --random` | Fill leaves with random demo values |

## Modes

### Minimal Mode (Default)

Generates a skeleton with only required fields and minimal valid values:

```bash
kehrnel-generate -t template.opt -o minimal.json
```

### Random Mode

Generates a composition with realistic random values for all fields:

```bash
kehrnel-generate -t template.opt -o random.json --random
```

## Examples

### Generate Minimal Skeleton

```bash
kehrnel-generate -t vital_signs.opt -o skeleton.json
```

### Generate with Random Data

```bash
kehrnel-generate -t vital_signs.opt --random
```

### Pipeline to jq for Inspection

```bash
kehrnel-generate -t template.opt | jq '.content[0]'
```

### Generate Multiple Samples

```bash
for i in {1..10}; do
  kehrnel-generate -t template.opt --random -o "sample_${i}.json"
done
```

## Output Format

The generated composition follows the openEHR canonical JSON format:

```json
{
  "_type": "COMPOSITION",
  "name": { "value": "Vital Signs" },
  "archetype_details": {
    "archetype_id": { "value": "openEHR-EHR-COMPOSITION.encounter.v1" },
    "template_id": { "value": "vital_signs.v1" },
    "rm_version": "1.0.4"
  },
  "language": {
    "_type": "CODE_PHRASE",
    "terminology_id": { "value": "ISO_639-1" },
    "code_string": "en"
  },
  "territory": {
    "_type": "CODE_PHRASE",
    "terminology_id": { "value": "ISO_3166-1" },
    "code_string": "US"
  },
  "category": {
    "_type": "DV_CODED_TEXT",
    "value": "event",
    "defining_code": {
      "terminology_id": { "value": "openehr" },
      "code_string": "433"
    }
  },
  "composer": {
    "_type": "PARTY_IDENTIFIED",
    "name": "Kehrnel Generator"
  },
  "content": [
    // Template-specific content...
  ]
}
```

## Validation Feedback

The generator automatically validates the output and reports any issues:

```
⚠  3 validation issues
```

This helps identify templates that may have constraints the generator doesn't fully satisfy.

## Use Cases

### Development Testing

Generate test data for API development:

```bash
# Create test composition
kehrnel-generate -t my_template.opt --random -o test_data.json

# Ingest via API
curl -X POST "http://localhost:8000/api/domains/openehr/ehr/test-001/composition" \
  -H "Content-Type: application/json" \
  -d @test_data.json
```

### Template Exploration

Understand the structure of an unfamiliar template:

```bash
kehrnel-generate -t complex_template.opt | jq 'keys'
kehrnel-generate -t complex_template.opt | jq '.content[].name.value'
```

### Mapping Workflow

Generate a base composition for mapping source data:

```bash
# Step 1: Generate skeleton
kehrnel-generate -t template.opt -o base.json

# Step 2: Use kehrnel-map to fill from source data
kehrnel-map -s source.csv -t webtemplate.json -p template.opt -o output/
```

## Related Commands

- [kehrnel-validate](/docs/cli/validate) - Validate generated compositions
- [kehrnel-skeleton](/docs/cli/mapping) - Generate mapping skeletons
- [kehrnel-map](/docs/cli/mapping) - Apply mappings to create compositions
