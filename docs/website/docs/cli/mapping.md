---
sidebar_position: 7
---

# Mapping Commands

Transform source data into openEHR compositions using declarative mappings.

## kehrnel-skeleton

Generate a mapping skeleton file from an OPT template.

### Synopsis

```bash
kehrnel-skeleton <TEMPLATE> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `TEMPLATE` | Path to OPT or WebTemplate JSON file |

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-o, --output PATH` | stdout | Output file path |
| `--helpers/--no-helpers` | `false` | Include GUI helper model |
| `--include-header/--no-include-header` | `false` | Include context/protocol/header fields |
| `--macros/--raw` | `true` | Use code/term shortcuts for DV_CODED_TEXT |
| `-l, --log LEVEL` | `simple` | Log level: `quiet`, `simple`, `debug` |

### Example

```bash
kehrnel-skeleton template.opt -o mapping.yaml
```

Generates a YAML skeleton:

```yaml
meta:
  template_id: vital_signs.v1
  version: "1.0"

mappings:
  /content[openEHR-EHR-OBSERVATION.blood_pressure.v2]/data/events[at0006]/data/items[at0004]/value:
    # DV_QUANTITY - systolic blood pressure
    get: ~
    transform: []

  /content[openEHR-EHR-OBSERVATION.blood_pressure.v2]/data/events[at0006]/data/items[at0005]/value:
    # DV_QUANTITY - diastolic blood pressure
    get: ~
    transform: []
```

---

## kehrnel-map

Build canonical openEHR compositions from source data using mappings.

### Synopsis

```bash
kehrnel-map [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `-m, --mapping PATH` | Mapping YAML/JSON file (optional with `--strategy`) |
| `-S, --strategy ID` | Strategy ID to auto-resolve mapping |
| `--strategies-root PATH` | Base directory for strategy packs |
| `-s PATH` | **Required.** Source data file (CSV, XML) |
| `-t PATH` | **Required.** WebTemplate JSON file |
| `-p PATH` | **Required.** OPT template file |
| `-o PATH` | Output path (file or directory, `-` for stdout) |
| `--strict` | Fail on validation errors |

### Mapping Grammar

The mapping file uses a path-keyed grammar:

```yaml
meta:
  template_id: vital_signs.v1
  translation:
    enabled: true
    source_lang: es
    target_lang: en
    cache_file: .kehrnel/translations.json

output:
  prune_empty: true
  composition_key: canonicalJSON
  filename: "{{ first.patient_id }}_{{ first.date }}.json"
  envelope:
    ehr_id:
      get: { column: patient_id }
    composition_version:
      set: 1

mappings:
  /content[openEHR-EHR-OBSERVATION.blood_pressure.v2]/data/events[at0006]/data/items[at0004]/value:
    get: { column: systolic }
    transform: [int]
    # Result: DV_QUANTITY with magnitude from systolic column

  /content[...]/items[at0005]/value:
    expr: "{{ first.diastolic | int }}"

  /content[...]/items[at0033]/value:
    get: { column: position }
    map:
      standing: "at0010"
      sitting: "at0011"
      lying: "at0012"
```

### Supported Source Formats

- **CSV**: Comma-separated values with header row
- **XML**: Structured XML documents

### Examples

#### Basic CSV to openEHR

```bash
kehrnel-map \
  -s patients.csv \
  -t webtemplate.json \
  -p template.opt \
  -m mapping.yaml \
  -o output/
```

#### Using Strategy Pack Mapping

```bash
# Auto-resolve mapping from strategy
kehrnel-map \
  -s data.csv \
  -t webtemplate.json \
  -p template.opt \
  -S openehr.rps_dual \
  -o output/
```

#### Strict Mode with Stdout

```bash
kehrnel-map \
  -s single_record.csv \
  -t webtemplate.json \
  -p template.opt \
  -m mapping.yaml \
  --strict \
  -o -
```

### Transformation Functions

Available in the `transform` array:

| Transform | Description |
|-----------|-------------|
| `int`, `to_int` | Convert to integer |
| `float`, `to_float` | Convert to float |
| `strip` | Remove whitespace |
| `date_iso` | Parse and format as ISO date |
| `datetime_iso` | Parse and format as ISO datetime |

### Value Mapping

Map source values to target values:

```yaml
map:
  "M": "male"
  "F": "female"
  ".*unknown.*": null
```

Range mapping for numeric values:

```yaml
map_ranges:
  "0..17": "pediatric"
  "18..64": "adult"
  "65..150": "geriatric"
```

---

## kehrnel-identify

Identify document types using pattern matching.

### Synopsis

```bash
kehrnel-identify [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `-d, --document PATH` | **Required.** File or directory to identify |
| `-o, --output PATH` | Output file (default: stdout) |
| `--glob PATTERN` | File pattern filter (default: `*`) |
| `--recursive/--no-recursive` | Walk subdirectories (default: yes) |
| `-p, --patterns PATH` | Extra pattern definition files (repeatable) |
| `--no-default` | Skip built-in patterns |
| `--debug` | Trace pattern evaluation |

### Example

```bash
# Identify all files in a directory
kehrnel-identify -d ./incoming/ -o results.json

# With custom patterns
kehrnel-identify -d ./docs/ -p custom_patterns.yaml --debug
```

### Output

```json
[
  {
    "file": "./incoming/lab_results.xml",
    "documentType": "lab_report",
    "confidence": 0.95,
    "matched_patterns": ["xml_root:LabReport", "contains:LOINC"]
  },
  {
    "file": "./incoming/prescription.pdf",
    "documentType": "unknown",
    "confidence": 0.0
  }
]
```

## Related

- [kehrnel-generate](/docs/cli/generate) - Generate base compositions
- [kehrnel-validate](/docs/cli/validate) - Validate generated compositions
