---
sidebar_position: 3
---

# kehrnel-transform

Transform compositions between canonical and flattened formats.

## Synopsis

```bash
kehrnel-transform <COMMAND> [OPTIONS]
```

## Commands

### flatten

Convert a canonical openEHR composition to the flattened RPS format.

```bash
kehrnel-transform flatten <SOURCE> [OPTIONS]
```

#### Arguments

| Argument | Description |
|----------|-------------|
| `SOURCE` | Path to canonical composition JSON file |

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-o, --output PATH` | `-` (stdout) | Output file path, or `-` for stdout |
| `-c PATH` | - | Override config JSON (currently unused) |

#### Output

The command produces a JSON object with two documents:

```json
{
  "base": {
    "_id": "composition-id",
    "ehr_id": "patient-001",
    "tid": 1,
    "n": { ... }
  },
  "search": {
    "_id": "composition-id",
    "ehr_id": "patient-001",
    "tid": 1,
    "sn": [ ... ]
  }
}
```

- **base**: Full flattened document for `compositions_rps` collection
- **search**: Slim projection for `compositions_search` collection

### expand

Reverse transformation from flattened back to canonical format.

```bash
kehrnel-transform expand <FLAT_FILE> [OPTIONS]
```

:::note
The `expand` command is currently disabled in the CLI. Use the strategy API's reverse-transform endpoint instead:

```bash
POST /api/strategies/openehr/rps_dual/transform/expand
```
:::

## Examples

### Transform to Stdout

```bash
kehrnel-transform flatten composition.json
```

### Transform to File

```bash
kehrnel-transform flatten composition.json -o flattened.json
```

### Pipeline Usage

```bash
# Transform and inspect with jq
kehrnel-transform flatten composition.json | jq '.base.n'

# Transform and extract search document
kehrnel-transform flatten composition.json | jq '.search' > search.json
```

### Inspect Reversed Paths

```bash
# View the encoded path structure
kehrnel-transform flatten composition.json | jq '.base.n | keys'
```

## Understanding the Output

The flattener performs several transformations:

1. **Path Reversal**: Archetype paths are reversed and encoded numerically
   - `content[0]/data/events[0]/data/items[at0004]` → `13.12.11`

2. **AT-Code Encoding**: `at` codes become negative integers
   - `at0004` → `-4`

3. **Archetype ID Encoding**: Full archetype IDs are mapped to integers
   - `openEHR-EHR-OBSERVATION.blood_pressure.v2` → `42`

4. **Search Nodes**: Critical data values are extracted to an array for Atlas Search indexing

## Related

- [Data Model](/docs/strategies/openehr-rps-dual/data-model) - Understanding the RPS format
- [Reversed Paths](/docs/concepts/reversed-paths) - Path encoding explained
