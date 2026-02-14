---
sidebar_position: 8
---

# Bundle Commands

Manage portable bundles of templates, mappings, and configurations.

## Overview

Bundles are self-contained packages that include everything needed for a specific use case:
- OPT templates
- WebTemplates
- Mapping configurations
- Sample data
- Metadata

## kehrnel-validate-bundle

Validate a bundle file structure and contents.

### Synopsis

```bash
kehrnel-validate-bundle <PATH>
```

### Arguments

| Argument | Description |
|----------|-------------|
| `PATH` | Path to bundle JSON file |

### Example

```bash
kehrnel-validate-bundle my_bundle.json
```

Output on success:
```
Bundle valid (digest sha256:abc123...)
```

Output on failure:
```
Bundle invalid:
- Missing required field: template_id
- Invalid domain: "unknown_domain"
```

---

## kehrnel-import-bundle

Import a bundle into the local store.

### Synopsis

```bash
kehrnel-import-bundle <PATH> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `PATH` | Path to bundle JSON file |

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--upsert` | `false` | Update if bundle already exists |
| `--store PATH` | `.kehrnel/bundles` | Custom store directory |

### Examples

```bash
# Import new bundle
kehrnel-import-bundle vital_signs_bundle.json

# Update existing bundle
kehrnel-import-bundle vital_signs_bundle.json --upsert

# Use custom store location
kehrnel-import-bundle bundle.json --store /var/kehrnel/bundles
```

Output:
```
Imported bundle vital_signs.v1 (sha256:abc123...)
```

---

## kehrnel-list-bundles

List all bundles in the store.

### Synopsis

```bash
kehrnel-list-bundles [OPTIONS]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--store PATH` | `.kehrnel/bundles` | Custom store directory |

### Example

```bash
kehrnel-list-bundles
```

Output:
```
┏━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━┓
┃ bundle_id           ┃ domain   ┃ kind        ┃ version ┃ digest         ┃
┡━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━┩
│ vital_signs.v1      │ openehr  │ template    │ 1.0.0   │ sha256:abc1... │
│ lab_results.v2      │ openehr  │ template    │ 2.1.0   │ sha256:def2... │
│ medication.v1       │ openehr  │ mapping     │ 1.0.0   │ sha256:ghi3... │
└─────────────────────┴──────────┴─────────────┴─────────┴────────────────┘
```

---

## kehrnel-export-bundle

Export a bundle from the store to a file.

### Synopsis

```bash
kehrnel-export-bundle <BUNDLE_ID> <OUTPUT> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `BUNDLE_ID` | ID of the bundle to export |
| `OUTPUT` | Output file path |

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--store PATH` | `.kehrnel/bundles` | Custom store directory |

### Example

```bash
kehrnel-export-bundle vital_signs.v1 ./exported_bundle.json
```

Output:
```
Wrote bundle to ./exported_bundle.json
```

---

## Bundle Format

Bundles follow a standard JSON structure:

```json
{
  "bundle_id": "vital_signs.v1",
  "domain": "openehr",
  "kind": "template",
  "version": "1.0.0",
  "metadata": {
    "title": "Vital Signs Template Bundle",
    "description": "Blood pressure and heart rate measurements",
    "author": "Healthcare Team",
    "created": "2025-01-15T10:00:00Z"
  },
  "contents": {
    "template": {
      "template_id": "vital_signs.v1",
      "opt": "<base64-encoded OPT>",
      "webtemplate": { ... }
    },
    "mappings": [
      {
        "name": "csv_import",
        "format": "csv",
        "config": { ... }
      }
    ],
    "samples": [
      { ... }
    ]
  }
}
```

## Workflow Example

```bash
# 1. Create a bundle for your template
cat > my_bundle.json << 'EOF'
{
  "bundle_id": "my_template.v1",
  "domain": "openehr",
  "kind": "template",
  "version": "1.0.0",
  ...
}
EOF

# 2. Validate
kehrnel-validate-bundle my_bundle.json

# 3. Import to local store
kehrnel-import-bundle my_bundle.json

# 4. List to verify
kehrnel-list-bundles

# 5. Export for sharing
kehrnel-export-bundle my_template.v1 share/my_bundle.json
```

## Related

- [kehrnel-validate-pack](/docs/cli/validate-pack) - Validate strategy packs
- [Strategy Packs](/docs/strategies/overview) - Understanding strategy architecture
