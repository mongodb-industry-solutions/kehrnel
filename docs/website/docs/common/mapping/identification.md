---
sidebar_position: 2
---

# Mapping Identification

Mapping identification decides **which mapping + template pair** applies to each incoming document.

If identification is weak, the rest of the pipeline is unreliable.

## Goals

- Deterministic routing (same input -> same mapping decision)
- No implicit fallback guesses in production
- Clear operator feedback when no mapping is eligible

## Recommended policy

1. Classify each document into a stable `documentType` key.
2. Keep an explicit `documentType -> targetTemplate -> mapping` association.
3. Transform only when both template and mapping are set.
4. Reject unknown/ambiguous inputs with actionable errors.

## Example type keys

- `fiche_tumour_cda`
- `biology_results_csv`
- `pmsi_cda`

## Practical matching signals

Use a small, explicit rule set:

- file extension and parseability (`.xml`, `.csv`)
- root structure markers (for CDA XML)
- required header columns (for CSV)
- optional confidence score for diagnostics only

## Example: CDA route

Input: `fiche_tumour.xml`

Expected decision:

- `documentType`: `fiche_tumour_cda`
- `mapping`: `tumour_mapping.yaml`
- `template`: `T-IGR-TUMOUR-SUMMARY.opt`

## Example: CSV route

Input: `biology.csv`

Expected decision:

- `documentType`: `biology_results_csv`
- `mapping`: `biology_mapping.yaml`
- `template`: `T-IGR-BIOLOGY.opt`

## Failure handling (recommended)

Return structured errors that the UI can render directly:

```json
{
  "error": "Unable to identify mapping",
  "details": {
    "documentType": null,
    "reason": "Required column 'IPP' not found"
  }
}
```

## Operational notes

- Log match decisions (`documentType`, matched pattern id, timestamp).
- Keep rule updates versioned.
- Treat identification rules as production configuration, not ad-hoc UI logic.
