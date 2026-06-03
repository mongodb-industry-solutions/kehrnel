# `fhir.rps_canonical` specification

Strategy pack metadata and sample API payloads for the Kehrnel runtime.

| File | Purpose |
|------|---------|
| `manifest.json` | Strategy pack manifest (discovered by runtime) |
| `schema.json`, `defaults.json`, `spec.json` | Activation config and pack spec |
| [`activate_dev.json`](activate_dev.json) | `POST /environments/dev/activate` |
| [`job_generate_small.json`](job_generate_small.json) | `POST /environments/dev/synthetic/jobs` (small Patient + Observation batch) |

Library-only smoke (no API): [`../scripts/spike_generate_and_search.py`](../scripts/spike_generate_and_search.py)

**Full testing guide:** [FHIR_TESTING.md](../../../../../../FHIR_TESTING.md) (kehrnel repo root)

## Activate and run a synthetic job

Requires API on `http://localhost:8080` (see kehrnel `README.md`).

```bash
# From kehrnel repo root
curl -X POST http://localhost:8080/environments/dev/activate \
  -H "Content-Type: application/json" \
  -d @src/kehrnel/engine/strategies/fhir/rps_canonical/specification/activate_dev.json

curl -X POST http://localhost:8080/environments/dev/synthetic/jobs \
  -H "Content-Type: application/json" \
  -d @src/kehrnel/engine/strategies/fhir/rps_canonical/specification/job_generate_small.json
```

PowerShell:

```powershell
$spec = "src/kehrnel/engine/strategies/fhir/rps_canonical/specification"
Invoke-RestMethod -Method POST -Uri "http://localhost:8080/environments/dev/activate" `
  -ContentType "application/json" -Body (Get-Content "$spec/activate_dev.json" -Raw)
```

### `synthetic_generate_batch` payload fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `resources` | `dict[str,int]` | yes* | ResourceType → count (*or `resource_counts`) |
| `seed` | int | no | Overrides config `generation.seed` |
| `schema_version` | str | no | `R5` default |
| `scenarios` | list[str] | no | e.g. `["Patient:deceased_datetime"]` |
| `store_canonical` | bool | no | default true |
| `dry_run` | bool | no | Generate in memory, no Mongo writes |
| `plan_only` | bool | no | Planned counts + dependency order only |
| `denormalize_after` | bool | no | Inline `fhir_denormalize` after save |
| `skip_auto_index` | bool | no | Skip `fhir_ensure_indexes` after denormalize |

After generation without `denormalize_after`, run `fhir_denormalize` before FHIR domain search (see [FHIR_TESTING.md](../../../../../../FHIR_TESTING.md)).
