# FHIR ContextObjects – Vitals Window (Strategy Pack)

This strategy ingests BIDMC-style `*_Numerics.csv` wearable/vitals rows or already-normalized samples and produces **ContextObjects** with `nodes[]` containing FHIR resources.

## Output (MongoDB document)
One document per time window:
- `schema.id`, `schema.version`, `schema.profile`
- `subjects[]` with patient/device references
- `interval.start`, `interval.end`, `interval.timezone`, `interval.windowSeconds`
- `nodes[]` with one node per resource (Observation per sample; Patient/Device optional)
- `provenance` with `strategy`, `source`, `run`, `ingestedAt`

Example node:
```json
{
  "p": "FHIR.Observation|http://loinc.org|85353-1",
  "kp": "/samples[12]",
  "li": 3,
  "t": "2025-01-01T00:01:12Z",
  "data": { "resourceType": "Observation", "id": "...", "component": [ ... ] }
}
```

## Runtime usage
Activate the strategy with defaults, then ingest by sending a payload with `csv_path` + `start_time_iso` or `samples`.

Payload example:
```json
{
  "csv_path": "data/bidmc_01_Numerics.csv",
  "start_time_iso": "2025-01-01T00:00:00Z",
  "patient_id": "patient-001",
  "device_id": "device-ppg-001",
  "source": { "dataset": "BIDMC" }
}
```

## Notes
- Uses adapters (storage/index_admin) provided by Kehrnel runtime.
- Default ID strategy is deterministic; re-ingest will skip existing `_id`s.
