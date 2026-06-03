# `fhir.rps_canonical` scripts

Runtime implementation and standalone helpers for this strategy pack.

| Module / script | Purpose |
|-----------------|---------|
| `strategy.py`, `bridge.py`, `generation.py`, … | Imported by Kehrnel runtime (`manifest.json` entrypoint) |
| [`spike_generate_and_search.py`](spike_generate_and_search.py) | Smoke: fhir-gen → MongoDB → denorm → FHIR search (no API) |

```bash
# From kehrnel repo root
python src/kehrnel/engine/strategies/fhir/rps_canonical/scripts/spike_generate_and_search.py --db fhir_kehrnel_spike
```

Pack specification and API sample JSON: [`../specification/`](../specification/README.md)
