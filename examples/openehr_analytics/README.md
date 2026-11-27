# openEHR analytics examples

Start with the [GUI and worker workflow](../../docs/openehr-analytics-integration.md).

- `config.canonical.json` and `composition.canonical.json`: small synthetic blood-pressure example.
- `config.ibm.json`: explicit IBM source decoder configuration; select reviewed rules for your exact templates.
- `approved_configuration.py`: download the approved HDL bundle and produce Kehrnel configuration, without activating a worker.
- `python_worker.py`: bounded MongoDB batches (`process_partition`) and individual uploaded compositions (`process_composition`) using the shared converter and store.
- `integration_walkthrough.py`: offline canonical/IBM parity and lifecycle example.
- `reuse_business_variable.py`: reuse existing analytical variables across exact template bindings.

In HDL, select a source environment and download the complete configuration from
Data Factory → Analytics Pipeline. It includes the materialized source profile,
source environment reference, output settings and approved rules. Keep credentials
in the worker's local secret store. No customer clinical defaults are approved or
activated by these examples.

A source namespace identifies the logical source and controls output identifiers.
Keep it stable across representations of the same source. Changing it is a data
migration, not renaming a display label.
