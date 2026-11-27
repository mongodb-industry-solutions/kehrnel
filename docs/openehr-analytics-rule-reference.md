# Clinical configuration language: developer reference

This describes the implemented Python interpreter, not a proposed general-purpose
transformation language. JSON is the portable format. A future YAML editor may emit
this JSON, but the standalone CLI does not currently accept YAML rules. Existing
HDL `analyticsTemplate.fields` selections and these rules have different contracts.

## Configuration layers

| Layer | Contents | Changes require |
|---|---|---|
| Source profile | Canonical/IBM format, envelope names, node encoding and pinned dictionaries | A versioned decoding release and replay compatibility review |
| Business variable catalog | Stable dotted key, label, type, allowed RM types, canonical unit and semantic identity | Compatibility check across all retained rule revisions |
| Exact template mapping | ENTRY/block boundaries, selected field paths, inheritance, subject/time and required values | A new per-template rule epoch |
| Retention | Retain, or expire facts using an explicit clock and duration | New rule epoch; backfill to change already stored output |
| Deployment | Target, prefix, credentials references, jobs, activation delivery and worker routing | Explicit deployment/release change; credentials never enter clinical rules |

Use [config.canonical.json](../examples/openehr_analytics/config.canonical.json) as
a minimal, executable **one-template** example. Generate and review a proposal for
each exact OPT using the [configuration workflow](openehr-analytics-integration.md).
The rule object contains `epoch`, `effective_at`,
`schema_version: 1`, `retention` and `catalog`. `RuleRevision.compile()` normalizes
it and calculates its digest; `document()` returns the immutable representation.
Do not hand-edit a document while retaining its old `revision` or `_id`.

## Source metadata compatibility

New OPT-derived drafts include the narrowly defined
`copied_composition_metadata/v1` policy. It tolerates an OPT-matched OBSERVATION
whose complete archetype metadata was copied from its COMPOSITION, without
modifying the source. Evidence is stored once in `source_refs`. Existing revisions
remain strict; enabling the policy requires a new rule epoch and review. See the
[policy, SDK upgrade and replay guide](openehr-analytics-source-compatibility.md).

## Source structure coverage and open slots

New OPT-derived rules retain a `source_coverage` inventory for the exact OPT.
Conversion distinguishes selected source ELEMENTs, known unselected fields and
unexpected structures. Unexpected content returns `needs_rules`, withholds clinical
facts for the composition, and keeps its activity/source reference. This also covers
valid open-slot fillers that have not yet been explicitly mapped; it does not declare
those fillers invalid. See [source coverage](openehr-analytics-source-compatibility.md).

Retained rules without this inventory report `not_checked`. Enabling coverage is a
new rule epoch, not an in-place change to a published configuration. The inventory
stores structural definitions once per rule; it adds no fields to ordinary facts.

## Business variables and fields

A variable describes what the value means:

```json
{
  "key": "blood_pressure.systolic",
  "label": "Systolic",
  "type": "quantity",
  "unit": "mm[Hg]",
  "rm_types": ["DV_QUANTITY"]
}
```

A template field binds that variable to a recorded ELEMENT:

```json
{
  "key": "blood_pressure.systolic",
  "path": "/content[openEHR-EHR-OBSERVATION.blood_pressure.v2]/data[at0001]/events[at0006]/data[at0003]/items[at0004]",
  "scope": "block",
  "missing_policy": "omit_optional",
  "conversions": {"unit_conversions": {"kPa": {"scale": 7.50061683}}}
}
```

The path is interpreter configuration; analysts query
`clinical.blood_pressure.systolic`. It is a nested MongoDB field, not a literal
field name containing a dot. Source paths are not clinical terminology mappings.
Terminology resolution concerns coding systems, code meanings and concept sets;
it is separate from choosing the source ELEMENT and its owning clinical instance.
The ingestion strategy does not call SNOMED or embed every fact.

The current output choices include `raw`, `quantity`, `measurement`, `number`,
`coded`, `boolean`, `date`, `text` and `identifier` (use the compiler as authority).
Generated defaults use `raw`: they retain the recorded DV object, for example
`{_type: DV_QUANTITY, magnitude: 120, units: mm[Hg]}`. A `quantity` mapping emits a
numeric magnitude in its declared canonical unit. That is an explicit representation
choice, not an implicit effect of selecting a field. Quantities currently use Python
numeric values, not Decimal128 arbitrary-precision arithmetic.

Keep one business key's type, canonical unit and declared semantic identity consistent
across templates and retained revisions. Changing a key from `raw` to `quantity`
is not an ordinary field-selection update while both revisions remain in a rule set.
Choose distinct compatible keys or design an explicit representation migration.
Identical labels alone do not establish equivalence across template versions. Cross-template
reuse is now an explicit authoring decision, separate from exact-source aliases:
see [business-variable reuse](openehr-analytics-business-reuse.md) for the JSON
payload, scalar/array handling, two-OPT example and maintenance rules.

## What becomes one fact

A group identifies one clinical block and contains its selected bindings. Its
`definition` is mandatory. Users should choose among valid suggested blocks, not
turn an arbitrary scalar node into a fact.

| ENTRY | Default proposal | Context that must retain its meaning |
|---|---|---|
| OBSERVATION | Each EVENT; a repeated clinical CLUSTER may form a finer block | Recorded event time; interval width/statistical meaning where applicable |
| EVALUATION | The evaluation ENTRY or a repeated clinical CLUSTER | Assertion scope and clinical subject |
| ACTION | The action ENTRY or repeated clinical CLUSTER | Recorded action state (`ism_transition`) |
| INSTRUCTION | ACTIVITY where present; instruction-level values remain distinct | Planned intent and recorded timing; not proof of performance |
| ADMIN_ENTRY | Administrative ENTRY or repeated clinical CLUSTER | Administrative meaning and owning subject |

`definition.entry_path` identifies the owning ENTRY and `entry_type` declares its RM
type. `anchor` selects the fact boundary within that ENTRY. Runtime validation checks
both the rule and the actual source type. Two BP events produce two facts, with each
event's systolic and diastolic kept together. A CLUSTER is not automatically a patient
or an encounter. Repeated fetus/newborn blocks require a reviewed subject policy.

`scope: block` confines a field to the selected block. A selected protocol qualifier
can use `scope: same_entry` with `inherit: ancestor` to inherit from its owning ENTRY.
Protocol is ENTRY data, not composition context. Parallel state HISTORY cannot be
silently paired with observation data HISTORY; it needs an explicit alignment design.
`multiple: true` permits repeated ELEMENT values only within the same parent container;
it does not align two independent arrays by position.

## Subject, clinical time and missing values

- `subject_mode: ehr_owner` with `subject: patient` is valid only when the actual
  clinical subject is the EHR owner. The runtime rejects a related-party ENTRY under
  this policy. It cannot infer every subject relationship encoded inside CLUSTERs.
- `subject_mode: bound_identity` requires exactly one `subject_identity: true`
  binding, a non-empty identifier, and its key in `required_fields`. Do not synthesize
  fetal identity from a label or assume newborn and mother share one subject ID.
- `time: nearest_recorded_time` uses the recorded instance's time. The explicit
  `allow_composition_time` flag controls fallback. A `mapped_field` time policy needs
  a registered required date key and forbids fallback.
- `missing_required: reject_composition` protects interpretation/identity/time fields.
  Removing an optional column cannot remove those required dependencies.
- `missing_policy: omit_optional` omits absent optional values. Defaults do not create
  thousands of empty columns or `quality.missing` entries.
- Explicit source `null_flavour` is retained as evidence. `retain_null_facts: true`
  can retain a fact whose only recorded content is null flavour. Unavailable numeric
  values are not converted to zero.

`quality` describes evidence from deterministic checks, such as recorded null reasons,
conversion provenance or absent ACTION state. It is not an AI judgment or an invented
clinical status. Empty quality objects are omitted. A missing optional field is not
necessarily a data-quality defect.

## Conversions that run today

`unit_conversions` maps an exact source-unit string to positive finite `scale` and
optional finite `offset`: `target = source × scale + offset`. Unlisted units are
rejected. UCUM spelling/synonym resolution and clinical dimensional compatibility
are not inferred. The curator approves conversions, including analyte-specific
molar/mass conversions. A rule for glucose must not become a generic laboratory rule.

Strict decimal text parsing is optional and requires `DV_TEXT` in `rm_types`:

```json
{
  "text_numeric_conversion": {
    "kind": "strict_decimal",
    "decimal_separator": ",",
    "source_unit": "mm[Hg]",
    "on_non_numeric": "preserve"
  }
}
```

This is a `conversions` fragment for a quantity variable. It accepts a complete
numeric string such as `120,5`, not a narrative such as `about 120 mmHg`.
`preserve` means no invented numeric output, quality evidence, and the original
text remaining in the source; it does not promise a second copy of that text inside
this typed fact. `reject` rejects nonnumeric input. Required-field policies still
apply if a parsed value is unavailable.

General formulas, BMI, risk scores, inferred diagnoses, reference-range flags,
cross-composition calculations and terminology-derived classifications are **not
implemented ingestion operators**. Do not add an arbitrary expression string and
expect it to run. Initially compute these in a reviewed query or separate derived
process. If persisted calculations are later added, they need a small typed operator
registry, explicit dependencies/unit/time/subject semantics, null handling, calculation
version and provenance—not executable Python in configuration.

## Retention and revisions

```json
{"mode": "delete_facts", "clock": "clinical_time", "days": 1825}
```

The alternative clock is `committed_time`; the default is `{"mode":"retain"}`.
A clinical-time TTL rule rejects fallback timestamps. Days are elapsed 24-hour
periods, not calendar years. TTL is asynchronous deletion, not archival, and does not
remove activity or source references. Reprocess old compositions to change their TTL.

`effective_at` applies to stable ingestion-event `accepted_at`, not automatically
to the composition's clinical date. Explicit backfills can pin a reviewed revision
for historical sources. Keep source, template, rule, schema and delivery release
versions distinct; see the integration guide for retry and rollback behavior.

## Generate, review and finalize a default

`propose_from_opt(xml, epoch=..., effective_at=...)` returns a draft with fields,
semantic identities, proposed groups, issues, review requirements and a candidate
rule. `finalize_proposal(proposal, exclude_field_ids=[...])` validates exclusions and
refuses selected unresolved fields. It returns a rule; it does not publish it or
create a backfill. Related-subject review remains necessary even when compilation
succeeds. Missing exact OPTs must stay visible as missing configuration.

Run the offline walkthrough to see pairing, conversion, future activation and an
explicit historical rule change using synthetic compositions:

```sh
PYTHONPATH=src python examples/openehr_analytics/integration_walkthrough.py
```
