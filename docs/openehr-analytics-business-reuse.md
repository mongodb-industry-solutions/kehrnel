# Reusing business variables across templates

Implemented 19 November 2026. This is the backend handoff for the Business variables
screen and the template Analytics editor in Healthcare Data Lab. The source review
covered HDL commit `14ad240d`; no HDL files were changed in this implementation.

## Assessment of the finding

The finding is correct for generated defaults. The OPT compiler's `semantic_key`
contains the template identifier, full path, archetype and value types. It is a
**source mapping identity**, stable across some non-structural OPT changes, not a
global clinical concept identifier. Removing the template identifier from that
compiler would change existing identities and could equate fields with different
roles, subjects or interpretation context.

The model could already consume deliberately authored cross-template mappings.
What was missing was a safe, supported authoring path from two real OPTs to the
same business variable. That path now exists as explicit business-variable reuse.
Exact-source aliases retain their existing behaviour; they cannot merge distinct
source identities. No SQL layer or new physical collection is introduced.

## Two identities, one output path

A source field keeps its own `field_id`, source path and source semantic key. A
business variable defines the output key, declared semantic identity, representation,
canonical unit (when applicable) and accepted RM value types.

The new `openehr-business-reuse/v1` decision binds a selected source field to an
existing variable from an exact retained rule revision. The existing variable's
`semantic_key` remains its declared identity for backwards compatibility; this does
**not** claim the two original source semantic keys are equal. Catalog bindings
now report `source_semantic_key` separately. A business contract digest identifies
the shared key/type/unit/semantic identity/RM-type contract.

The decision is stored in the template rule, on the source field binding. It retains
source definition and types, target revision and contract, shape policy and the
curator's explanation. It changes the immutable rule revision and therefore its
preview/review evidence. Nothing is copied into every fact, and workers perform no
catalog or terminology network lookup per fact.

## Current boundary

The server permits reuse only when:

- The source is selected and resolved in the exact OPT proposal.
- The target key exists in the exact retained rule revision supplied by the caller.
- Both bindings refer to the **same versioned archetype and archetype-relative
  element path**. Matching labels are insufficient.
- Output type and canonical unit agree. Source RM types are covered by the target
  contract; a narrower source type set is acceptable.
- Source value shape is explicitly preserved. An array is not collapsed to its
  first value. A binding remains within its existing clinical fact boundary.
- The curator supplies a reason and completes the ordinary preview/review process.

This verifies structural and representation compatibility, not clinical equivalence
by itself. Review the analyte, method, specimen, time, subject and interpretation
qualifiers as relevant. Reuse of different archetypes, changed archetype versions,
terminology-equivalent elements or arbitrary clinical formulas remains outside this
first contract. Unit conversions still use the existing explicit conversion rules;
reuse itself performs no conversion.

## Payload for Claude

Use the existing `analytics_validate_draft`, `analytics_preview_draft` and
`analytics_review_draft` operations. Keep the exact OPT and proposal revision and
pass existing retained rules through the authoring configuration. Add:

```json
{
  "edits": {
    "reuse_variables": {
      "<selected source field_id from the proposal>": {
        "key": "laboratory_test_analyte.test_result",
        "rule_revision": "<exact retained target rule revision>",
        "shape_policy": "preserve_source",
        "reason": "Same analyte-result slot; retain units, repetitions and analyte context."
      }
    }
  }
}
```

Keep existing exclusions, group settings, subject bindings and conversions. Reuse
runs after those edits. When a quantity requires a different representation or unit,
configure its existing variable/conversion edits first; the output contract must
match the target. The old proposal key is used by those edits; the final rule and
`review.business_reuse` show the new output key. Reuse never silently resolves a
`PROTOCOL_CORRELATION_REVIEW` issue or a related-subject decision.

The proposal's fields now include `source_definition` (archetype ID and relative
path). The catalog's variable rows include `business_contract_digest` and
`value_shapes`; its binding rows include `source_definition`, `source_semantic_key`,
`source_rm_types`, `value_shape` and `reuse` (null for a normal binding).
These additions use the existing catalog operation and response shape. The existing
HDL route forwards the `items` fields unchanged.

In the editor, label this **Reuse an existing business variable**, separate from
renaming a label or choosing a terminology concept. Offer structural matches as
candidates, show the target type/unit and per-binding shape, and collect the reason.
A candidate is not automatically equivalent or approved. The server is authoritative
when the selection is compiled. Keep Save draft and Approve release distinct.

## Real two-template example

The example uses the repository OPTs `sample_laboratory_v0.4` and `T-IGR-BIOLOGY`.
Both contain `openEHR-EHR-CLUSTER.laboratory_test_analyte.v1 /items[at0001]`.
Their source semantic keys and full paths differ. The first template permits typed
quantity, text and quantity interval values; the second permits quantity and text.
The first binding stores a value; the second permits an array of complete values.

Both now feed `clinical.laboratory_test_analyte.test_result`. The catalog reports
**two templates** and `value_shapes: ["array", "value"]`. Source values, units and
fact IDs remain unchanged apart from the deliberately selected business output key.
The generated example produced 18 facts containing the shared variable across the
two synthetic input compositions; this is fixture evidence, not production volume.

Run from the Kehrnel checkout:

```sh
PYTHONPATH=src .venv/bin/python examples/openehr_analytics/reuse_business_variable.py \
  --output /tmp/business-reuse.recorded.json
```

The recording includes rules, authoring edits, unresolved-protocol exclusion made
explicitly for the fixture, catalog and binding responses, projected value examples,
OPT digests and hashes of the actual analytics code. It uses real templates with
synthetic compositions. No database writes, activation or network access occur.
Claude can regenerate and consume it in the existing HDL fixture location. The
old `record-cross-template.py` attempt must use `edits.reuse_variables`, not an alias
between distinct source semantic keys.

## Query semantics

A shared result slot is not a claim that glucose and haemoglobin are interchangeable.
Raw values retain their type and unit. Queries should state whether they count facts,
individual values or distinct patients. MongoDB `$unwind` handles an object as one
value and expands an array, so it can normalize this variation at query time:

```javascript
db.facts.aggregate([
  { $match: {
    "context.provider_id": "example-hospital",
    "context.event_at": { $gte: ISODate("2026-01-01"), $lt: ISODate("2027-01-01") },
    "clinical.laboratory_test_analyte.test_result": { $exists: true }
  } },
  { $unwind: "$clinical.laboratory_test_analyte.test_result" },
  { $match: { "clinical.laboratory_test_analyte.test_result._type": "DV_QUANTITY" } },
  { $group: {
    _id: {
      template: "$context.template_name",
      unit: "$clinical.laboratory_test_analyte.test_result.units"
    },
    recorded_values: { $sum: 1 }
  } }
])
```

This illustrative MQL counts quantity values by template and recorded unit. It is
not an executed MongoDB benchmark, nor an aggregation endpoint newly added to HDL.
For clinical comparisons, constrain the analyte and other necessary qualifiers too.
For paired predicates inside a repeated value, use `$elemMatch` or match after
unwinding so the predicates concern the same value. Fact boundaries still keep
related values associated with their clinical event or other configured block.

## Maintenance and checks

Old source mappings and output documents are unchanged. Reusing a variable in an
already configured template requires a new epoch/effective time and normal approval.
Keep historical revisions for replay. Reproject existing compositions explicitly if
the common output key should also cover historical data; changing the rule alone
does not rewrite those documents. Retained old rules may still expose the earlier
variable in the catalog. Reuse edits must remain part of the saved draft inputs.

Focused tests cover the two real OPTs, both value shapes, projection without source
mutation or fact-boundary changes, unrelated/unknown/stale targets, excluded fields,
representation mismatch, required reason, raw value-type checks, immutable target
contract checks, future epochs and the retained protocol-review failure.

## Remaining HDL adjustments found in the source review

Continue the Analytics editor body; it does not need to wait for more backend design.
The current Business variables implementation has a sound read-only separation, but
these details should be corrected before relying on it with thousands of templates:

1. Show `value_shapes` and each binding's `value_shape`. Update the explanation of
   reuse from "alias to its exact semantic identity" to the two identities above.
2. The binding detail fetch is capped at 100 and currently has no paging controls.
   Use `total` and offset paging, and forward `next_offset` from the Kehrnel response
   if using it. Do not show the first 100 as the full set of feeding templates.
3. "Other variables with this label" is currently computed from the visible page.
   Scope that wording to the page or perform a global label lookup; "None" on one
   page is not evidence there are none in the catalog.
4. The server currently reads only active activation configuration. The editor must
   also be able to browse saved drafts before activation, with their state clearly
   distinguished. Do not activate a strategy just to make its draft variables visible.
5. Reset selected variable, bindings and paging on environment change and discard
   stale responses. Provide keyboard-operable variable selection, not only a clickable
   table row. These are UI follow-ups, not changes made to Claude's checkout here.

The 857 HDL test count is Claude's report; this backend change did not rerun that
whole UI suite. The Kehrnel implementation and recording have their own checks.
