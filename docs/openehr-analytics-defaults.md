# Start with all clinical values, then reduce the selection

Kehrnel can now generate a default mapping proposal from each exact OPT. Generation
is offline and deterministic. It uses the existing openEHR semantic catalog, not
an LLM, labels guessed from a record, or a discovery query per incoming composition.
The generated proposal and immutable rule are ordinary JSON.

## Default policy

- Select every clinical **ELEMENT** defined beneath an ENTRY in the OPT. This is
  not a promise to copy every RM attribute or an unconstrained archetype slot.
- Store only populated selected values. An absent optional field creates neither
  a BSON placeholder nor a repeated `quality.missing` entry. Explicit null flavours
  are preserved, including a fact whose only recorded value is a null flavour.
- Preserve the complete recorded DV value initially, including magnitude and units,
  coding system/code/text, ordinal symbol, precision and other value attributes.
  These appear beneath business field names as small objects. The rule type is
  `raw`: this is a lossless typed starting representation, not a claim that every
  field is already reduced to the most economical business scalar.
- Keep composition metadata in the existing activity/source-reference model.
  The proposal lists composition-context ELEMENTs separately, unselected for facts;
  it does not silently duplicate the entire composition context into each fact.
- Do not guess unit conversions, terminology equivalence, calculated clinical
  classifications, or patient identity. These require explicit rules.
- Large multimedia/binary variants and structurally unresolved fields require a
  representation decision. They remain selected and visible in the proposal;
  finalization refuses to silently discard them.

## Fact boundaries

| ENTRY type | Default boundary |
|---|---|
| OBSERVATION | A recorded EVENT; repeated clinical CLUSTERs below it become individual block facts. Systolic and diastolic in the same block stay together. |
| EVALUATION | The evaluation, with separate facts for repeated clinical CLUSTERs. |
| ACTION | The action, with separate repeated CLUSTERs; action state remains interpretation context. |
| INSTRUCTION | Each ACTIVITY where present; instruction-level clinical values remain separate. Planned timing is retained. |
| ADMIN_ENTRY | The administrative entry, with separate repeated CLUSTERs. |

This is a conservative starting proposal, not an automatic clinical endorsement of
all possible CLUSTER boundaries. A potentially repeated CLUSTER is treated as a
block, using OPT occurrences and containing-attribute cardinality. The reviewer
can later consolidate appropriate blocks, but must preserve the instance boundaries.
Clinical state HISTORY is separate from observation data HISTORY; no implicit
position-based alignment is invented.

A repeated single ELEMENT can use `multiple: true`: its complete DV values form
one array. Runtime validation rejects that flag if the matches cross different
parent containers. Two independently flattened arrays are not treated as paired
measurements. A qualifier from a single, unambiguous protocol is inherited only
from the same ENTRY instance; repeated or unanchored protocol structures require
review. Selecting all fields does not override these protections.

## What the generated files contain

The earlier `examples/openehr_analytics/default-proposals/index.json` lists ten draft
configurations generated from the ten locally available CatSalut OPTs:
**436 selected clinical fields and 88 proposed block definitions**. The files contain
no patient values, credentials, or live database changes. Their `effective_at`
and epoch are initial draft settings; choose the intended activation date and epoch
before use.

Each proposal contains the exact template/source digest, a complete field inventory,
proposed business names and semantic identities, block anchors, unresolved decisions,
and an executable candidate rule. The candidate is a review artifact. Alias conflicts
are disambiguated, and rule-set compilation rejects one business alias bound to
conflicting declared semantic identities. Supply a shared `aliases` registry to
`propose_from_opt()` when generating many templates. Do not infer cross-version
clinical equivalence solely from matching field labels.

The supported starting scope is the **provided OPT**, not every template in the
source database. The 2,460 previously unmapped sampled compositions belonged to
134 template names/versions without reference rules. Their exact OPTs or approved
bindings are still needed; a sample of source values cannot reveal every optional
field their templates permit.

## Generate, inspect and select

No environment file or MongoDB connection is required:

```sh
PYTHONPATH=src .venv/bin/python -m kehrnel.engine.domains.openehr.analytics.cli \
  propose-defaults --opt /path/to/template.opt \
  --effective-at 2026-11-18T00:00:00Z --epoch 1 \
  --output /private/tmp/template-proposal.json

PYTHONPATH=src .venv/bin/python -m kehrnel.engine.domains.openehr.analytics.cli \
  finalize-defaults --proposal /private/tmp/template-proposal.json \
  --exclude-field openehr-field:sha256:FIELD_TO_REMOVE \
  --output /private/tmp/template-rule.json
```

Omit `--exclude-field` to keep every selected clinical field. The finalizer validates
exclusions and refuses selected unresolved fields. Adjusting business aliases, fact
boundaries or subject policies is a rule review, not merely hiding a table column.
After an activation, changing selection requires a new rule epoch/revision and the
existing explicit choice of prospective application or historical backfill.

The strategy operation `analytics_default_mapping` accepts `opt_xml`, `effective_at`
and an optional `epoch`; it returns the same proposal without storage access.
Healthcare Data Lab can call this when an OPT is uploaded. The UI upload/save flow
has **not been wired to that operation in this change**. Conversion still consumes
pinned rules; it does not infer a fresh schema for each composition.

## Source and subject exceptions

New OPT-derived drafts carry the narrow
[copied-composition metadata compatibility policy](openehr-analytics-source-compatibility.md).
It verifies the OBSERVATION against the exact OPT and preserves all source values;
it does not normalize or repair the source. The exception appears once in
`source_refs`. Existing revisions without this policy remain strict until a new
epoch is reviewed and approved. Other identity and subject failures still fail.

The original 33 mapping failures spanned five exact template versions:

| Template | Repeated-field failures | Related-subject failures |
|---|---:|---:|
| PO_Attention_to_the_mother_during_labour_v0.19_FORMULARIS | 6 | 5 |
| PO_Mothers_Puerperium_v0.21_FORMULARIS | 0 | 4 |
| PO_Obstetric_process_v0.8_FORMULARIS | 0 | 4 |
| PO_Pregnancy_End_v0.10_FORMULARIS | 0 | 9 |
| air_adverse_reaction_record_v1 | 5 | 0 |

These are sampled record counts, not the number of rules to write. One corrected
rule can handle many records. Related subjects remain an explicit review item even
when a field is selected by default. Never substitute the EHR owner for a fetus,
newborn, relative, or other related subject without sufficient identity evidence.
A clinically different subject may also be expressed inside a CLUSTER; the general
ENTRY subject check alone cannot establish those semantics. Review those block
policies before activation.
