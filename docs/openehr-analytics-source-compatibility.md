# Copied composition metadata: analytics compatibility

Implemented 19 November 2026. The customer confirmed that this pattern also occurs
in production. IBM's canonical CDR export shows an OBSERVATION whose
`archetype_node_id` identifies its observation archetype while
`archetype_details.archetype_id` identifies the containing COMPOSITION.

The [openEHR RM 1.0.4 LOCATABLE description](https://specifications.openehr.org/releases/RM/Release-1.0.4/common.html#_locatable_class)
says that, at an archetype root, `archetype_node_id` is the stringified archetype ID
in `archetype_details`. This is descriptive text, not a separately listed equality
invariant on that page. Acceptance by a CDR does not establish conformance.
The analytics policy handles the known source pattern without declaring it valid
canonical metadata or repairing it.

## Exact boundary

`copied_composition_metadata/v1` accepts a conflicting observation only when all
these conditions hold:

- The rule was generated from the exact OPT and retains its SHA-256 digest.
- The canonical COMPOSITION has the rule's exact template identifier, and its
  own node ID and declared archetype ID agree with the OPT composition archetype.
- The affected object is an OBSERVATION. Its node ID and its location uniquely
  match an observation archetype path recorded from that OPT.
- Its **entire** `archetype_details` object equals the containing COMPOSITION's
  details. Merely having a different archetype ID is insufficient.

Canonical input and decoded IBM input use the same check. Node codes and paths
continue to be decoded using the pinned source profile and dictionaries.
The decoder does not overwrite the recorded node or metadata identifiers.

Wrong RM types, unknown paths, partial metadata copies, unrelated identifiers,
internal CLUSTER conflicts and ambiguous matches still fail. Related-subject,
repetition, conversion and clinical mapping checks continue independently. This
is an identity exception, not full RM/OPT validation or permission to infer missing
clinical meaning.

## Storage and performance

Rules select clinical blocks using the verified `archetype_node_id`. Neither the
source envelope nor its canonical object is modified. Existing facts and activity
keep their current shapes. Only affected composition `source_refs` gain:

```json
{
  "source_metadata_compatibility": {
    "policy": "copied_composition_metadata/v1",
    "opt_digest": "sha256:<exact OPT digest>",
    "source_modified": false,
    "exceptions": [{
      "location": "/content/0",
      "opt_path": "/content[openEHR-EHR-OBSERVATION.act.v0]",
      "archetype_node_id": "openEHR-EHR-OBSERVATION.act.v0",
      "declared_archetype_id": "openEHR-EHR-COMPOSITION.self_reported_data.v1",
      "decision": "map_using_opt_matched_node_identity"
    }]
  }
}
```

This is an illustrative shape, not a published ACT mapping. The source reference
already links the original composition/version and rule revision. Its exception
list distinguishes repeated observation instances. A receipt diagnostic contains
only `SOURCE_METADATA_COMPATIBILITY`, the exception count and source reference.
Nothing is added to each fact; unaffected source references gain no empty object.

The OPT is compiled once during authoring. Workers compile policy data once with
the rule. The existing node traversal detects conflicts; additional path resolution
runs only on a conflict and is cached per affected archetype within that composition.
No terminology call, OPT download or dictionary lookup is added per fact. This is
an implementation property, not a claim of measured production throughput.

## Authoring and maintenance

New `analytics_default_mapping` proposals and `compile_draft` candidates include
the policy by default. They remain drafts requiring normal preview, review and
approval. Authoring accepts `edits.source_metadata_policy` as either
`copied_composition_metadata/v1` or `strict`; strict removes the exception policy.
The browser does not supply arbitrary eligible paths through this edit setting.

Existing released rules without the policy retain strict behaviour. Do not rewrite
a retained revision or regenerate a customer's mappings to enable compatibility.
The SDK provides an upgrade that preserves selected fields, conversions, aliases
and retention:

```python
from kehrnel.engine.domains.openehr.analytics.metadata_compatibility import upgrade_rule

candidate = upgrade_rule(
    existing_rule,
    exact_opt_xml,
    epoch=existing_rule["epoch"] + 1,
    effective_at="2026-11-20T00:00:00Z",  # choose the intended activation boundary
)
```

This returns a candidate, performs no writes, requires an exact template-name match
and validates the new epoch/time against the previous rule. Inspect the OPT digest
and preview real examples before approving it. The changed policy changes the rule
revision and invalidates earlier preview/review evidence. The digest identifies
configuration; it is not approval or proof of clinical correctness.

For prospective use, deliver the approved release at a batch boundary and retain
old revisions for replay. Rule selection uses the stable accepted event time, not
the clinical observation date. For historical output, explicitly schedule bounded
backfill jobs for the affected exact template and pin the new revision, following
[the integration guide](openehr-analytics-integration.md#5-configuration-changes-and-historical-data).
An earlier failure is not retried merely by downloading a new configuration. The
same source version can be projected under a higher rule epoch; repeating that
source/rule combination remains idempotent.

HDL should explain the policy beside rule validation and show compatibility
warnings in preview with their source reference. A successful metadata check must
not be displayed as successful ingestion when a separate mapping check fails.
This backend change does not install new UI controls or activate an environment.

## Validation and remaining scope

Tests cover both canonical and IBM-formatted input, unchanged clinical values and
fact identities, preserved source, repeated observations, strict mode, excluded
conflicts, rule epochs, activation dates and replay decisions.

Deployment qualification must test source identity and related-subject cases against
the approved templates. Synthetic tests do not establish clinical validity or
production-scale throughput.
