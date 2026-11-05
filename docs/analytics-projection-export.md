# Analytics projection export

The deployment compiler derives the Search projection and complete Atlas Search
definition from the same selected data-model fields. Projection algorithm
version 2 resolves older HDL selections ending at a DV object (`/value`) to its
scalar attribute: `value.value` for dates/text/booleans/durations/URIs,
`value.id` for identifiers, `value.magnitude` for quantities/counts, and
`value.defining_code.code_string` for coded text. Explicit extracts are retained.

AQL path splitting preserves predicates containing `name/value` and quoted
labels containing `/`. Compiled rules include `_labels` entries with encoded
selector paths and expected names. Writers must check each entry against its
corresponding node or ancestor. Ingest and rebuild correlate ancestor names
using the flattened nodes' original depth-first traversal order. Do not reorder
the canonical `cn` array before projecting.

Different logical fields can use the same physical Search path with different
scalar types. The Search definition includes all types as an array of mappings;
document children are retained when a path also has a scalar mapping. Conflicting
types for the same logical template/path remain errors.

Secondary projectors may derive literal, compact-prefix and undotted negative
integer `at` codes without allocating dictionary values. Explicit dictionary
entries take precedence. Sequential/dotted allocations and archetype IDs still
require an exact snapshot. Exports report incomplete projection readiness when
those mappings are missing.

Healthcare Data Lab's current analytics configuration export is
`GET /api/integrations/v1/strategies/{strategyId}/analytics-config`, authenticated
with an environment key carrying `analytics:read`. Its projection, dictionary
and index data come from one revision. It does not activate a runtime or apply
indexes in the consumer's Atlas environment.
