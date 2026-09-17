# CDISC operational support assessment

## Positioning

CDISC is semantically rich but operationally passive. SEND, SDTM, ADaM, and
TIG define study concepts, domains, variables, controlled terminology, and
exchange structures. They do not define a general-purpose interaction for
cross-domain scientific questions comparable to FHIR REST and Search.

The CDISC SDR strategy therefore preserves the standard boundary and adds a
governed operational layer. MongoDB is positioned as the live study-evidence
fabric, not merely as another place to copy XPT tables and not as a replacement
for SAS statistical analysis.

## Implemented capability map

| Layer | Current support |
| --- | --- |
| Source evidence | Checksum-addressed artifacts, Dataset-JSON, SAS XPORT, Define-XML, package import/export |
| Canonical model | Immutable study snapshots, dataset metamodels, original CDISC variables, typed facets, entity references, schema versions |
| Governance | Staged/published/superseded lifecycle, validation runs and findings, waivers, standards packages, transformation lineage |
| Operational query | Bounded `cdisc-query/v1`, tenant and publication constraints, controlled paths/operators, compiled MongoDB plans |
| Operational analysis | Bounded `cdisc-analysis/v1`, declared groupings and metrics, executed aggregation plan and interaction provenance |
| Retrieval | Lexical, semantic, and hybrid retrieval over published records, with explicit fallback behavior |
| Projections | Entities, subject timelines, analysis traceability, product evidence, and SEND `SubjectExposureResponse` objects |
| Portability | Semantically equivalent Dataset-JSON/XPT exports and checksum-protected solution evidence packages |
| Test data | Deterministic profile-aware synthetic studies with recipes, expected signals, anomalies, and watermarks |

## Scientific operational object

The first explicit object is `SubjectExposureResponse` (`schemaVersion 1.0.0`).
It is materialized for each SEND subject and composes canonical rows into:

- subject and treatment-group context;
- administered treatment, dose, units, and route;
- body-weight and gain evidence (`BW`, `BG`);
- food consumption (`FW`);
- clinical observations (`CL`);
- laboratory measurements (`LB`);
- gross and microscopic pathology (`MA`, `MI`);
- organ measurements (`OM`);
- toxicokinetic concentration and exposure (`PC`, `PP`);
- study phase and disposition (`SE`, `DS`).

Each dimension declares whether evidence exists, its contributing domains,
record count, study days, and canonical source record ids. The object is a
rebuildable projection; it is not a new source of truth.

## Interaction provenance

The strategy manifest now declares four separate interaction contracts:

- CDISC dataset exchange — standard-native;
- governed study query — Kehrnel-compiled;
- grouped study analysis — Kehrnel-compiled;
- scientific operational evidence objects — hybrid.

The environment capability response exposes these declarations. Compiled query
and analysis evidence includes the matching interaction contract, authority,
and full executed plan.

## Deliberate boundaries

The strategy does not claim to be an EDC, LIMS, submission authoring system,
complete CDISC conformance engine, or SAS replacement. It also does not invent
reference intervals, abnormality flags, causal relationships, or biological
meaning absent from the source and configured terminology.

Future increments should add scientific objects only when a concrete workload
requires them. Each object needs a schema version, source-domain declaration,
deterministic compiler, source-row lineage, and representative regression
questions before it becomes part of the supported contract.
