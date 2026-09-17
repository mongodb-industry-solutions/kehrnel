# Interaction contracts

Kehrnel operationalizes industry standards without implying that every
standard defines the same interaction model. Each strategy can therefore
publish `interaction_contracts` alongside its operations.

An interaction contract answers three questions:

1. **Who defines the interaction?** The standard, Kehrnel, or a solution.
2. **What contract is executed?** For example FHIR Search or
   `cdisc-query/v1`.
3. **How does it relate to the standard?**
   - `native`: the standard defines the interaction.
   - `compiled`: Kehrnel defines an operational contract over the standard's
     data and semantics.
   - `hybrid`: a governed object or workflow combines native evidence with
     compiled capabilities.

This classification is per capability, not per standard. A CDISC strategy can
use native Dataset-JSON exchange and also expose compiled cross-domain study
queries. A FHIR strategy can expose native REST/search interactions and add a
hybrid semantic-retrieval projection.

The active environment capabilities endpoint returns these contracts at the
domain and environment levels. When a query compiler can identify exactly one
matching contract, the query plan's `explain.interaction` records its id, mode,
authority, standard, and contract. This makes the semantic boundary visible to
applications, evaluators, and audit logs.

Canonical standard data remains authoritative. Compiled and hybrid objects are
rebuildable, versioned, and traceable to their source evidence.
