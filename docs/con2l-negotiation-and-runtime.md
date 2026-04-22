# Con2L negotiation and runtime

Con2L is treated here as a conversation-to-context contract protocol, not only a final query syntax.

Stages:

1. `draft`
- user-facing intent shape
- request IR
- requested points
- scope and assertion hints

2. `resolved`
- chosen ContextObject definition
- confidence
- matched and missing requested points
- clarification need

3. `executable`
- source definition
- scope
- subject filter
- predicates
- projection

4. `compiled`
- deterministic Mongo query plan

The kernel op that manages the full cycle is:

- `negotiate_con2l`

The lower-level compile-only op is:

- `compile_con2l`
