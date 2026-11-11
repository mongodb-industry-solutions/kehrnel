# openEHR Query Endpoints

- `GET /api/domains/openehr/query`
- `POST /api/domains/openehr/query/aql`
- `POST /api/domains/openehr/query/aql/mql`
- `POST /api/domains/openehr/query/aql/parse`
- `POST /api/domains/openehr/query/aql/validate`
- `POST /api/domains/openehr/query/ast`
- `POST /api/domains/openehr/query/ast/debug`
- `GET /api/domains/openehr/query/strategy/info`
- `GET /api/domains/openehr/query/{name}`
- `PUT /api/domains/openehr/query/{name}`
- `DELETE /api/domains/openehr/query/{name}`

This family covers AQL execution, compilation helpers, AST tooling, and stored queries.

The public AQL endpoints use parity mode by default. Send
`X-AQL-Feature-Mode: extended` to opt into Kehrnel extensions such as
`EXISTS`, `NOT EXISTS`, basic `NOT CONTAINS`, and path-to-path comparisons.
Callers should not depend on a direct strategy runtime accepting an extension
without the header; feature-mode enforcement is being aligned across runtime
paths.

For the exact supported subset and deployment-specific qualifications, see:

- [AQL support matrix](/docs/strategies/openehr/rps-dual/query-translation)
