# Tenant context catalog publication

HDL can normalize tenant-authored ContextObject definitions and publish them into:

- `kehrnel_context_catalog`

Each published definition includes:

- identity
- subject kinds
- assertion types
- Blocks
- terminology bindings
- relation signals
- retrieval hints
- resolution hints

The kernel can then resolve contracts in published mode by loading definitions from that collection through the storage adapter.

This is the main path used by HDL when it calls:

- `resolve_context_contract`
- `negotiate_con2l`
- `summarize_object_map`
