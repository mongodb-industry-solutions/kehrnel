# Context Maps runtime

Context Maps are the source-to-context mapping contract.

They exist because source documents and source feeds do not arrive in ContextObject shape automatically.

Today the runtime support is summary-first:

- `summarize_object_map`

That op validates the current mapping asset against the currently available ContextObject definitions and reports:

- target definition match
- rule counts
- required rule counts
- covered blocks
- missing blocks

This is the stepping stone toward full Context Map execution in the kernel.
