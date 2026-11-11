# IBM Exact Variant

This pack targets the IBM openEHR reversed-path model exactly as persisted:

- `/` path separator
- compact atcodes such as `A1`
- `li` sibling indexes instead of `pi` path-instance arrays
- `"$>"`-prefixed shortcut values such as `"$>dt"` and `"$>C"`
- IBM-style top-level envelope fields like `_id`, `version`, `template`, `template_id`, `creation_date`, and `metrics`

It reuses the shared `openehr.rps_dual` query/compiler infrastructure, but uses
IBM-specific flattening and reverse-transform logic so existing IBM collections
can be queried without reshaping them.

