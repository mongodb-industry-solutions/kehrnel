# Legacy AST contract (derived from ASTValidator expectations)

Top-level keys expected:
- select: {"columns": { "0": {...}, ... }}
- from: (optional; may be unused)
- contains: optional
- where: optional (comparison or logical)
- orderBy: optional {"columns": {"0": {"alias": str, "direction": "ASC|DESC"}}}
- limit: optional int
- offset: optional int

Select column structure (legacy patterns):
- {"alias": "<alias>", "value": {"path": "<aql_path>"}}

Where clause shape (options):
- Direct condition: {"path": "<aql_path>", "operator": "<OP>", "value": <value>}
- Logical: {"operator": "AND"|"OR", "conditions": {"0": <cond>, "1": <cond>, ...}}
- Legacy comparison: {"type": "comparison", "left": {"path": ...}, "operator": "=", "right": {"value": ...}}

Contains:
- Often absent in practice; if present, should be a dict understood by ConditionProcessor/FormatResolver.

OrderBy:
- {"columns": {"0": {"alias": "<alias-or-path>", "direction": "ASC"|"DESC"}}}

Notes:
- ASTValidator.detect_key_aliases expects aliases; we supply ehr_alias="e", composition_alias="c".
- Builders tolerate direct condition format; prefer that over legacy "type": "comparison" when possible.
