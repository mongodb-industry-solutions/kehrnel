"""
Combined FHIR resource spec from local schema/indexes (data under repo ``schema/``).

    python -m fhir_search_to_mql.schema.build_indexes
    python -m fhir_search_to_mql.schema.resource_spec Condition
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, List, Optional

from fhir_search_to_mql.schema.schema_lib import (
    brief_to_dict,
    build_brief,
    get_resource_from_index,
    load_resources_index,
)
from fhir_search_to_mql.schema.search_package_loader import (
    SearchParamRow,
    expression_denorm_hint,
    search_parameters_for_resource,
)


def _format_targets(targets: List[str], max_show: int = 6) -> str:
    if not targets:
        return ""
    if len(targets) <= max_show:
        return f" target={targets}"
    shown = targets[:max_show]
    return f" target={shown} …(+{len(targets) - max_show})"


def _format_structure_text(resource: str, version: str, data: Dict[str, Any]) -> List[str]:
    lines = [
        f"# {resource} structure ({version})",
        data.get("description", ""),
        "",
        f"required: {', '.join(data.get('required') or []) or '(none)'}",
        "",
        "## Top-level fields",
    ]
    for f in data.get("fields") or []:
        if str(f.get("name", "")).startswith("_") and f.get("name") != "_id":
            continue
        card = "0..*" if f.get("array") else "0..1"
        req = " REQUIRED" if f.get("required") else ""
        lines.append(f"  {f['name']} ({card}) -> {f.get('ref') or 'scalar'}{req}")

    poly = data.get("polymorphic") or {}
    if poly:
        lines.append("")
        lines.append("## Polymorphic [x] groups")
        for prefix, variants in poly.items():
            lines.append(f"  {prefix}[x]: {', '.join(variants)}")

    bb = data.get("backbone_elements") or []
    bb_fields = data.get("backbone_fields") or {}
    if bb:
        lines.append("")
        lines.append("## Backbone elements")
        for name in bb:
            lines.append(f"  {name}:")
            for nf in bb_fields.get(name) or []:
                arr = "[]" if nf.get("array") else ""
                lines.append(f"    {nf['name']}{arr}: {nf.get('ref') or '?'}")

    hints = data.get("denorm_hints") or []
    if hints:
        lines.append("")
        lines.append("## Denormalization hints")
        for h in hints:
            lines.append(
                f"  {h['field']} ({h['cardinality']}, {h['type']}): {h['extractor']}"
            )
    return lines


def _format_search_text(resource: str, version: str, rows: List[SearchParamRow]) -> List[str]:
    lines = [
        f"# {resource} search parameters ({version}, schema/indexes)",
        f"Total: {len(rows)}",
        "",
    ]
    current_source = None
    for row in rows:
        if row.source != current_source:
            current_source = row.source
            lines.append(f"## {current_source}")
        hint = expression_denorm_hint(row.expression)
        targets = _format_targets(row.target)
        lines.append(f"  {row.code} ({row.type}) expr={row.expression!r}{targets}")
        if hint and hint != row.code:
            lines.append(f"      -> denorm field hint: {hint}")
    return lines


def get_structure(resource: str, version: str) -> Dict[str, Any]:
    cached = get_resource_from_index(resource, version)
    if cached:
        return cached
    return brief_to_dict(build_brief(resource, version))


def combined_spec(
    resource: str,
    version: str = "R5",
    *,
    include_special: bool = False,
) -> Dict[str, Any]:
    structure = get_structure(resource, version)
    search_rows = search_parameters_for_resource(
        resource, version, include_special=include_special
    )
    return {
        "resource": resource,
        "fhir_version": version.upper(),
        "structure": structure,
        "search_parameters": [r.to_dict() for r in search_rows],
        "search_parameter_count": len(search_rows),
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="FHIR resource spec from local indexes")
    parser.add_argument("resource", help="Resource type, e.g. Condition")
    parser.add_argument("--version", default="R5", help="R5 or R6")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--structure-only", action="store_true")
    parser.add_argument("--search-only", action="store_true")
    parser.add_argument("--include-special", action="store_true")
    args = parser.parse_args(argv)

    if not load_resources_index(args.version) and not args.json:
        print(
            "Note: run python -m fhir_search_to_mql.schema.build_indexes",
            file=sys.stderr,
        )

    try:
        if args.structure_only:
            data = get_structure(args.resource, args.version)
            if args.json:
                print(json.dumps({"resource": args.resource, "structure": data}, indent=2))
            else:
                print("\n".join(_format_structure_text(args.resource, args.version.upper(), data)))
            return 0

        if args.search_only:
            rows = search_parameters_for_resource(
                args.resource, args.version, include_special=args.include_special
            )
            if not rows:
                print("No search parameters found", file=sys.stderr)
                return 1
            if args.json:
                print(json.dumps({
                    "resource": args.resource,
                    "search_parameters": [r.to_dict() for r in rows],
                }, indent=2))
            else:
                print("\n".join(_format_search_text(args.resource, args.version.upper(), rows)))
            return 0

        spec = combined_spec(
            args.resource, args.version, include_special=args.include_special
        )
    except (KeyError, FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(spec, indent=2))
    else:
        parts = _format_structure_text(
            args.resource, spec["fhir_version"], spec["structure"]
        )
        parts.append("")
        rows = [SearchParamRow.from_dict(d) for d in spec["search_parameters"]]
        parts.extend(_format_search_text(args.resource, spec["fhir_version"], rows))
        parts.append("")
        parts.append("## Compartments — src/fhir_search_to_mql/compartments/definitions/*.json")
        print("\n".join(parts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
