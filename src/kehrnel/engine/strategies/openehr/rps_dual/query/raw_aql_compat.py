from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence


CANONICAL_COLLECTION = "compositions"


@dataclass
class RawAqlSelect:
    path: str
    alias: str


@dataclass
class RawAqlPredicate:
    path: str
    operator: str
    value: Any


@dataclass
class RawAqlOrder:
    path: str
    direction: int


@dataclass
class RawAqlQuery:
    scope: str
    ehr_alias: str | None
    version_alias: str | None
    composition_alias: str | None
    composition_archetype: str | None
    selects: List[RawAqlSelect]
    predicates: List[RawAqlPredicate]
    order_by: List[RawAqlOrder]
    limit: int | None
    offset: int | None


def _scan_boundaries(text: str) -> Sequence[tuple[int, str]]:
    keywords = ("SELECT", "FROM", "WHERE", "ORDER BY", "LIMIT", "OFFSET")
    matches: list[tuple[int, str]] = []
    upper = text.upper()
    in_single = False
    in_double = False
    depth = 0
    idx = 0

    while idx < len(text):
        ch = text[idx]
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if ch in "([{":
                depth += 1
            elif ch in ")]}":
                depth = max(0, depth - 1)

            if depth == 0:
                for keyword in keywords:
                    if upper.startswith(keyword, idx):
                        before_ok = idx == 0 or not upper[idx - 1].isalnum()
                        after_pos = idx + len(keyword)
                        after_ok = after_pos >= len(upper) or not upper[after_pos].isalnum()
                        if before_ok and after_ok:
                            matches.append((idx, keyword))
                            idx += len(keyword)
                            break
                else:
                    idx += 1
                    continue
                continue
        idx += 1

    return matches


def _extract_clauses(aql_text: str) -> Dict[str, str]:
    text = aql_text.strip()
    boundaries = list(_scan_boundaries(text))
    if not boundaries:
        raise ValueError("AQL query must include SELECT and FROM clauses")

    clauses: Dict[str, str] = {}
    for index, (start, keyword) in enumerate(boundaries):
        end = boundaries[index + 1][0] if index + 1 < len(boundaries) else len(text)
        clauses[keyword] = text[start + len(keyword):end].strip()

    if "SELECT" not in clauses or "FROM" not in clauses:
        raise ValueError("AQL query must include SELECT and FROM clauses")
    return clauses


def _split_top_level(text: str, delimiter: str) -> List[str]:
    delimiter_upper = delimiter.upper()
    parts: list[str] = []
    current: list[str] = []
    in_single = False
    in_double = False
    depth = 0
    idx = 0
    upper = text.upper()

    while idx < len(text):
        ch = text[idx]
        if ch == "'" and not in_double:
            in_single = not in_single
            current.append(ch)
            idx += 1
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            current.append(ch)
            idx += 1
            continue

        if not in_single and not in_double:
            if ch in "([{":
                depth += 1
            elif ch in ")]}":
                depth = max(0, depth - 1)

            if depth == 0 and upper.startswith(delimiter_upper, idx):
                after_pos = idx + len(delimiter)
                if delimiter.isalnum():
                    before_ok = idx == 0 or not upper[idx - 1].isalnum()
                    after_ok = after_pos >= len(upper) or not upper[after_pos].isalnum()
                else:
                    before_ok = True
                    after_ok = True
                if before_ok and after_ok:
                    part = "".join(current).strip()
                    if part:
                        parts.append(part)
                    current = []
                    idx += len(delimiter)
                    continue

        current.append(ch)
        idx += 1

    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def _strip_quotes(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _parse_literal(raw_value: str, params: Dict[str, Any], missing_params: set[str]) -> Any:
    value = raw_value.strip()
    if value.startswith("$"):
        name = value[1:]
        if name in params:
            return params[name]
        missing_params.add(name)
        return value

    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]

    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value


def _parse_selects(select_clause: str) -> List[RawAqlSelect]:
    selects: list[RawAqlSelect] = []
    for item in _split_top_level(select_clause, ","):
        match = re.match(r"^(.*?)\s+AS\s+([A-Za-z_][\w]*)$", item.strip(), flags=re.IGNORECASE)
        if not match:
            raise ValueError(f"Unsupported SELECT expression: {item}")
        selects.append(RawAqlSelect(path=match.group(1).strip(), alias=match.group(2).strip()))
    return selects


def _parse_predicate(text: str, params: Dict[str, Any], missing_params: set[str]) -> RawAqlPredicate:
    match = re.match(r"^(.*?)\s*(>=|<=|!=|=|>|<)\s*(.+)$", text.strip(), flags=re.IGNORECASE)
    if not match:
        raise ValueError(f"Unsupported predicate: {text}")
    return RawAqlPredicate(
        path=match.group(1).strip(),
        operator=match.group(2),
        value=_parse_literal(match.group(3).strip(), params, missing_params),
    )


def _parse_order(order_clause: str) -> List[RawAqlOrder]:
    orders: list[RawAqlOrder] = []
    for item in _split_top_level(order_clause, ","):
        match = re.match(r"^(.*?)(?:\s+(ASC|DESC))?$", item.strip(), flags=re.IGNORECASE)
        if not match:
            raise ValueError(f"Unsupported ORDER BY expression: {item}")
        direction = -1 if (match.group(2) or "ASC").upper() == "DESC" else 1
        orders.append(RawAqlOrder(path=match.group(1).strip(), direction=direction))
    return orders


def _parse_from(
    from_clause: str,
    params: Dict[str, Any],
    missing_params: set[str],
) -> tuple[str | None, str | None, str | None, str | None, list[RawAqlPredicate]]:
    predicates: list[RawAqlPredicate] = []

    ehr_match = re.search(r"\bEHR\s+([A-Za-z_]\w*)(?:\[(.*?)\])?", from_clause, flags=re.IGNORECASE | re.DOTALL)
    ehr_alias = ehr_match.group(1) if ehr_match else None
    if ehr_match and ehr_match.group(2):
        predicates.append(_parse_predicate(ehr_match.group(2), params, missing_params))

    version_match = re.search(r"\bVERSION\s+([A-Za-z_]\w*)", from_clause, flags=re.IGNORECASE)
    version_alias = version_match.group(1) if version_match else None

    comp_match = re.search(
        r"\bCOMPOSITION\s+([A-Za-z_]\w*)(?:\[(.*?)\])?",
        from_clause,
        flags=re.IGNORECASE | re.DOTALL,
    )
    composition_alias = comp_match.group(1) if comp_match else None
    composition_archetype = _strip_quotes(comp_match.group(2)) if comp_match and comp_match.group(2) else None

    if not composition_alias:
        raise ValueError("AQL query must CONTAINS COMPOSITION <alias>[...]")

    return ehr_alias, version_alias, composition_alias, composition_archetype, predicates


def parse_raw_aql(aql_text: str, params: Dict[str, Any] | None = None) -> tuple[RawAqlQuery, list[str]]:
    params = params or {}
    missing_params: set[str] = set()
    clauses = _extract_clauses(aql_text)

    selects = _parse_selects(clauses["SELECT"])
    ehr_alias, version_alias, composition_alias, composition_archetype, from_predicates = _parse_from(
        clauses["FROM"],
        params,
        missing_params,
    )

    predicates = list(from_predicates)
    if clauses.get("WHERE"):
        predicates.extend(
            _parse_predicate(item, params, missing_params)
            for item in _split_top_level(clauses["WHERE"], "AND")
        )

    order_by = _parse_order(clauses["ORDER BY"]) if clauses.get("ORDER BY") else []
    limit = int(clauses["LIMIT"]) if clauses.get("LIMIT") else None
    offset = int(clauses["OFFSET"]) if clauses.get("OFFSET") else None

    scope = "patient" if any(pred.path == "ehr_id/value" or pred.path.endswith("/ehr_id/value") for pred in predicates) else "cross_patient"
    return (
        RawAqlQuery(
            scope=scope,
            ehr_alias=ehr_alias,
            version_alias=version_alias,
            composition_alias=composition_alias,
            composition_archetype=composition_archetype,
            selects=selects,
            predicates=predicates,
            order_by=order_by,
            limit=limit,
            offset=offset,
        ),
        sorted(missing_params),
    )


def _maybe_datetime(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    candidate = value.strip()
    if not candidate:
        return candidate
    try:
        return datetime.fromisoformat(candidate.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return value


def _relative_composition_path(path: str, composition_alias: str | None) -> str:
    if composition_alias and path.startswith(f"{composition_alias}/"):
        return path[len(composition_alias) + 1 :]
    return path


def _special_projection(path: str, parsed: RawAqlQuery) -> Any | None:
    if parsed.ehr_alias and path == f"{parsed.ehr_alias}/ehr_id/value":
        return "$ehr_id"
    if parsed.version_alias and path == f"{parsed.version_alias}/commit_audit/time_committed/value":
        return "$time_created"
    if parsed.composition_alias and path == f"{parsed.composition_alias}/uid/value":
        return "$data.uid.value"
    if parsed.composition_alias and path == f"{parsed.composition_alias}/archetype_details/template_id/value":
        return "$data.archetype_details.template_id.value"
    return None


def _special_match(pred: RawAqlPredicate, parsed: RawAqlQuery) -> Dict[str, Any] | None:
    value = _maybe_datetime(pred.value)
    if pred.path == "ehr_id/value" or (parsed.ehr_alias and pred.path == f"{parsed.ehr_alias}/ehr_id/value"):
        return _comparison_clause("ehr_id", pred.operator, value)
    if parsed.version_alias and pred.path == f"{parsed.version_alias}/commit_audit/time_committed/value":
        return _comparison_clause("time_created", pred.operator, value)
    if parsed.composition_alias and pred.path == f"{parsed.composition_alias}/archetype_details/template_id/value":
        return _comparison_clause("data.archetype_details.template_id.value", pred.operator, value)
    if parsed.composition_alias and pred.path == f"{parsed.composition_alias}/uid/value":
        return _comparison_clause("data.uid.value", pred.operator, value)
    if parsed.composition_alias and pred.path == f"{parsed.composition_alias}/archetype_node_id":
        return _comparison_clause("data.archetype_node_id", pred.operator, value)
    return None


def _comparison_clause(field: str, operator: str, value: Any) -> Dict[str, Any]:
    if operator == "=":
        return {field: value}
    if operator == "!=":
        return {field: {"$ne": value}}
    mongo_ops = {
        ">": "$gt",
        ">=": "$gte",
        "<": "$lt",
        "<=": "$lte",
    }
    if operator not in mongo_ops:
        raise ValueError(f"Unsupported operator: {operator}")
    return {field: {mongo_ops[operator]: value}}


def _build_projection_expression(path: str, parsed: RawAqlQuery) -> Any:
    special = _special_projection(path, parsed)
    if special is not None:
        return special

    if parsed.composition_alias and path.startswith(f"{parsed.composition_alias}/"):
        return _build_canonical_navigation_expression(
            _relative_composition_path(path, parsed.composition_alias)
        )

    raise ValueError(f"Unsupported projection path: {path}")


def _parse_segment(segment: str) -> tuple[str, str | None]:
    match = re.match(r"^([^\[\]]+)(?:\[(.+)\])?$", segment)
    if not match:
        raise ValueError(f"Unsupported path segment: {segment}")
    return match.group(1), match.group(2)


def _candidate_match_expr(candidate_expr: Any, selector: str) -> Dict[str, Any]:
    return {
        "$and": [
            {"$eq": [{"$type": candidate_expr}, "object"]},
            {
                "$or": [
                    {"$eq": [f"{candidate_expr}.archetype_node_id", selector]},
                    {
                        "$eq": [
                            f"{candidate_expr}.archetype_details.archetype_id.value",
                            selector,
                        ]
                    },
                ]
            },
        ]
    }


def _project_target_expr(key: str) -> Dict[str, Any]:
    return {
        "$cond": [
            {"$eq": [{"$type": "$$this"}, "object"]},
            {"$getField": {"field": key, "input": "$$this"}},
            None,
        ]
    }


def _step_expr(values_expr: Any, key: str, selector: str | None) -> Dict[str, Any]:
    target_expr = _project_target_expr(key)
    if selector is None:
        emitted_expr: Any = {
            "$cond": [
                {"$eq": ["$$target", None]},
                [],
                {
                    "$cond": [
                        {"$isArray": "$$target"},
                        "$$target",
                        ["$$target"],
                    ]
                },
            ]
        }
    else:
        emitted_expr = {
            "$cond": [
                {"$eq": ["$$target", None]},
                [],
                {
                    "$cond": [
                        {"$isArray": "$$target"},
                        {
                            "$filter": {
                                "input": "$$target",
                                "as": "candidate",
                                "cond": _candidate_match_expr("$$candidate", selector),
                            }
                        },
                        {
                            "$cond": [
                                _candidate_match_expr("$$target", selector),
                                ["$$target"],
                                [],
                            ]
                        },
                    ]
                },
            ]
        }

    return {
        "$reduce": {
            "input": values_expr,
            "initialValue": [],
            "in": {
                "$let": {
                    "vars": {
                        "target": target_expr,
                    },
                    "in": {
                        "$concatArrays": [
                            "$$value",
                            emitted_expr,
                        ]
                    },
                }
            },
        }
    }


def _collapse_values_expr(values_expr: Any) -> Dict[str, Any]:
    return {
        "$let": {
            "vars": {"vals": values_expr},
            "in": {
                "$cond": [
                    {"$eq": [{"$size": "$$vals"}, 0]},
                    None,
                    {
                        "$cond": [
                            {"$eq": [{"$size": "$$vals"}, 1]},
                            {"$arrayElemAt": ["$$vals", 0]},
                            "$$vals",
                        ]
                    },
                ]
            },
        }
    }


def _build_canonical_navigation_expression(relative_path: str) -> Dict[str, Any]:
    segments = [segment for segment in relative_path.split("/") if segment]
    values_expr: Any = ["$data"]
    for segment in segments:
        key, selector = _parse_segment(segment)
        values_expr = _step_expr(values_expr, key, selector)
    return _collapse_values_expr(values_expr)


def compile_raw_aql_pipeline(
    aql_text: str,
    params: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    parsed, missing_params = parse_raw_aql(aql_text, params=params)
    if missing_params:
        return {
            "ok": False,
            "missing_params": missing_params,
            "parsed": parsed,
        }

    match_clauses: list[Dict[str, Any]] = []
    if parsed.composition_archetype:
        match_clauses.append({"data.archetype_node_id": parsed.composition_archetype})

    for predicate in parsed.predicates:
        special = _special_match(predicate, parsed)
        if special is None:
            raise ValueError(f"Unsupported WHERE predicate path: {predicate.path}")
        match_clauses.append(special)

    pipeline: list[Dict[str, Any]] = []
    if match_clauses:
        if len(match_clauses) == 1:
            pipeline.append({"$match": match_clauses[0]})
        else:
            pipeline.append({"$match": {"$and": match_clauses}})

    add_fields: Dict[str, Any] = {}
    selected_paths = {select.path: select.alias for select in parsed.selects}
    for select in parsed.selects:
        add_fields[select.alias] = _build_projection_expression(select.path, parsed)

    sort_spec: Dict[str, int] = {}
    for index, order in enumerate(parsed.order_by):
        sort_field = selected_paths.get(order.path)
        if not sort_field:
            hidden_name = f"__sort_{index}"
            add_fields[hidden_name] = _build_projection_expression(order.path, parsed)
            sort_field = hidden_name
        sort_spec[sort_field] = order.direction

    if add_fields:
        pipeline.append({"$addFields": add_fields})
    if sort_spec:
        pipeline.append({"$sort": sort_spec})
    if parsed.offset:
        pipeline.append({"$skip": parsed.offset})
    if parsed.limit:
        pipeline.append({"$limit": parsed.limit})

    projection = {select.alias: f"${select.alias}" for select in parsed.selects}
    projection["_id"] = 0
    pipeline.append({"$project": projection})

    return {
        "ok": True,
        "pipeline": pipeline,
        "collection": CANONICAL_COLLECTION,
        "scope": parsed.scope,
        "parsed": parsed,
        "missing_params": [],
        "mode": "raw_aql_canonical_compat",
        "warnings": [
            {
                "code": "raw_aql_canonical_fallback",
                "message": "Raw AQL compiled via canonical composition compatibility mode instead of the encoded RPS planner.",
            }
        ],
    }
