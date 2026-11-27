"""
_aql.py — ContextObjects AQL → MongoDB aggregation pipeline compiler
=====================================================================

Compiles an AQL-style query against ContextObjects into a MongoDB aggregation
pipeline that runs against the search collection (context_objects_search).

Supported AQL subset
---------------------
  SELECT
      c/data[<nodeId>]/value  [AS <alias>],
      c/objectType,
      c/recordId
  FROM ContextObject c
  [WHERE
      c/objectType = '<model-id>'
      AND|OR  c/data[<nodeId>]/value  =|!=|>|>=|<|<=|LIKE  <literal>
      AND     c/data[<nodeId>]/value  IN (<v1>, <v2>, …)
  ]
  [ORDER BY c/data[<nodeId>]/value [ASC|DESC]]
  [LIMIT <n>]
  [OFFSET <n>]
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


# ── Exception ─────────────────────────────────────────────────────────────────

class AQLCompileError(ValueError):
    pass


# ── Field name constants (overridable via config) ─────────────────────────────

_DEFAULT_SN  = "sn"
_DEFAULT_RID = "rid"
_DEFAULT_OT  = "ot"


def _sn_field(config: dict | None)  -> str:
    return ((config or {}).get("fields") or {}).get("search_nodes", _DEFAULT_SN)

def _rid_field(config: dict | None) -> str:
    return ((config or {}).get("fields") or {}).get("record_id", _DEFAULT_RID)

def _ot_field(config: dict | None)  -> str:
    return ((config or {}).get("fields") or {}).get("object_type", _DEFAULT_OT)

def _search_collection(config: dict | None) -> str:
    colls = ((config or {}).get("collections") or {})
    return (colls.get("search") or {}).get("name") or "context_objects_search"


# ── Public API ────────────────────────────────────────────────────────────────

def compile_aql(aql: str, config: dict | None = None) -> dict:
    """Compile *aql* to a MongoDB aggregation pipeline dict.

    Returns:
        {
          "pipeline":      [...],
          "collection":    "context_objects_search",
          "select_fields": [...],
          "debug":         {"parsed": {...}},
        }

    Raises:
        AQLCompileError: on any parse or semantic error.
    """
    cfg_query   = (config or {}).get("query") or {}
    default_lim = int(cfg_query.get("default_limit", 100))
    max_lim     = int(cfg_query.get("max_limit", 1000))

    tokens   = _tokenise(aql)
    parsed   = _parse(tokens)
    pipeline = _build_pipeline(parsed, default_lim, max_lim, config)

    return {
        "pipeline":      pipeline,
        "collection":    _search_collection(config),
        "select_fields": parsed["select"],
        "debug":         {"parsed": parsed},
    }


# ══ Tokeniser ═════════════════════════════════════════════════════════════════

_TOKEN_RE = re.compile(
    r"""
    (?P<string>  '(?:[^'\\]|\\.)*' )        |
    (?P<number>  -?\d+(?:\.\d+)? )          |
    (?P<kw>      (?:SELECT|FROM|WHERE|ORDER\s+BY|LIMIT|OFFSET|
                    AND|OR|NOT|AS|IN|LIKE|IS|NULL|
                    ASC|DESC|CONTAINS|MATCHES)
                 (?=\s|$|\() )              |
    (?P<path>    [a-zA-Z_]\w*
                 (?:/(?:[a-zA-Z_]\w*|\[\w+\]))*
                 (?:/\w+)?  )               |
    (?P<op>      !=|>=|<=|[=<>!] )          |
    (?P<paren>   [(),] )                    |
    (?P<ws>      \s+ )
    """,
    re.VERBOSE | re.IGNORECASE,
)


@dataclass
class _Tok:
    kind:  str
    value: str


def _tokenise(aql: str) -> list[_Tok]:
    tokens = []
    for m in _TOKEN_RE.finditer(aql.strip()):
        kind  = m.lastgroup
        value = m.group()
        if kind == "ws":
            continue
        if kind == "kw":
            tokens.append(_Tok("kw", value.upper().replace("\t", " ").strip()))
        elif kind == "string":
            tokens.append(_Tok("string", value[1:-1]))
        elif kind == "number":
            tokens.append(_Tok("number", value))
        elif kind == "path":
            tokens.append(_Tok("path", value))
        elif kind == "op":
            tokens.append(_Tok("op", value))
        elif kind == "paren":
            tokens.append(_Tok("paren", value))
    return tokens


# ══ Parser ════════════════════════════════════════════════════════════════════

class _Parser:
    def __init__(self, tokens: list[_Tok]):
        self._t   = tokens
        self._pos = 0

    def peek(self, offset: int = 0) -> _Tok | None:
        i = self._pos + offset
        return self._t[i] if i < len(self._t) else None

    def consume(self, kind: str | None = None, value: str | None = None) -> _Tok:
        tok = self.peek()
        if tok is None:
            raise AQLCompileError("Unexpected end of query")
        if kind and tok.kind != kind:
            raise AQLCompileError(f"Expected {kind}, got {tok.kind} ({tok.value!r})")
        if value and tok.value.upper() != value.upper():
            raise AQLCompileError(f"Expected {value!r}, got {tok.value!r}")
        self._pos += 1
        return tok

    def match(self, kind: str | None = None, value: str | None = None) -> bool:
        tok = self.peek()
        if tok is None:
            return False
        if kind and tok.kind != kind:
            return False
        if value and tok.value.upper() != value.upper():
            return False
        return True

    def optional(self, kind: str, value: str | None = None) -> _Tok | None:
        if self.match(kind, value):
            return self.consume()
        return None


def _parse(tokens: list[_Tok]) -> dict:
    p = _Parser(tokens)
    result: dict = {
        "select": [], "from": {}, "where": None,
        "order_by": [], "limit": None, "offset": None,
    }

    p.consume("kw", "SELECT")
    result["select"] = _parse_select(p)

    p.consume("kw", "FROM")
    result["from"] = _parse_from(p)

    if p.match("kw", "WHERE"):
        p.consume("kw", "WHERE")
        result["where"] = _parse_or_expr(p)

    if p.match("kw", "ORDER BY"):
        p.consume("kw", "ORDER BY")
        result["order_by"] = _parse_order_by(p)

    if p.match("kw", "LIMIT"):
        p.consume("kw", "LIMIT")
        result["limit"] = int(p.consume("number").value)

    if p.match("kw", "OFFSET"):
        p.consume("kw", "OFFSET")
        result["offset"] = int(p.consume("number").value)

    return result


# ── SELECT ────────────────────────────────────────────────────────────────────

def _parse_select(p: _Parser) -> list[dict]:
    fields = [_parse_select_field(p)]
    while p.match("paren", ","):
        p.consume()
        fields.append(_parse_select_field(p))
    return fields


def _parse_select_field(p: _Parser) -> dict:
    tok   = p.consume("path")
    alias = None
    if p.match("kw", "AS"):
        p.consume()
        alias = p.consume("path").value
    return {"path": tok.value, "alias": alias or _path_to_alias(tok.value)}


def _path_to_alias(path: str) -> str:
    parts = path.split("/")
    clean = [
        p[1:-1] if p.startswith("[") and p.endswith("]") else p
        for p in parts if p not in ("c",)
    ]
    return "_".join(clean) if clean else path.replace("/", "_").replace("[", "").replace("]", "")


# ── FROM ──────────────────────────────────────────────────────────────────────

def _parse_from(p: _Parser) -> dict:
    tok = p.consume("path")
    if tok.value.lower() != "contextobject":
        raise AQLCompileError(f"FROM must reference ContextObject, got {tok.value!r}")
    alias_tok = p.optional("path")
    return {"type": "ContextObject", "alias": alias_tok.value if alias_tok else "c"}


# ── WHERE (recursive descent) ─────────────────────────────────────────────────

def _parse_or_expr(p: _Parser) -> dict:
    left = _parse_and_expr(p)
    while p.match("kw", "OR"):
        p.consume()
        right = _parse_and_expr(p)
        left  = {"op": "or", "operands": [left, right]}
    return left


def _parse_and_expr(p: _Parser) -> dict:
    left = _parse_unary_expr(p)
    while p.match("kw", "AND"):
        p.consume()
        right = _parse_unary_expr(p)
        left  = {"op": "and", "operands": [left, right]}
    return left


def _parse_unary_expr(p: _Parser) -> dict:
    if p.match("kw", "NOT"):
        p.consume()
        return {"op": "not", "operand": _parse_primary_expr(p)}
    return _parse_primary_expr(p)


def _parse_primary_expr(p: _Parser) -> dict:
    if p.match("paren", "("):
        p.consume()
        expr = _parse_or_expr(p)
        p.consume("paren", ")")
        return expr
    return _parse_predicate(p)


def _parse_predicate(p: _Parser) -> dict:
    lhs = p.consume("path").value

    if p.match("kw", "IN"):
        p.consume()
        p.consume("paren", "(")
        values = [_parse_literal(p)]
        while p.match("paren", ","):
            p.consume()
            values.append(_parse_literal(p))
        p.consume("paren", ")")
        return {"op": "in", "path": lhs, "values": values}

    if p.match("kw", "LIKE"):
        p.consume()
        return {"op": "like", "path": lhs, "value": _parse_literal(p)}

    op = p.consume("op").value

    if p.match("path") and "/" in (p.peek() or _Tok("", "")).value:
        rhs = p.consume("path").value
        return {"op": op, "path": lhs, "rhs_path": rhs}

    return {"op": op, "path": lhs, "value": _parse_literal(p)}


def _parse_literal(p: _Parser) -> Any:
    tok = p.peek()
    if tok is None:
        raise AQLCompileError("Expected literal value")
    if tok.kind == "string":
        p.consume()
        return tok.value
    if tok.kind == "number":
        p.consume()
        v = tok.value
        return float(v) if "." in v else int(v)
    if tok.kind == "path" and tok.value.upper() in ("TRUE", "FALSE"):
        p.consume()
        return tok.value.upper() == "TRUE"
    if tok.kind == "path" and tok.value.upper() == "NULL":
        p.consume()
        return None
    raise AQLCompileError(f"Unexpected literal: {tok.value!r}")


# ── ORDER BY ──────────────────────────────────────────────────────────────────

def _parse_order_by(p: _Parser) -> list[dict]:
    clauses = [_parse_order_term(p)]
    while p.match("paren", ","):
        p.consume()
        clauses.append(_parse_order_term(p))
    return clauses


def _parse_order_term(p: _Parser) -> dict:
    path_tok  = p.consume("path")
    dir_tok   = p.optional("kw")
    direction = "asc"
    if dir_tok:
        if dir_tok.value.upper() == "DESC":
            direction = "desc"
        elif dir_tok.value.upper() != "ASC":
            raise AQLCompileError(f"Expected ASC or DESC, got {dir_tok.value!r}")
    return {"path": path_tok.value, "direction": direction}


# ══ Pipeline builder ══════════════════════════════════════════════════════════

def _build_pipeline(
    parsed: dict,
    default_lim: int,
    max_lim: int,
    config: dict | None,
) -> list[dict]:
    pipeline: list[dict] = []
    alias = parsed["from"].get("alias", "c")
    sn    = _sn_field(config)
    rid   = _rid_field(config)
    ot    = _ot_field(config)

    # 1. objectType fast filter (hoisted from WHERE — uses idx_search_ot)
    ot_filter = _extract_objecttype_filter(parsed.get("where"), alias, ot)
    if ot_filter:
        pipeline.append({"$match": {ot: ot_filter}})

    # 2. WHERE → $match on search nodes
    where_node = parsed.get("where")
    if where_node:
        match_stage = _where_to_match(where_node, alias, sn, ot)
        if match_stage:
            pipeline.append({"$match": match_stage})

    # 3. $project — expose requested SELECT paths
    project_stage = _build_project(parsed["select"], alias, sn, rid, ot, config)
    if project_stage:
        pipeline.append({"$project": project_stage})

    # 4. ORDER BY
    if parsed.get("order_by"):
        sort_doc: dict = {}
        for term in parsed["order_by"]:
            field_name = _path_to_project_key(term["path"], alias)
            sort_doc[field_name] = 1 if term["direction"] == "asc" else -1
        pipeline.append({"$sort": sort_doc})

    # 5. OFFSET
    if parsed.get("offset"):
        pipeline.append({"$skip": int(parsed["offset"])})

    # 6. LIMIT
    limit = parsed.get("limit") or default_lim
    pipeline.append({"$limit": min(int(limit), max_lim)})

    return pipeline


# ── objectType hoisting ───────────────────────────────────────────────────────

def _extract_objecttype_filter(where: dict | None, alias: str, ot: str) -> str | None:
    if where is None:
        return None
    if where.get("op") == "=" and _is_ot_path(where.get("path", ""), alias, ot):
        return where.get("value")
    if where.get("op") == "and":
        for operand in where.get("operands", []):
            v = _extract_objecttype_filter(operand, alias, ot)
            if v is not None:
                return v
    return None


def _is_ot_path(path: str, alias: str, ot_field: str) -> bool:
    return path in (f"{alias}/objectType", "objectType", f"{alias}/{ot_field}", ot_field)


# ── WHERE → MongoDB match ──────────────────────────────────────────────────────

def _where_to_match(node: dict, alias: str, sn: str, ot: str) -> dict:
    op = node.get("op")

    if op == "and":
        parts = [_where_to_match(o, alias, sn, ot) for o in node.get("operands", [])]
        parts = [p for p in parts if p]
        if not parts:
            return {}
        if len(parts) == 1:
            return parts[0]
        return {"$and": parts}

    if op == "or":
        parts = [_where_to_match(o, alias, sn, ot) for o in node.get("operands", [])]
        parts = [p for p in parts if p]
        return {"$or": parts} if parts else {}

    if op == "not":
        inner = _where_to_match(node.get("operand", {}), alias, sn, ot)
        return {"$nor": [inner]} if inner else {}

    path = node.get("path", "")
    if _is_ot_path(path, alias, ot):
        return {}   # already hoisted

    return _predicate_to_match(node, alias, sn)


_NODE_PATH_RE = re.compile(r"\[(\w+)\]")


def _predicate_to_match(pred: dict, alias: str, sn: str) -> dict:
    path     = pred.get("path", "")
    op       = pred.get("op", "=")
    value    = pred.get("value")
    node_id  = _path_to_node_id(path, alias)

    if not node_id:
        return {}

    elem_cond: dict = {"nid": node_id}

    if op == "in":
        elem_cond["v"] = {"$in": pred.get("values", [])}
    elif op == "like":
        pattern = re.escape(str(value)).replace(r"\%", ".*").replace(r"\_", ".")
        elem_cond["v"] = {"$regex": f"^{pattern}$", "$options": "i"}
    else:
        mongo_op = _op_to_mongo(op)
        elem_cond["v"] = value if mongo_op == "$eq" else {mongo_op: value}

    return {sn: {"$elemMatch": elem_cond}}


def _op_to_mongo(op: str) -> str:
    return {"=": "$eq", "!=": "$ne", ">": "$gt", ">=": "$gte", "<": "$lt", "<=": "$lte"}.get(op, "$eq")


def _path_to_node_id(path: str, alias: str) -> str | None:
    matches = _NODE_PATH_RE.findall(path)
    return matches[-1] if matches else None


def _path_to_project_key(path: str, alias: str) -> str:
    clean = path
    if clean.startswith(f"{alias}/"):
        clean = clean[len(alias) + 1:]
    return clean.replace("/", ".").replace("[", "_").replace("]", "") or path


# ── $project builder ──────────────────────────────────────────────────────────

def _build_project(
    select_fields: list[dict],
    alias: str,
    sn: str,
    rid: str,
    ot: str,
    config: dict | None,
) -> dict:
    nf = (config or {}).get("node_fields") or {}
    nid_f = nf.get("node_id", "nid")
    v_f   = nf.get("value", "v")

    project: dict = {rid: 1, ot: 1}

    for sf in select_fields:
        path = sf["path"]

        if _is_ot_path(path, alias, ot):
            project["objectType"] = f"${ot}"
            continue

        rid_paths = (f"{alias}/recordId", "recordId", f"{alias}/{rid}", rid)
        if path in rid_paths:
            project["recordId"] = f"${rid}"
            continue

        node_id   = _path_to_node_id(path, alias)
        alias_key = sf.get("alias") or _path_to_alias(path)

        if not node_id:
            continue

        project[alias_key] = {
            "$let": {
                "vars": {
                    "matched": {
                        "$arrayElemAt": [
                            {
                                "$filter": {
                                    "input": f"${sn}",
                                    "as":    "node",
                                    "cond":  {"$eq": [f"$$node.{nid_f}", node_id]},
                                }
                            },
                            0,
                        ]
                    }
                },
                "in": f"$$matched.{v_f}",
            }
        }

    return project
