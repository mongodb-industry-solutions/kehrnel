"""
_synthetic.py — ContextObjects synthetic instance generator
============================================================

Generates ContextObject instances from a semantic model schema for testing and
demonstration. Label-hinted generators produce realistic-looking values.
"""

from __future__ import annotations

import datetime
import random
import string
from typing import Any


_RNG = random.Random()


def generate(
    schema: dict,
    count:  int = 10,
    opts:   dict | None = None,
) -> list[dict]:
    """Generate *count* synthetic ContextObject instances from *schema*.

    Args:
        schema: Semantic model schema (id, title, nodes).
        count:  Number of instances to generate.
        opts:   Optional overrides:
                  seed (int)             — random seed
                  record_id_prefix (str) — prefix for recordIds
                  include_optional (bool)— whether to include optional nodes
                  version (str)          — version string

    Returns:
        List of ContextObject instance dicts ready for transform().
    """
    opts = opts or {}
    _RNG.seed(opts.get("seed"))

    object_type      = schema.get("id") or "unknown"
    schema_nodes     = schema.get("nodes") or {}
    prefix           = opts.get("record_id_prefix") or f"syn-{object_type[:8]}-"
    include_optional = opts.get("include_optional", True)
    version          = opts.get("version", "1.0")

    instances = []
    for i in range(count):
        data = _gen_nodes(schema_nodes, include_optional=include_optional, depth=0)
        instances.append({
            "objectType": object_type,
            "recordId":   f"{prefix}{_short_id()}",
            "version":    version,
            "data":       data,
            "meta": {
                "source":    "synthetic",
                "generated": _now_iso(),
                "index":     i,
            },
        })

    return instances


def _gen_nodes(schema_nodes: dict, include_optional: bool, depth: int) -> dict:
    if depth > 10:
        return {}

    result: dict[str, Any] = {}

    for node_id, node_schema in schema_nodes.items():
        if not isinstance(node_schema, dict):
            continue

        required = node_schema.get("required", False)

        if not required and include_optional and _RNG.random() < 0.2:
            continue
        if not required and not include_optional:
            continue

        node_type = str(node_schema.get("type") or "string").lower()
        children  = node_schema.get("children") or {}

        node: dict[str, Any] = {"_type": node_type}

        if node_type not in ("object",):
            value = _gen_value(node_type, node_schema)
            node["_value"] = value

            if node_type == "coded_text":
                bindings = node_schema.get("bindings") or []
                if bindings:
                    chosen = _RNG.choice(bindings)
                    node["_code"]   = chosen.get("code")
                    node["_system"] = node_schema.get("system") or chosen.get("system")
                    node["_value"]  = chosen.get("display") or chosen.get("code")
                else:
                    node["_code"]   = _random_code()
                    node["_system"] = node_schema.get("system") or "urn:synthetic"
            elif node_type == "quantity":
                node["_unit"] = node_schema.get("unit") or _random_unit()

        if children:
            node.update(_gen_nodes(children, include_optional, depth + 1))

        result[node_id] = node

    return result


def _gen_value(dtype: str, schema: dict) -> Any:
    if dtype in ("string", "text"):
        return _gen_string(schema)
    if dtype == "integer":
        return _RNG.randint(int(schema.get("min", 0)), int(schema.get("max", 100)))
    if dtype in ("number", "decimal", "float"):
        return round(_RNG.uniform(float(schema.get("min", 0.0)), float(schema.get("max", 100.0))), 2)
    if dtype == "boolean":
        return _RNG.choice([True, False])
    if dtype == "datetime":
        return _random_date()
    if dtype == "coded_text":
        return _random_word()
    if dtype == "quantity":
        return round(_RNG.uniform(float(schema.get("min", 0.0)), float(schema.get("max", 200.0))), 1)
    return _random_word()


_FIRST_NAMES = ["Alice", "Bob", "Carol", "David", "Eva", "Frank", "Grace",
                "Henry", "Ivy", "James", "Karen", "Leo", "Maria", "Noah",
                "Olivia", "Paul", "Quinn", "Rachel", "Sam", "Tina"]
_LAST_NAMES  = ["Smith", "Jones", "Williams", "Brown", "Davis", "Miller",
                "Wilson", "Moore", "Taylor", "Anderson", "Thomas", "Jackson"]
_CITIES      = ["Springfield", "Riverdale", "Shelbyville", "Greenville",
                "Fairview", "Madison", "Georgetown", "Arlington"]
_STREETS     = ["Oak St", "Maple Ave", "Elm Dr", "Cedar Blvd", "Pine Rd",
                "Washington St", "Lake View Ln", "Sunset Blvd"]
_WORDS       = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot",
                "golf", "hotel", "india", "juliet", "kilo", "lima"]
_UNITS       = ["kg", "cm", "mmHg", "bpm", "mg/dL", "mL", "°C", "mmol/L"]


def _gen_string(schema: dict) -> str:
    label = str(schema.get("label") or "").lower()
    if any(w in label for w in ("name", "first", "last", "surname")):
        return f"{_RNG.choice(_FIRST_NAMES)} {_RNG.choice(_LAST_NAMES)}"
    if any(w in label for w in ("email", "mail")):
        fn = _RNG.choice(_FIRST_NAMES).lower()
        ln = _RNG.choice(_LAST_NAMES).lower()
        return f"{fn}.{ln}@example.com"
    if any(w in label for w in ("phone", "tel", "mobile")):
        return f"+1-{_RNG.randint(200,999)}-{_RNG.randint(100,999)}-{_RNG.randint(1000,9999)}"
    if any(w in label for w in ("city", "town")):
        return _RNG.choice(_CITIES)
    if any(w in label for w in ("country",)):
        return _RNG.choice(["US", "DE", "GB", "FR", "AU", "CA"])
    if any(w in label for w in ("address", "street")):
        return f"{_RNG.randint(1,999)} {_RNG.choice(_STREETS)}"
    if any(w in label for w in ("zip", "postal", "postcode")):
        return f"{_RNG.randint(10000,99999)}"
    if any(w in label for w in ("id", "identifier")):
        return _short_id(8)
    max_len = schema.get("maxLength") or 50
    n_words = max(1, min(max_len // 6, 5))
    return " ".join(_random_word() for _ in range(n_words))


def _random_date() -> str:
    base  = datetime.date(1970, 1, 1)
    end   = datetime.date(2025, 12, 31)
    delta = (end - base).days
    return (base + datetime.timedelta(days=_RNG.randint(0, delta))).isoformat()


def _random_word() -> str:
    return _RNG.choice(_WORDS)


def _random_code() -> str:
    return "".join(_RNG.choices(string.ascii_uppercase, k=3))


def _random_unit() -> str:
    return _RNG.choice(_UNITS)


def _short_id(n: int = 6) -> str:
    return "".join(_RNG.choices(string.ascii_lowercase + string.digits, k=n))


def _now_iso() -> str:
    return datetime.datetime.utcnow().isoformat() + "Z"
