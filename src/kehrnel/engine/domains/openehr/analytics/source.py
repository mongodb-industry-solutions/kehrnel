"""Canonical path resolution and stable digests for reviewed clinical bindings."""

from hashlib import sha256
import json
import re
from functools import lru_cache


class SourceError(ValueError):
    pass


def digest(value):
    return sha256(
        json.dumps(value, sort_keys=True, default=str, ensure_ascii=False).encode()
    ).hexdigest()


@lru_cache(maxsize=32768)
def segments(path):
    # AQL fixed-name predicates may contain slashes; split outside brackets.
    items = []
    buf = ""
    depth = 0
    for c in path.strip("/"):
        if c == "/" and depth == 0:
            items.append(buf)
            buf = ""
            continue
        buf += c
        if c == "[":
            depth += 1
        elif c == "]":
            depth -= 1
    if buf:
        items.append(buf)
    parts = []
    for item in items:
        m = re.fullmatch(r"([A-Za-z_][\w]*)(?:\[([^\]]+)\])?", item)
        if not m:
            raise SourceError("Unsupported template binding path")
        if m[2]:
            selector_parts(m[2])
        parts.append(m.groups())
    return tuple(parts)


@lru_cache(maxsize=32768)
def selector_parts(selector):
    m = re.fullmatch(r"([^ \]]+)(?: and name/value='((?:[^']|'')*)')?", selector)
    if not m:
        raise SourceError("Unsupported template selector")
    return m[1], m[2].replace("''", "'") if m[2] is not None else None


def matches_selector(item, selector):
    if not selector:
        return True
    node, name = selector_parts(selector)
    return (
        isinstance(item, dict)
        and item.get("archetype_node_id") == node
        and (name is None or item.get("name", {}).get("value") == name)
    )


def resolve(root, path):
    """Values and structural identities, including qualified sibling names."""
    current = [(root, ())]
    for attr, selector in segments(path):
        following = []
        for value, identity in current:
            if not isinstance(value, dict):
                continue
            child = value.get(attr)
            values = (
                list(enumerate(child)) if isinstance(child, list) else [(-1, child)]
            )
            for index, item in values:
                if item is None or not matches_selector(item, selector):
                    continue
                following.append((item, identity + ((attr, selector, index),)))
        current = following
    return current


def validate_archetype_identities(root, policy=None, coverage=None):
    """Validate identities, recording only the explicitly OPT-bound exception.

    No source values are changed. Ordinary internal-node and unrelated archetype
    conflicts remain rejected; this is not full RM or OPT validation.
    """
    pending, exceptions, cache = [(root, coverage.initial if coverage else None, ())], [], {}
    while pending:
        node, cursor, location = pending.pop()
        if isinstance(node, dict):
            states = coverage.observe(node, cursor, location) if coverage else None
            details = node.get("archetype_details")
            if isinstance(details, dict):
                identifier = details.get("archetype_id")
                if isinstance(identifier, dict):
                    identifier = identifier.get("value")
                node_id = node.get("archetype_node_id")
                if isinstance(identifier, str) and isinstance(node_id, str) and identifier != node_id:
                    match = policy.matching_path(root, node, cache) if policy else None
                    if match is None:
                        raise SourceError(
                            "Conflicting source archetype identities; review archetype_node_id and archetype_details before projection"
                        )
                    opt_path, identity = match
                    tokens = []
                    for attribute, _, index in identity:
                        tokens.append(attribute)
                        if index >= 0:
                            tokens.append(str(index))
                    exception_location = "/" + "/".join(tokens)
                    exceptions.append({
                        "location": exception_location, "opt_path": opt_path,
                        "archetype_node_id": node_id,
                        "declared_archetype_id": identifier,
                        "decision": "map_using_opt_matched_node_identity",
                    })
            for attribute, value in node.items():
                if isinstance(value, (dict, list)):
                    pending.append((value, coverage.descend(states, attribute) if coverage else None,
                                    location + (attribute,) if coverage else ()))
        elif isinstance(node, list):
            for index, value in enumerate(node):
                if isinstance(value, (dict, list)):
                    pending.append((value, cursor, location + (index,) if coverage else ()))
    return sorted(exceptions, key=lambda item: item["location"])
