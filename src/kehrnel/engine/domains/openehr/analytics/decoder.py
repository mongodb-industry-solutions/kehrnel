"""Strict IBM/RPS decoding. Dictionary inversion happens once per worker."""

from copy import deepcopy
from dataclasses import dataclass, field
from functools import lru_cache
from .source import SourceError, digest


@dataclass(frozen=True)
class IBMFields:
    nodes: str = "cn"
    path: str = "p"
    instances: str = "li"
    keys: str = "kp"
    data: str = "data"
    separator: str = ":"
    instance_mode: str = "full_chain"
    value_prefix: str = "$>"
    at_prefixes: dict = field(default_factory=dict)


class IBMDecoder:
    def __init__(self, codes, shortcuts, fields=None):
        self.fields = fields or IBMFields()
        if self.fields.instance_mode not in ("full_chain", "local_preorder"):
            raise SourceError("Unsupported IBM instance encoding")
        self.keys = self._inverse(
            {**shortcuts.get("items", {}), **shortcuts.get("keys", {})}
        )
        self.values = self._inverse(shortcuts.get("values", {}))
        archetypes = dict(codes.get("ar_code", {}))
        for rm, subtree in codes.items():
            if not rm.startswith("openEHR-") or not isinstance(subtree, dict):
                continue
            for name, versions in subtree.items():
                for version, code in versions.items():
                    archetypes[f"{rm}.{name}.{version}"] = code
        self.codes = self._inverse(archetypes)
        ats = self._inverse(codes.get("at", {}))
        if set(self.codes) & set(ats):
            raise SourceError("Archetype and node dictionaries overlap")
        self.codes.update(ats)
        for token, selector in codes.get("selectors", {}).items():
            if str(token) in self.codes and self.codes[str(token)] != selector:
                raise SourceError("Conflicting explicit node-code definition")
            self.codes[str(token)] = selector
        self.revision = digest(
            {
                "codes": self.codes,
                "keys": self.keys,
                "values": self.values,
                "fields": self.fields.__dict__,
            }
        )

    @staticmethod
    def _inverse(mapping):
        out = {}
        for original, code in mapping.items():
            token = str(code)
            if token in out and out[token] != original:
                raise SourceError("Ambiguous source dictionary")
            out[token] = original
        return out

    @lru_cache(maxsize=32768)
    def selector(self, code):
        key = str(code)
        if key in self.codes:
            return self.codes[key]
        if key.startswith(("openEHR-", "at")):
            return key
        import re

        for prefix, original in self.fields.at_prefixes.items():
            if key.startswith(prefix):
                expanded = original + key[len(prefix) :]
                # A specialized root may consume every base digit in the prefix:
                # legacy B.0.2 is at0.0.2. Validate the expanded identifier.
                if re.fullmatch(r"at[0-9]+(?:\.[0-9]+)*", expanded):
                    return expanded
        try:
            number = int(key)
        except (ValueError, TypeError):
            raise SourceError("Unresolved archetype/node code: " + key) from None
        if -1000000 < number < 0:
            return "at" + str(-number).zfill(4)
        if number == 0:
            return None
        raise SourceError("Unresolved archetype/node code: " + key)

    def expand(self, value):
        if isinstance(value, list):
            return [self.expand(v) for v in value]
        if isinstance(value, str):
            prefix = self.fields.value_prefix
            if (prefix and value.startswith(prefix)) or (
                not prefix and value in self.values
            ):
                code = value[len(prefix) :]
                if code not in self.values and value not in self.values:
                    raise SourceError("Unresolved value shortcut")
                return self.values.get(code, self.values.get(value))
            return value
        if not isinstance(value, dict):
            return value
        out = {}
        for short, raw in value.items():
            key = self.keys.get(short, short)
            if key in out:
                raise SourceError("Conflicting long/short source keys")
            val = self.expand(raw)
            if key == "archetype_node_id":
                val = self.selector(val)
                if val is None:
                    continue
            if key == "archetype_id":
                if isinstance(val, (int, str)):
                    val = {"value": self.selector(val)}
                elif isinstance(val, dict) and "value" in val:
                    val = {**val, "value": self.selector(val["value"])}
            out[key] = val
        return out

    def decode(self, document):
        f = self.fields
        nodes = document.get(f.nodes)
        if not isinstance(nodes, list) or not nodes:
            raise SourceError("IBM composition has no source nodes")
        normalized = []
        seen = set()
        stack = []
        for node in nodes:
            if (
                not isinstance(node, dict)
                or not isinstance(node.get(f.path), str)
                or not isinstance(node.get(f.data), dict)
            ):
                raise SourceError("Malformed IBM composition node")
            path = tuple(node[f.path].split(f.separator))
            for code in path:
                self.selector(code)
            if f.instance_mode == "local_preorder":
                index = node.get(f.instances, -1)
                if type(index) is not int:
                    raise SourceError("Legacy li must be a local integer index")
                while stack and len(stack[-1][0]) >= len(path):
                    stack.pop()
                if len(path) > 1 and (not stack or stack[-1][0] != path[1:]):
                    raise SourceError(
                        "Legacy local-index nodes must be contiguous depth-first preorder"
                    )
                indices = [index] + (list(stack[-1][1]) if stack else [])
                stack.append((path, tuple(indices)))
            else:
                indices = node.get(f.instances, [-1] * len(path))
            if (
                not isinstance(indices, list)
                or len(indices) != len(path)
                or any(type(i) is not int or i < -1 or i > 1000000 for i in indices)
            ):
                raise SourceError(
                    "Instance chain must align with every reversed-path segment"
                )
            keys = node.get(f.keys, [])
            if not isinstance(keys, list) or any(
                not isinstance(k, str) or not k for k in keys
            ):
                raise SourceError("A node requires its structural key path")
            identity = (path, tuple(indices))
            if identity in seen:
                raise SourceError("Duplicate path and instance identity")
            seen.add(identity)
            normalized.append((identity, keys, node[f.data]))
        normalized.sort(key=lambda n: len(n[0][0]))
        attached = {}
        root = None
        for (path, indices), keys, payload in normalized:
            data = self.expand(payload)
            if len(path) == 1:
                if root is not None:
                    raise SourceError("Multiple composition roots")
                root = data
            else:
                parent = attached.get((path[1:], indices[1:]))
                if parent is None:
                    raise SourceError("Missing parent instance; incomplete IBM lineage")
                if not keys:
                    raise SourceError(
                        "Missing structural key path; cannot reconstruct source"
                    )
                keys = [self.keys.get(k, k) for k in keys]
                for key in keys[:-1]:
                    parent = parent.setdefault(key, {})
                    if not isinstance(parent, dict):
                        raise SourceError("Key path crosses an unresolved list")
                key = keys[-1]
                index = indices[0]
                if index >= 0:
                    children = parent.setdefault(key, [])
                    if not isinstance(children, list):
                        raise SourceError(
                            "List attachment conflicts with source object"
                        )
                    if index > len(nodes) + len(children):
                        raise SourceError(
                            "Sparse instance index exceeds the source-node bound"
                        )
                    while len(children) <= index:
                        children.append(None)
                    children[index] = data
                else:
                    if key in ("content", "items", "events", "activities"):
                        raise SourceError(
                            "A repeated source attribute requires its instance index"
                        )
                    parent[key] = data
            attached[(path, indices)] = data
        if not isinstance(root, dict) or root.get("_type") != "COMPOSITION":
            raise SourceError("Decoded root is not an openEHR COMPOSITION")
        return root
