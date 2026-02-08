"""Reverse transformer for CompositionFlattener outputs."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from .encoding import PathCodec
from .flattener_f import CompositionFlattener


class CompositionUnflattener:
    """
    Reverse flattener_f outputs back to a nested composition.

    It shares dictionaries/shortcuts/separator with the corresponding flattener.
    """

    def __init__(self, *, codec: PathCodec, shortcuts: Dict[str, str], nodes_field: str = "cn", path_field: str = "p", data_field: str = "data", list_index_field: str = "li", kp_field: str = "kp"):
        self.codec = codec
        self.separator = codec.separator
        self.shortcuts = shortcuts or {}
        self.inv_shortcuts = {v: k for k, v in self.shortcuts.items()}
        self.nodes_field = nodes_field
        self.path_field = path_field
        self.data_field = data_field
        self.list_index_field = list_index_field
        self.kp_field = kp_field

    @classmethod
    async def create(
        cls,
        *,
        db,
        config: dict,
        mappings_path: str,
        mappings_content=None,
        coding_opts: Optional[Dict[str, Any]] = None,
    ):
        """
        Build an unflattener using the same configuration/dictionaries as the flattener.
        """
        flattener = await CompositionFlattener.create(
            db=db,
            config=config,
            mappings_path=mappings_path,
            mappings_content=mappings_content,
            field_map=None,
            coding_opts=coding_opts,
        )
        return cls(
            codec=flattener.path_codec,
            shortcuts=flattener.shortcut_keys,
            nodes_field=flattener.cf_nodes,
            path_field=flattener.cf_path,
            data_field=flattener.cf_data,
            list_index_field="li",
            kp_field="kp",
        )

    def _expand_key(self, key: str) -> str:
        return self.inv_shortcuts.get(key, key)

    def unflatten(self, base_doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Rebuild a nested composition structure from a flattened base document.
        """
        nodes = base_doc.get(self.nodes_field) or []
        if not isinstance(nodes, list):
            return {}

        normalized: List[Dict[str, Any]] = []
        for node in nodes:
            if not isinstance(node, dict):
                continue
            path_val = node.get(self.path_field)
            if not isinstance(path_val, str):
                continue
            parts = path_val.split(self.separator)
            data = node.get(self.data_field) or {}
            if isinstance(data, dict):
                data = self.codec.expand_keys(data)
                ani = data.get("archetype_node_id")
                if ani is not None:
                    data["archetype_node_id"] = self.codec._selector_from_code(ani)
            normalized.append(
                {
                    "path": path_val,
                    "parts": parts,
                    "data": data,
                    "kp": node.get(self.kp_field) or [],
                    "li": node.get(self.list_index_field),
                }
            )

        normalized.sort(key=lambda n: len(n["parts"]))
        obj_by_path: Dict[str, Any] = {}
        root_obj: Dict[str, Any] | None = None

        for node in normalized:
            path_str = node["path"]
            data_obj = dict(node["data"]) if isinstance(node["data"], dict) else {}
            obj_by_path[path_str] = data_obj

            if len(node["parts"]) == 1:
                root_obj = data_obj
                continue

            parent_path = self.separator.join(node["parts"][1:])
            parent_obj = obj_by_path.get(parent_path)
            if not isinstance(parent_obj, dict):
                continue

            kp_chain = node["kp"] if isinstance(node["kp"], list) else []
            attr_name = self._expand_key(kp_chain[0]) if kp_chain else None
            if not attr_name:
                attr_name = "_children"

            li = node["li"]
            if li is not None:
                container = parent_obj.setdefault(attr_name, [])
                if not isinstance(container, list):
                    container = []
                    parent_obj[attr_name] = container
                while len(container) <= int(li):
                    container.append({})
                container[int(li)] = data_obj
            else:
                parent_obj[attr_name] = data_obj

        return root_obj or {}
