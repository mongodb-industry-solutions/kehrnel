"""Reverse transformer for IBM-exact flattened openEHR documents."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from kehrnel.engine.strategies.openehr.rps_dual.ingest.encoding import PathCodec
from kehrnel.engine.strategies.openehr.rps_dual_ibm.ingest.flattener import IBMCompositionFlattener


class IBMCompositionUnflattener:
    """Rebuild compositions from IBM-style flattened documents using `li` semantics."""

    def __init__(
        self,
        *,
        codec: PathCodec,
        shortcuts: Dict[str, str],
        value_shortcuts: Dict[str, str],
        nodes_field: str = "cn",
        path_field: str = "p",
        data_field: str = "data",
        kp_field: str = "kp",
        li_field: str = "li",
    ):
        self.codec = codec
        self.separator = codec.separator
        self.shortcuts = shortcuts or {}
        self.inv_shortcuts = {v: k for k, v in self.shortcuts.items()}
        self.value_shortcuts = value_shortcuts or {}
        self.inv_value_shortcuts = {v: k for k, v in self.value_shortcuts.items()}

        self.nodes_field = nodes_field
        self.path_field = path_field
        self.data_field = data_field
        self.kp_field = kp_field
        self.li_field = li_field

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
        flattener = await IBMCompositionFlattener.create(
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
            value_shortcuts=flattener.shortcut_vals,
            nodes_field=flattener.cf_nodes,
            path_field=flattener.cf_path,
            data_field=flattener.cf_data,
            kp_field="kp",
            li_field=flattener.cf_pi,
        )

    def _expand_key(self, key: str) -> str:
        return self.inv_shortcuts.get(key, key)

    def _expand_value_shortcuts(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            return {
                self._expand_key(key): self._expand_value_shortcuts(value)
                for key, value in obj.items()
            }
        if isinstance(obj, list):
            return [self._expand_value_shortcuts(item) for item in obj]
        if isinstance(obj, str) and obj.startswith("$>"):
            return self.inv_value_shortcuts.get(obj[2:], obj[2:])
        return obj

    def _restore_archetype_details(self, data: Dict[str, Any]) -> Dict[str, Any]:
        ad = data.get("archetype_details")
        if not isinstance(ad, dict):
            return data
        ai = ad.get("archetype_id")
        if isinstance(ai, dict):
            value = ai.get("value")
            if value is not None:
                ai["value"] = self.codec._selector_from_code(value)
        return data

    @staticmethod
    def _direct_list_index(value: Any) -> Optional[int]:
        try:
            index = int(value)
        except Exception:
            return None
        return index if index >= 0 else None

    def unflatten(self, base_doc: Dict[str, Any]) -> Dict[str, Any]:
        nodes = base_doc.get(self.nodes_field) or []
        if not isinstance(nodes, list):
            return {}

        instances_by_path: Dict[str, List[Dict[str, Any]]] = {}
        root_obj: Optional[Dict[str, Any]] = None

        for node in nodes:
            if not isinstance(node, dict):
                continue

            path_val = node.get(self.path_field)
            if not isinstance(path_val, str):
                continue

            parts = path_val.split(self.separator)
            data = node.get(self.data_field) or {}
            if isinstance(data, dict):
                data = self._expand_value_shortcuts(data)
                ani = data.get("archetype_node_id")
                if ani is not None:
                    data["archetype_node_id"] = self.codec._selector_from_code(ani)
                data = self._restore_archetype_details(data)

            if len(parts) == 1:
                root_obj = data
                instances_by_path.setdefault(path_val, []).append(data)
                continue

            parent_path = self.separator.join(parts[1:])
            parent_candidates = instances_by_path.get(parent_path) or []
            if not parent_candidates:
                continue
            parent_obj = parent_candidates[-1]

            kp_chain = node.get(self.kp_field) if isinstance(node.get(self.kp_field), list) else []
            attr_name = self._expand_key(kp_chain[0]) if kp_chain else "_children"
            li = self._direct_list_index(node.get(self.li_field))

            if li is not None:
                container = parent_obj.setdefault(attr_name, [])
                if not isinstance(container, list):
                    container = []
                    parent_obj[attr_name] = container
                while len(container) <= li:
                    container.append({})
                container[li] = data
            else:
                parent_obj[attr_name] = data

            instances_by_path.setdefault(path_val, []).append(data)

        return root_obj or {}

