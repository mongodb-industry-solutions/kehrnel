"""Reverse transformer for CompositionFlattener outputs (pi-only)."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from .encoding import PathCodec
from .flattener import CompositionFlattener


class CompositionUnflattener:
    """
    Rebuild nested composition from flattened base document.

    Expected node fields (post field-map):
      - p  : leaf-to-root encoded path segments joined by separator
      - kp : key path chain (list[str]); kp[0] is the parent attribute name
      - pi : leaf-to-root list instance chain aligned with p segments
            pi[i] is list index for segment i, or -1 if not a list element.
            pi may be omitted if all values are -1.
      - data : payload for that node
    """

    def __init__(
        self,
        *,
        codec: PathCodec,
        shortcuts: Dict[str, str],
        nodes_field: str = "cn",
        path_field: str = "p",
        data_field: str = "data",
        kp_field: str = "kp",
        pi_field: str = "pi",
    ):
        self.codec = codec
        self.separator = codec.separator
        self.shortcuts = shortcuts or {}
        self.inv_shortcuts = {v: k for k, v in self.shortcuts.items()}

        self.nodes_field = nodes_field
        self.path_field = path_field
        self.data_field = data_field
        self.kp_field = kp_field
        self.pi_field = pi_field

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
            kp_field="kp",
            pi_field=flattener.cf_pi,  # typically "pi"
        )

    @classmethod
    def from_flattener(cls, flattener: CompositionFlattener) -> "CompositionUnflattener":
        """Build an unflattener wired to an already-initialized flattener."""
        return cls(
            codec=flattener.path_codec,
            shortcuts=flattener.shortcut_keys,
            nodes_field=flattener.cf_nodes,
            path_field=flattener.cf_path,
            data_field=flattener.cf_data,
            kp_field="kp",
            pi_field=flattener.cf_pi,
        )

    def _expand_key(self, key: str) -> str:
        return self.inv_shortcuts.get(key, key)

    @staticmethod
    def _direct_list_index(pi_val: Any) -> Optional[int]:
        """
        Returns the direct list index (pi[0]) only if it is >= 0.
        Your flattener uses -1 for "not a list element".
        """
        if isinstance(pi_val, (list, tuple)) and pi_val:
            try:
                idx = int(pi_val[0])
                return idx if idx >= 0 else None
            except Exception:
                return None
        return None

    def unflatten(self, base_doc: Dict[str, Any]) -> Dict[str, Any]:
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
                # expand shortcut keys back to full keys
                data = self.codec.expand_keys(data)

                # restore selector string from encoded archetype_node_id (int -> "openEHR-...")
                ani = data.get("archetype_node_id")
                if ani is not None:
                    data["archetype_node_id"] = self.codec._selector_from_code(ani)

            normalized.append(
                {
                    "path": path_val,
                    "parts": parts,
                    "data": data,
                    "kp": node.get(self.kp_field) or [],
                    "pi": node.get(self.pi_field),  # may be missing
                }
            )

        # Ensure parents exist before children
        normalized.sort(key=lambda n: len(n["parts"]))

        obj_by_path: Dict[str, Any] = {}
        root_obj: Optional[Dict[str, Any]] = None

        for n in normalized:
            path_str = n["path"]
            data_obj = dict(n["data"]) if isinstance(n["data"], dict) else {}
            obj_by_path[path_str] = data_obj

            # root node has only one segment
            if len(n["parts"]) == 1:
                root_obj = data_obj
                continue

            # p is leaf-to-root, so parent path drops the leaf segment
            parent_path = self.separator.join(n["parts"][1:])
            parent_obj = obj_by_path.get(parent_path)
            if not isinstance(parent_obj, dict):
                continue

            kp_chain = n["kp"] if isinstance(n["kp"], list) else []
            attr_name = self._expand_key(kp_chain[0]) if kp_chain else "_children"

            idx = self._direct_list_index(n.get("pi"))

            if idx is not None:
                container = parent_obj.setdefault(attr_name, [])
                if not isinstance(container, list):
                    container = []
                    parent_obj[attr_name] = container

                while len(container) <= idx:
                    container.append({})

                container[idx] = data_obj
            else:
                parent_obj[attr_name] = data_obj

        return root_obj or {}
