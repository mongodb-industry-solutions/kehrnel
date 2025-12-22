from __future__ import annotations

from typing import Any, Dict, List, Tuple

from src.transform._bulk_flatten import walk
from src.transform.at_code_codec import AtCodeCodec
from src.transform.shortcuts import ShortcutApplier


def flatten_nodes(
    composition: Dict[str, Any],
    *,
    codec: AtCodeCodec,
    shortcuts: ShortcutApplier,
) -> Tuple[List[dict], str]:
    """
    Flatten a canonical openEHR composition into node list using existing walk().
    Returns (cn, template_id).
    """
    root_sid = composition.get("archetype_node_id") or "unknown"
    template_id = codec.alloc("ar_code", root_sid)
    cn: List[dict] = []
    walk(
        composition,
        (),
        cn,
        kp_chain=[],
        list_index=None,
        codec=codec,
        shortcuts=shortcuts,
        local=type("L", (), {})(),  # fresh local state
    )
    for n in cn:
        n.pop("_anc", None)
    return cn, template_id
